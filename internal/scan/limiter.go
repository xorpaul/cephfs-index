package scan

import (
	"context"
	"slices"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// limiter bounds in-flight RADOS ops with an AIMD window driven by observed
// client-side latency, plus an optional ops/s token bucket.
type limiter struct {
	mu       sync.Mutex
	cond     *sync.Cond
	inflight int
	limit    int
	max      int
	ceiling  time.Duration
	samples  []time.Duration
	bucket   *rate.Limiter
	lastP99  time.Duration
	lastP50  time.Duration
}

func newLimiter(max int, opsPerSec float64, ceiling time.Duration) *limiter {
	l := &limiter{limit: max, max: max, ceiling: ceiling}
	l.cond = sync.NewCond(&l.mu)
	if opsPerSec > 0 {
		burst := int(opsPerSec)
		if burst < 1 {
			burst = 1
		}
		l.bucket = rate.NewLimiter(rate.Limit(opsPerSec), burst)
	}
	return l
}

func (l *limiter) acquire(ctx context.Context) error {
	if l.bucket != nil {
		// Not bucket.Wait: it fails early when the token would arrive after
		// the ctx deadline, which would drop work before the deadline.
		r := l.bucket.Reserve()
		if d := r.Delay(); d > 0 {
			t := time.NewTimer(d)
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
				r.Cancel()
				return ctx.Err()
			}
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for l.inflight >= l.limit {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		l.cond.Wait()
	}
	l.inflight++
	return nil
}

func (l *limiter) release(d time.Duration) {
	l.mu.Lock()
	l.inflight--
	l.samples = append(l.samples, d)
	l.mu.Unlock()
	l.cond.Signal()
}

// adjust halves the window when p99 latency exceeds the ceiling and grows it
// by max/16 otherwise. Called periodically by the scanner.
func (l *limiter) adjust() {
	l.mu.Lock()
	s := l.samples
	l.samples = nil
	if len(s) > 0 {
		slices.Sort(s)
		l.lastP50 = s[len(s)/2]
		l.lastP99 = s[(len(s)*99)/100]
		if l.ceiling > 0 && l.lastP99 > l.ceiling {
			l.limit = max(1, l.limit/2)
		} else if l.limit < l.max {
			l.limit = min(l.max, l.limit+max(1, l.max/16))
		}
	}
	l.mu.Unlock()
	l.cond.Broadcast()
}

func (l *limiter) wake() { l.cond.Broadcast() }

func (l *limiter) snapshot() (limit int, p50, p99 time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.limit, l.lastP50, l.lastP99
}
