package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"runtime/debug"
	"runtime/pprof"
	"strings"
	"syscall"
	"time"

	"github.com/ceph/go-ceph/rados"

	"github.com/xorpaul/cephfs-index/internal/fsmap"
	"github.com/xorpaul/cephfs-index/internal/radosstore"
	"github.com/xorpaul/cephfs-index/internal/scan"
)

// walkOpts are the flags shared by every command that walks a metadata pool.
type walkOpts struct {
	fsName, conf, id, crushLoc, cpuProfile string
	inflight                               int
	maxOps                                 float64
	ceiling                                time.Duration
	page                                   uint64
	localize, flush                        bool
	progress                               time.Duration
	gogc                                   int
}

func (o *walkOpts) register(fs *flag.FlagSet) {
	fs.StringVar(&o.fsName, "fs", "", "filesystem name (required)")
	fs.StringVar(&o.conf, "conf", "/etc/ceph/ceph.conf", "ceph.conf path")
	fs.StringVar(&o.id, "id", "admin", "cephx client id")
	fs.IntVar(&o.inflight, "max-inflight", 32, "maximum concurrent omap reads")
	fs.Float64Var(&o.maxOps, "max-ops", 0, "maximum omap reads per second (0 = unlimited)")
	fs.DurationVar(&o.ceiling, "latency-ceiling", 50*time.Millisecond, "halve concurrency when p99 omap latency exceeds this (0 = off)")
	fs.Uint64Var(&o.page, "page-size", 1024, "omap entries per read")
	fs.BoolVar(&o.localize, "localize-reads", false, "read from the nearest replica, per --crush-location (costs CPU; only useful when the pool's primaries are in the other DC)")
	fs.StringVar(&o.crushLoc, "crush-location", "", "client crush_location for localize-reads, e.g. datacenter=dc1 (default: ceph.conf value)")
	fs.BoolVar(&o.flush, "flush-journal", false, "run 'ceph tell mds.<fs>:<rank> flush journal' for every active rank before the walk; without it, metadata still only in the MDS journal is missed")
	fs.DurationVar(&o.progress, "progress", 5*time.Second, "progress interval on stderr (0 = off)")
	fs.StringVar(&o.cpuProfile, "cpuprofile", "", "write a Go CPU profile to this file")
	fs.IntVar(&o.gogc, "gogc", 0, "Go GC target percentage (0 = $GOGC if set, else 400; -1 = GC off). The heap is soft-capped at $GOMEMLIMIT, default a quarter of RAM")
}

// session is an open connection to the metadata pool of one filesystem.
type session struct {
	log            *slog.Logger
	conn           *rados.Conn
	fs             fsmap.FS
	ioctx          *rados.IOContext
	loc, primaryDC string
	flushed        bool
	cleanup        []func()
}

func (s *session) close() {
	for i := len(s.cleanup) - 1; i >= 0; i-- {
		s.cleanup[i]()
	}
}

// open applies the runtime settings, connects, resolves the filesystem and
// On error the session is already closed. The journal flush is separate: see flushJournals.
func (o *walkOpts) open(ctx context.Context) (s *session, err error) {
	switch {
	case o.gogc != 0:
		debug.SetGCPercent(o.gogc)
	case os.Getenv("GOGC") == "":
		debug.SetGCPercent(400)
	}
	// The names map (build) and dirs map (probe --match) are live data that
	// GOGC=400 lets the heap grow to 5x of. Without an explicit GOMEMLIMIT,
	// cap the heap softly at a quarter of RAM: the node also runs a mon.
	if os.Getenv("GOMEMLIMIT") == "" {
		var si syscall.Sysinfo_t
		if syscall.Sysinfo(&si) == nil {
			debug.SetMemoryLimit(int64(si.Totalram) * int64(si.Unit) / 4)
		}
	}
	s = &session{log: slog.New(slog.NewTextHandler(logWriter(), nil))}
	defer func() {
		if err != nil {
			s.close()
		}
	}()
	if o.cpuProfile != "" {
		pf, err := os.Create(o.cpuProfile)
		if err != nil {
			return nil, fmt.Errorf("cpuprofile: %w", err)
		}
		if err := pprof.StartCPUProfile(pf); err != nil {
			pf.Close()
			return nil, fmt.Errorf("cpuprofile: %w", err)
		}
		s.cleanup = append(s.cleanup, func() { pprof.StopCPUProfile(); pf.Close() })
	}

	if s.conn, err = rados.NewConnWithUser(o.id); err != nil {
		return nil, fmt.Errorf("rados conn: %w", err)
	}
	if err := s.conn.ReadConfigFile(o.conf); err != nil {
		return nil, fmt.Errorf("read config %s: %w", o.conf, err)
	}
	if s.loc, err = clientCrushLocation(s.conn, o.crushLoc); err != nil {
		return nil, fmt.Errorf("crush_location: %w", err)
	}
	if err := s.conn.Connect(); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	s.cleanup = append(s.cleanup, s.conn.Shutdown)

	if s.fs, err = fsmap.Lookup(s.conn, o.fsName); err != nil {
		return nil, fmt.Errorf("lookup filesystem: %w", err)
	}
	s.primaryDC, err = fsmap.PrimaryBucket(s.conn, s.fs.MetadataPool)
	if err != nil {
		s.log.Warn("cannot determine primary DC of metadata pool", "pool", s.fs.MetadataPool, "err", err)
	}
	if dc := locDatacenter(s.loc); !o.localize && dc != "" && s.primaryDC != "" && dc != s.primaryDC {
		s.log.Warn("metadata primaries are in the other DC: every read crosses DCs. Run from a "+s.primaryDC+" mon, or pass --localize-reads",
			"pool", s.fs.MetadataPool, "primary_dc", s.primaryDC, "client_dc", dc)
	}
	if s.ioctx, err = s.conn.OpenIOContext(s.fs.MetadataPool); err != nil {
		return nil, fmt.Errorf("open pool %s: %w", s.fs.MetadataPool, err)
	}
	s.cleanup = append(s.cleanup, s.ioctx.Destroy)
	return s, nil
}

func (s *session) scanner(o *walkOpts, sink scan.Sink) *scan.Scanner {
	return scan.New(radosstore.New(s.ioctx, o.localize), scan.Config{
		MaxInflight: o.inflight, MaxOps: o.maxOps, LatencyCeiling: o.ceiling,
		PageSize: o.page, Log: s.log,
	}, sink)
}

// header prints the settings line that starts every run.
func (s *session) header(o *walkOpts, extra string) {
	fmt.Fprintf(os.Stderr, "%s  fs=%s pool=%s max-inflight=%d max-ops=%g latency-ceiling=%s localize-reads=%t crush_location=%q primary_dc=%q journal_flushed=%t gogc=%d gomemlimit=%dMiB%s\n",
		ts(), s.fs.Name, s.fs.MetadataPool, o.inflight, o.maxOps, o.ceiling, o.localize, s.loc, s.primaryDC, s.flushed, gcPercent(), debug.SetMemoryLimit(-1)>>20, extra)
}

// statsLine formats the scan counters for a progress line.
func statsLine(sc *scan.Scanner, el time.Duration) string {
	st := &sc.Stats
	sec := el.Seconds()
	limit, p50, p99 := sc.Limiter()
	return fmt.Sprintf("objects=%d (%.0f/s) entries=%d (%.0f/s) dirs=%d fragmented=%d  limit=%d p50=%s p99=%s  enoent=%d missed_dirs=%d decode_err=%d op_err=%d snap_skipped=%d%s",
		st.Objects.Load(), float64(st.Objects.Load())/sec, st.Entries.Load(), float64(st.Entries.Load())/sec,
		st.Dirs.Load(), st.Fragmented.Load(), limit, fmtDur(p50), fmtDur(p99),
		st.NotFound.Load(), st.MissedDirs.Load(), st.DecodeErrors.Load(), st.OpErrors.Load(), st.SnapSkipped.Load(),
		progressETA(sc, el))
}

// progressETA compares the walk with the rstat totals of the start dir's
// children. rstat propagates lazily, so the total is approximate and the
// percentage can end slightly off 100.
func progressETA(sc *scan.Scanner, el time.Duration) string {
	exp, final := sc.Expected()
	done := sc.Stats.Entries.Load()
	if !final || exp <= 0 || done == 0 {
		return ""
	}
	eta := "0s"
	if left := exp - done; left > 0 {
		eta = time.Duration(float64(left) / float64(done) * float64(el)).Round(time.Second).String()
	}
	return fmt.Sprintf("  pct=%.1f%% of %d eta=%s", 100*float64(done)/float64(exp), exp, eta)
}

// every calls fn at each interval until the returned stop func is called.
func every(interval time.Duration, fn func()) (stop func()) {
	if interval <= 0 {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				fn()
			case <-done:
				return
			}
		}
	}()
	return func() { close(done) }
}

// flushJournals runs --flush-journal. Callers invoke it only once the build
// can actually proceed (index created, PG lock taken): a flush trims the
// journal and can make a standby-replay MDS respawn, so it must not happen
// for a run that then fails on setup.
func (s *session) flushJournals(ctx context.Context, o *walkOpts) error {
	if !o.flush {
		return nil
	}
	if err := flushJournal(ctx, s.log, o, s.fs); err != nil {
		return err
	}
	s.flushed = true
	return nil
}

// flushJournal asks every active rank to write its journal out to the
// dirfrag objects. This is the only MDS contact the tool makes.
func flushJournal(ctx context.Context, log *slog.Logger, o *walkOpts, f fsmap.FS) error {
	for _, r := range f.Ranks {
		target := fmt.Sprintf("mds.%s:%d", f.Name, r)
		rctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		t0 := time.Now()
		out, err := exec.CommandContext(rctx, "ceph", "--conf", o.conf, "--id", o.id, "tell", target, "flush", "journal").CombinedOutput()
		cancel()
		if err != nil {
			return fmt.Errorf("ceph tell %s flush journal: %w: %s", target, err, bytes.TrimSpace(out))
		}
		log.Info("journal flushed", "target", target, "took", time.Since(t0).Round(time.Millisecond).String())
	}
	return nil
}

func ts() string { return time.Now().Format("15:04:05") }

// fmtDur formats latencies in ASCII ("430us", "1.61ms"): the mon terminals
// are not UTF-8 and show Duration.String's µ as "_".
func fmtDur(d time.Duration) string {
	d = d.Round(10 * time.Microsecond)
	if d < time.Millisecond {
		return fmt.Sprintf("%dus", d.Microseconds())
	}
	return d.String()
}

// colorWriter colours whole WARN/ERROR log lines. slog's TextHandler writes
// one record per Write call and escapes ANSI codes inside values, so the
// line is wrapped here instead of via ReplaceAttr.
type colorWriter struct{ w io.Writer }

func (c colorWriter) Write(p []byte) (int, error) {
	var color string
	switch {
	case bytes.Contains(p, []byte(" level=ERROR ")):
		color = "\x1b[31m"
	case bytes.Contains(p, []byte(" level=WARN ")):
		color = "\x1b[33m"
	default:
		return c.w.Write(p)
	}
	line := bytes.TrimSuffix(p, []byte("\n"))
	if _, err := fmt.Fprintf(c.w, "%s%s\x1b[0m\n", color, line); err != nil {
		return 0, err
	}
	return len(p), nil
}

func logWriter() io.Writer {
	if os.Getenv("NO_COLOR") != "" {
		return os.Stderr
	}
	if fi, err := os.Stderr.Stat(); err != nil || fi.Mode()&os.ModeCharDevice == 0 {
		return os.Stderr
	}
	return colorWriter{os.Stderr}
}

func gcPercent() int {
	p := debug.SetGCPercent(-1)
	debug.SetGCPercent(p)
	return p
}

// clientCrushLocation sets the client's crush_location so localize-reads can
// prefer OSDs in the same datacenter. Precedence: flag, then ceph.conf.
func clientCrushLocation(conn *rados.Conn, flagVal string) (string, error) {
	loc := flagVal
	if loc == "" {
		if cur, err := conn.GetConfigOption("crush_location"); err == nil && cur != "" {
			return cur, nil
		}
	}
	if loc == "" {
		return "", nil
	}
	return loc, conn.SetConfigOption("crush_location", loc)
}

// locDatacenter extracts the datacenter value from a crush_location string
// such as "datacenter=dc1" or "host=x datacenter=dc1".
func locDatacenter(loc string) string {
	for _, kv := range strings.FieldsFunc(loc, func(r rune) bool { return r == ' ' || r == ',' || r == ';' }) {
		if v, ok := strings.CutPrefix(kv, "datacenter="); ok {
			return v
		}
	}
	return ""
}
