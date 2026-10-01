// Package scan walks a CephFS directory tree by reading dirfrag objects from
// the metadata pool. It never talks to an MDS.
package scan

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xorpaul/cephfs-index/internal/decode"
)

// ErrNotFound is returned by a Store for a missing object.
var ErrNotFound = errors.New("object not found")

type KV struct {
	Key   string
	Value []byte
}

// Store is the read-only view of the metadata pool the scanner needs.
type Store interface {
	// OmapPage returns up to max omap entries of oid with keys > after, and
	// whether more entries follow.
	OmapPage(oid, after string, max uint64) ([]KV, bool, error)
	// ReadAll returns the full data of oid.
	ReadAll(oid string) ([]byte, error)
}

const RootIno = 1

// Entry types.
const (
	TypeFile     = 'f'
	TypeDir      = 'd'
	TypeSymlink  = 'l'
	TypeHardlink = 'h' // remote dentry: metadata lives with the primary dentry
	TypeOther    = 'o' // device node, FIFO or socket
)

// TypeCounts counts entries by type. It is safe for concurrent use.
type TypeCounts struct {
	Files, Dirs, Symlinks, Hardlinks, Special atomic.Int64
}

// Add counts one entry of type t (a Type* constant).
func (c *TypeCounts) Add(t byte) {
	switch t {
	case TypeFile:
		c.Files.Add(1)
	case TypeDir:
		c.Dirs.Add(1)
	case TypeSymlink:
		c.Symlinks.Add(1)
	case TypeHardlink:
		c.Hardlinks.Add(1)
	default:
		c.Special.Add(1)
	}
}

// SetMeta stores the non-dir counts in an index's meta map as files,
// symlinks, hardlinks and special; the writers already store dirs, and
// files + dirs + symlinks + hardlinks + special = entries. Counts are of
// entries as walked, so a dir seen twice during the walk counts twice, like
// "dirs".
func (c *TypeCounts) SetMeta(meta map[string]string) {
	for k, v := range map[string]*atomic.Int64{
		"files": &c.Files, "symlinks": &c.Symlinks, "hardlinks": &c.Hardlinks, "special": &c.Special,
	} {
		meta[k] = strconv.FormatInt(v.Load(), 10)
	}
}

type Entry struct {
	Parent    uint64
	Name      string
	Ino       uint64 // target ino for hardlinks
	Type      byte
	UID, GID  uint32
	Size      uint64
	Mtime     int64
	RCtime    int64 // dirs only
	RemoteIno uint64
}

// Sink receives entries. Emit is called concurrently from worker goroutines;
// the slice is not reused after Emit returns.
type Sink interface {
	Emit(batch []Entry)
}

type Config struct {
	MaxInflight    int
	MaxOps         float64
	LatencyCeiling time.Duration
	PageSize       uint64
	Log            *slog.Logger
}

type Stats struct {
	Objects      atomic.Int64
	Pages        atomic.Int64
	Entries      atomic.Int64
	Dirs         atomic.Int64
	Fragmented   atomic.Int64 // dirs split into more than one dirfrag
	NotFound     atomic.Int64 // missing dirfrag of a dir whose rstat says it is empty
	MissedDirs   atomic.Int64 // missing dirfrag of a dir whose rstat says it has content
	DecodeErrors atomic.Int64
	OpErrors     atomic.Int64
	SnapSkipped  atomic.Int64
	// Expected is the entry count below the start dir according to the
	// rstat of its direct children (rfiles + rsubdirs, which counts the dir
	// itself). Complete once all start-dir objects are read.
	Expected atomic.Int64
}

type item struct {
	dir uint64
	oid string
	// nonEmpty: the parent dentry's rstat reports files/subdirs below this
	// dir, so a missing object means a subtree the walk cannot see.
	nonEmpty bool
}

type Scanner struct {
	store Store
	cfg   Config
	sink  Sink
	lim   *limiter
	Stats Stats

	start    uint64
	rootLeft atomic.Int64 // start-dir objects not yet read

	mu      sync.Mutex
	cond    *sync.Cond
	stack   []item
	pending int
	done    bool
	errLogs atomic.Int64
}

func New(store Store, cfg Config, sink Sink) *Scanner {
	if cfg.MaxInflight < 1 {
		cfg.MaxInflight = 1
	}
	if cfg.PageSize == 0 {
		cfg.PageSize = 1024
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	s := &Scanner{store: store, cfg: cfg, sink: sink,
		lim: newLimiter(cfg.MaxInflight, cfg.MaxOps, cfg.LatencyCeiling)}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *Scanner) Limiter() (limit int, p50, p99 time.Duration) { return s.lim.snapshot() }

// Expected returns Stats.Expected and whether it is final.
func (s *Scanner) Expected() (int64, bool) { return s.Stats.Expected.Load(), s.rootLeft.Load() == 0 }

// RootObjects returns the dirfrag objects of directory ino. For the root
// the fragtree comes from the "1.00000000.inode" object; for any other start
// directory its fragtree is unknown here, so only frag 0 is assumed.
func (s *Scanner) RootObjects(ino uint64) ([]string, error) {
	if ino != RootIno {
		return []string{decode.ObjectName(ino, 0)}, nil
	}
	oid := decode.ObjectName(RootIno, 0) + ".inode"
	buf, err := s.store.ReadAll(oid)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", oid, err)
	}
	in, err := decode.DecodeRootInode(buf)
	if err != nil {
		return nil, fmt.Errorf("decode %s: %w", oid, err)
	}
	var out []string
	for _, f := range decode.Leaves(in.FragTree) {
		out = append(out, decode.ObjectName(RootIno, f))
	}
	return out, nil
}

// Run walks the tree below startIno until it is exhausted or ctx is done.
func (s *Scanner) Run(ctx context.Context, startIno uint64) error {
	objs, err := s.RootObjects(startIno)
	if err != nil {
		return err
	}
	s.start = startIno
	s.rootLeft.Store(int64(len(objs)))
	roots := make([]item, len(objs))
	for i, o := range objs {
		roots[i] = item{dir: startIno, oid: o, nonEmpty: true}
	}
	s.push(roots)

	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				s.lim.adjust()
			case <-ctx.Done():
				s.mu.Lock()
				s.done = true
				s.mu.Unlock()
				s.cond.Broadcast()
				s.lim.wake()
				return
			case <-stop:
				return
			}
		}
	}()
	defer close(stop)

	var wg sync.WaitGroup
	for i := 0; i < s.cfg.MaxInflight; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.worker(ctx)
		}()
	}
	wg.Wait()
	s.lim.adjust() // fold in the last samples: runs shorter than a tick would report p50=0
	return ctx.Err()
}

func (s *Scanner) push(items []item) {
	if len(items) == 0 {
		return
	}
	s.mu.Lock()
	s.pending += len(items)
	s.stack = append(s.stack, items...)
	s.mu.Unlock()
	if len(items) == 1 {
		s.cond.Signal()
	} else {
		s.cond.Broadcast()
	}
}

// pop returns false when the walk is complete or cancelled. LIFO order keeps
// the frontier small (depth x fanout instead of a whole BFS level).
func (s *Scanner) pop() (item, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.stack) == 0 && s.pending > 0 && !s.done {
		s.cond.Wait()
	}
	if s.done || len(s.stack) == 0 {
		return item{}, false
	}
	it := s.stack[len(s.stack)-1]
	s.stack = s.stack[:len(s.stack)-1]
	return it, true
}

func (s *Scanner) finish() {
	s.mu.Lock()
	s.pending--
	last := s.pending == 0
	if last {
		s.done = true
	}
	s.mu.Unlock()
	if last {
		s.cond.Broadcast()
	}
}

func (s *Scanner) worker(ctx context.Context) {
	for {
		it, ok := s.pop()
		if !ok {
			return
		}
		s.scanObject(ctx, it)
		s.finish()
	}
}

func (s *Scanner) logSample(msg string, args ...any) {
	if s.errLogs.Add(1) <= 20 {
		s.cfg.Log.Warn(msg, args...)
	}
}

func (s *Scanner) scanObject(ctx context.Context, it item) {
	if it.dir == s.start {
		defer s.rootLeft.Add(-1)
	}
	after := ""
	for {
		if err := s.lim.acquire(ctx); err != nil {
			return
		}
		t0 := time.Now()
		kvs, more, err := s.store.OmapPage(it.oid, after, s.cfg.PageSize)
		s.lim.release(time.Since(t0))
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				if it.nonEmpty && after == "" {
					// Likely a fragtree in the parent dentry that is older
					// than the dir's actual split: the subtree is skipped.
					s.Stats.MissedDirs.Add(1)
					s.logSample("dirfrag missing for non-empty dir", "oid", it.oid)
				} else {
					s.Stats.NotFound.Add(1)
				}
			} else {
				s.Stats.OpErrors.Add(1)
				s.logSample("omap read failed", "oid", it.oid, "err", err)
			}
			return
		}
		batch := make([]Entry, 0, len(kvs))
		var children []item
		for _, kv := range kvs {
			if e, ok := s.entry(it, kv.Key, kv.Value, &children); ok {
				batch = append(batch, e)
			}
		}
		s.Stats.Pages.Add(1)
		if len(batch) > 0 {
			s.Stats.Entries.Add(int64(len(batch)))
			s.sink.Emit(batch)
		}
		// Children are queued only after their dir entries reached the sink,
		// so a sink always sees a directory before anything inside it.
		s.push(children)
		if !more || len(kvs) == 0 {
			break
		}
		after = kvs[len(kvs)-1].Key
	}
	s.Stats.Objects.Add(1)
}

func (s *Scanner) entry(it item, key string, val []byte, children *[]item) (Entry, bool) {
	d, err := decode.DecodeDentry(key, val)
	if err != nil {
		s.Stats.DecodeErrors.Add(1)
		s.logSample("dentry decode failed", "oid", it.oid, "key", key, "err", err)
		return Entry{}, false
	}
	if d.Snap != decode.NoSnap {
		s.Stats.SnapSkipped.Add(1)
		return Entry{}, false
	}
	e := Entry{Parent: it.dir, Name: d.Name}
	if d.IsRemote() {
		if it.dir == s.start {
			s.Stats.Expected.Add(1)
		}
		e.Type, e.Ino, e.RemoteIno = TypeHardlink, d.RemoteIno, d.RemoteIno
		return e, true
	}
	in := d.Inode
	if it.dir == s.start {
		if in.IsDir() {
			s.Stats.Expected.Add(in.RFiles + in.RSubdirs)
		} else {
			s.Stats.Expected.Add(1)
		}
	}
	e.Ino, e.UID, e.GID, e.Size, e.Mtime = in.Ino, in.UID, in.GID, in.Size, in.Mtime.Unix()
	switch {
	case in.IsDir():
		e.Type, e.RCtime = TypeDir, in.RCtime.Unix()
		s.Stats.Dirs.Add(1)
		// rsubdirs counts the dir itself, so an empty dir has a sum of 1.
		nonEmpty := in.RFiles+in.RSubdirs > 1
		leaves := decode.Leaves(in.FragTree)
		if len(leaves) > 1 {
			s.Stats.Fragmented.Add(1)
		}
		for _, f := range leaves {
			*children = append(*children, item{dir: in.Ino, oid: decode.ObjectName(in.Ino, f), nonEmpty: nonEmpty})
		}
	case in.IsReg():
		e.Type = TypeFile
	case in.IsSymlink():
		e.Type = TypeSymlink
	default:
		e.Type = TypeOther
	}
	return e, true
}
