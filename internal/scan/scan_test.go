package scan

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/xorpaul/cephfs-index/internal/cephenc"
	"github.com/xorpaul/cephfs-index/internal/decode"
)

type fakeStore struct {
	omaps map[string]map[string][]byte
	data  map[string][]byte
}

func (f *fakeStore) OmapPage(oid, after string, max uint64) ([]KV, bool, error) {
	m, ok := f.omaps[oid]
	if !ok {
		return nil, false, ErrNotFound
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		if k > after {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	more := uint64(len(keys)) > max
	if more {
		keys = keys[:max]
	}
	out := make([]KV, len(keys))
	for i, k := range keys {
		out[i] = KV{Key: k, Value: m[k]}
	}
	return out, more, nil
}

func (f *fakeStore) ReadAll(oid string) ([]byte, error) {
	d, ok := f.data[oid]
	if !ok {
		return nil, ErrNotFound
	}
	return d, nil
}

type recSink struct {
	mu      sync.Mutex
	seen    map[uint64]bool // dirs already emitted
	entries []Entry
	orphans []Entry // emitted before their parent dir
}

func (r *recSink) Emit(b []Entry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range b {
		if e.Parent != RootIno && !r.seen[e.Parent] {
			r.orphans = append(r.orphans, e)
		}
		if e.Type == TypeDir {
			r.seen[e.Ino] = true
		}
		r.entries = append(r.entries, e)
	}
}

func file(ino uint64, uid uint32) []byte {
	return cephenc.PrimaryValue(cephenc.Fixture{In: decode.Inode{Ino: ino, Mode: decode.SIFREG | 0o644, UID: uid, GID: uid, Nlink: 1, Size: 1}}, "")
}

func dir(ino uint64, tree map[decode.Frag]int32) []byte {
	return dirR(ino, tree, 1)
}

func dirR(ino uint64, tree map[decode.Frag]int32, rfiles int64) []byte {
	return cephenc.PrimaryValue(cephenc.Fixture{In: decode.Inode{Ino: ino, Mode: decode.SIFDIR | 0o755, UID: 7, Nlink: 1, RFiles: rfiles, RSubdirs: 1, RCtime: decode.Time{Sec: 99}, FragTree: tree}}, "")
}

func buildTree() (*fakeStore, int) {
	fs := &fakeStore{omaps: map[string]map[string][]byte{}, data: map[string][]byte{}}
	// Root split into two frags.
	rootTree := map[decode.Frag]int32{0: 1}
	fs.data["1.00000000.inode"] = cephenc.RootInodeObject(rootTree)
	left, right := decode.Frag(0).Child(1, 0), decode.Frag(0).Child(1, 1)
	fs.omaps[decode.ObjectName(1, left)] = map[string][]byte{
		"home_head":    dir(0x100, nil),
		"empty_head":   dirR(0x101, nil, 0), // no object: never committed
		"stale_head":   dirR(0x104, nil, 5), // rstat says content, but frag 0 object is gone
		"hl_head":      cephenc.RemoteValue(0x200, 8),
		"old_file_10":  file(0x300, 1), // snapshot dentry, skipped
		"corrupt_head": {1, 2, 3},
	}
	// "big" dir fragmented into 4 frags.
	bigTree := map[decode.Frag]int32{0: 2}
	fs.omaps[decode.ObjectName(1, right)] = map[string][]byte{"big_head": dir(0x102, bigTree)}
	want := 5 // home, empty, stale, hl, big
	home := map[string][]byte{}
	for i := 0; i < 2500; i++ { // > several pages
		home[fmt.Sprintf("f%05d_head", i)] = file(uint64(0x1000+i), 3000000001)
	}
	home["plugins_head"] = dir(0x103, nil)
	fs.omaps["100.00000000"] = home
	want += len(home)
	fs.omaps["103.00000000"] = map[string][]byte{"x.php_head": file(0x5000, 42)}
	want++
	for _, l := range decode.Leaves(bigTree) {
		m := map[string][]byte{}
		for i := 0; i < 10; i++ {
			m[fmt.Sprintf("%x_%d_head", uint32(l), i)] = file(uint64(0x6000)+uint64(l)>>20+uint64(i), 5)
		}
		fs.omaps[decode.ObjectName(0x102, l)] = m
		want += len(m)
	}
	return fs, want
}

func TestScanTree(t *testing.T) {
	store, want := buildTree()
	sink := &recSink{seen: map[uint64]bool{}}
	s := New(store, Config{MaxInflight: 64, PageSize: 100, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}, sink)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.Run(ctx, RootIno); err != nil {
		t.Fatal(err)
	}
	if len(sink.entries) != want {
		t.Fatalf("entries = %d, want %d", len(sink.entries), want)
	}
	if len(sink.orphans) != 0 {
		t.Fatalf("%d entries emitted before their parent dir, e.g. %+v", len(sink.orphans), sink.orphans[0])
	}
	st := &s.Stats
	if st.NotFound.Load() != 1 || st.MissedDirs.Load() != 1 || st.DecodeErrors.Load() != 1 || st.SnapSkipped.Load() != 1 || st.OpErrors.Load() != 0 {
		t.Fatalf("stats: enoent=%d missed=%d decode=%d snap=%d op=%d", st.NotFound.Load(), st.MissedDirs.Load(), st.DecodeErrors.Load(), st.SnapSkipped.Load(), st.OpErrors.Load())
	}
	// Root entries: home 1+1, empty 0+1, stale 5+1, hl 1, big 1+1. The
	// snapshot and the corrupt dentry don't count.
	if exp, final := s.Expected(); exp != 12 || !final {
		t.Fatalf("Expected = %d (final %v), want 12", exp, final)
	}
	if st.Fragmented.Load() != 1 {
		t.Fatalf("fragmented = %d, want 1", st.Fragmented.Load())
	}
	var mu, hl *Entry
	for i := range sink.entries {
		e := &sink.entries[i]
		switch e.Name {
		case "plugins":
			mu = e
		case "hl":
			hl = e
		}
	}
	if mu == nil || mu.Type != TypeDir || mu.Parent != 0x100 || mu.UID != 7 || mu.RCtime != 99 {
		t.Fatalf("plugins entry: %+v", mu)
	}
	if hl == nil || hl.Type != TypeHardlink || hl.RemoteIno != 0x200 {
		t.Fatalf("hardlink entry: %+v", hl)
	}
}

func TestScanCancel(t *testing.T) {
	store, _ := buildTree()
	s := New(store, Config{MaxInflight: 4, PageSize: 10, MaxOps: 20}, &recSink{seen: map[uint64]bool{}})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, RootIno) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected context error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestLimiterBackoff(t *testing.T) {
	l := newLimiter(64, 0, 10*time.Millisecond)
	for i := 0; i < 100; i++ {
		l.samples = append(l.samples, 50*time.Millisecond)
	}
	l.adjust()
	if limit, _, _ := l.snapshot(); limit != 32 {
		t.Fatalf("limit after slow window = %d, want 32", limit)
	}
	l.samples = []time.Duration{time.Millisecond}
	l.adjust()
	if limit, _, _ := l.snapshot(); limit != 36 {
		t.Fatalf("limit after fast window = %d, want 36", limit)
	}
}

func TestTypeCounts(t *testing.T) {
	var c TypeCounts
	for _, typ := range []byte{TypeFile, TypeFile, TypeFile, TypeDir, TypeSymlink, TypeHardlink, TypeHardlink, TypeOther} {
		c.Add(typ)
	}
	meta := map[string]string{"dirs": "1"}
	c.SetMeta(meta)
	want := map[string]string{"dirs": "1", "files": "3", "symlinks": "1", "hardlinks": "2", "special": "1"}
	for k, v := range want {
		if meta[k] != v {
			t.Errorf("meta %s = %q, want %q", k, meta[k], v)
		}
	}
	if len(meta) != len(want) || c.Dirs.Load() != 1 {
		t.Errorf("meta %v, dirs %d", meta, c.Dirs.Load())
	}
}
