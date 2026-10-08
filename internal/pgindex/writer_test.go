package pgindex

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xorpaul/cephfs-index/internal/scan"
)

type row struct {
	k, slot, part int
	seq           int64
	ino           uint64
}

// TestChunkRouting drives the COPY-session protocol (begin / Next / row)
// from several concurrent workers without a database and checks that seq
// follows emit order, no batch straddles a chunk, every row lands in its
// chunk's seq window, and each session gets its own leaf: a unique part,
// at most one leaf per worker and chunk.
func TestChunkRouting(t *testing.T) {
	const (
		workers = 4
		chunkN  = 1000
		total   = 20_000
	)
	w := &Writer{
		opts: Options{ChunkRows: chunkN},
		ch:   make(chan []scan.Entry, 8),
		jobs: make(chan *leaf, 4096),
		dead: make(chan struct{}),
	}
	ctx := context.Background()

	var mu sync.Mutex
	var rows []row
	perWorker := make([]map[int]int, workers) // chunk -> sessions, per worker
	var wg sync.WaitGroup
	for i := range workers {
		perWorker[i] = map[int]int{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				src := &batchSource{w: w, ctx: ctx}
				l, err := src.begin(ctx)
				if err != nil {
					t.Error(err)
					return
				}
				if l == nil {
					return
				}
				perWorker[i][l.c.k]++
				for src.Next() {
					seq, e := src.row()
					mu.Lock()
					rows = append(rows, row{l.c.k, l.slot, l.part, seq, e.Ino})
					mu.Unlock()
				}
				if src.eof || src.ctxDone {
					return
				}
			}
		}()
	}

	rng := rand.New(rand.NewPCG(1, 2))
	var ino uint64
	for ino < total {
		n := 1 + rng.IntN(300)
		b := make([]scan.Entry, 0, n)
		for range n {
			ino++
			b = append(b, scan.Entry{Ino: ino, Type: scan.TypeFile})
		}
		w.ch <- b
	}
	close(w.ch)
	wg.Wait()

	if len(rows) != int(ino) {
		t.Fatalf("rows copied = %d, emitted %d", len(rows), ino)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ino < rows[j].ino })
	for i, r := range rows {
		if r.ino != uint64(i+1) {
			t.Fatalf("row %d: ino %d, want %d (lost or duplicated)", i, r.ino, i+1)
		}
		if i > 0 && r.seq <= rows[i-1].seq {
			t.Fatalf("seq not increasing in emit order at ino %d: %d after %d", r.ino, r.seq, rows[i-1].seq)
		}
		lo, hi := int64(r.k)*chunkN, int64(r.k+1)*chunkN
		if r.seq < lo || r.seq >= hi {
			t.Fatalf("ino %d seq %d outside chunk %d range [%d,%d)", r.ino, r.seq, r.k, lo, hi)
		}
	}

	// Leaves: parts 0..n-1 in creation order, slots per chunk 0..m-1 and
	// fewer than workers, names unique, one leaf per worker and chunk.
	if int(w.created.Load()) != len(w.leaves) || w.nextPart != len(w.leaves) {
		t.Fatalf("created %d, leaves %d, nextPart %d", w.created.Load(), len(w.leaves), w.nextPart)
	}
	names := map[string]bool{}
	for i, l := range w.leaves {
		if l.part != i || l.slot >= workers || names[l.table()] {
			t.Fatalf("leaf %d: part %d slot %d table %s", i, l.part, l.slot, l.table())
		}
		names[l.table()] = true
	}
	for i, m := range perWorker {
		for k, n := range m {
			if n != 1 {
				t.Fatalf("worker %d wrote %d leaves in chunk %d", i, n, k)
			}
		}
	}
	var sum int64
	for _, c := range w.chunks {
		sum += c.rows.Load()
		if c.slots == 0 || c.slots > workers {
			t.Fatalf("chunk %d has %d leaves", c.k, c.slots)
		}
	}
	if sum != int64(ino) || w.Rows.Load() != int64(ino) {
		t.Fatalf("chunk rows sum %d, Rows %d, want %d", sum, w.Rows.Load(), ino)
	}
	t.Logf("%d rows in %d chunks, %d leaves", ino, len(w.chunks), len(w.leaves))
}

// TestChunksDoesNotBlock guards against the progress line deadlocking with a
// COPY worker that holds recvMu while waiting on an empty, still-open ch —
// the state at the end of every scan, before Finish closes ch.
func TestChunksDoesNotBlock(t *testing.T) {
	w := &Writer{
		opts: Options{ChunkRows: 1000},
		ch:   make(chan []scan.Entry),
		jobs: make(chan *leaf, 16),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	go func() {
		src := &batchSource{w: w, ctx: ctx}
		close(started)
		src.begin(ctx) // blocks in take() holding recvMu
	}()
	<-started
	time.Sleep(50 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		w.Chunks()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Chunks() blocked while a worker waits on ch")
	}
}

func TestChunkStmts(t *testing.T) {
	got := chunkStmts("a02_new", "entries_00003_1", 4)
	if len(got) != 4 {
		t.Fatalf("chunkStmts: %q", got)
	}
	if want := `CREATE INDEX "entries_00003_1_name" ON "a02_new"."entries_00003_1" (name) INCLUDE (parent, ino, type, uid, size, mtime, ctime)`; got[1] != want {
		t.Errorf("name index:\n got %s\nwant %s", got[1], want)
	}
	if !strings.HasPrefix(got[2], `INSERT INTO "a02_new".dirs`) {
		t.Errorf("dirs upsert must follow the index: %s", got[2])
	}
	// COPY FREEZE set the visibility map; the vacuum only reads it (no
	// FREEZE, which would make it aggressive) and sets relallvisible.
	if want := `VACUUM (ANALYZE) "a02_new"."entries_00003_1"`; got[3] != want {
		t.Errorf("last statement %s", got[3])
	}
}

func TestPostLoadStmts(t *testing.T) {
	c0, c1 := &chunk{k: 0}, &chunk{k: 1}
	leaves := []*leaf{{c: c0, slot: 0, part: 0}, {c: c0, slot: 1, part: 1}, {c: c1, slot: 0, part: 2}}
	got := postLoadStmts("a02_new", leaves)
	all := strings.Join(got, "\n")
	// The parent index must have the same INCLUDE list as the leaf
	// indexes, or ATTACH PARTITION of the indexes fails.
	for _, want := range []string{
		`CREATE TABLE "a02_new".entries (`,
		`) PARTITION BY RANGE (part)`,
		`ALTER TABLE "a02_new".entries ATTACH PARTITION "a02_new"."entries_00000_1" FOR VALUES FROM (1) TO (2)`,
		`ALTER TABLE "a02_new".entries ATTACH PARTITION "a02_new"."entries_00001_0" FOR VALUES FROM (2) TO (3)`,
		`CREATE INDEX entries_name ON ONLY "a02_new".entries (name) INCLUDE (parent, ino, type, uid, size, mtime, ctime)`,
		`ALTER INDEX "a02_new".entries_name ATTACH PARTITION "a02_new"."entries_00001_0_name"`,
		`CREATE UNIQUE INDEX dirs_ino_cover ON "a02_new".dirs (ino) INCLUDE (parent, name)`,
	} {
		if !strings.Contains(all, want) {
			t.Errorf("missing %s in\n%s", want, all)
		}
	}
	if last := got[len(got)-1]; last != `VACUUM (FREEZE, ANALYZE, BUFFER_USAGE_LIMIT '2GB') "a02_new".dirs` {
		t.Errorf("last statement: %s", last)
	}
}

// TestCopyReader decodes the binary COPY stream and checks the header,
// every field of every row in entryCols order, and the trailer, including
// names with bytes that text COPY would have to escape.
func TestCopyReader(t *testing.T) {
	w := &Writer{opts: Options{ChunkRows: 1000}, ch: make(chan []scan.Entry, 4)}
	entries := []scan.Entry{
		{Parent: 1, Name: "index.php", Ino: 0x100, Type: scan.TypeFile, UID: 33, GID: 34, Size: 1234, Mtime: 1700000000, RCtime: 1700000001},
		{Parent: 0x100, Name: "tab\there\nnl\\bs\xff\x80", Ino: 0x101, Type: scan.TypeDir, UID: 1 << 31, Mtime: -1},
	}
	w.ch <- entries
	close(w.ch)
	src := &batchSource{w: w, ctx: context.Background()}
	l, err := src.begin(context.Background())
	if err != nil || l == nil {
		t.Fatal(l, err)
	}
	b, err := io.ReadAll(&copyReader{src: src, part: 7})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(b, copyHeader) || len(copyHeader) != 19 {
		t.Fatalf("header % x", b[:min(len(b), 19)])
	}
	b = b[len(copyHeader):]
	u16 := func() uint16 { v := binary.BigEndian.Uint16(b); b = b[2:]; return v }
	field := func() []byte {
		n := binary.BigEndian.Uint32(b)
		v := b[4 : 4+n]
		b = b[4+n:]
		return v
	}
	i8 := func() int64 { return int64(binary.BigEndian.Uint64(field())) }
	for i, e := range entries {
		if n := u16(); int(n) != len(entryCols) {
			t.Fatalf("row %d: %d fields", i, n)
		}
		seq, parent, name, ino := i8(), i8(), string(field()), i8()
		typ, part := field(), binary.BigEndian.Uint16(field())
		uid, gid, size, mtime, ctime, rctime := i8(), i8(), i8(), i8(), i8(), i8()
		if seq != int64(i+1) || parent != int64(e.Parent) || name != e.Name || ino != int64(e.Ino) ||
			string(typ) != string([]byte{e.Type}) || part != 7 || uid != int64(e.UID) || gid != int64(e.GID) ||
			size != int64(e.Size) || mtime != e.Mtime || ctime != e.Ctime || rctime != e.RCtime {
			t.Errorf("row %d decoded to seq=%d parent=%d name=%q ino=%d type=%q part=%d uid=%d gid=%d size=%d mtime=%d ctime=%d rctime=%d",
				i, seq, parent, name, ino, typ, part, uid, gid, size, mtime, ctime, rctime)
		}
	}
	if u16() != 0xffff || len(b) != 0 {
		t.Errorf("trailer: % x", b)
	}
	if w.Rows.Load() != 2 || w.Dirs.Load() != 1 {
		t.Errorf("Rows %d Dirs %d", w.Rows.Load(), w.Dirs.Load())
	}
}

func TestNamedPathsStmts(t *testing.T) {
	stmts := namedPathsStmts("a07_new")
	var insert, swap int
	for i, q := range stmts {
		if strings.HasPrefix(q, `INSERT INTO "a07_new".named_paths_new`+"\n") {
			insert = i
			if !strings.Contains(q, `FROM "a07_new".entries WHERE name = ANY($1::text[])`) || !strings.Contains(q, `up.depth <= 4096`) || strings.Contains(q, " JOIN top") || strings.Contains(q, "seq") {
				t.Errorf("paths insert: %s", q)
			}
		}
		if q == `DROP TABLE IF EXISTS "a07_new".named_paths, "a07_new".named_paths_names` {
			swap = i
		}
	}
	// The current tables are dropped only after the long insert, right
	// before the renames, so readers wait for the swap only.
	if insert == 0 || swap <= insert || swap != len(stmts)-5 {
		t.Errorf("insert at %d, drop of current tables at %d of %d:\n%s", insert, swap, len(stmts), strings.Join(stmts, "\n"))
	}
	if got := ParseNames(" mu-plugins, ,wp-content "); strings.Join(got, "|") != "mu-plugins|wp-content" {
		t.Errorf("ParseNames: %q", got)
	}
}
