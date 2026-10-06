// Package index stores a scanned CephFS tree in one SQLite file per volume
// and searches it.
package index

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	_ "github.com/mattn/go-sqlite3"

	"github.com/xorpaul/cephfs-index/internal/scan"
)

const schema = `
CREATE TABLE meta    (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE names   (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE dirs    (ino INTEGER PRIMARY KEY, parent INTEGER NOT NULL, name_id INTEGER NOT NULL, rctime INTEGER NOT NULL);
CREATE TABLE entries (name_id INTEGER NOT NULL, parent INTEGER NOT NULL, ino INTEGER NOT NULL, type INTEGER NOT NULL,
                      uid INTEGER NOT NULL, gid INTEGER NOT NULL, size INTEGER NOT NULL, mtime INTEGER NOT NULL, ctime INTEGER NOT NULL);
`

// Built after the load: maintaining them during 1e8+ random inserts is far
// slower than one sort at the end.
const postLoad = `
CREATE UNIQUE INDEX names_name ON names(name);
CREATE INDEX entries_name_id ON entries(name_id);
`

// Writer is a scan.Sink that loads entries into a new SQLite file. A single
// goroutine does all writes; Emit blocks when it falls behind, which slows
// the scan down instead of growing memory.
type Writer struct {
	path  string
	db    *sql.DB
	conn  *sql.Conn
	ch    chan []scan.Entry
	dead  chan struct{} // closed when the load loop fails
	done  chan struct{} // closed when the load loop has returned
	err   error         // valid once dead or done is closed
	once  sync.Once
	names map[string]int64

	Rows, Names, Dirs atomic.Int64
	Types             scan.TypeCounts
	DupDirs           int64 // dir inos seen more than once; valid after Finish
}

// Create starts a new index file at path, replacing any leftover file.
// SQLite's sort temp files go next to it (SQLITE_TMPDIR), not to /tmp.
func Create(ctx context.Context, path string) (*Writer, error) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	os.Setenv("SQLITE_TMPDIR", filepath.Dir(path))
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		return nil, err
	}
	w := &Writer{path: path, db: db, conn: conn, ch: make(chan []scan.Entry, 256),
		dead: make(chan struct{}), done: make(chan struct{}), names: map[string]int64{}}
	// Nothing is read until the file is renamed into place, and a crash just
	// means a rebuild, so journaling and fsync are off during the load.
	for _, q := range []string{
		"PRAGMA journal_mode = OFF", "PRAGMA synchronous = OFF", "PRAGMA locking_mode = EXCLUSIVE",
		"PRAGMA cache_size = -262144", "PRAGMA temp_store = FILE", schema,
	} {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			w.close()
			os.Remove(path)
			return nil, fmt.Errorf("%s: %w", strings.Fields(q)[0], err)
		}
	}
	go w.loop(ctx)
	return w, nil
}

// Emit implements scan.Sink.
func (w *Writer) Emit(b []scan.Entry) {
	select {
	case w.ch <- b:
	case <-w.dead:
	}
}

// Dead is closed when the writer has failed; Err then returns the cause.
func (w *Writer) Dead() <-chan struct{} { return w.dead }
func (w *Writer) Err() error            { return w.err }

// Queue returns the number of batches waiting for the writer and the
// capacity. A full queue means the writer, not the scan, is the bottleneck.
func (w *Writer) Queue() (int, int) { return len(w.ch), cap(w.ch) }

func (w *Writer) loop(ctx context.Context) {
	defer close(w.done)
	if err := w.load(ctx); err != nil {
		w.err = err
		close(w.dead)
	}
}

func (w *Writer) load(ctx context.Context) error {
	tx, err := w.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	names, err := newBulk(ctx, tx, "INSERT INTO names", 2)
	if err != nil {
		return err
	}
	// A dir renamed during the walk can be seen under both parents. The
	// later sighting wins for path reconstruction; a plain INSERT would fail
	// the whole build on the primary key.
	dirs, err := newBulk(ctx, tx, "INSERT OR REPLACE INTO dirs", 4)
	if err != nil {
		return err
	}
	entries, err := newBulk(ctx, tx, "INSERT INTO entries", 9)
	if err != nil {
		return err
	}
	for b := range w.ch {
		for i := range b {
			e := &b[i]
			nid, ok := w.names[e.Name]
			if !ok {
				nid = int64(len(w.names)) + 1
				w.names[e.Name] = nid
				if err := names.add(nid, e.Name); err != nil {
					return err
				}
			}
			w.Types.Add(e.Type)
			if e.Type == scan.TypeDir {
				if err := dirs.add(int64(e.Ino), int64(e.Parent), nid, e.RCtime); err != nil {
					return err
				}
				w.Dirs.Add(1)
			}
			if err := entries.add(nid, int64(e.Parent), int64(e.Ino), int64(e.Type), int64(e.UID), int64(e.GID), int64(e.Size), e.Mtime, e.Ctime); err != nil {
				return err
			}
		}
		w.Rows.Add(int64(len(b)))
		w.Names.Store(int64(len(w.names)))
	}
	for _, b := range []*bulk{names, dirs, entries} {
		if err := b.flush(); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Finish waits for all queued entries, builds the indexes, writes meta and
// moves the file to dst. The name map is released first: it is the largest
// allocation and not needed for the index build.
func (w *Writer) Finish(ctx context.Context, meta map[string]string, dst string) error {
	w.once.Do(func() { close(w.ch) })
	<-w.done
	if w.err != nil {
		w.Abort()
		return w.err
	}
	w.names = nil
	err := func() error {
		var n int64
		if err := w.conn.QueryRowContext(ctx, "SELECT count(*) FROM dirs").Scan(&n); err != nil {
			return err
		}
		w.DupDirs = w.Dirs.Load() - n
		if meta == nil {
			meta = map[string]string{}
		}
		// Counted here, after the queue is drained: the caller's view of the
		// counters is taken while batches may still be queued.
		meta["dup_dirs"] = strconv.FormatInt(w.DupDirs, 10)
		meta["entries"] = strconv.FormatInt(w.Rows.Load(), 10)
		meta["dirs"] = strconv.FormatInt(w.Dirs.Load(), 10)
		meta["names"] = strconv.FormatInt(w.Names.Load(), 10)
		w.Types.SetMeta(meta)
		if _, err := w.conn.ExecContext(ctx, postLoad); err != nil {
			return fmt.Errorf("create indexes: %w", err)
		}
		for k, v := range meta {
			if _, err := w.conn.ExecContext(ctx, "INSERT INTO meta (key, value) VALUES (?, ?)", k, v); err != nil {
				return fmt.Errorf("meta: %w", err)
			}
		}
		return nil
	}()
	if cerr := w.close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = syncFile(w.path)
	}
	if err == nil {
		err = os.Rename(w.path, dst)
	}
	if err == nil {
		err = syncFile(filepath.Dir(dst))
	}
	if err != nil {
		os.Remove(w.path)
	}
	return err
}

// Abort stops the writer and deletes the file.
func (w *Writer) Abort() {
	w.once.Do(func() { close(w.ch) })
	<-w.done
	w.close()
	os.Remove(w.path)
}

func (w *Writer) close() error {
	err := w.conn.Close()
	if cerr := w.db.Close(); err == nil {
		err = cerr
	}
	return err
}

func syncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// bulk batches rows into multi-row INSERTs, which cuts the per-statement
// overhead of database/sql and cgo by the batch size.
type bulk struct {
	ctx    context.Context
	tx     *sql.Tx
	insert string // "INSERT [OR ...] INTO table"
	cols   int
	full   *sql.Stmt
	args   []any
}

const bulkRows = 64

func newBulk(ctx context.Context, tx *sql.Tx, insert string, cols int) (*bulk, error) {
	b := &bulk{ctx: ctx, tx: tx, insert: insert, cols: cols, args: make([]any, 0, cols*bulkRows)}
	var err error
	b.full, err = tx.PrepareContext(ctx, b.stmt(bulkRows))
	return b, err
}

func (b *bulk) stmt(rows int) string {
	row := "(" + strings.TrimSuffix(strings.Repeat("?,", b.cols), ",") + ")"
	return b.insert + " VALUES " + strings.TrimSuffix(strings.Repeat(row+",", rows), ",")
}

func (b *bulk) add(vals ...any) error {
	b.args = append(b.args, vals...)
	if len(b.args) < cap(b.args) {
		return nil
	}
	_, err := b.full.ExecContext(b.ctx, b.args...)
	b.args = b.args[:0]
	return err
}

func (b *bulk) flush() error {
	if len(b.args) == 0 {
		return nil
	}
	_, err := b.tx.ExecContext(b.ctx, b.stmt(len(b.args)/b.cols), b.args...)
	b.args = b.args[:0]
	return err
}
