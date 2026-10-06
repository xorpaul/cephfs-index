// Package pgindex stores a scanned CephFS tree in a PostgreSQL schema and
// searches it. Schema layout:
//
//	<fs>.entries            partitioned by RANGE (part); no storage of its own
//	<fs>.entries_NNNNN_S    UNLOGGED leaf tables: one per chunk (ChunkRows seq
//	                        values) and COPY stream S, each with its own part
//	<fs>.dirs               UNLOGGED — one row per unique dir ino, upserted per leaf
//	<fs>.meta               LOGGED   — key/value metadata; survives crash recovery
//
// During the scan, each COPY worker writes its share of the current chunk
// into its own leaf table, created in the same transaction as a COPY FREEZE:
// the rows are written frozen and the pages all-visible, so index-only scans
// work without a VACUUM pass over the table (on HDD storage a per-chunk
// VACUUM took ~20 min). Once a leaf's COPY commits, a background builder
// indexes it and upserts its dir rows into dirs while the scan continues, so
// at the end only the last leaves and the (metadata-only) attach are left.
//
// Rows are partitioned by part, not seq: the COPY streams of a chunk write
// interleaved seq values, and seq must stay in emit order for the dup-dir
// tiebreak.
//
// UNLOGGED tables are emptied by Postgres crash recovery. Open detects this
// and returns an error so callers rebuild rather than silently return 0 results.
//
// Password must come from /root/.pgpass; do not embed it in the DSN.
package pgindex

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xorpaul/cephfs-index/internal/scan"
)

var entryCols = []string{"seq", "parent", "name", "ino", "type", "part", "uid", "gid", "size", "mtime", "ctime", "rctime"}

// Options tunes the load. Zero values fall back to the defaults below.
type Options struct {
	CopyWorkers        int    // parallel COPY streams into entries
	MaintenanceWorkMem string // per post-load and chunk-builder session, e.g. "4GB"
	ParallelWorkers    int    // parallel workers for the final post-load session (attach, dirs index and VACUUM)
	ChunkRows          int64  // seq values per chunk; each COPY stream writes one leaf table per chunk
	IndexBuilders      int    // leaf tables indexed concurrently during the scan
	ChunkWorkers       int    // parallel workers per leaf index build

	ProgressEvery time.Duration     // post-load progress poll interval; 0 disables
	ProgressLog   func(line string) // receives one line per active post-load session
}

func (o *Options) defaults() {
	if o.CopyWorkers <= 0 {
		o.CopyWorkers = 4
	}
	if o.MaintenanceWorkMem == "" {
		o.MaintenanceWorkMem = "4GB"
	}
	if o.ParallelWorkers < 0 {
		o.ParallelWorkers = 0
	}
	if o.ChunkRows <= 0 {
		o.ChunkRows = 50_000_000
	}
	if o.ChunkRows < 1_000_000 {
		o.ChunkRows = 1_000_000
	}
	if o.IndexBuilders <= 0 {
		o.IndexBuilders = 2
	}
	if o.ChunkWorkers < 0 {
		o.ChunkWorkers = 0
	}
}

// Writer loads a CephFS scan into a PostgreSQL schema via the COPY protocol.
// It acquires an advisory lock so concurrent builds on the same fs fail fast.
type Writer struct {
	pool     *pgxpool.Pool
	fsName   string
	staging  string
	opts     Options
	lockConn *pgxpool.Conn // holds pg_advisory_lock for the duration of the build

	ch      chan []scan.Entry
	dead    chan struct{}
	done    chan struct{}
	once    sync.Once
	errOnce sync.Once
	err     error

	// recvMu serialises batch dequeue + seq reservation across COPY workers,
	// so seq order equals scan emit order (the dup-dir tiebreaker relies on it).
	// It also guards pending, eof, cur, chunks, leaves and nextPart.
	recvMu   sync.Mutex
	seq      int64          // last reserved seq
	pending  [][]scan.Entry // dequeued but not reserved: belonged to a later chunk than the worker's COPY
	eof      bool           // ch closed and drained
	cur      *chunk         // chunk currently receiving reservations
	chunks   []*chunk       // all chunks in order
	leaves   []*leaf        // all leaf tables in creation order
	nextPart int            // part of the next leaf

	jobs      chan *leaf // committed leaves for the builders
	buildDone chan struct{}
	created   atomic.Int64 // leaves; read without recvMu: a worker holds it while blocked on ch
	built     atomic.Int64

	pidMu sync.Mutex
	pids  map[int32]struct{} // backends running post-load or chunk-build statements

	Rows    atomic.Int64
	Dirs    atomic.Int64
	Types   scan.TypeCounts
	DupDirs int64 // valid after Finish
}

// NewPool creates a pgxpool configured for cephfs-index. The DSN must not
// contain a password — pgx reads /root/.pgpass automatically.
func NewPool(ctx context.Context, dsn string, maxConns int32) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	// SQL_ASCII databases accept any byte sequence. Setting client_encoding
	// explicitly avoids confusion if the client's default differs.
	config.ConnConfig.RuntimeParams["client_encoding"] = "SQL_ASCII"
	config.MaxConns = maxConns
	return pgxpool.NewWithConfig(ctx, config)
}

// chunk is the seq window [k*ChunkRows, (k+1)*ChunkRows). A COPY session
// ends at a chunk boundary, so each worker writes at most one leaf per chunk.
type chunk struct {
	k      int
	lo, hi int64
	slots  int // leaves started in this chunk; guarded by recvMu
	rows   atomic.Int64
}

// leaf is one COPY session's table: the rows one worker wrote in one chunk.
type leaf struct {
	c    *chunk
	slot int // index among the chunk's leaves
	part int // partition key value, unique per leaf
	rows int64
}

func (l *leaf) table() string { return fmt.Sprintf("entries_%05d_%d", l.c.k, l.slot) }

// maxPart is the largest part a SMALLINT holds: with 4 workers and the
// minimum ChunkRows that is ~8 billion entries.
const maxPart = 32767

// PoolSize returns the connections a build with these options needs:
// the advisory-lock conn, the COPY workers, one for chunk DDL, the chunk
// builders, the final post-load session and one for progress polling.
func PoolSize(o Options) int32 {
	o.defaults()
	return int32(o.CopyWorkers + o.IndexBuilders + 4)
}

const entryColsDDL = `
	seq    BIGINT   NOT NULL,
	parent BIGINT   NOT NULL,
	name   TEXT     NOT NULL,
	ino    BIGINT   NOT NULL,
	type   CHAR(1)  NOT NULL,
	part   SMALLINT NOT NULL,
	uid    BIGINT   NOT NULL,
	gid    BIGINT   NOT NULL,
	size   BIGINT   NOT NULL,
	mtime  BIGINT   NOT NULL,
	ctime  BIGINT   NOT NULL,
	rctime BIGINT   NOT NULL`

// Create acquires an advisory lock for fsName, drops any leftover staging
// schema from a failed previous run, creates fresh tables, and starts the
// COPY worker goroutines.
func Create(ctx context.Context, pool *pgxpool.Pool, fsName string, opts Options) (*Writer, error) {
	opts.defaults()
	if err := validateName(fsName); err != nil {
		return nil, err
	}
	staging := fsName + "_new"

	lockConn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire lock conn: %w", err)
	}
	var locked bool
	if err := lockConn.QueryRow(ctx,
		`SELECT pg_try_advisory_lock(hashtext($1)::bigint)`,
		"cephfs-index:"+fsName).Scan(&locked); err != nil {
		lockConn.Release()
		return nil, fmt.Errorf("advisory lock: %w", err)
	}
	if !locked {
		lockConn.Release()
		return nil, fmt.Errorf("advisory lock for %q is held: another build is running (or previous run did not clean up)", fsName)
	}

	setup, err := pool.Acquire(ctx)
	if err != nil {
		lockConn.Release()
		return nil, err
	}
	defer setup.Release()

	if _, err := setup.Exec(ctx, `DROP SCHEMA IF EXISTS `+qi(staging)+` CASCADE`); err != nil {
		lockConn.Release()
		return nil, fmt.Errorf("drop old staging schema: %w", err)
	}
	_, err = setup.Exec(ctx, `
		CREATE SCHEMA `+qi(staging)+`;
		CREATE UNLOGGED TABLE `+qi(staging)+`.dirs (
			ino    BIGINT PRIMARY KEY,
			parent BIGINT NOT NULL,
			name   TEXT   NOT NULL,
			seq    BIGINT NOT NULL
		);
		CREATE TABLE `+qi(staging)+`.meta (
			key   TEXT PRIMARY KEY,
			value TEXT NOT NULL
		);
	`)
	if err != nil {
		lockConn.Release()
		return nil, fmt.Errorf("create staging schema: %w", err)
	}

	w := &Writer{
		pool:      pool,
		fsName:    fsName,
		staging:   staging,
		opts:      opts,
		lockConn:  lockConn,
		ch:        make(chan []scan.Entry, 256),
		dead:      make(chan struct{}),
		done:      make(chan struct{}),
		jobs:      make(chan *leaf, 4096),
		buildDone: make(chan struct{}),
		pids:      map[int32]struct{}{},
	}
	var bwg sync.WaitGroup
	for range opts.IndexBuilders {
		bwg.Add(1)
		go func() {
			defer bwg.Done()
			w.builder(ctx)
		}()
	}
	go func() {
		bwg.Wait()
		close(w.buildDone)
	}()
	var wg sync.WaitGroup
	for range opts.CopyWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.copyWorker(ctx); err != nil {
				w.fail(err)
			}
		}()
	}
	go func() {
		wg.Wait()
		close(w.done)
	}()
	return w, nil
}

// fail records the first worker error and signals Dead. The build's guard
// context is cancelled on Dead, which stops the remaining workers.
func (w *Writer) fail(err error) {
	w.errOnce.Do(func() {
		w.err = err
		close(w.dead)
	})
}

// copyWorker runs one COPY per chunk: it reserves a batch, creates its leaf
// table and COPYs into it until the next batch belongs to a later chunk,
// then commits, hands the leaf to the builders and starts over. Separate
// COPYs per chunk let the builders index finished leaves while the scan
// continues.
func (w *Writer) copyWorker(ctx context.Context) error {
	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	for {
		src := &batchSource{w: w, ctx: ctx}
		l, err := src.begin(ctx)
		if err != nil || l == nil {
			return err
		}
		if err := w.copyLeaf(ctx, conn, l, src); err != nil {
			return fmt.Errorf("copy into %s: %w", l.table(), err)
		}
		if src.ctxDone {
			return nil
		}
		w.jobs <- l
		if src.eof {
			return nil
		}
	}
}

// copyLeaf creates l's table and fills it with COPY FREEZE in one
// transaction, which FREEZE requires. The CHECK equals the later partition
// bound, so ATTACH PARTITION can skip its validation scan; it must be part
// of CREATE TABLE, adding it afterwards would scan the table. pgx's CopyFrom
// cannot pass FREEZE, so the binary COPY stream is written by copyReader.
func (w *Writer) copyLeaf(ctx context.Context, conn *pgxpool.Conn, l *leaf, src *batchSource) error {
	t := qi(w.staging) + "." + qi(l.table())
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, fmt.Sprintf(`CREATE UNLOGGED TABLE %s (%s,
		CHECK (part >= %d AND part < %d))`, t, entryColsDDL, l.part, l.part+1)); err != nil {
		return err
	}
	r := &copyReader{src: src, part: int16(l.part)}
	sql := fmt.Sprintf(`COPY %s (%s) FROM STDIN WITH (FORMAT binary, FREEZE)`, t, strings.Join(entryCols, ", "))
	_, err = tx.Conn().PgConn().CopyFrom(ctx, r, sql)
	if err := src.copyErr(err); err != nil {
		return err
	}
	if src.ctxDone {
		return nil // rolled back; the build is being cancelled
	}
	l.rows = r.rows
	return tx.Commit(ctx)
}

// take returns the next batch in emit order: stashed batches first, then
// the channel. Caller holds recvMu. ok=false on EOF or ctx cancellation.
func (w *Writer) take(s *batchSource) ([]scan.Entry, bool) {
	if len(w.pending) > 0 {
		b := w.pending[0]
		w.pending = w.pending[1:]
		return b, true
	}
	if w.eof {
		s.eof = true
		return nil, false
	}
	select {
	case b, ok := <-w.ch:
		if !ok {
			w.eof = true
			s.eof = true
			return nil, false
		}
		return b, true
	case <-s.ctx.Done():
		s.ctxDone = true
		return nil, false
	}
}

// placement returns where a batch of n rows would start and which chunk it
// lands in. A batch never straddles a boundary: if it would, it starts at
// the next chunk's first seq. The gap is harmless, seq only has to grow.
func (w *Writer) placement(n int) (start int64, k int) {
	N := w.opts.ChunkRows
	start = w.seq + 1
	if (start+int64(n)-1)/N != start/N {
		start = (start/N + 1) * N
	}
	return start, int(start / N)
}

// rotate makes chunk k current. Caller holds recvMu. Leaves are created by
// the COPY workers, so there is no DDL here.
func (w *Writer) rotate(k int) {
	N := w.opts.ChunkRows
	c := &chunk{k: k, lo: int64(k) * N, hi: int64(k+1) * N}
	w.cur = c
	w.chunks = append(w.chunks, c)
}

// closeLast closes the job queue. Called once all COPY workers have exited,
// so every committed leaf is already queued.
func (w *Writer) closeLast() {
	close(w.jobs)
}

// nameIndexInclude are the entries columns a search reads, stored in the
// name index so a search is an index-only scan: on HDD storage each heap
// fetch is a random read (18 s for 501 rows of a common name).
const nameIndexInclude = `parent, ino, type, uid, size, mtime, ctime`

// chunkStmts builds a committed leaf: the covering name index, its dir rows
// upserted into dirs, then VACUUM (ANALYZE). COPY FREEZE already wrote the
// rows frozen and set the visibility map, so this vacuum skips every page
// and only reads the map; it is there because ANALYZE alone leaves
// pg_class.relallvisible at 0, and with 0 the planner prices every
// index-only fetch as a heap fetch and picks a bitmap scan instead. It also
// resets the insert counter, so autovacuum leaves the leaf alone.
func chunkStmts(staging, table string, workers int) []string {
	t := qi(staging) + "." + qi(table)
	return []string{
		fmt.Sprintf(`ALTER TABLE %s SET (parallel_workers = %d)`, t, workers),
		fmt.Sprintf(`CREATE INDEX %s ON %s (name) INCLUDE (%s)`, qi(table+"_name"), t, nameIndexInclude),
		// Keep the last sighting of each dir (highest seq): a dir renamed
		// mid-walk appears twice, possibly in chunks built out of order.
		// DISTINCT ON dedups within the chunk (ON CONFLICT cannot touch a
		// row twice) and yields ino order, so concurrent builders lock
		// rows in the same order and cannot deadlock.
		fmt.Sprintf(`INSERT INTO %s.dirs AS d (ino, parent, name, seq)
				SELECT DISTINCT ON (ino) ino, parent, name, seq FROM %s WHERE type = 'd' ORDER BY ino, seq DESC
				ON CONFLICT (ino) DO UPDATE SET parent = excluded.parent, name = excluded.name, seq = excluded.seq
				WHERE excluded.seq > d.seq`, qi(staging), t),
		`VACUUM (ANALYZE) ` + t,
	}
}

// builder indexes committed leaves while the scan continues.
func (w *Writer) builder(ctx context.Context) {
	for l := range w.jobs {
		if ctx.Err() != nil {
			continue
		}
		start := time.Now()
		err := w.runChain(ctx, w.opts.ChunkWorkers, chunkStmts(w.staging, l.table(), w.opts.ChunkWorkers))
		if err != nil {
			if ctx.Err() == nil {
				w.fail(fmt.Errorf("index %s: %w", l.table(), err))
			}
			continue
		}
		w.built.Add(1)
		if w.opts.ProgressLog != nil {
			w.opts.ProgressLog(fmt.Sprintf("table %s indexed: rows=%d took=%s", l.table(), l.rows, time.Since(start).Round(time.Second)))
		}
	}
}

// Chunks returns (leaf tables created, leaf tables indexed). It must not take recvMu:
// a COPY worker holds it while waiting on ch, and ch is only closed by Finish.
func (w *Writer) Chunks() (int, int) {
	return int(w.created.Load()), int(w.built.Load())
}

// Emit implements scan.Sink.
func (w *Writer) Emit(b []scan.Entry) {
	select {
	case w.ch <- b:
	case <-w.dead:
	}
}

func (w *Writer) Dead() <-chan struct{} { return w.dead }
func (w *Writer) Err() error            { return w.err }
func (w *Writer) Queue() (int, int)     { return len(w.ch), cap(w.ch) }

// Finish drains the COPY workers, waits for the chunk builders (including
// the last chunk), attaches the chunks under a partitioned entries table,
// writes meta, and atomically swaps the staging schema to finalSchema.
// finalSchema is typically fsName (full build) or fsName+"_partial".
func (w *Writer) Finish(ctx context.Context, meta map[string]string, finalSchema string) error {
	if err := validateName(finalSchema); err != nil {
		w.dropAndUnlock()
		return err
	}
	w.once.Do(func() { close(w.ch) })
	<-w.done
	w.closeLast()

	pollCtx, stopPoll := context.WithCancel(ctx)
	stopProgress := w.reportPostLoad(pollCtx)
	defer func() { stopPoll(); stopProgress() }()

	<-w.buildDone
	if w.err != nil {
		err := w.err
		w.dropAndUnlock()
		return err
	}

	s := qi(w.staging)
	if err := w.runChain(ctx, w.opts.ParallelWorkers, postLoadStmts(w.staging, w.leaves)); err != nil {
		w.dropAndUnlock()
		return fmt.Errorf("post-load: %w", err)
	}

	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		w.dropAndUnlock()
		return err
	}
	defer conn.Release()

	var dirCount int64
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM `+s+`.dirs`).Scan(&dirCount); err != nil {
		w.dropAndUnlock()
		return err
	}
	w.DupDirs = w.Dirs.Load() - dirCount

	if meta == nil {
		meta = map[string]string{}
	}
	meta["entries"] = fmt.Sprintf("%d", w.Rows.Load())
	meta["dirs"] = fmt.Sprintf("%d", w.Dirs.Load())
	meta["dup_dirs"] = fmt.Sprintf("%d", w.DupDirs)
	w.Types.SetMeta(meta)
	// layout 2: leaves partitioned by part and filled with COPY FREEZE,
	// covering name and dirs indexes (index-only searches).
	meta["layout"] = "2"
	for k, v := range meta {
		if _, err := conn.Exec(ctx, `INSERT INTO `+s+`.meta (key, value) VALUES ($1, $2)`, k, v); err != nil {
			w.dropAndUnlock()
			return fmt.Errorf("meta: %w", err)
		}
	}

	// Atomic swap: DDL is transactional in Postgres.
	tx, err := conn.Begin(ctx)
	if err != nil {
		w.dropAndUnlock()
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DROP SCHEMA IF EXISTS `+qi(finalSchema)+` CASCADE`); err != nil {
		w.dropAndUnlock()
		return err
	}
	if _, err := tx.Exec(ctx, `ALTER SCHEMA `+qi(w.staging)+` RENAME TO `+qi(finalSchema)); err != nil {
		w.dropAndUnlock()
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		w.dropAndUnlock()
		return err
	}
	w.lockConn.Release()
	return nil
}

// postLoadStmts assembles the partitioned entries table from the built
// leaves and finishes dirs. Each leaf's CHECK matches its bound, so ATTACH
// skips validation; the ON ONLY parent index (same columns and INCLUDE list
// as the leaf indexes, or ATTACH fails) becomes valid once every leaf's
// name index is attached. dirs gets a covering index on ino, so resolving a
// path level is an index-only scan too; it is built here rather than being
// the primary key so the per-leaf upserts during the scan maintain a narrow
// index. dirs was upserted, not COPY FROZEN, so it needs a VACUUM for its
// visibility map. BUFFER_USAGE_LIMIT lifts VACUUM's default 2MB ring, in
// which it has to write back every page it freezes before reading on.
func postLoadStmts(staging string, leaves []*leaf) []string {
	s := qi(staging)
	stmts := []string{`CREATE TABLE ` + s + `.entries (` + entryColsDDL + `) PARTITION BY RANGE (part)`}
	for _, l := range leaves {
		stmts = append(stmts, fmt.Sprintf(`ALTER TABLE %s.entries ATTACH PARTITION %s.%s FOR VALUES FROM (%d) TO (%d)`,
			s, s, qi(l.table()), l.part, l.part+1))
	}
	stmts = append(stmts, `CREATE INDEX entries_name ON ONLY `+s+`.entries (name) INCLUDE (`+nameIndexInclude+`)`)
	for _, l := range leaves {
		stmts = append(stmts, fmt.Sprintf(`ALTER INDEX %s.entries_name ATTACH PARTITION %s.%s`, s, s, qi(l.table()+"_name")))
	}
	return append(stmts,
		`CREATE UNIQUE INDEX dirs_ino_cover ON `+s+`.dirs (ino) INCLUDE (parent, name)`,
		`VACUUM (FREEZE, ANALYZE, BUFFER_USAGE_LIMIT '2GB') `+s+`.dirs`,
	)
}

// runChain executes stmts in order on one session tuned for bulk sorts and
// index builds. SET (not SET LOCAL) is used because the conn is not in a
// transaction; the settings are reset before it returns to the pool.
func (w *Writer) runChain(ctx context.Context, p int, stmts []string) error {
	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	defer conn.Exec(context.Background(), `RESET ALL`)
	pid := int32(conn.Conn().PgConn().PID())
	w.pidMu.Lock()
	w.pids[pid] = struct{}{}
	w.pidMu.Unlock()
	defer func() {
		w.pidMu.Lock()
		delete(w.pids, pid)
		w.pidMu.Unlock()
	}()

	setup := []string{
		`SET maintenance_work_mem = ` + quoteLit(w.opts.MaintenanceWorkMem),
		`SET work_mem = ` + quoteLit(w.opts.MaintenanceWorkMem),
		fmt.Sprintf(`SET max_parallel_maintenance_workers = %d`, p),
		fmt.Sprintf(`SET max_parallel_workers_per_gather = %d`, p),
	}
	for _, q := range append(setup, stmts...) {
		if _, err := conn.Exec(ctx, q); err != nil {
			return fmt.Errorf("%s: %w", firstLine(q), err)
		}
	}
	return nil
}

// reportPostLoad polls pg_stat_activity and pg_stat_progress_create_index
// for the chunk-build and post-load sessions and passes one line per active
// session to opts.ProgressLog. The dirs CTAS has no progress view; only
// elapsed is shown.
func (w *Writer) reportPostLoad(ctx context.Context) (stop func()) {
	if w.opts.ProgressLog == nil || w.opts.ProgressEvery <= 0 {
		return func() {}
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(w.opts.ProgressEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			w.pidMu.Lock()
			ids := make([]int32, 0, len(w.pids))
			for p := range w.pids {
				ids = append(ids, p)
			}
			w.pidMu.Unlock()
			if len(ids) == 0 {
				continue
			}
			w.logPostLoad(ctx, ids)
		}
	}()
	return func() { cancel(); <-done }
}

func (w *Writer) logPostLoad(ctx context.Context, ids []int32) {
	rows, err := w.pool.Query(ctx, `
		SELECT a.pid, extract(epoch FROM now() - a.query_start)::float8, a.query,
		       coalesce(p.phase, v.phase, ''),
		       coalesce(p.blocks_done, v.heap_blks_scanned, 0), coalesce(p.blocks_total, v.heap_blks_total, 0),
		       coalesce(p.tuples_done, 0), coalesce(p.tuples_total, 0),
		       (SELECT count(*) FROM pg_stat_activity w WHERE w.leader_pid = a.pid)
		FROM pg_stat_activity a
		LEFT JOIN pg_stat_progress_create_index p ON p.pid = a.pid
		LEFT JOIN pg_stat_progress_vacuum v ON v.pid = a.pid
		WHERE a.pid = ANY($1) AND a.state = 'active'
		ORDER BY a.pid`, ids)
	if err != nil {
		if ctx.Err() == nil {
			w.opts.ProgressLog("post-load progress query: " + err.Error())
		}
		return
	}
	defer rows.Close()
	for rows.Next() {
		var pid int32
		var secs float64
		var query, phase string
		var bd, bt, td, tt, workers int64
		if err := rows.Scan(&pid, &secs, &query, &phase, &bd, &bt, &td, &tt, &workers); err != nil {
			return
		}
		line := fmt.Sprintf("post-load pid=%d  %s  running=%s", pid, stmtLabel(query), time.Duration(secs*float64(time.Second)).Round(time.Second))
		line += fmt.Sprintf("  workers=%d", workers)
		if phase != "" {
			line += "  phase=" + strconv.Quote(phase)
		}
		switch {
		case tt > 0:
			line += fmt.Sprintf("  tuples=%d/%d (%.1f%%)", td, tt, 100*float64(td)/float64(tt))
		case bt > 0:
			line += fmt.Sprintf("  blocks=%d/%d (%.1f%%)", bd, bt, 100*float64(bd)/float64(bt))
		}
		w.opts.ProgressLog(line)
	}
}

// stmtLabel shortens a post-load statement to e.g. `CREATE INDEX ON "a02_new".entries (name)`.
func stmtLabel(q string) string {
	q = strings.Join(strings.Fields(q), " ")
	if i := strings.Index(q, " AS SELECT"); i >= 0 {
		q = q[:i]
	}
	if len(q) > 70 {
		q = q[:70] + "…"
	}
	return q
}

func quoteLit(v string) string { return `'` + strings.ReplaceAll(v, `'`, `''`) + `'` }

func firstLine(q string) string {
	q = strings.TrimSpace(q)
	if i := strings.IndexByte(q, '\n'); i >= 0 {
		return q[:i]
	}
	return q
}

// Abort stops the writer and drops the staging schema.
func (w *Writer) Abort() {
	w.fail(errors.New("aborted"))
	w.once.Do(func() { close(w.ch) })
	<-w.done
	w.closeLast()
	<-w.buildDone
	w.dropAndUnlock()
}

func (w *Writer) dropAndUnlock() {
	ctx := context.Background()
	conn, err := w.pool.Acquire(ctx)
	if err == nil {
		conn.Exec(ctx, `DROP SCHEMA IF EXISTS `+qi(w.staging)+` CASCADE`)
		conn.Release()
	}
	w.lockConn.Release()
}

// batchSource implements pgx.CopyFromSource for one COPY into one chunk.
// It respects ctx cancellation so a failed worker does not leave the
// goroutine stuck on a channel receive.
type batchSource struct {
	w       *Writer
	ctx     context.Context
	c       *chunk
	batch   []scan.Entry
	i       int
	seq     int64 // seq of batch[0]
	ctxDone bool
	eof     bool
}

// begin reserves the first batch of a new COPY session, rotating to a new
// chunk if the batch lands beyond the current one, and assigns the
// session's leaf. It returns nil on EOF or cancellation.
func (s *batchSource) begin(ctx context.Context) (*leaf, error) {
	w := s.w
	w.recvMu.Lock()
	defer w.recvMu.Unlock()
	b, ok := w.take(s)
	if !ok {
		return nil, nil
	}
	if w.nextPart > maxPart {
		return nil, fmt.Errorf("more than %d leaf tables: raise --pg-chunk-rows", maxPart+1)
	}
	start, k := w.placement(len(b))
	if w.cur == nil || k > w.cur.k {
		w.rotate(k)
	}
	s.c = w.cur
	l := &leaf{c: s.c, slot: s.c.slots, part: w.nextPart}
	s.c.slots++
	w.nextPart++
	w.leaves = append(w.leaves, l)
	w.created.Add(1)
	s.accept(b, start)
	return l, nil
}

func (s *batchSource) accept(b []scan.Entry, start int64) {
	s.batch, s.i, s.seq = b, 0, start
	s.w.seq = start + int64(len(b)) - 1
	s.c.rows.Add(int64(len(b)))
}

func (s *batchSource) Next() bool {
	for s.i >= len(s.batch) {
		if !s.recv() {
			return false
		}
	}
	return true
}

// recv reserves the next batch for this session's chunk. A batch that would
// land in a later chunk is stashed unreserved for the next session and ends
// this COPY.
func (s *batchSource) recv() bool {
	w := s.w
	w.recvMu.Lock()
	defer w.recvMu.Unlock()
	b, ok := w.take(s)
	if !ok {
		return false
	}
	start, k := w.placement(len(b))
	if k != s.c.k {
		w.pending = append(w.pending, b)
		return false
	}
	s.accept(b, start)
	return true
}

// row returns the next entry and its seq, and counts it.
func (s *batchSource) row() (int64, *scan.Entry) {
	e := &s.batch[s.i]
	seq := s.seq + int64(s.i)
	s.i++
	s.w.Rows.Add(1)
	s.w.Types.Add(e.Type)
	if e.Type == scan.TypeDir {
		s.w.Dirs.Add(1)
	}
	return seq, e
}

// copyReader encodes a COPY session's rows in PostgreSQL's binary COPY
// format, in entryCols order, pulling them from src as the server reads.
type copyReader struct {
	src     *batchSource
	part    int16
	buf     []byte
	off     int
	started bool
	done    bool
	rows    int64
}

// copyHeader is the binary COPY signature, a zero flags field and a zero
// header-extension length.
var copyHeader = []byte("PGCOPY\n\xff\r\n\x00\x00\x00\x00\x00\x00\x00\x00\x00")

func (r *copyReader) Read(p []byte) (int, error) {
	for r.off >= len(r.buf) {
		if r.done {
			return 0, io.EOF
		}
		r.buf, r.off = r.buf[:0], 0
		if !r.started {
			r.buf = append(r.buf, copyHeader...)
			r.started = true
		}
		for len(r.buf) < 64<<10 {
			if !r.src.Next() {
				r.buf = binary.BigEndian.AppendUint16(r.buf, 0xffff) // file trailer
				r.done = true
				break
			}
			seq, e := r.src.row()
			r.buf = appendRow(r.buf, seq, e, r.part)
			r.rows++
		}
	}
	n := copy(p, r.buf[r.off:])
	r.off += n
	return n, nil
}

// appendRow appends one binary COPY tuple: field count, then each field as
// a 4-byte length and its bytes (int8 and int2 big-endian; text and
// char(1) as raw bytes, which SQL_ASCII accepts unconverted).
func appendRow(b []byte, seq int64, e *scan.Entry, part int16) []byte {
	i8 := func(v int64) {
		b = binary.BigEndian.AppendUint32(b, 8)
		b = binary.BigEndian.AppendUint64(b, uint64(v))
	}
	b = binary.BigEndian.AppendUint16(b, uint16(len(entryCols)))
	i8(seq)
	i8(int64(e.Parent))
	b = binary.BigEndian.AppendUint32(b, uint32(len(e.Name)))
	b = append(b, e.Name...)
	i8(int64(e.Ino))
	b = binary.BigEndian.AppendUint32(b, 1)
	b = append(b, e.Type)
	b = binary.BigEndian.AppendUint32(b, 2)
	b = binary.BigEndian.AppendUint16(b, uint16(part))
	i8(int64(e.UID))
	i8(int64(e.GID))
	i8(int64(e.Size))
	i8(e.Mtime)
	i8(e.Ctime)
	i8(e.RCtime)
	return b
}

// copyErr returns a nil error when the COPY was stopped by context
// cancellation (the caller checks guardCtx.Err() instead).
func (s *batchSource) copyErr(err error) error {
	if s.ctxDone || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	return err
}

// qi returns a double-quoted PostgreSQL identifier safe for embedding in SQL.
func qi(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// validateName rejects fs/schema names that contain characters outside
// [a-z0-9_]. This prevents SQL injection via the qi() helper and matches
// typical CephFS volume names.
func validateName(name string) error {
	if name == "" {
		return fmt.Errorf("schema name is empty")
	}
	for _, c := range name {
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_') {
			return fmt.Errorf("schema name %q contains invalid character %q (only [a-z0-9_] allowed)", name, c)
		}
	}
	return nil
}
