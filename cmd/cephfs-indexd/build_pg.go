package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/xorpaul/cephfs-index/internal/pgindex"
	"github.com/xorpaul/cephfs-index/internal/scan"
)

// pgOpts is registered on both the build FlagSet and the buildPG one, so
// build's own Parse accepts the flags before delegating here.
type pgOpts struct{ pgindex.Options }

func (p *pgOpts) register(fs *flag.FlagSet) {
	fs.IntVar(&p.CopyWorkers, "pg-copy-workers", 4, "parallel COPY streams into the entries table (--pg-dsn mode)")
	fs.StringVar(&p.MaintenanceWorkMem, "pg-maintenance-mem", "4GB", "maintenance_work_mem and work_mem for each of the two post-load sessions (--pg-dsn mode)")
	fs.Int64Var(&p.ChunkRows, "pg-chunk-rows", 50_000_000, "seq values per chunk; each COPY stream writes one UNLOGGED leaf table per chunk with COPY FREEZE, indexed while the scan continues (--pg-dsn mode)")
	fs.IntVar(&p.IndexBuilders, "pg-index-builders", 2, "leaf tables indexed concurrently during the scan (--pg-dsn mode)")
	fs.IntVar(&p.ChunkWorkers, "pg-chunk-workers", 4, "parallel workers per leaf index build; kept low so builds don't starve the COPY on shared disks (--pg-dsn mode)")
	fs.IntVar(&p.ParallelWorkers, "pg-parallel-workers", 10, "parallel workers for the final post-load session; the heavy work now runs in the chunk builders (--pg-dsn mode)")
}

func buildPG(args []string, dsn string) int {
	fs := flag.NewFlagSet("build", flag.ExitOnError)
	var o walkOpts
	o.register(fs)
	var po pgOpts
	po.register(fs)
	prefix := fs.String("prefix", "", "path of the filesystem root in search output (default /<fs>)")
	duration := fs.Duration("duration", 0, "stop the walk after this long and keep the result as <fs>_partial schema (0 = full walk)")
	// These flags appear in args when delegated from build() but are not used
	// in PG mode; declare them so FlagSet.Parse does not fail on unknown flags.
	_ = fs.String("db-dir", "/var/lib/cephfs-index", "ignored in --pg-dsn mode")
	_ = fs.Float64("min-free-pct", 35, "ignored in --pg-dsn mode")
	_ = fs.String("pg-dsn", "", "ignored (value passed directly)")
	_ = fs.Parse(args)

	if o.fsName == "" {
		fmt.Fprintln(os.Stderr, "--fs is required")
		return 2
	}
	if *prefix == "" {
		*prefix = "/" + o.fsName
	}
	po.ProgressEvery = o.progress
	po.ProgressLog = func(line string) { fmt.Fprintf(os.Stderr, "%s  %s\n", ts(), line) }

	sigCtx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool, err := pgindex.NewPool(sigCtx, dsn, pgindex.PoolSize(po.Options))
	if err != nil {
		fmt.Fprintln(os.Stderr, "pg pool:", err)
		return 1
	}
	defer pool.Close()

	s, err := o.open(sigCtx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer s.close()

	guardCtx, abort := context.WithCancelCause(sigCtx)
	defer abort(nil)

	w, err := pgindex.Create(guardCtx, pool, s.fs.Name, po.Options)
	if err != nil {
		s.log.Error("create pg index", "fs", s.fs.Name, "err", err)
		return 1
	}
	if err := s.flushJournals(guardCtx, &o); err != nil {
		w.Abort()
		s.log.Error("flush journal, staging schema deleted", "err", err)
		return 1
	}
	go func() {
		select {
		case <-w.Dead():
			abort(fmt.Errorf("pg writer: %w", w.Err()))
		case <-guardCtx.Done():
		}
	}()

	scanCtx := guardCtx
	if *duration > 0 {
		var c context.CancelFunc
		scanCtx, c = context.WithTimeout(guardCtx, *duration)
		defer c()
	}

	sc := s.scanner(&o, w)
	s.header(&o, fmt.Sprintf(" pg-schema=%s prefix=%s pg-copy-workers=%d pg-maintenance-mem=%s pg-parallel-workers=%d pg-chunk-rows=%d pg-index-builders=%d pg-chunk-workers=%d",
		s.fs.Name, *prefix, po.CopyWorkers, po.MaintenanceWorkMem, po.ParallelWorkers, po.ChunkRows, po.IndexBuilders, po.ChunkWorkers))
	started := time.Now()
	report := func(final bool) {
		tag := ""
		if final {
			tag = "done  "
		}
		el := time.Since(started)
		q, qcap := w.Queue()
		nc, nb := w.Chunks()
		fmt.Fprintf(os.Stderr, "%s  %s%s  db_rows=%d queue=%d/%d tables=%d/%d indexed  %s\n",
			ts(), tag, statsLine(sc, el), w.Rows.Load(), q, qcap, nb, nc, el.Round(time.Second))
	}
	stopProgress := every(o.progress, func() { report(false) })
	err = sc.Run(scanCtx, scan.RootIno)
	stopProgress()
	report(true)
	scanDur := time.Since(started)

	if guardCtx.Err() != nil {
		w.Abort()
		s.log.Error("aborted, staging schema deleted", "cause", context.Cause(guardCtx))
		return 1
	}
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		w.Abort()
		s.log.Error("scan", "err", err)
		return 1
	}
	complete := err == nil
	finalSchema := s.fs.Name
	if !complete {
		finalSchema += "_partial"
	}

	st := &sc.Stats
	host, _ := os.Hostname()
	fsid, _ := s.conn.GetFSID()
	i64 := func(v int64) string { return strconv.FormatInt(v, 10) }
	meta := map[string]string{
		"fs": s.fs.Name, "pool": s.fs.MetadataPool, "fsid": fsid, "host": host, "prefix": *prefix,
		"started_at":      started.UTC().Format(time.RFC3339),
		"scan_seconds":    strconv.FormatFloat(scanDur.Seconds(), 'f', 0, 64),
		"complete":        strconv.FormatBool(complete),
		"journal_flushed": strconv.FormatBool(s.flushed),
		"missed_dirs":     i64(st.MissedDirs.Load()), "enoent": i64(st.NotFound.Load()),
		"decode_errors": i64(st.DecodeErrors.Load()), "op_errors": i64(st.OpErrors.Load()),
	}
	fmt.Fprintf(os.Stderr, "%s  building indexes, writing meta, swapping schema\n", ts())
	t1 := time.Now()
	if err := w.Finish(guardCtx, meta, finalSchema); err != nil {
		s.log.Error("finish pg index, staging schema deleted", "err", err, "cause", context.Cause(guardCtx))
		return 1
	}
	fmt.Fprintf(os.Stderr, "%s  wrote schema %s: entries=%d dup_dirs=%d  scan=%s index=%s\n",
		ts(), finalSchema, w.Rows.Load(), w.DupDirs,
		scanDur.Round(time.Second), time.Since(t1).Round(time.Second))
	if st.DecodeErrors.Load() > 0 || st.OpErrors.Load() > 0 {
		return 1
	}
	return 0
}
