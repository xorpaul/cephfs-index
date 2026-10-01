package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/xorpaul/cephfs-index/internal/index"
	"github.com/xorpaul/cephfs-index/internal/scan"
)

func build(args []string) int {
	fs := flag.NewFlagSet("build", flag.ExitOnError)
	var o walkOpts
	o.register(fs)
	dbDir := fs.String("db-dir", "/var/lib/cephfs-index", "index directory; created 0700, refused if group/world accessible")
	prefix := fs.String("prefix", "", "path of the filesystem root in search output (default /<fs>)")
	minFree := fs.Float64("min-free-pct", 35, "abort and delete the new index when free space on --db-dir drops below this percentage (mon_data_avail_warn is 30%)")
	duration := fs.Duration("duration", 0, "stop the walk after this long and keep the result as <fs>.db.partial (0 = full walk)")
	pgDSN := fs.String("pg-dsn", os.Getenv("CEPHFS_INDEX_PG_DSN"), "PostgreSQL DSN for pgindex backend; password must not be included — use /root/.pgpass (also via $CEPHFS_INDEX_PG_DSN)")
	var po pgOpts
	po.register(fs)
	_ = fs.Parse(args)

	if *pgDSN != "" {
		return buildPG(args, *pgDSN)
	}

	if o.fsName == "" {
		fmt.Fprintln(os.Stderr, "--fs is required")
		return 2
	}
	if *prefix == "" {
		*prefix = "/" + o.fsName
	}
	if err := checkDBDir(*dbDir); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if pct, _, err := freeSpace(*dbDir); err != nil || pct < *minFree {
		fmt.Fprintf(os.Stderr, "free space on %s: %.1f%% (%v), --min-free-pct is %.0f\n", *dbDir, pct, err, *minFree)
		return 1
	}

	sigCtx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	s, err := o.open(sigCtx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer s.close()

	// guardCtx ends the whole run (scan and index build) on a signal, low
	// disk space or a writer failure. --duration only ends the scan.
	guardCtx, abort := context.WithCancelCause(sigCtx)
	defer abort(nil)
	tmp := filepath.Join(*dbDir, s.fs.Name+".db.new")
	w, err := index.Create(guardCtx, tmp)
	if err != nil {
		s.log.Error("create index", "path", tmp, "err", err)
		return 1
	}
	if err := s.flushJournals(guardCtx, &o); err != nil {
		w.Abort()
		s.log.Error("flush journal, new index deleted", "err", err)
		return 1
	}
	go func() {
		select {
		case <-w.Dead():
			abort(fmt.Errorf("index writer: %w", w.Err()))
		case <-guardCtx.Done():
		}
	}()
	stopGuard := every(5*time.Second, func() {
		if pct, _, err := freeSpace(*dbDir); err == nil && pct < *minFree {
			abort(fmt.Errorf("free space on %s is %.1f%%, below --min-free-pct %.0f", *dbDir, pct, *minFree))
		}
	})
	defer stopGuard()

	scanCtx := guardCtx
	if *duration > 0 {
		var c context.CancelFunc
		scanCtx, c = context.WithTimeout(guardCtx, *duration)
		defer c()
	}

	sc := s.scanner(&o, w)
	s.header(&o, fmt.Sprintf(" db=%s prefix=%s min-free-pct=%.0f", tmp, *prefix, *minFree))
	started := time.Now()
	report := func(final bool) {
		tag := ""
		if final {
			tag = "done  "
		}
		el := time.Since(started)
		q, qcap := w.Queue()
		fmt.Fprintf(os.Stderr, "%s  %s%s  db_rows=%d names=%d queue=%d/%d  %s\n",
			ts(), tag, statsLine(sc, el), w.Rows.Load(), w.Names.Load(), q, qcap, el.Round(time.Second))
	}
	stopProgress := every(o.progress, func() { report(false) })
	err = sc.Run(scanCtx, scan.RootIno)
	stopProgress()
	report(true)
	scanDur := time.Since(started)

	if guardCtx.Err() != nil {
		w.Abort()
		s.log.Error("aborted, new index deleted", "cause", context.Cause(guardCtx))
		return 1
	}
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		w.Abort()
		s.log.Error("scan", "err", err)
		return 1
	}
	complete := err == nil
	dst := filepath.Join(*dbDir, s.fs.Name+".db")
	if !complete {
		dst += ".partial"
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
	fmt.Fprintf(os.Stderr, "%s  building indexes\n", ts())
	t1 := time.Now()
	if err := w.Finish(guardCtx, meta, dst); err != nil {
		s.log.Error("finish index, new index deleted", "err", err, "cause", context.Cause(guardCtx))
		return 1
	}
	var size int64
	if fi, err := os.Stat(dst); err == nil {
		size = fi.Size()
	}
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	rows := max(1, w.Rows.Load())
	fmt.Fprintf(os.Stderr, "%s  wrote %s: entries=%d names=%d (%.1f%% distinct) dup_dirs=%d size=%d (%.1f bytes/entry)  scan=%s index=%s  max_rss=%dMiB\n",
		ts(), dst, w.Rows.Load(), w.Names.Load(), 100*float64(w.Names.Load())/float64(rows), w.DupDirs, size, float64(size)/float64(rows),
		scanDur.Round(time.Second), time.Since(t1).Round(time.Second), ru.Maxrss/1024)
	if st.DecodeErrors.Load() > 0 || st.OpErrors.Load() > 0 {
		return 1
	}
	return 0
}

// checkDBDir creates the index directory, which holds every user path
// and uid, and refuses one that others can read.
func checkDBDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	fi, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("%s is mode %o: the index contains user paths, chmod 700 it", dir, fi.Mode().Perm())
	}
	return nil
}

func freeSpace(dir string) (pct float64, avail uint64, err error) {
	var st syscall.Statfs_t
	if err = syscall.Statfs(dir, &st); err != nil {
		return 0, 0, err
	}
	return 100 * float64(st.Bavail) / float64(st.Blocks), st.Bavail * uint64(st.Bsize), nil
}
