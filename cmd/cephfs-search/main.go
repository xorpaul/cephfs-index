// cephfs-search finds entries by name in the indexes written by
// "cephfs-indexd build" and prints uid<TAB>path, one per line.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/xorpaul/cephfs-index/internal/index"
	"github.com/xorpaul/cephfs-index/internal/pgindex"
	"github.com/xorpaul/cephfs-index/internal/scan"
)

var types = map[string]byte{"": 0, "file": scan.TypeFile, "dir": scan.TypeDir, "symlink": scan.TypeSymlink, "hardlink": scan.TypeHardlink}

func main() { os.Exit(run()) }

func run() int {
	fs := flag.NewFlagSet("cephfs-search", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: cephfs-search [flags] PATTERN

PATTERN is a Go RE2 regex matched against entry names (not paths); no
backreferences or lookaround. Patterns starting with ^ and a literal, such as
'^node_modules$', use the name index; others scan all distinct names.

In SQLite mode a host only holds the indexes of the volumes built on it.

flags:
`)
		fs.PrintDefaults()
	}
	dbDir := fs.String("db-dir", "/var/lib/cephfs-index", "index directory (SQLite mode)")
	fsList := fs.String("fs", "", "comma-separated volumes to search (default: every <fs>.db in --db-dir, or all schemas in --pg-dsn mode)")
	db := fs.String("db", "", "search this index file instead, e.g. a .db.partial (SQLite mode only)")
	typ := fs.String("type", "", "only 'file', 'dir', 'symlink' or 'hardlink'")
	uid := fs.Int64("uid", -1, "only entries owned by this uid")
	workers := fs.Int("workers", runtime.NumCPU(), "parallel name scans for patterns without a literal prefix (SQLite mode)")
	pgDSN := fs.String("pg-dsn", os.Getenv("CEPHFS_INDEX_PG_DSN"), "PostgreSQL DSN for pgindex backend; password must not be included — use /root/.pgpass (also via $CEPHFS_INDEX_PG_DSN)")
	_ = fs.Parse(os.Args[1:])
	if fs.NArg() != 1 {
		fs.Usage()
		return 2
	}
	pattern := fs.Arg(0)
	re, err := regexp.Compile(pattern)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bad pattern:", err)
		return 2
	}
	t, ok := types[*typ]
	if !ok {
		fmt.Fprintln(os.Stderr, "bad --type:", *typ)
		return 2
	}

	var paths []string
	if *pgDSN == "" {
		var err error
		paths, err = indexPaths(*db, *dbDir, *fsList)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()
	q := index.Query{Re: re, Pattern: pattern, Type: t, UID: *uid, Workers: *workers}
	how := "full name scan"
	if p := index.NamePrefix(pattern); p != "" {
		how = fmt.Sprintf("name index, prefix %q", p)
	} else if lits := index.RequiredLiterals(pattern); *pgDSN != "" && len(lits) > 0 {
		how = fmt.Sprintf("full name scan, server-side substring filter %q", lits)
	}

	emit := func(m index.Match) error {
		u := strconv.FormatUint(uint64(m.UID), 10)
		if m.Type == scan.TypeHardlink {
			u = "-" // remote dentry: the owner is on the primary dentry
		}
		_, err := fmt.Fprintf(out, "%s\t%d\t%d\t%s\n", u, m.Size, m.Mtime, m.Path)
		return err
	}

	t0 := time.Now()
	var total int64

	if *pgDSN != "" {
		pool, err := pgindex.NewPool(ctx, *pgDSN, 4)
		if err != nil {
			fmt.Fprintln(os.Stderr, "pg pool:", err)
			return 1
		}
		defer pool.Close()

		var schemas []string
		if *fsList != "" {
			schemas = strings.Split(*fsList, ",")
		} else {
			schemas, err = pgindex.Schemas(ctx, pool)
			if err != nil {
				fmt.Fprintln(os.Stderr, "list schemas:", err)
				return 1
			}
			if len(schemas) == 0 {
				fmt.Fprintln(os.Stderr, "no cephfs-index schemas found in database")
				return 1
			}
		}

		for _, name := range schemas {
			d, err := pgindex.Open(ctx, pool, name)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 1
			}
			fmt.Fprintf(os.Stderr, "# %s\n", describe(name, d.Meta))
			var n int64
			err = d.Search(ctx, q, func(m index.Match) error {
				n++
				return emit(m)
			})
			total += n
			if err == nil {
				err = out.Flush()
				fmt.Fprintf(os.Stderr, "# %s: %s\n", name, d.Stats)
			}
			if errors.Is(err, syscall.EPIPE) {
				return 0
			}
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 1
			}
		}
	} else {
		for _, p := range paths {
			x, err := index.Open(p)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 1
			}
			fmt.Fprintf(os.Stderr, "# %s\n", describe(p, x.Meta))
			var n int64
			err = x.Search(ctx, q, func(m index.Match) error {
				n++
				return emit(m)
			})
			x.Close()
			total += n
			if err == nil {
				err = out.Flush()
			}
			if errors.Is(err, syscall.EPIPE) {
				return 0 // e.g. piped into head
			}
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 1
			}
		}
	}

	fmt.Fprintf(os.Stderr, "# %d matches in %s (%s)\n", total, time.Since(t0).Round(time.Millisecond), how)
	return 0
}

func indexPaths(db, dir, fsList string) ([]string, error) {
	if db != "" {
		return []string{db}, nil
	}
	if fsList == "" {
		paths, err := filepath.Glob(filepath.Join(dir, "*.db"))
		if err == nil && len(paths) == 0 {
			err = fmt.Errorf("no indexes in %s", dir)
		}
		return paths, err
	}
	var paths []string
	for _, f := range strings.Split(fsList, ",") {
		p := filepath.Join(dir, f+".db")
		if _, err := os.Stat(p); err != nil {
			return nil, fmt.Errorf("no index for %s on this host (%v); build it here with cephfs-indexd build, or search with --pg-dsn", f, err)
		}
		paths = append(paths, p)
	}
	return paths, nil
}

// describe says how current and complete an index is.
func describe(path string, m map[string]string) string {
	s := fmt.Sprintf("%s: %s, index of %s (%s entries, walk took %ss", m["fs"], m["prefix"], m["started_at"], m["entries"], m["scan_seconds"])
	if m["complete"] != "true" {
		s += ", PARTIAL walk"
	}
	if m["journal_flushed"] != "true" {
		s += ", journal not flushed"
	}
	if n := m["missed_dirs"]; n != "" && n != "0" {
		s += ", missed_dirs=" + n
	}
	if n := m["op_errors"]; n != "" && n != "0" {
		s += ", op_errors=" + n
	}
	return s + ") " + path
}
