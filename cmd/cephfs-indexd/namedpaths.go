package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/xorpaul/cephfs-index/internal/pgindex"
)

// namedPaths (re)builds named_paths on existing schemas, so configured
// names get fast searches without waiting for the next build.
func namedPaths(args []string) int {
	fs := flag.NewFlagSet("named-paths", flag.ExitOnError)
	dsn := fs.String("pg-dsn", os.Getenv("CEPHFS_INDEX_PG_DSN"), "PostgreSQL DSN; password must not be included — use ~/.pgpass (also via $CEPHFS_INDEX_PG_DSN)")
	volumes := fs.String("fs", "", "comma-separated schemas (volumes) to update, or 'all' (required)")
	names := fs.String("names", "", "comma-separated entry names whose full paths are precomputed, e.g. mu-plugins (required); replaces the schema's current list")
	_ = fs.Parse(args)

	nameList := pgindex.ParseNames(*names)
	if *dsn == "" || *volumes == "" || len(nameList) == 0 {
		fmt.Fprintln(os.Stderr, "--pg-dsn, --fs and --names are required")
		return 2
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	pool, err := pgindex.NewPool(ctx, *dsn, 2)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pg pool:", err)
		return 1
	}
	defer pool.Close()

	schemas := pgindex.ParseNames(*volumes)
	if *volumes == "all" {
		if schemas, err = pgindex.Schemas(ctx, pool); err != nil {
			fmt.Fprintln(os.Stderr, "list schemas:", err)
			return 1
		}
	}
	rc := 0
	for _, s := range schemas {
		// A running build of this volume ends with DROP SCHEMA <fs>; waiting
		// for this long read, it would queue every search behind it.
		var building bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)`, s+"_new").Scan(&building); err != nil {
			fmt.Fprintf(os.Stderr, "%s  %s: %v\n", ts(), s, err)
			return 1
		}
		if building {
			fmt.Fprintf(os.Stderr, "%s  %s: skipped, a build is running (%s_new exists); its own --pg-named-paths applies, or retry afterwards\n", ts(), s, s)
			rc = 1
			continue
		}
		t0 := time.Now()
		n, err := pgindex.BuildNamedPaths(ctx, pool, s, nameList)
		if err == nil {
			_, err = pool.Exec(ctx, `INSERT INTO `+qi(s)+`.meta (key, value) VALUES ('named_paths', $1) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`, strings.Join(nameList, ","))
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s  %s: %v\n", ts(), s, err)
			rc = 1
			if ctx.Err() != nil {
				return rc
			}
			continue
		}
		fmt.Fprintf(os.Stderr, "%s  %s: named_paths %s: %d rows in %s\n", ts(), s, strings.Join(nameList, ","), n, time.Since(t0).Round(time.Second))
	}
	return rc
}

func qi(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }
