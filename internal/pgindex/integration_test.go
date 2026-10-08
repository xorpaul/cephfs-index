package pgindex

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xorpaul/cephfs-index/internal/index"
	"github.com/xorpaul/cephfs-index/internal/scan"
)

// TestBuildAgainstPostgres builds a small volume through the real writer and
// checks the layout-2 promises on a live server: COPY FREEZE leaves are
// all-visible, name and dirs lookups are index-only scans without heap
// fetches, and search results and paths are right. It needs a server where
// the DSN's user may create databases, e.g.
//
//	CEPHFS_INDEX_TEST_DSN='host=localhost port=55432 user=postgres password=postgres dbname=postgres' go test -run TestBuildAgainstPostgres ./internal/pgindex/
func TestBuildAgainstPostgres(t *testing.T) {
	dsn := os.Getenv("CEPHFS_INDEX_TEST_DSN")
	if dsn == "" {
		t.Skip("CEPHFS_INDEX_TEST_DSN not set")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	const db = "cephfs_index_it"
	for _, q := range []string{
		`DROP DATABASE IF EXISTS ` + db,
		`CREATE DATABASE ` + db + ` ENCODING 'SQL_ASCII' LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0`,
	} {
		if _, err := admin.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	// A later keyword wins in a keyword/value DSN.
	pool, err := NewPool(ctx, dsn+" dbname="+db, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	// 1000 dirs under the root with 2500 files each, one of them index.php:
	// 2.5M entries, three 1M-row chunks, up to 4 leaves each.
	const dirs, files = 1000, 2500
	opts := Options{CopyWorkers: 4, ChunkRows: 1_000_000, IndexBuilders: 2, MaintenanceWorkMem: "256MB"}
	w, err := Create(ctx, pool, "t01", opts)
	if err != nil {
		t.Fatal(err)
	}
	ino := uint64(1 << 20)
	for d := range dirs {
		dino := uint64(1000 + d)
		batch := []scan.Entry{{Parent: scan.RootIno, Name: fmt.Sprintf("d%d", d), Ino: dino, Type: scan.TypeDir, UID: 1, Mtime: 1700000000}}
		for f := range files {
			ino++
			name := fmt.Sprintf("f%d.txt", f)
			if f == 0 {
				name = "index.php"
			}
			if d == 7 && f == 1 {
				name = "tab\there\\and\xffbyte"
			}
			batch = append(batch, scan.Entry{Parent: dino, Name: name, Ino: ino, Type: scan.TypeFile, UID: uint32(d), Size: uint64(f), Mtime: 1700000000 + int64(f)})
		}
		w.Emit(batch)
	}
	if err := w.Finish(ctx, map[string]string{"prefix": "/mnt/t01", "complete": "true"}, "t01"); err != nil {
		t.Fatal(err)
	}

	var leaves, allVisible int
	if err := pool.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE c.relallvisible = c.relpages AND c.relpages > 0)
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 't01' AND c.relname ~ '^entries_[0-9]+_[0-9]+$'`).Scan(&leaves, &allVisible); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d leaves, %d all-visible", leaves, allVisible)
	if leaves < 3 || allVisible != leaves {
		t.Errorf("%d leaves, %d all-visible: COPY FREEZE did not set the visibility map", leaves, allVisible)
	}
	meta := map[string]string{}
	mrows, err := pool.Query(ctx, `SELECT key, value FROM t01.meta`)
	if err != nil {
		t.Fatal(err)
	}
	for mrows.Next() {
		var k, v string
		mrows.Scan(&k, &v)
		meta[k] = v
	}
	mrows.Close()
	for k, want := range map[string]string{
		"layout": "2", "entries": fmt.Sprint(dirs * (files + 1)), "dirs": fmt.Sprint(dirs),
		"files": fmt.Sprint(dirs * files), "symlinks": "0", "hardlinks": "0", "special": "0",
	} {
		if meta[k] != want {
			t.Errorf("meta %s = %q, want %q", k, meta[k], want)
		}
	}

	// The queries cephfs-search and other readers run, with default planner
	// settings: index-only, no heap fetches. (enable_indexscan = off, which
	// a reader might set to force bitmap scans, also rules out index-only
	// scans, so it must not be used on layout 2.)
	for _, tc := range []struct{ label, sql string }{
		{"name", `SELECT parent, name, ino, type, uid, size, mtime FROM t01.entries WHERE name = 'index.php' LIMIT 501`},
		{"dirs", `SELECT ino, parent, name FROM t01.dirs WHERE ino = ANY('{1000,1001,1500,1999}'::bigint[])`},
	} {
		tx, _ := pool.Begin(ctx)
		var raw []byte
		if err := tx.QueryRow(ctx, `EXPLAIN (ANALYZE, FORMAT JSON) `+tc.sql).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		tx.Rollback(ctx)
		plan := string(raw)
		fetches := heapFetches(t, raw)
		t.Logf("%s: index-only=%v heap fetches=%d", tc.label, strings.Contains(plan, `"Index Only Scan"`), fetches)
		if !strings.Contains(plan, `"Index Only Scan"`) || strings.Contains(plan, `"Bitmap Heap Scan"`) || fetches != 0 {
			t.Errorf("%s plan is not index-only without heap fetches:\n%s", tc.label, plan)
		}
	}

	d, err := Open(ctx, pool, "t01")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		pattern string
		n       int
		path    string
	}{
		{`^index\.php$`, dirs, "/mnt/t01/d999/index.php"},
		{`^tab\there`, 1, "/mnt/t01/d7/tab\there\\and\xffbyte"},
	} {
		var got []string
		err := d.Search(ctx, index.Query{Re: regexp.MustCompile(tc.pattern), Pattern: tc.pattern, UID: -1}, func(m index.Match) error {
			got = append(got, m.Path)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, p := range got {
			found = found || p == tc.path
		}
		if len(got) != tc.n || !found {
			t.Errorf("%s: %d matches (want %d), %q found=%v", tc.pattern, len(got), tc.n, tc.path, found)
		}
	}
}

// heapFetches sums "Heap Fetches" over an EXPLAIN (ANALYZE, FORMAT JSON) plan.
func heapFetches(t *testing.T, raw []byte) int {
	var plans []map[string]any
	if err := json.Unmarshal(raw, &plans); err != nil {
		t.Fatal(err)
	}
	var sum func(n map[string]any) int
	sum = func(n map[string]any) int {
		total := 0
		if v, ok := n["Heap Fetches"].(float64); ok {
			total += int(v)
		}
		if kids, ok := n["Plans"].([]any); ok {
			for _, k := range kids {
				total += sum(k.(map[string]any))
			}
		}
		return total
	}
	return sum(plans[0]["Plan"].(map[string]any))
}

// TestNamedPathsAgainstPostgres checks that named_paths holds exactly the
// paths Search resolves level by level: entries under the root, nested
// dirs, a parent missing from dirs, non-dir entries of the same name and a
// name that is not covered. Same DSN requirements as TestBuildAgainstPostgres.
func TestNamedPathsAgainstPostgres(t *testing.T) {
	dsn := os.Getenv("CEPHFS_INDEX_TEST_DSN")
	if dsn == "" {
		t.Skip("CEPHFS_INDEX_TEST_DSN not set")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	const db = "cephfs_index_it_named"
	for _, q := range []string{
		`DROP DATABASE IF EXISTS ` + db,
		`CREATE DATABASE ` + db + ` ENCODING 'SQL_ASCII' LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0`,
	} {
		if _, err := admin.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	pool, err := NewPool(ctx, dsn+" dbname="+db, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	opts := Options{CopyWorkers: 2, ChunkRows: 1000, IndexBuilders: 1, MaintenanceWorkMem: "64MB", NamedPaths: []string{"mu-plugins", "absent"}}
	w, err := Create(ctx, pool, "t02", opts)
	if err != nil {
		t.Fatal(err)
	}
	d := func(parent, ino uint64, name string) scan.Entry {
		return scan.Entry{Parent: parent, Name: name, Ino: ino, Type: scan.TypeDir, UID: 33, Mtime: 1700000000, Ctime: 1700000100}
	}
	w.Emit([]scan.Entry{
		d(scan.RootIno, 0x100, "mu-plugins"),
		d(scan.RootIno, 0x101, "site"),
		d(0x101, 0x102, "wp-content"),
		d(0x102, 0x103, "mu-plugins"),
		d(0x103, 0x104, "mu-plugins"),
		d(0xdead, 0x105, "mu-plugins"),
		{Parent: 0x106, Name: "mu-plugins", Ino: 0x200, Type: scan.TypeFile, UID: 7, Size: 42, Mtime: 1, Ctime: 2},
		{Parent: 0x101, Name: "mu-plugins", Ino: 0x200, Type: scan.TypeHardlink},
		d(0x102, 0x106, "plugins"),
	})
	if err := w.Finish(ctx, map[string]string{"prefix": "/mnt/t02", "complete": "true"}, "t02"); err != nil {
		t.Fatal(err)
	}

	var covered []string
	if err := pool.QueryRow(ctx, `SELECT array_agg(name ORDER BY name) FROM t02.named_paths_names`).Scan(&covered); err != nil {
		t.Fatal(err)
	}
	var metaNames string
	pool.QueryRow(ctx, `SELECT value FROM t02.meta WHERE key = 'named_paths'`).Scan(&metaNames)
	if strings.Join(covered, ",") != "absent,mu-plugins" || metaNames != "mu-plugins,absent" {
		t.Errorf("covered %q, meta %q", covered, metaNames)
	}

	rows, err := pool.Query(ctx, `SELECT path, type, uid, size, mtime, ctime FROM t02.named_paths WHERE name = 'mu-plugins'`)
	if err != nil {
		t.Fatal(err)
	}
	named := map[string]string{}
	for rows.Next() {
		var p, typ string
		var uid, size, mtime, ctime int64
		if err := rows.Scan(&p, &typ, &uid, &size, &mtime, &ctime); err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(p, "/") {
			p = "/mnt/t02" + p
		}
		named[p] = fmt.Sprintf("%s %d %d %d %d", typ, uid, size, mtime, ctime)
	}
	rows.Close()

	db2, err := Open(ctx, pool, "t02")
	if err != nil {
		t.Fatal(err)
	}
	resolved := map[string]string{}
	err = db2.Search(ctx, index.Query{Re: regexp.MustCompile(`^mu-plugins$`), Pattern: `^mu-plugins$`, UID: -1}, func(m index.Match) error {
		resolved[m.Path] = fmt.Sprintf("%c %d %d %d %d", m.Type, m.UID, m.Size, m.Mtime, m.Ctime)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resolved) != 6 || fmt.Sprint(named) != fmt.Sprint(resolved) {
		t.Errorf("named_paths differs from resolved paths:\n named    %v\n resolved %v", named, resolved)
	}
	for _, p := range []string{
		"/mnt/t02/mu-plugins",                            // dir under the root
		"/mnt/t02/site/wp-content/mu-plugins",            // nested dir
		"/mnt/t02/site/wp-content/mu-plugins/mu-plugins", // dir inside a match
		"<ino 0xdead>/mu-plugins",                        // parent missing from dirs
		"/mnt/t02/site/wp-content/plugins/mu-plugins",    // file of the same name
		"/mnt/t02/site/mu-plugins",                       // hardlink
	} {
		if _, ok := named[p]; !ok {
			t.Errorf("missing %q in %v", p, named)
		}
	}
}
