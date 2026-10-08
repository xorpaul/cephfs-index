package pgindex

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Named paths: for a few configured entry names (e.g. "mu-plugins") the full
// path of every entry with that name is precomputed into <schema>.named_paths,
// so an exact-name search reads one small index range instead of walking
// dirs up to the root for each match, which costs one random read per level
// when dirs is not cached. <schema>.named_paths_names lists the names the
// table covers, so a reader can tell "covered, no rows match the filters"
// from "not covered".
//
// path is relative to the filesystem root ("/a/b/name"; the reader prepends
// meta.prefix) or starts with "<ino 0x...>" for a chain broken by a dir
// missing from dirs, and is capped at maxPathDepth dirs, the same strings
// the readers' level-by-level path resolution builds.

const maxPathDepth = 4096

func namedPathsStmts(schema string) []string {
	s := qi(schema)
	return []string{
		// Recursion with a hash or merge join would scan all of dirs once per
		// tree level; one index lookup per ancestor is far cheaper here.
		`SET LOCAL enable_hashjoin = off`,
		`SET LOCAL enable_mergejoin = off`,
		// Built under _new names and swapped in at the end, so readers of the
		// current tables only wait for the swap, not for the whole build.
		`DROP TABLE IF EXISTS ` + s + `.named_paths_new, ` + s + `.named_paths_names_new`,
		`CREATE TABLE ` + s + `.named_paths_names_new (name TEXT PRIMARY KEY)`,
		`INSERT INTO ` + s + `.named_paths_names_new SELECT DISTINCT unnest($1::text[])`,
		`CREATE TABLE ` + s + `.named_paths_new (
	name  TEXT    NOT NULL,
	path  TEXT    NOT NULL,
	type  CHAR(1) NOT NULL,
	uid   BIGINT  NOT NULL,
	size  BIGINT  NOT NULL,
	mtime BIGINT  NOT NULL,
	ctime BIGINT  NOT NULL)`,
		// The entry's columns ride along through the recursion, so the last
		// step needs no join back to the matches (a nested loop between two
		// CTE scans, with hash joins off, would be quadratic in the matches).
		// Matches are numbered with row_number() rather than keyed by seq:
		// every other column is in the covering name index, so the start is
		// an index-only scan instead of one heap read per match.
		fmt.Sprintf(`INSERT INTO %[1]s.named_paths_new
WITH RECURSIVE up AS (
	SELECT row_number() OVER () AS id, name, type, uid, size, mtime, ctime, parent AS cur, '/' || name AS path, 0 AS depth
	FROM %[1]s.entries WHERE name = ANY($1::text[])
	UNION ALL
	SELECT up.id, up.name, up.type, up.uid, up.size, up.mtime, up.ctime, d.parent, '/' || d.name || up.path, up.depth + 1
	FROM up JOIN %[1]s.dirs d ON d.ino = up.cur
	WHERE up.cur <> 1 AND up.depth <= %[2]d
), top AS (
	SELECT DISTINCT ON (id) * FROM up ORDER BY id, depth DESC
)
SELECT name,
	CASE WHEN cur = 1 THEN path ELSE '<ino 0x' || to_hex(cur) || '>' || path END,
	type, uid, size, mtime, ctime
FROM top
ORDER BY name, 2`, s, maxPathDepth),
		`CREATE INDEX named_paths_new_name ON ` + s + `.named_paths_new (name)`,
		`ANALYZE ` + s + `.named_paths_new`,
		`DROP TABLE IF EXISTS ` + s + `.named_paths, ` + s + `.named_paths_names`,
		`ALTER TABLE ` + s + `.named_paths_new RENAME TO named_paths`,
		`ALTER TABLE ` + s + `.named_paths_names_new RENAME TO named_paths_names`,
		`ALTER INDEX ` + s + `.named_paths_new_name RENAME TO named_paths_name`,
		`ALTER INDEX ` + s + `.named_paths_names_new_pkey RENAME TO named_paths_names_pkey`,
	}
}

// BuildNamedPaths (re)creates schema.named_paths for names in one
// transaction, so concurrent searches see either the old or the new table.
// It returns the number of rows written.
func BuildNamedPaths(ctx context.Context, pool *pgxpool.Pool, schema string, names []string) (int64, error) {
	if err := validateName(schema); err != nil {
		return 0, err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	var rows int64
	for _, q := range namedPathsStmts(schema) {
		var args []any
		if strings.Contains(q, "$1") {
			args = append(args, names)
		}
		tag, err := tx.Exec(ctx, q, args...)
		if err != nil {
			return 0, fmt.Errorf("named paths for %s: %s: %w", schema, firstLine(q), err)
		}
		if strings.HasPrefix(q, "INSERT INTO "+qi(schema)+".named_paths_new\n") {
			rows = tag.RowsAffected()
		}
	}
	return rows, tx.Commit(ctx)
}

// ParseNames splits a comma-separated list of entry names, dropping blanks.
func ParseNames(s string) []string {
	var out []string
	for _, n := range strings.Split(s, ",") {
		if n = strings.TrimSpace(n); n != "" {
			out = append(out, n)
		}
	}
	return out
}

// buildNamedPaths fills the staging schema's named_paths. A failure only
// costs speed, so it is logged and the build goes on without the table.
func (w *Writer) buildNamedPaths(ctx context.Context) bool {
	if len(w.opts.NamedPaths) == 0 {
		return false
	}
	log := w.opts.ProgressLog
	if log == nil {
		log = func(string) {}
	}
	t0 := time.Now()
	n, err := BuildNamedPaths(ctx, w.pool, w.staging, w.opts.NamedPaths)
	if err != nil {
		log(fmt.Sprintf("warning: %v (searches for these names fall back to normal path resolution)", err))
		return false
	}
	log(fmt.Sprintf("named paths %s: %d rows in %s", strings.Join(w.opts.NamedPaths, ","), n, time.Since(t0).Round(time.Second)))
	return true
}
