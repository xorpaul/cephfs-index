package pgindex

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xorpaul/cephfs-index/internal/index"
	"github.com/xorpaul/cephfs-index/internal/scan"
)

// DB is an open connection to a PostgreSQL index schema.
type DB struct {
	pool   *pgxpool.Pool
	fsName string
	Meta   map[string]string
	prefix string // filesystem root path, without trailing slash

	Stats SearchStats // filled by Search
}

// SearchStats splits a search's time between the name query and path
// reconstruction.
type SearchStats struct {
	Query      time.Duration // name query incl. reading all candidate rows
	Candidates int           // rows returned by the name query
	Resolve    time.Duration // walking parents up to the root via dirs
	DirsLooked int           // dir rows fetched while resolving
	RoundTrips int           // dirs queries issued
}

func (s SearchStats) String() string {
	return fmt.Sprintf("query %s (%d candidates), paths %s (%d dirs in %d queries)",
		s.Query.Round(time.Millisecond), s.Candidates, s.Resolve.Round(time.Millisecond), s.DirsLooked, s.RoundTrips)
}

// searchParallelWorkers caps parallel workers for a full-scan search; the
// server's max_parallel_workers still applies.
const searchParallelWorkers = 8

type matchRow struct {
	parent int64
	name   string
	ino    int64
	typ    byte
	uid    int64
	size   int64
	mtime  int64
	ctime  int64
}

// Open reads the meta table of fsName schema and checks that the UNLOGGED
// entries table survived Postgres crash recovery.
func Open(ctx context.Context, pool *pgxpool.Pool, fsName string) (*DB, error) {
	if err := validateName(fsName); err != nil {
		return nil, err
	}
	rows, err := pool.Query(ctx, `SELECT key, value FROM `+qi(fsName)+`.meta`)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", fsName, err)
	}
	defer rows.Close()
	meta := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		meta[k] = v
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// The meta table is LOGGED and survives crash recovery; entries is UNLOGGED
	// and does not. Detect the mismatch so callers get a clear error instead of
	// silently returning 0 results.
	if meta["entries"] != "" && meta["entries"] != "0" {
		var hasRows bool
		if err := pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM `+qi(fsName)+`.entries LIMIT 1)`).Scan(&hasRows); err != nil {
			return nil, fmt.Errorf("crash check for %s: %w", fsName, err)
		}
		if !hasRows {
			return nil, fmt.Errorf("schema %s: entries table is empty (UNLOGGED tables are cleared by Postgres crash recovery); rebuild with cephfs-indexd build --pg-dsn", fsName)
		}
	}

	d := &DB{pool: pool, fsName: fsName, Meta: meta}
	d.prefix = strings.TrimSuffix(meta["prefix"], "/")
	return d, nil
}

// Schemas returns schema names that look like live cephfs-index schemas
// (they have a meta table and do not end in _new or _partial).
func Schemas(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT n.nspname
		FROM pg_catalog.pg_namespace n
		JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid
		WHERE c.relname = 'meta'
		  AND n.nspname NOT LIKE '%_new'
		  AND n.nspname NOT LIKE '%_partial'
		ORDER BY n.nspname`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// Search calls emit for every entry whose name matches q.Re. Results are
// collected in memory, then paths are resolved in batches to minimise
// round trips to the dirs table.
//
// Postgres is used as a pre-filter only: the btree index on name handles
// prefix patterns; all other patterns do a sequential scan. Every row is
// re-checked against q.Re client-side because Postgres uses ARE regex dialect
// which differs from Go RE2 (e.g. \b means backspace in ARE, not word boundary).
func (d *DB) Search(ctx context.Context, q index.Query, emit func(index.Match) error) error {
	var sb strings.Builder
	var args []any
	p := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}

	sb.WriteString(`SELECT parent, name, ino, type, uid, size, mtime, ctime FROM ` + qi(d.fsName) + `.entries WHERE `)

	fullScan := true
	if prefix := index.NamePrefix(q.Pattern); prefix != "" {
		fullScan = false
		sb.WriteString(`name >= ` + p(prefix))
		if hi, ok := index.UpperBound(prefix); ok {
			sb.WriteString(` AND name < ` + p(hi))
		}
		// No server-side regex: the btree range already bounds the result set,
		// and client-side re-check below filters to exact matches.
	} else if lits := index.RequiredLiterals(q.Pattern); len(lits) > 0 {
		// Sequential scan, but filtered server-side on substrings every match
		// must contain, so only candidates cross the wire. strpos compares
		// bytes and needs no regex dialect translation.
		for i, l := range lits {
			if i > 0 {
				sb.WriteString(` AND `)
			}
			sb.WriteString(`strpos(name, ` + p(l) + `) > 0`)
		}
	} else {
		// No usable literal: every row is sent and filtered client-side.
		sb.WriteString(`true`)
	}

	if q.Type != 0 {
		sb.WriteString(` AND type = ` + p(string([]byte{q.Type})))
	}
	if q.UID >= 0 {
		// Hardlinks carry no owner (stored as 0); they never match a uid filter.
		sb.WriteString(` AND uid = ` + p(q.UID) + ` AND type != ` + p(string([]byte{scan.TypeHardlink})))
	}

	// A full scan returns few candidates, but the planner estimates a third
	// of the table for strpos(...) > 0 (6.9M estimated vs 10 real on a 20M-entry volume), prices the
	// Gather of that many tuples and picks a single-process seq scan. The
	// session settings make parallel plans cheap so the scan is split.
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if fullScan {
		for _, q := range []string{
			`SET LOCAL parallel_setup_cost = 0`,
			`SET LOCAL parallel_tuple_cost = 0`,
			fmt.Sprintf(`SET LOCAL max_parallel_workers_per_gather = %d`, searchParallelWorkers),
		} {
			if _, err := tx.Exec(ctx, q); err != nil {
				return err
			}
		}
	}
	d.Stats = SearchStats{}
	t0 := time.Now()
	pgRows, err := tx.Query(ctx, sb.String(), args...)
	if err != nil {
		return err
	}

	var hits []matchRow
	candidates := 0
	for pgRows.Next() {
		candidates++
		var r matchRow
		var typ string
		if err := pgRows.Scan(&r.parent, &r.name, &r.ino, &typ, &r.uid, &r.size, &r.mtime, &r.ctime); err != nil {
			pgRows.Close()
			return err
		}
		r.typ = typ[0]
		// Client-side re-check guards against Postgres ARE vs Go RE2 differences.
		if !q.Re.MatchString(r.name) {
			continue
		}
		hits = append(hits, r)
	}
	pgRows.Close()
	if err := pgRows.Err(); err != nil {
		return err
	}

	d.Stats.Query = time.Since(t0)
	d.Stats.Candidates = candidates
	t1 := time.Now()
	dirs, err := d.resolveDirs(ctx, hits)
	d.Stats.Resolve = time.Since(t1)
	if err != nil {
		return err
	}

	for i := range hits {
		r := &hits[i]
		dir := dirs[r.parent]
		if err := emit(index.Match{
			Type:  r.typ,
			UID:   uint32(r.uid),
			Size:  r.size,
			Mtime: r.mtime,
			Ctime: r.ctime,
			Path:  dir + "/" + r.name,
		}); err != nil {
			return err
		}
	}
	return nil
}

// resolveDirs builds a map from dir ino → full path for every parent ino
// referenced in hits. It fetches ancestors level by level with
// ino = ANY($1::bigint[]) (about one query per tree level), then resolves
// all paths once, memoized, so the work is linear in the dirs fetched.
type dirInfo struct {
	parent int64
	name   string
}

func (d *DB) resolveDirs(ctx context.Context, hits []matchRow) (map[int64]string, error) {
	info := map[int64]dirInfo{}
	orphan := map[int64]bool{} // referenced but not in dirs (not seen during the walk)

	var level []int64
	queued := map[int64]bool{}
	want := func(ino int64) {
		if ino == scan.RootIno || queued[ino] {
			return
		}
		queued[ino] = true
		level = append(level, ino)
	}
	for i := range hits {
		want(hits[i].parent)
	}

	const batchSize = 10000
	for len(level) > 0 {
		cur := level
		level = nil
		for off := 0; off < len(cur); off += batchSize {
			batch := cur[off:min(off+batchSize, len(cur))]
			d.Stats.RoundTrips++
			rows, err := d.pool.Query(ctx,
				`SELECT ino, parent, name FROM `+qi(d.fsName)+`.dirs WHERE ino = ANY($1::bigint[])`,
				batch)
			if err != nil {
				return nil, err
			}
			for rows.Next() {
				var ino int64
				var di dirInfo
				if err := rows.Scan(&ino, &di.parent, &di.name); err != nil {
					rows.Close()
					return nil, err
				}
				info[ino] = di
				d.Stats.DirsLooked++
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return nil, err
			}
			for _, ino := range batch {
				if di, ok := info[ino]; ok {
					want(di.parent)
				} else {
					orphan[ino] = true
				}
			}
		}
	}

	parents := make([]int64, len(hits))
	for i := range hits {
		parents[i] = hits[i].parent
	}
	return buildPaths(d.prefix, info, orphan, parents), nil
}

// buildPaths returns the full path of every ino in want (plus the ancestors
// resolved on the way), walking info up to the root once per chain and
// memoizing. Inos missing from info or listed in orphan resolve to
// "<ino 0x...>" and their descendants hang off that placeholder.
func buildPaths(prefix string, info map[int64]dirInfo, orphan map[int64]bool, want []int64) map[int64]string {
	paths := map[int64]string{scan.RootIno: prefix}
	var chain []int64
	for _, ino := range want {
		chain = chain[:0]
		base := ""
		for x := ino; ; {
			if p, ok := paths[x]; ok {
				base = p
				break
			}
			di, ok := info[x]
			if orphan[x] || !ok || len(chain) > 4096 {
				base = fmt.Sprintf("<ino 0x%x>", x)
				paths[x] = base
				break
			}
			chain = append(chain, x)
			x = di.parent
		}
		for i := len(chain) - 1; i >= 0; i-- {
			base = base + "/" + info[chain[i]].name
			paths[chain[i]] = base
		}
	}
	return paths
}
