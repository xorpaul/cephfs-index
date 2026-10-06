package index

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"regexp/syntax"
	"strconv"
	"strings"
	"sync"

	"github.com/xorpaul/cephfs-index/internal/scan"
)

// Index is an open, read-only index file.
type Index struct {
	db   *sql.DB
	Meta map[string]string
	root string // path of the root dir, without trailing slash
}

func Open(path string) (*Index, error) {
	// immutable: the file is only ever replaced by rename, never modified,
	// so SQLite can skip locking entirely.
	db, err := sql.Open("sqlite3", "file:"+(&url.URL{Path: path}).EscapedPath()+"?mode=ro&immutable=1")
	if err != nil {
		return nil, err
	}
	x := &Index{db: db, Meta: map[string]string{}}
	rows, err := db.Query("SELECT key, value FROM meta")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	defer rows.Close()
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			db.Close()
			return nil, err
		}
		x.Meta[k] = v
	}
	if err := rows.Err(); err != nil {
		db.Close()
		return nil, err
	}
	x.root = strings.TrimSuffix(x.Meta["prefix"], "/")
	return x, nil
}

func (x *Index) Close() error { return x.db.Close() }

type Query struct {
	Re      *regexp.Regexp
	Pattern string // source of Re, used to derive a literal prefix
	Type    byte   // scan.Type*, 0 = any
	UID     int64  // -1 = any
	Workers int    // parallel name scans when there is no literal prefix
}

type Match struct {
	Type  byte
	UID   uint32
	Size  int64
	Mtime int64 // Unix seconds
	Ctime int64 // Unix seconds
	Path  string
}

// Search calls emit for every entry whose name matches. Matching names are
// found first (a B-tree range for patterns with a literal prefix, otherwise a
// parallel scan of the distinct names), then their entries are looked up.
func (x *Index) Search(ctx context.Context, q Query, emit func(Match) error) error {
	ids, err := x.matchNames(ctx, q)
	if err != nil {
		return err
	}
	paths := &pathCache{db: x.db, root: x.root, memo: map[int64]string{}}
	defer paths.close()

	idList := make([]int64, 0, len(ids))
	for id := range ids {
		idList = append(idList, id)
	}
	const chunk = 500
	for len(idList) > 0 {
		n := min(chunk, len(idList))
		batch := idList[:n]
		idList = idList[n:]

		var sb strings.Builder
		sb.WriteString("SELECT name_id, parent, type, uid, size, mtime, ctime FROM entries WHERE name_id IN (")
		args := make([]any, 0, n+2)
		for i, id := range batch {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteByte('?')
			args = append(args, id)
		}
		sb.WriteByte(')')
		if q.Type != 0 {
			sb.WriteString(" AND type = ?")
			args = append(args, int64(q.Type))
		}
		if q.UID >= 0 {
			// Hardlinks carry no owner (stored as 0), so they never match a uid.
			sb.WriteString(" AND uid = ? AND type != ?")
			args = append(args, q.UID, int64(scan.TypeHardlink))
		}
		type hit struct {
			nameID, parent, typ, uid, size, mtime, ctime int64
		}
		var hits []hit
		rows, err := x.db.QueryContext(ctx, sb.String(), args...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var h hit
			if err := rows.Scan(&h.nameID, &h.parent, &h.typ, &h.uid, &h.size, &h.mtime, &h.ctime); err != nil {
				rows.Close()
				return err
			}
			hits = append(hits, h)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, h := range hits {
			dir, err := paths.dir(ctx, h.parent, 0)
			if err != nil {
				return err
			}
			if err := emit(Match{
				Type: byte(h.typ), UID: uint32(h.uid),
				Size: h.size, Mtime: h.mtime, Ctime: h.ctime,
				Path: dir + "/" + ids[h.nameID],
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (x *Index) matchNames(ctx context.Context, q Query) (map[int64]string, error) {
	out := map[int64]string{}
	if p := NamePrefix(q.Pattern); p != "" {
		query, args := "SELECT id, name FROM names WHERE name >= ?", []any{p}
		if hi, ok := upperBound(p); ok {
			query += " AND name < ?"
			args = append(args, hi)
		}
		err := scanNames(ctx, x.db, q.Re, query, args, func(id int64, name string) { out[id] = name })
		return out, err
	}

	var maxID int64
	if err := x.db.QueryRowContext(ctx, "SELECT coalesce(max(id), 0) FROM names").Scan(&maxID); err != nil {
		return nil, err
	}
	workers := max(1, q.Workers)
	step := maxID/int64(workers) + 1
	parts := make([]map[int64]string, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		lo := int64(i)*step + 1
		parts[i] = map[int64]string{}
		wg.Go(func() {
			errs[i] = scanNames(ctx, x.db, q.Re, "SELECT id, name FROM names WHERE id >= ? AND id < ?", []any{lo, lo + step},
				func(id int64, name string) { parts[i][id] = name })
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	for _, p := range parts {
		for id, name := range p {
			out[id] = name
		}
	}
	return out, nil
}

func scanNames(ctx context.Context, db *sql.DB, re *regexp.Regexp, query string, args []any, add func(int64, string)) error {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	var id int64
	var name sql.RawBytes
	for rows.Next() {
		if err := rows.Scan(&id, &name); err != nil {
			return err
		}
		if re.Match(name) {
			add(id, string(name))
		}
	}
	return rows.Err()
}

// NamePrefix returns the literal every matching name must start with, or "".
// Only patterns anchored with ^ qualify: regexp.LiteralPrefix also reports a
// prefix for unanchored patterns, whose matches may start mid-name.
func NamePrefix(pattern string) string {
	re, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return ""
	}
	re = re.Simplify()
	if re.Op != syntax.OpConcat || len(re.Sub) < 2 {
		return ""
	}
	if op := re.Sub[0].Op; op != syntax.OpBeginText && op != syntax.OpBeginLine {
		return ""
	}
	lit := re.Sub[1]
	if lit.Op != syntax.OpLiteral || lit.Flags&syntax.FoldCase != 0 {
		return ""
	}
	return string(lit.Rune)
}

// RequiredLiterals returns literal substrings (at least minLit bytes) that
// every match of pattern must contain. A backend can pre-filter on them with
// a plain substring test and then re-check the full regex: the filter is
// always a superset, whatever the backend's own regex dialect. Case-folded
// literals and alternations contribute nothing.
func RequiredLiterals(pattern string) []string {
	const minLit = 2
	re, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return nil
	}
	var out []string
	var walk func(*syntax.Regexp)
	walk = func(r *syntax.Regexp) {
		switch r.Op {
		case syntax.OpLiteral:
			if r.Flags&syntax.FoldCase == 0 && len(string(r.Rune)) >= minLit {
				out = append(out, string(r.Rune))
			}
		case syntax.OpConcat:
			for _, sub := range r.Sub {
				walk(sub)
			}
		case syntax.OpCapture, syntax.OpPlus:
			walk(r.Sub[0])
		case syntax.OpRepeat:
			if r.Min >= 1 {
				walk(r.Sub[0])
			}
		}
	}
	walk(re.Simplify())
	return out
}

// UpperBound returns the smallest string greater than every string with
// prefix p (comparisons are bytewise).
func UpperBound(p string) (string, bool) { return upperBound(p) }

// upperBound is the internal alias.
func upperBound(p string) (string, bool) {
	b := []byte(p)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			b[i]++
			return string(b[:i+1]), true
		}
	}
	return "", false
}

type pathCache struct {
	db   *sql.DB
	stmt *sql.Stmt
	root string
	memo map[int64]string
}

func (p *pathCache) close() {
	if p.stmt != nil {
		p.stmt.Close()
	}
}

func (p *pathCache) dir(ctx context.Context, ino int64, depth int) (string, error) {
	if ino == scan.RootIno {
		return p.root, nil
	}
	if s, ok := p.memo[ino]; ok {
		return s, nil
	}
	if p.stmt == nil {
		var err error
		p.stmt, err = p.db.PrepareContext(ctx, "SELECT d.parent, n.name FROM dirs d JOIN names n ON n.id = d.name_id WHERE d.ino = ?")
		if err != nil {
			return "", err
		}
	}
	var parent int64
	var name string
	err := p.stmt.QueryRowContext(ctx, ino).Scan(&parent, &name)
	var s string
	switch {
	case errors.Is(err, sql.ErrNoRows), depth > 4096:
		s = "<ino 0x" + strconv.FormatUint(uint64(ino), 16) + ">"
	case err != nil:
		return "", err
	default:
		pp, err := p.dir(ctx, parent, depth+1)
		if err != nil {
			return "", err
		}
		s = pp + "/" + name
	}
	p.memo[ino] = s
	return s, nil
}
