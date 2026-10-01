package index

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/xorpaul/cephfs-index/internal/scan"
)

func build(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	ctx := context.Background()
	w, err := Create(ctx, filepath.Join(dir, "vol2.db.new"))
	if err != nil {
		t.Fatal(err)
	}
	// Two batches, parent dirs before their contents, as the scanner emits.
	w.Emit([]scan.Entry{
		{Parent: 1, Name: "home", Ino: 0x100, Type: scan.TypeDir, UID: 7, RCtime: 99},
		{Parent: 1, Name: "a b.php", Ino: 0x101, Type: scan.TypeFile, UID: 5},
		{Parent: 1, Name: "hl", Ino: 0x200, Type: scan.TypeHardlink, RemoteIno: 0x200},
	})
	w.Emit([]scan.Entry{
		{Parent: 0x100, Name: "plugins", Ino: 0x103, Type: scan.TypeDir, UID: 7},
		{Parent: 0x100, Name: "index.php", Ino: 0x104, Type: scan.TypeFile, UID: 5, Size: 10, Mtime: 1700000000},
		{Parent: 0x103, Name: "index.php", Ino: 0x105, Type: scan.TypeFile, UID: 42},
		{Parent: 0x999, Name: "orphan.php", Ino: 0x106, Type: scan.TypeFile, UID: 1}, // parent dir never emitted
		{Parent: 0x100, Name: "pluginsX", Ino: 0x107, Type: scan.TypeFile, UID: 7},
	})
	dst := filepath.Join(dir, "vol2.db")
	if err := w.Finish(ctx, map[string]string{"fs": "vol2", "prefix": "/mnt/cephfs/vol2/"}, dst); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "vol2.db.new")); !os.IsNotExist(err) {
		t.Fatalf("temp file left behind: %v", err)
	}
	if w.Rows.Load() != 8 || w.Names.Load() != 7 || w.Dirs.Load() != 2 {
		t.Fatalf("rows=%d names=%d dirs=%d", w.Rows.Load(), w.Names.Load(), w.Dirs.Load())
	}
	return dst
}

func search(t *testing.T, x *Index, pattern string, typ byte, uid int64) []string {
	t.Helper()
	var got []string
	q := Query{Re: regexp.MustCompile(pattern), Pattern: pattern, Type: typ, UID: uid, Workers: 3}
	err := x.Search(context.Background(), q, func(m Match) error {
		got = append(got, string(m.Type)+" "+m.Path)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(got)
	return got
}

func TestRoundTrip(t *testing.T) {
	x, err := Open(build(t))
	if err != nil {
		t.Fatal(err)
	}
	defer x.Close()
	if x.Meta["fs"] != "vol2" || x.Meta["entries"] != "8" || x.Meta["names"] != "7" || x.Meta["dirs"] != "2" {
		t.Fatalf("meta = %v", x.Meta)
	}
	// Exact type counts: 5 files, 2 dirs, 1 hardlink = 8 entries.
	for k, want := range map[string]string{"files": "5", "symlinks": "0", "hardlinks": "1", "special": "0"} {
		if x.Meta[k] != want {
			t.Errorf("meta %s = %q, want %q", k, x.Meta[k], want)
		}
	}
	cases := []struct {
		pattern string
		typ     byte
		uid     int64
		want    []string
	}{
		{"^plugins$", scan.TypeDir, -1, []string{"d /mnt/cephfs/vol2/home/plugins"}},
		{"^plugins", 0, -1, []string{"d /mnt/cephfs/vol2/home/plugins", "f /mnt/cephfs/vol2/home/pluginsX"}},
		// An unknown parent dir is not necessarily below the root, so no prefix.
		{`\.php$`, 0, -1, []string{"f /mnt/cephfs/vol2/a b.php", "f /mnt/cephfs/vol2/home/index.php", "f /mnt/cephfs/vol2/home/plugins/index.php", "f <ino 0x999>/orphan.php"}},
		{`^index\.php$`, 0, 42, []string{"f /mnt/cephfs/vol2/home/plugins/index.php"}},
		{"^hl$", 0, -1, []string{"h /mnt/cephfs/vol2/hl"}},
		{"^hl$", 0, 0, nil}, // hardlinks have no owner and never match --uid
		{"(?i)^HOME$", 0, -1, []string{"d /mnt/cephfs/vol2/home"}},
		{"nomatch", 0, -1, nil},
	}
	for _, c := range cases {
		if got := search(t, x, c.pattern, c.typ, c.uid); !slices.Equal(got, c.want) {
			t.Errorf("%q type=%q uid=%d:\n got  %q\n want %q", c.pattern, c.typ, c.uid, got, c.want)
		}
	}
}

// A dir renamed during the walk is seen under its old and its new parent.
func TestDuplicateDir(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	w, err := Create(ctx, filepath.Join(dir, "d.db.new"))
	if err != nil {
		t.Fatal(err)
	}
	w.Emit([]scan.Entry{
		{Parent: 1, Name: "old", Ino: 0x10, Type: scan.TypeDir},
		{Parent: 1, Name: "new", Ino: 0x11, Type: scan.TypeDir},
		{Parent: 0x10, Name: "moved", Ino: 0x20, Type: scan.TypeDir},
	})
	w.Emit([]scan.Entry{
		{Parent: 0x11, Name: "moved", Ino: 0x20, Type: scan.TypeDir},
		{Parent: 0x20, Name: "f", Ino: 0x30, Type: scan.TypeFile, UID: 9},
	})
	dst := filepath.Join(dir, "d.db")
	if err := w.Finish(ctx, map[string]string{"prefix": "/"}, dst); err != nil {
		t.Fatal(err)
	}
	if w.DupDirs != 1 {
		t.Fatalf("DupDirs = %d, want 1", w.DupDirs)
	}
	x, err := Open(dst)
	if err != nil {
		t.Fatal(err)
	}
	defer x.Close()
	if x.Meta["dup_dirs"] != "1" {
		t.Fatalf("meta dup_dirs = %q", x.Meta["dup_dirs"])
	}
	if got := search(t, x, "^f$", 0, -1); !slices.Equal(got, []string{"f /new/moved/f"}) {
		t.Fatalf("got %q", got)
	}
}

func TestAbort(t *testing.T) {
	p := filepath.Join(t.TempDir(), "x.db.new")
	w, err := Create(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	w.Emit([]scan.Entry{{Parent: 1, Name: "f", Ino: 2, Type: scan.TypeFile}})
	w.Abort()
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Fatalf("file not removed: %v", err)
	}
}

func TestNamePrefix(t *testing.T) {
	for pattern, want := range map[string]string{
		"^plugins$":      "plugins",
		`^settings\.db$`: "settings.db",
		"^tmp_":          "tmp_",
		"plugins":        "", // unanchored: may match mid-name
		"(?i)^abc":       "",
		"^a|^b":          "",
		"^(abc)":         "",
		"^":              "",
	} {
		if got := NamePrefix(pattern); got != want {
			t.Errorf("NamePrefix(%q) = %q, want %q", pattern, got, want)
		}
	}
	if hi, ok := upperBound("ab\xff"); !ok || hi != "ac" {
		t.Errorf("upperBound = %q %v", hi, ok)
	}
}

// BenchmarkWrite loads 1M entries in scanner-sized batches with 30% distinct
// names, then builds the indexes. Compare the rate with the scan's entries/s.
func BenchmarkWrite(b *testing.B) {
	const n, batch = 1_000_000, 1024
	for range b.N {
		w, err := Create(context.Background(), filepath.Join(b.TempDir(), "b.db.new"))
		if err != nil {
			b.Fatal(err)
		}
		for i := 0; i < n; i += batch {
			es := make([]scan.Entry, batch)
			for j := range es {
				k := i + j
				es[j] = scan.Entry{Parent: uint64(k/50 + 2), Name: "name-" + strconv.Itoa(k%(n*3/10)), Ino: uint64(k + 2), Type: scan.TypeFile, UID: 3000000001, Size: 4096, Mtime: 1700000000}
				if k%10 == 0 {
					es[j].Type = scan.TypeDir
				}
			}
			w.Emit(es)
		}
		if err := w.Finish(context.Background(), nil, filepath.Join(b.TempDir(), "b.db")); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(n*b.N)/b.Elapsed().Seconds(), "entries/s")
}

func TestRequiredLiterals(t *testing.T) {
	for _, tc := range []struct {
		pattern string
		want    []string
	}{
		{`app-config\.php`, []string{"app-config.php"}},
		{`^plugins$`, []string{"plugins"}},
		{`settings\.db$`, []string{"settings.db"}},
		{`tmp_[0-9a-z]+\.tmp`, []string{"tmp_", ".tmp"}},
		{`foo(bar)+baz`, []string{"foo", "bar", "baz"}},
		{`x?yz.*abc`, []string{"yz", "abc"}},
		{`(?i)app-config`, nil},
		{`foo|bar`, nil},
		{`(ab)*cd`, []string{"cd"}},
		{`[ab]c`, nil},
		{`.`, nil},
		{`(`, nil},
	} {
		got := RequiredLiterals(tc.pattern)
		if strings.Join(got, "\x00") != strings.Join(tc.want, "\x00") {
			t.Errorf("RequiredLiterals(%q) = %q, want %q", tc.pattern, got, tc.want)
		}
	}
}
