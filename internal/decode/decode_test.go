package decode_test

import (
	"reflect"
	"testing"

	"github.com/xorpaul/cephfs-index/internal/cephenc"
	. "github.com/xorpaul/cephfs-index/internal/decode"
)

func TestDecodePrimaryFile(t *testing.T) {
	f := cephenc.Fixture{In: Inode{
		Ino: 0x10000abcdef, Mode: SIFREG | 0o644, UID: 3000000001, GID: 3000000001,
		Nlink: 1, Size: 12345, Ctime: Time{Sec: 1700000000, Nsec: 5}, Mtime: Time{Sec: 1700000100},
		RBytes: 12345, RFiles: 1, RCtime: Time{Sec: 1700000200},
	}, Clients: 2}
	d, err := DecodeDentry("cache_7f3a9c2e1b_head", cephenc.PrimaryValue(f, ""))
	if err != nil {
		t.Fatal(err)
	}
	if d.Name != "cache_7f3a9c2e1b" || d.Snap != NoSnap || !d.IsPrimary() {
		t.Fatalf("bad dentry header: %+v", d)
	}
	if !reflect.DeepEqual(*d.Inode, f.In) {
		t.Fatalf("inode mismatch\n got %+v\nwant %+v", *d.Inode, f.In)
	}
}

func TestDecodePrimaryDirFragmentedAndSymlink(t *testing.T) {
	root := Frag(0)
	tree := map[Frag]int32{root: 1, root.Child(1, 1): 2}
	dir := cephenc.Fixture{In: Inode{
		Ino: 0x10000000001, Mode: SIFDIR | 0o755, UID: 10, GID: 20, Nlink: 1,
		RFiles: 100, RSubdirs: 3, RCtime: Time{Sec: 1800000000}, FragTree: tree,
	}, LegacyLayout: true}
	d, err := DecodeDentry("plugins_head", cephenc.PrimaryValue(dir, "alt"))
	if err != nil {
		t.Fatal(err)
	}
	if !d.Inode.IsDir() || !reflect.DeepEqual(d.Inode.FragTree, tree) || string(d.AlternateName) != "alt" {
		t.Fatalf("bad dir dentry: %+v %+v", d, d.Inode)
	}
	leaves := map[string]bool{}
	for _, l := range Leaves(d.Inode.FragTree) {
		leaves[ObjectName(d.Inode.Ino, l)] = true
	}
	want := map[string]bool{
		"10000000001.01000000": true, // 0*/1
		"10000000001.03800000": true, // 10*/3
		"10000000001.03a00000": true,
		"10000000001.03c00000": true,
		"10000000001.03e00000": true,
	}
	if !reflect.DeepEqual(leaves, want) {
		t.Fatalf("leaves = %v, want %v", leaves, want)
	}

	ln := cephenc.Fixture{In: Inode{Ino: 0x10000000002, Mode: SIFLNK | 0o777, Symlink: "../target", Nlink: 1}}
	d, err = DecodeDentry("link_head", cephenc.PrimaryValue(ln, ""))
	if err != nil {
		t.Fatal(err)
	}
	if d.Inode.Symlink != "../target" || d.Inode.FragTree != nil {
		t.Fatalf("bad symlink: %+v", d.Inode)
	}
}

func TestDecodeRemote(t *testing.T) {
	d, err := DecodeDentry("hard_link_name_head", cephenc.RemoteValue(0x10000000042, 8))
	if err != nil {
		t.Fatal(err)
	}
	if d.Name != "hard_link_name" || !d.IsRemote() || d.RemoteIno != 0x10000000042 || d.RemoteDType != 8 {
		t.Fatalf("bad remote: %+v", d)
	}
}

func TestParseKeySnap(t *testing.T) {
	name, snap, err := ParseKey("a_b_1f")
	if err != nil || name != "a_b" || snap != 0x1f {
		t.Fatalf("got %q %x %v", name, snap, err)
	}
	if _, _, err := ParseKey("nounderscore"); err == nil {
		t.Fatal("expected error")
	}
}

func TestDecodeRootInode(t *testing.T) {
	in, err := DecodeRootInode(cephenc.RootInodeObject(nil))
	if err != nil {
		t.Fatal(err)
	}
	if in.Ino != 1 || !in.IsDir() {
		t.Fatalf("bad root: %+v", in)
	}
	if got := Leaves(in.FragTree); len(got) != 1 || ObjectName(1, got[0]) != "1.00000000" {
		t.Fatalf("root leaves %v", got)
	}
}

func TestTruncated(t *testing.T) {
	v := cephenc.PrimaryValue(cephenc.Fixture{In: Inode{Mode: SIFREG}}, "")
	for _, n := range []int{0, 5, 9, 20, len(v) / 2} {
		if _, err := DecodeDentry("x_head", v[:n]); err == nil {
			t.Fatalf("truncated at %d: expected error", n)
		}
	}
}
