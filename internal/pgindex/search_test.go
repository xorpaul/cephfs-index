package pgindex

import (
	"testing"

	"github.com/xorpaul/cephfs-index/internal/scan"
)

func TestBuildPaths(t *testing.T) {
	root := int64(scan.RootIno)
	info := map[int64]dirInfo{
		10: {root, "home"},
		11: {10, "alice"},
		12: {11, "project"},
		13: {12, "content"},
		14: {13, "plugins"},
		20: {12, "other"},
		30: {99, "lost"}, // parent 99 never fetched
		40: {41, "child"},
	}
	orphan := map[int64]bool{41: true}
	// Deepest first, so the shared ancestors are resolved by the first walk
	// and reused by later ones.
	want := []int64{14, 20, 13, root, 30, 40, 41}
	got := buildPaths("/mnt/cephfs/vol1", info, orphan, want)
	exp := map[int64]string{
		14:   "/mnt/cephfs/vol1/home/alice/project/content/plugins",
		20:   "/mnt/cephfs/vol1/home/alice/project/other",
		13:   "/mnt/cephfs/vol1/home/alice/project/content",
		root: "/mnt/cephfs/vol1",
		30:   "<ino 0x63>/lost",
		40:   "<ino 0x29>/child",
		41:   "<ino 0x29>",
	}
	for ino, p := range exp {
		if got[ino] != p {
			t.Errorf("path(%d) = %q, want %q", ino, got[ino], p)
		}
	}
}
