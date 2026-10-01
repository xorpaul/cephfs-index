// cephfs-dentry-decode decodes one raw CephFS dentry value (or the root inode
// object) dumped with the rados CLI. Pure Go, no librados needed.
//
//	rados -p cephfs_metadata getomapval <dir-ino-hex>.00000000 <name>_head /tmp/v.bin
//	cephfs-dentry-decode <name>_head /tmp/v.bin
//
//	rados -p cephfs_metadata get 1.00000000.inode /tmp/root.bin
//	cephfs-dentry-decode --root /tmp/root.bin
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/xorpaul/cephfs-index/internal/decode"
)

type out struct {
	Name          string   `json:"name,omitempty"`
	Snap          string   `json:"snap,omitempty"`
	Marker        string   `json:"marker,omitempty"`
	Ino           string   `json:"ino,omitempty"`
	Type          string   `json:"type,omitempty"`
	Mode          string   `json:"mode,omitempty"`
	UID           uint32   `json:"uid"`
	GID           uint32   `json:"gid"`
	Nlink         int32    `json:"nlink,omitempty"`
	Size          uint64   `json:"size"`
	Mtime         string   `json:"mtime,omitempty"`
	Ctime         string   `json:"ctime,omitempty"`
	RCtime        string   `json:"rctime,omitempty"`
	RFiles        int64    `json:"rfiles,omitempty"`
	RSubdirs      int64    `json:"rsubdirs,omitempty"`
	RBytes        int64    `json:"rbytes,omitempty"`
	Symlink       string   `json:"symlink,omitempty"`
	FragObjects   []string `json:"frag_objects,omitempty"`
	RemoteIno     string   `json:"remote_ino,omitempty"`
	RemoteDType   uint8    `json:"remote_d_type,omitempty"`
	AlternateName string   `json:"alternate_name,omitempty"`
}

func fromInode(o *out, in *decode.Inode) {
	o.Ino = fmt.Sprintf("0x%x", in.Ino)
	o.Mode = fmt.Sprintf("%o", in.Mode)
	switch {
	case in.IsDir():
		o.Type = "dir"
	case in.IsReg():
		o.Type = "file"
	case in.IsSymlink():
		o.Type = "symlink"
	default:
		o.Type = "other"
	}
	o.UID, o.GID, o.Nlink, o.Size = in.UID, in.GID, in.Nlink, in.Size
	o.Mtime, o.Ctime = in.Mtime.String(), in.Ctime.String()
	o.Symlink = in.Symlink
	if in.IsDir() {
		o.RCtime = in.RCtime.String()
		o.RFiles, o.RSubdirs, o.RBytes = in.RFiles, in.RSubdirs, in.RBytes
		for _, f := range decode.Leaves(in.FragTree) {
			o.FragObjects = append(o.FragObjects, decode.ObjectName(in.Ino, f))
		}
	}
}

func main() {
	root := flag.Bool("root", false, "decode a root inode object (1.00000000.inode) instead of a dentry")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s KEY FILE | --root FILE\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	var o out
	switch {
	case *root && flag.NArg() == 1:
		data, err := os.ReadFile(flag.Arg(0))
		if err != nil {
			fatal(err)
		}
		in, err := decode.DecodeRootInode(data)
		if err != nil {
			fatal(err)
		}
		fromInode(&o, in)
	case !*root && flag.NArg() == 2:
		data, err := os.ReadFile(flag.Arg(1))
		if err != nil {
			fatal(err)
		}
		d, err := decode.DecodeDentry(flag.Arg(0), data)
		if err != nil {
			fatal(err)
		}
		o.Name, o.Marker = d.Name, string(d.Marker)
		if d.Snap == decode.NoSnap {
			o.Snap = "head"
		} else {
			o.Snap = fmt.Sprintf("0x%x", d.Snap)
		}
		o.AlternateName = string(d.AlternateName)
		if d.Inode != nil {
			fromInode(&o, d.Inode)
		} else {
			o.Type = "hardlink"
			o.RemoteIno = fmt.Sprintf("0x%x", d.RemoteIno)
			o.RemoteDType = d.RemoteDType
		}
	default:
		flag.Usage()
		os.Exit(2)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(o)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
