// Package cephenc re-implements the MDS-side Ceph encoders (Reef v18.2.8)
// needed to build test fixtures for the decoder and scanner.
package cephenc

import (
	"encoding/binary"

	"github.com/xorpaul/cephfs-index/internal/decode"
)

// enc mirrors the Ceph encoders used by the MDS (little-endian, u32-length strings).
type Enc struct{ B []byte }

func (e *Enc) U8(v uint8)          { e.B = append(e.B, v) }
func (e *Enc) U32(v uint32)        { e.B = binary.LittleEndian.AppendUint32(e.B, v) }
func (e *Enc) U64(v uint64)        { e.B = binary.LittleEndian.AppendUint64(e.B, v) }
func (e *Enc) Str(s string)        { e.U32(uint32(len(s))); e.B = append(e.B, s...) }
func (e *Enc) Utime(t decode.Time) { e.U32(t.Sec); e.U32(t.Nsec) }

// start/finish mirror ENCODE_START(v, compat) / ENCODE_FINISH.
func (e *Enc) Start(v, compat uint8) int {
	e.U8(v)
	e.U8(compat)
	e.U32(0)
	return len(e.B)
}
func (e *Enc) Finish(pos int) {
	binary.LittleEndian.PutUint32(e.B[pos-4:], uint32(len(e.B)-pos))
}

type Fixture struct {
	In           decode.Inode
	Clients      int
	LegacyLayout bool
}

// inodeT mirrors inode_t::encode (ENCODE_START(19, 6)) including every
// trailing field, so the decoder's struct-length skip is exercised.
func (e *Enc) InodeT(f Fixture) {
	in := f.In
	p := e.Start(19, 6)
	e.U64(in.Ino)
	e.U32(0) // rdev
	e.Utime(in.Ctime)
	e.U32(in.Mode)
	e.U32(in.UID)
	e.U32(in.GID)
	e.U32(uint32(in.Nlink))
	e.U8(0)                               // anchored
	e.B = append(e.B, make([]byte, 8)...) // dir_layout
	if f.LegacyLayout {
		e.U32(4 << 20) // stripe_unit: low byte 0 marks the legacy raw layout
		e.B = append(e.B, make([]byte, 24)...)
	} else {
		lp := e.Start(2, 2)
		e.U32(4 << 20)
		e.U32(1)
		e.U32(4 << 20)
		e.U64(^uint64(0)) // pool_id -1
		e.Str("")
		e.Finish(lp)
	}
	e.U64(in.Size)
	e.U32(1)          // truncate_seq
	e.U64(^uint64(0)) // truncate_size
	e.U64(0)          // truncate_from
	e.U32(0)          // truncate_pending
	e.Utime(in.Mtime)
	e.Utime(decode.Time{Sec: 1})
	e.U32(0) // time_warp_seq
	e.U32(uint32(f.Clients))
	for i := 0; i < f.Clients; i++ {
		e.U64(uint64(4000 + i))
		cp := e.Start(2, 2)
		e.U64(0)
		e.U64(1 << 22)
		e.U64(1)
		e.Finish(cp)
	}
	dp := e.Start(3, 2) // dirstat
	e.U64(1)
	e.Utime(decode.Time{})
	e.U64(3)
	e.U64(4)
	e.U64(0)
	e.Finish(dp)
	for _, acc := range []bool{false, true} { // rstat, accounted_rstat
		np := e.Start(3, 2)
		e.U64(2)
		e.U64(uint64(in.RBytes))
		e.U64(uint64(in.RFiles))
		e.U64(uint64(in.RSubdirs))
		e.U64(0) // ranchors
		e.U64(0) // rsnaps
		if acc {
			e.Utime(decode.Time{})
		} else {
			e.Utime(in.RCtime)
		}
		e.Finish(np)
	}
	e.U64(7) // version
	e.U64(0) // file_data_version
	e.U64(0) // xattr_version
	e.U64(0) // backtrace_version
	e.U32(0) // old_pools
	e.U64(0) // max_size_ever
	e.U64(0) // inline_data.version
	e.U32(0) // inline_data len
	qp := e.Start(1, 1)
	e.U64(0)
	e.U64(0)
	e.Finish(qp)
	e.Str("") // stray_prior_path
	e.U64(0)  // last_scrub_version
	e.Utime(decode.Time{})
	e.Utime(decode.Time{})    // btime
	e.U64(0)                  // change_attr
	e.U32(uint32(0xffffffff)) // export_pin
	e.U64(0)                  // export_ephemeral_random_pin (double)
	e.U8(0)                   // export_ephemeral_distributed_pin
	e.U8(0)                   // fscrypt flag
	e.U32(0)                  // fscrypt_auth
	e.U32(0)                  // fscrypt_file
	e.U32(0)                  // fscrypt_last_block
	e.Finish(p)
}

// storeBare mirrors CDir::_encode_primary_inode_base's body after ENCODE_START(6, 4).
func (e *Enc) StoreBare(f Fixture) {
	e.InodeT(f)
	if f.In.Mode&decode.SIFMT == decode.SIFLNK {
		e.Str(f.In.Symlink)
	}
	e.U32(uint32(len(f.In.FragTree)))
	for fr, b := range f.In.FragTree {
		e.U32(uint32(fr))
		e.U32(uint32(b))
	}
	e.U32(1) // xattrs map with one entry
	e.Str("user.x")
	e.Str("y")
	e.U32(0) // snap blob (empty bufferlist)
	e.U32(0) // old_inodes
	e.U64(2) // oldest_snap
	e.U32(0) // damage_flags
}

func PrimaryValue(f Fixture, alt string) []byte {
	e := &Enc{}
	e.U64(2) // first snapid
	e.U8('i')
	outer := e.Start(2, 1)
	e.Str(alt)
	inner := e.Start(6, 4)
	e.StoreBare(f)
	e.Finish(inner)
	e.Finish(outer)
	return e.B
}

// RemoteValue mirrors CDentry::encode_remote for a hardlink dentry ('l').
func RemoteValue(ino uint64, dtype uint8) []byte {
	e := &Enc{}
	e.U64(2)
	e.U8('l')
	p := e.Start(2, 1)
	e.U64(ino)
	e.U8(dtype)
	e.Str("")
	e.Finish(p)
	return e.B
}

// RootInodeObject mirrors the data of "1.00000000.inode": magic string, then
// InodeStoreBase::encode (ENCODE_START(6, 4)).
func RootInodeObject(tree map[decode.Frag]int32) []byte {
	e := &Enc{}
	e.Str(decode.OnDiskMagic)
	p := e.Start(6, 4)
	e.StoreBare(Fixture{In: decode.Inode{Ino: 1, Mode: decode.SIFDIR | 0o755, Nlink: 1, FragTree: tree}})
	e.Finish(p)
	return e.B
}
