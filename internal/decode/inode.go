package decode

import (
	"fmt"
	"time"
)

const (
	SIFMT  = 0o170000
	SIFDIR = 0o040000
	SIFREG = 0o100000
	SIFLNK = 0o120000
)

type Time struct {
	Sec, Nsec uint32
}

func (t Time) Unix() int64     { return int64(t.Sec) }
func (t Time) Time() time.Time { return time.Unix(int64(t.Sec), int64(t.Nsec)).UTC() }
func (t Time) IsZero() bool    { return t.Sec == 0 && t.Nsec == 0 }
func (t Time) String() string  { return t.Time().Format(time.RFC3339Nano) }

// Inode holds the subset of inode_t (plus InodeStore extras) the indexer needs.
type Inode struct {
	Ino      uint64
	Mode     uint32
	UID      uint32
	GID      uint32
	Nlink    int32
	Size     uint64
	Ctime    Time
	Mtime    Time
	RBytes   int64
	RFiles   int64
	RSubdirs int64
	RCtime   Time
	Symlink  string
	// FragTree maps split frag -> number of bits it is split by (fragtree_t._splits).
	FragTree map[Frag]int32
}

func (i *Inode) IsDir() bool     { return i.Mode&SIFMT == SIFDIR }
func (i *Inode) IsSymlink() bool { return i.Mode&SIFMT == SIFLNK }
func (i *Inode) IsReg() bool     { return i.Mode&SIFMT == SIFREG }

// decodeInodeT mirrors inode_t::decode, DECODE_START_LEGACY_COMPAT_LEN(19, 6, 6).
// Fields after rstat are skipped via the struct length.
func decodeInodeT(r *reader, in *Inode) error {
	v, end, err := r.start(6, 6)
	if err != nil {
		return fmt.Errorf("inode_t header: %w", err)
	}
	if end < 0 {
		return fmt.Errorf("inode_t struct_v %d predates length encoding; unsupported", v)
	}
	if in.Ino, err = r.u64(); err != nil {
		return err
	}
	if _, err = r.u32(); err != nil { // rdev
		return err
	}
	if in.Ctime, err = r.utime(); err != nil {
		return err
	}
	if in.Mode, err = r.u32(); err != nil {
		return err
	}
	if in.UID, err = r.u32(); err != nil {
		return err
	}
	if in.GID, err = r.u32(); err != nil {
		return err
	}
	nl, err := r.u32()
	if err != nil {
		return err
	}
	in.Nlink = int32(nl)
	if err = r.skip(1); err != nil { // anchored (removed field)
		return err
	}
	if v >= 4 {
		if err = r.skip(8); err != nil { // ceph_dir_layout, raw 8 bytes
			return err
		}
	}
	if err = skipFileLayout(r); err != nil {
		return fmt.Errorf("file_layout_t: %w", err)
	}
	if in.Size, err = r.u64(); err != nil {
		return err
	}
	// truncate_seq u32, truncate_size u64, truncate_from u64
	if err = r.skip(4 + 8 + 8); err != nil {
		return err
	}
	if v >= 5 {
		if err = r.skip(4); err != nil { // truncate_pending
			return err
		}
	}
	if in.Mtime, err = r.utime(); err != nil {
		return err
	}
	// atime utime_t, time_warp_seq u32
	if err = r.skip(8 + 4); err != nil {
		return err
	}
	if v < 3 {
		return fmt.Errorf("inode_t struct_v %d: legacy client_ranges unsupported", v)
	}
	n, err := r.u32() // map<client_t, client_writeable_range_t>
	if err != nil {
		return err
	}
	for i := uint32(0); i < n; i++ {
		if err = r.skip(8); err != nil { // client_t (int64)
			return err
		}
		if err = r.skipStruct(2, 2); err != nil {
			return fmt.Errorf("client_writeable_range_t: %w", err)
		}
	}
	if err = r.skipStruct(2, 2); err != nil { // dirstat frag_info_t
		return fmt.Errorf("frag_info_t: %w", err)
	}
	if err = decodeNestInfo(r, in); err != nil {
		return fmt.Errorf("nest_info_t: %w", err)
	}
	return r.finish(end)
}

// skipFileLayout mirrors file_layout_t::decode: a leading zero byte means the
// legacy raw ceph_file_layout (7 x __le32), otherwise DECODE_START(2).
func skipFileLayout(r *reader) error {
	if err := r.need(1); err != nil {
		return err
	}
	if r.b[r.off] == 0 {
		return r.skip(28)
	}
	_, end, err := r.startPlain()
	if err != nil {
		return err
	}
	return r.finish(end)
}

// decodeNestInfo mirrors nest_info_t::decode, DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2).
func decodeNestInfo(r *reader, in *Inode) error {
	_, end, err := r.start(2, 2)
	if err != nil {
		return err
	}
	if _, err = r.u64(); err != nil { // version
		return err
	}
	var x uint64
	if x, err = r.u64(); err != nil {
		return err
	}
	in.RBytes = int64(x)
	if x, err = r.u64(); err != nil {
		return err
	}
	in.RFiles = int64(x)
	if x, err = r.u64(); err != nil {
		return err
	}
	in.RSubdirs = int64(x)
	if err = r.skip(8 + 8); err != nil { // ranchors (removed), rsnaps
		return err
	}
	if in.RCtime, err = r.utime(); err != nil {
		return err
	}
	return r.finish(end)
}

// decodeStoreBare mirrors the start of InodeStoreBase::decode_bare: inode_t,
// symlink target (symlinks only), dirfragtree. xattrs, snap blob, old inodes
// and the trailing fields are not needed and are left unread.
func decodeStoreBare(r *reader) (*Inode, error) {
	in := &Inode{}
	if err := decodeInodeT(r, in); err != nil {
		return nil, err
	}
	if in.IsSymlink() {
		s, err := r.str()
		if err != nil {
			return nil, fmt.Errorf("symlink: %w", err)
		}
		in.Symlink = s
	}
	ft, err := decodeFragTree(r)
	if err != nil {
		return nil, fmt.Errorf("dirfragtree: %w", err)
	}
	in.FragTree = ft
	return in, nil
}

// DecodeRootInode parses the data of the "<ino>.00000000.inode" object used
// for the root (ino 1): magic string, then InodeStoreBase::decode
// (DECODE_START_LEGACY_COMPAT_LEN(5, 4, 4)).
func DecodeRootInode(data []byte) (*Inode, error) {
	r := &reader{b: data}
	magic, err := r.str()
	if err != nil {
		return nil, err
	}
	if magic != OnDiskMagic {
		return nil, fmt.Errorf("unexpected magic %q", magic)
	}
	_, end, err := r.start(4, 4)
	if err != nil {
		return nil, err
	}
	in, err := decodeStoreBare(r)
	if err != nil {
		return nil, err
	}
	return in, r.finish(end)
}

const OnDiskMagic = "ceph fs volume v011"
