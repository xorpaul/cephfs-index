package decode

import (
	"fmt"
	"strconv"
	"strings"
)

// NoSnap is CEPH_NOSNAP, the snapid of the live ("head") dentry.
const NoSnap = ^uint64(0) - 1

// Dentry is one decoded dirfrag omap value.
type Dentry struct {
	Name  string
	Snap  uint64 // NoSnap for "_head" keys
	First uint64
	// Marker is 'i'/'I' (primary: Inode set) or 'l'/'L' (remote/hardlink).
	Marker        byte
	Inode         *Inode
	RemoteIno     uint64
	RemoteDType   uint8
	AlternateName []byte
}

func (d *Dentry) IsPrimary() bool { return d.Marker == 'i' || d.Marker == 'I' }
func (d *Dentry) IsRemote() bool  { return d.Marker == 'l' || d.Marker == 'L' }

// ParseKey mirrors dentry_key_t::decode_helper: "<name>_head" or "<name>_<snap hex>".
func ParseKey(key string) (name string, snap uint64, err error) {
	i := strings.LastIndexByte(key, '_')
	if i < 0 {
		return "", 0, fmt.Errorf("dentry key %q has no '_' separator", key)
	}
	name, suf := key[:i], key[i+1:]
	if suf == "head" {
		return name, NoSnap, nil
	}
	snap, err = strconv.ParseUint(suf, 16, 64)
	if err != nil {
		return "", 0, fmt.Errorf("dentry key %q: bad snap suffix: %w", key, err)
	}
	return name, snap, nil
}

// DecodeDentry mirrors CDir::_load_dentry for one omap (key, value) pair.
func DecodeDentry(key string, val []byte) (*Dentry, error) {
	name, snap, err := ParseKey(key)
	if err != nil {
		return nil, err
	}
	d := &Dentry{Name: name, Snap: snap}
	r := &reader{b: val}
	if d.First, err = r.u64(); err != nil {
		return nil, err
	}
	if d.Marker, err = r.u8(); err != nil {
		return nil, err
	}
	switch d.Marker {
	case 'i':
		// ENCODE_START(2, 1): alternate_name, then InodeStore
		// (_encode_primary_inode_base: ENCODE_START(6, 4)).
		v, end, err := r.startPlain()
		if err != nil {
			return nil, err
		}
		if v >= 2 {
			if d.AlternateName, err = r.bytes(); err != nil {
				return nil, err
			}
		}
		_, iend, err := r.start(4, 4)
		if err != nil {
			return nil, err
		}
		if d.Inode, err = decodeStoreBare(r); err != nil {
			return nil, err
		}
		if err = r.finish(iend); err != nil {
			return nil, err
		}
		if err = r.finish(end); err != nil {
			return nil, err
		}
	case 'I':
		if d.Inode, err = decodeStoreBare(r); err != nil {
			return nil, err
		}
	case 'l':
		v, end, err := r.startPlain()
		if err != nil {
			return nil, err
		}
		if d.RemoteIno, err = r.u64(); err != nil {
			return nil, err
		}
		if d.RemoteDType, err = r.u8(); err != nil {
			return nil, err
		}
		if v >= 2 {
			if d.AlternateName, err = r.bytes(); err != nil {
				return nil, err
			}
		}
		if err = r.finish(end); err != nil {
			return nil, err
		}
	case 'L':
		if d.RemoteIno, err = r.u64(); err != nil {
			return nil, err
		}
		if d.RemoteDType, err = r.u8(); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("dentry %q: unknown marker %q", key, d.Marker)
	}
	return d, nil
}
