// Package decode parses CephFS (Reef, v18.2.x) on-disk metadata as stored in
// the metadata pool: dirfrag omap dentries and the root inode object.
// Layouts follow src/mds/CDir.cc (_load_dentry, _omap_commit_ops),
// src/mds/CInode.cc (InodeStoreBase::decode_bare) and
// src/include/cephfs/types.h (inode_t::decode) at tag v18.2.8.
package decode

import (
	"encoding/binary"
	"errors"
	"fmt"
)

var ErrShort = errors.New("decode: buffer too short")

type reader struct {
	b   []byte
	off int
}

func (r *reader) need(n int) error {
	if n < 0 || r.off+n > len(r.b) {
		return fmt.Errorf("%w: need %d at offset %d, have %d", ErrShort, n, r.off, len(r.b))
	}
	return nil
}

func (r *reader) u8() (uint8, error) {
	if err := r.need(1); err != nil {
		return 0, err
	}
	v := r.b[r.off]
	r.off++
	return v, nil
}

func (r *reader) u32() (uint32, error) {
	if err := r.need(4); err != nil {
		return 0, err
	}
	v := binary.LittleEndian.Uint32(r.b[r.off:])
	r.off += 4
	return v, nil
}

func (r *reader) u64() (uint64, error) {
	if err := r.need(8); err != nil {
		return 0, err
	}
	v := binary.LittleEndian.Uint64(r.b[r.off:])
	r.off += 8
	return v, nil
}

func (r *reader) skip(n int) error {
	if err := r.need(n); err != nil {
		return err
	}
	r.off += n
	return nil
}

func (r *reader) bytes() ([]byte, error) {
	n, err := r.u32()
	if err != nil {
		return nil, err
	}
	if err := r.need(int(n)); err != nil {
		return nil, err
	}
	v := r.b[r.off : r.off+int(n)]
	r.off += int(n)
	return v, nil
}

func (r *reader) str() (string, error) {
	b, err := r.bytes()
	return string(b), err
}

func (r *reader) utime() (Time, error) {
	s, err := r.u32()
	if err != nil {
		return Time{}, err
	}
	ns, err := r.u32()
	return Time{Sec: s, Nsec: ns}, err
}

// start mirrors __DECODE_START_LEGACY_COMPAT_LEN(v, compatv, lenv, skip_v=0).
// It returns struct_v and the absolute end offset (-1 when the struct carries
// no length, which only happens for pre-lenv encodings).
func (r *reader) start(compatv, lenv uint8) (uint8, int, error) {
	v, err := r.u8()
	if err != nil {
		return 0, 0, err
	}
	if v >= compatv {
		if _, err := r.u8(); err != nil {
			return 0, 0, err
		}
	}
	end := -1
	if v >= lenv {
		n, err := r.u32()
		if err != nil {
			return 0, 0, err
		}
		if err := r.need(int(n)); err != nil {
			return 0, 0, err
		}
		end = r.off + int(n)
	}
	return v, end, nil
}

// startPlain mirrors DECODE_START(v, bl): struct_v, struct_compat, struct_len.
func (r *reader) startPlain() (uint8, int, error) {
	return r.start(0, 0)
}

// finish mirrors DECODE_FINISH: jump to the struct end, skipping unknown
// trailing fields written by newer encoders.
func (r *reader) finish(end int) error {
	if end < 0 {
		return nil
	}
	if r.off > end {
		return fmt.Errorf("decode: overran struct end %d (at %d)", end, r.off)
	}
	r.off = end
	return nil
}

// skipStruct skips a whole versioned struct with a length field.
func (r *reader) skipStruct(compatv, lenv uint8) error {
	_, end, err := r.start(compatv, lenv)
	if err != nil {
		return err
	}
	if end < 0 {
		return errors.New("decode: cannot skip struct without length")
	}
	return r.finish(end)
}
