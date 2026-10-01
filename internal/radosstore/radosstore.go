// Package radosstore implements scan.Store on top of librados.
package radosstore

import (
	"errors"

	"github.com/ceph/go-ceph/rados"

	"github.com/xorpaul/cephfs-index/internal/scan"
)

type Store struct {
	ioctx *rados.IOContext
	flags rados.OperationFlags
}

// New wraps ioctx. With localize, reads are served by the nearest replica
// (per the client's crush_location); the Objecter falls back to the primary
// when a replica cannot serve the read (-EAGAIN).
func New(ioctx *rados.IOContext, localize bool) *Store {
	f := rados.OperationNoFlag
	if localize {
		f = rados.OperationLocalizeReads
	}
	return &Store{ioctx: ioctx, flags: f}
}

func (s *Store) OmapPage(oid, after string, max uint64) ([]scan.KV, bool, error) {
	op := rados.CreateReadOp()
	defer op.Release()
	step := op.GetOmapValues(after, "", max)
	if err := op.Operate(s.ioctx, oid, s.flags); err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			return nil, false, scan.ErrNotFound
		}
		return nil, false, err
	}
	var out []scan.KV
	for {
		kv, err := step.Next()
		if err != nil {
			return nil, false, err
		}
		if kv == nil {
			break
		}
		out = append(out, scan.KV{Key: kv.Key, Value: kv.Value})
	}
	return out, step.More(), nil
}

func (s *Store) ReadAll(oid string) ([]byte, error) {
	st, err := s.ioctx.Stat(oid)
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			return nil, scan.ErrNotFound
		}
		return nil, err
	}
	buf := make([]byte, st.Size)
	var off uint64
	for off < st.Size {
		n, err := s.ioctx.Read(oid, buf[off:], off)
		if err != nil {
			return nil, err
		}
		if n == 0 {
			break
		}
		off += uint64(n)
	}
	return buf[:off], nil
}
