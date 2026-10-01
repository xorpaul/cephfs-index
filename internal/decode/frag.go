package decode

import "fmt"

// Frag is frag_t: high 8 bits = number of bits, low 24 bits = value.
type Frag uint32

func MakeFrag(bits, value uint32) Frag {
	return Frag(bits<<24 | (value & (0xffffff << (24 - bits)) & 0xffffff))
}

func (f Frag) Bits() uint32  { return uint32(f) >> 24 }
func (f Frag) Value() uint32 { return uint32(f) & 0xffffff }

// Child mirrors ceph_frag_make_child.
func (f Frag) Child(by, i uint32) Frag {
	nb := f.Bits() + by
	return MakeFrag(nb, f.Value()|(i<<(24-nb)))
}

// Leaves mirrors fragtree_t::get_leaves: expand every split starting at the
// root frag and return the unsplit leaves, i.e. the dirfrags that have objects.
func Leaves(tree map[Frag]int32) []Frag {
	var out []Frag
	stack := []Frag{0}
	for len(stack) > 0 {
		t := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		nb := tree[t]
		if nb <= 0 {
			out = append(out, t)
			continue
		}
		for i := uint32(0); i < 1<<uint32(nb); i++ {
			stack = append(stack, t.Child(uint32(nb), i))
		}
	}
	return out
}

// ObjectName mirrors InodeStoreBase::get_object_name: "%llx.%08llx".
func ObjectName(ino uint64, f Frag) string {
	return fmt.Sprintf("%x.%08x", ino, uint32(f))
}

// decodeFragTree mirrors fragtree_t::decode: compact_map<frag_t,int32_t>.
func decodeFragTree(r *reader) (map[Frag]int32, error) {
	n, err := r.u32()
	if err != nil {
		return nil, err
	}
	if n > 1<<20 {
		return nil, fmt.Errorf("implausible fragtree size %d", n)
	}
	var m map[Frag]int32
	if n > 0 {
		m = make(map[Frag]int32, n)
	}
	for i := uint32(0); i < n; i++ {
		f, err := r.u32()
		if err != nil {
			return nil, err
		}
		b, err := r.u32()
		if err != nil {
			return nil, err
		}
		m[Frag(f)] = int32(b)
	}
	return m, nil
}
