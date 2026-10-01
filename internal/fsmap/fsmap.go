// Package fsmap resolves CephFS filesystem names to their metadata pools.
package fsmap

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/ceph/go-ceph/rados"
)

type FS struct {
	Name         string
	MetadataPool string
	Ranks        []int // ranks in the "in" set
}

type fsDump struct {
	Filesystems []struct {
		MDSMap struct {
			FSName       string `json:"fs_name"`
			MetadataPool int64  `json:"metadata_pool"`
			In           []int  `json:"in"`
		} `json:"mdsmap"`
	} `json:"filesystems"`
}

// List returns all filesystems from `fs dump`, sorted by name.
func List(conn *rados.Conn) ([]FS, error) {
	cmd, _ := json.Marshal(map[string]string{"prefix": "fs dump", "format": "json"})
	buf, info, err := conn.MonCommand(cmd)
	if err != nil {
		return nil, fmt.Errorf("fs dump: %w (%s)", err, info)
	}
	var d fsDump
	if err := json.Unmarshal(buf, &d); err != nil {
		return nil, fmt.Errorf("fs dump: %w", err)
	}
	var out []FS
	for _, f := range d.Filesystems {
		pool, err := conn.GetPoolByID(f.MDSMap.MetadataPool)
		if err != nil {
			return nil, fmt.Errorf("fs %s: metadata pool %d: %w", f.MDSMap.FSName, f.MDSMap.MetadataPool, err)
		}
		out = append(out, FS{Name: f.MDSMap.FSName, MetadataPool: pool, Ranks: f.MDSMap.In})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// PrimaryBucket returns the CRUSH bucket of the first "take" step of pool's
// rule, without a device-class suffix (e.g. "dc1" for "dc1~ssd"). For a
// stretch rule that takes one datacenter first, this is the DC holding the
// primaries.
func PrimaryBucket(conn *rados.Conn, pool string) (string, error) {
	var p struct {
		CrushRule string `json:"crush_rule"`
	}
	if err := monJSON(conn, map[string]string{"prefix": "osd pool get", "pool": pool, "var": "crush_rule", "format": "json"}, &p); err != nil {
		return "", err
	}
	var r struct {
		Steps []struct {
			Op       string `json:"op"`
			ItemName string `json:"item_name"`
		} `json:"steps"`
	}
	if err := monJSON(conn, map[string]string{"prefix": "osd crush rule dump", "name": p.CrushRule, "format": "json"}, &r); err != nil {
		return "", err
	}
	for _, s := range r.Steps {
		if s.Op == "take" {
			name, _, _ := strings.Cut(s.ItemName, "~")
			return name, nil
		}
	}
	return "", fmt.Errorf("crush rule %s has no take step", p.CrushRule)
}

func monJSON(conn *rados.Conn, cmd map[string]string, out any) error {
	b, _ := json.Marshal(cmd)
	buf, info, err := conn.MonCommand(b)
	if err != nil {
		return fmt.Errorf("%s: %w (%s)", cmd["prefix"], err, info)
	}
	if err := json.Unmarshal(buf, out); err != nil {
		return fmt.Errorf("%s: %w", cmd["prefix"], err)
	}
	return nil
}

func Lookup(conn *rados.Conn, name string) (FS, error) {
	all, err := List(conn)
	if err != nil {
		return FS{}, err
	}
	for _, f := range all {
		if f.Name == name {
			return f, nil
		}
	}
	return FS{}, fmt.Errorf("filesystem %q not found", name)
}
