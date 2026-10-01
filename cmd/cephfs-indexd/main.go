// cephfs-indexd builds file indexes of CephFS volumes by reading the metadata
// pool directly with librados. It needs no mount and contacts no MDS, except
// for the opt-in --flush-journal.
package main

import (
	"fmt"
	"os"
)

func usage() {
	fmt.Fprintf(os.Stderr, `usage: cephfs-indexd <command> [flags]

commands:
  build   walk one filesystem and write <db-dir>/<fs>.db for cephfs-search
  probe   walk one filesystem read-only, report throughput/latency, optionally
          print uid<TAB>path for names matching --match (no index is written)

run "cephfs-indexd <command> -h" for flags
`)
	os.Exit(2)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "build":
		os.Exit(build(os.Args[2:]))
	case "probe":
		os.Exit(probe(os.Args[2:]))
	default:
		usage()
	}
}
