# cephfs-index

A file index for CephFS volumes that is built by reading the metadata pool directly with librados. It does not mount the filesystem.

**The MDS is not contacted.** There is no kernel mount and no MDS session. The one exception is the opt-in `--flush-journal`, which runs `ceph tell mds.<fs>:<rank> flush journal` once per rank before the walk. Walking a large volume through a mount pulls every cold inode into the MDS cache, pushes out the hot working set, and triggers cap recall (`MDS_CLIENT_RECALL` / `MDS_CLIENT_LATE_RELEASE`). This tool reads the same metadata straight from the metadata pool's OSDs instead. On a billion-entry volume a full walk takes about 30 minutes at ~600k entries/s.

Three binaries:

- **`cephfs-indexd`**: `build` walks a volume and writes an index (SQLite or PostgreSQL). `probe` walks without writing an index, to measure throughput or print matching names.
- **`cephfs-search`**: regex search over entry names in either backend. It prints uid, size, mtime and path.
- **`cephfs-dentry-decode`**: decodes one raw dentry omap value or the root inode, dumped with the `rados` CLI. Pure Go, static.

## How it works

- Each directory fragment is a RADOS object `<dir-ino-hex>.<frag-hex>` in the metadata pool.
- Its omap keys are dentry names, `<name>_head` for the live version. Snapshot dentries `<name>_<snapid>` are skipped.
- A primary dentry value embeds the full inode: mode, uid, gid, size, mtime, rstat and the dirfragtree.
- The walk starts at the root (`1.00000000.inode` holds the root fragtree). For each child directory it derives the child's dirfrag object names from the fragtree embedded in the dentry.
- Stray dirs, the journal and other MDS system objects are not reachable from the root, so they are skipped, and so are deleted but not yet purged files.
- Hardlinks (remote dentries) carry only the target inode number. They are reported as type `hardlink` with uid `-`.
- Dentries that are dirty in the MDS journal but not yet flushed to RADOS are not visible. On busy volumes that is a handful of new dirs. On quiet volumes it can be days of changes, because quiet ranks trim their journal rarely (one test volume had 15% of its entries only in the journal). `--flush-journal` closes the gap at the cost of one `ceph tell` per rank (see the caveat below).
- The on-disk root inode's own rstat is stale (from creation time). Progress and ETA use the sum of the root's children's rstat instead.

The decoder follows Ceph **v18.2** (`CDir::_load_dentry`, `InodeStoreBase::decode_bare`, `inode_t::decode`, `fragtree_t`). Versioned structs are skipped by their encoded length, so fields added by newer releases don't break decoding as long as the prefix layout stays the same. Re-validate the decoder (see [Validating the decoder](#validating-the-decoder)) after every Ceph major upgrade.

### Counters

The progress line on stderr shows objects/s, entries/s, the current in-flight window (`limit`), p50/p99 omap latency, `pct … eta …`, and these counters:

- `decode_err` must be 0. The first 20 failures are logged with oid and key.
- `enoent` counts missing dirfrags of directories whose rstat says they are empty (empty dirs that were never committed). These are expected.
- `missed_dirs` counts missing dirfrags of directories whose rstat says they have content (`rfiles + rsubdirs > 1`; `rsubdirs` counts the dir itself). Each one is a subtree the walk could not see, in practice because its contents are still only in the MDS journal. It should be at or near 0 after `--flush-journal`. Rstat lags a little, so an occasional false positive is possible, typically a run of sibling dirs with consecutive inode numbers that were created during the walk.
- `pct=… of N` appears once the start dir's objects have been read. N is the sum of `rfiles + rsubdirs` (dirs) or 1 (other entries) over the start dir's children, which equals `ceph.dir.rentries` minus the start dir itself. Rstat propagates lazily, so the final percentage can end slightly off 100%.

## OSD load controls

`ionice` and `nice` don't affect network ops, so the walk is throttled explicitly:

| Flag | Default | Effect |
|---|---|---|
| `--max-inflight` | 32 | Concurrent omap reads (upper bound of the adaptive window) |
| `--latency-ceiling` | 50ms | The window halves when client-side p99 omap latency over 2s exceeds this, then recovers by max/16 per 2s |
| `--max-ops` | 0 (off) | Token bucket, omap reads/s |
| `--page-size` | 1024 | Omap entries per read |
| `--localize-reads` | false | Serve reads from the nearest replica. An unstable replica returns EAGAIN and the Objecter retries on the primary. It costs measurable client CPU (~1.7 cores at 16 in-flight) and 10–15% throughput when the primaries are already local, so it is off by default. On a stretch cluster, run the indexer on a host in the DC that holds the metadata pool's primaries instead. The first line prints `primary_dc` (the first `take` step of the pool's CRUSH rule), and a warning is logged when it differs from the client's datacenter while localize-reads is off. |
| `--crush-location` | auto | The client's `crush_location` for localize-reads, e.g. `datacenter=dc1`. Precedence: the flag, then ceph.conf. The value in use is printed on the first line. |
| `--flush-journal` | false | Before the walk, run `ceph tell mds.<fs>:<rank> flush journal` for every rank in the `in` set (5 min timeout each). It only runs after the index has been created and, with `--pg-dsn`, the build lock has been taken, so a run that fails during setup does not flush. The first line and the index meta record `journal_flushed`. **The flush trims the journal at once and can make a healthy standby-replay daemon respawn** (`respawning since we fell behind journal`) and come back with a cold cache, even when it was only a few KB behind. Use it on quiet volumes, where unflushed metadata can sit in the journal for days. On busy volumes the gain is small. |
| `--gogc` | `$GOGC`, else 400 | Go GC target percentage; `-1` turns GC off. The heap can grow to (1 + GOGC/100) × live data, so it is soft-capped by `GOMEMLIMIT`, which defaults to a quarter of RAM when unset. Both values are printed on the first line. GOGC=400 used about 3× less CPU per entry than GOGC=100 in A/B runs. |
| `--duration` | 0 (off) | Stop the walk early and keep the result as `<fs>_partial` (PostgreSQL) or `<fs>.db.partial` (SQLite). |
| `--prefix` | `/<fs>` | Path of the filesystem root in search output, e.g. where the volume is mounted on clients. |

## Index backends

The index holds every path and uid on the volume. Treat it like the filesystem metadata itself: keep the SQLite directory root-only (the build refuses a `--db-dir` that isn't mode 0700) and restrict access to the PostgreSQL database.

### PostgreSQL (`--pg-dsn`)

One schema per volume. The build writes to `<fs>_new` and renames it to `<fs>` at the end, so searches keep seeing the previous complete index until then. An advisory lock prevents concurrent builds of the same volume. Create the database with `ENCODING 'SQL_ASCII'` so any byte sequence is accepted as a filename. Don't put a password in the DSN: use `~/.pgpass` (`$CEPHFS_INDEX_PG_DSN` is read as the default for `--pg-dsn`).

Schema layout (`meta.layout = 2`):

| Table | Purpose |
|---|---|
| `<fs>.entries` | Partitioned by `part`, no storage of its own |
| `<fs>.entries_NNNNN_S` | UNLOGGED leaf tables, one per chunk of `--pg-chunk-rows` rows (default 50M) and COPY stream `S` |
| `<fs>.dirs` | UNLOGGED, `(ino, parent, name)`, upserted per leaf, used to rebuild paths |
| `<fs>.meta` | LOGGED key/value metadata |

Each of the `--pg-copy-workers` COPY streams writes its share of the current chunk into its own leaf table. It creates the table and fills it with `COPY … WITH (FORMAT binary, FREEZE)` in one transaction, which FREEZE requires. The rows are written frozen and their pages are marked all-visible as they load, so no VACUUM pass has to read and rewrite them. The partition key is a `part SMALLINT` per leaf, not the sequence number, because a chunk's COPY streams interleave sequence numbers. Each leaf's `CHECK (part >= P AND part < P+1)` makes the final ATTACH metadata-only.

When a leaf's COPY commits, a background builder (`--pg-index-builders`, default 2, each with `--pg-chunk-workers` parallel workers) indexes it while the scan continues:

1. A covering index on `name` that also stores `parent, ino, type, uid, size, mtime`.
2. An upsert of the leaf's dir rows into `dirs`. The primary key is `ino`, and the highest sequence number wins, so the last sighting of a dir that was renamed mid-walk is kept even when leaves finish out of order.
3. `VACUUM (ANALYZE)`. It skips every page, since they are all frozen, and only reads the visibility map. It is there because `ANALYZE` alone leaves `pg_class.relallvisible` at 0, which makes the planner price index-only scans as heap fetches.

Post-load adds a covering `dirs (ino) INCLUDE (parent, name)` index and runs `VACUUM (FREEZE, ANALYZE)` on `dirs`, the one table that was upserted rather than COPY-frozen. Name searches and the parent lookups that build paths are then index-only scans with `Heap Fetches: 0`, which matters on HDD storage where each heap fetch is a random read. The covering indexes cost roughly 40 bytes more per entry. The progress line shows `tables=<indexed>/<created>` and `queue=N/256`. A queue that stays near 256 means the COPY writer, not the scan, is the bottleneck.

UNLOGGED tables are emptied by PostgreSQL crash recovery. `cephfs-search` detects an empty `entries` and asks for a rebuild.

### SQLite (`--db-dir`)

One file per volume, `<db-dir>/<fs>.db` (default `/var/lib/cephfs-index`):

| Table | Rows | Purpose |
|---|---|---|
| `names(id, name)` | one per distinct name | Unique index on `name`. A regex search scans only this table. |
| `entries(name_id, parent, ino, type, uid, gid, size, mtime)` | one per dentry | Index on `name_id` |
| `dirs(ino, parent, name_id, rctime)` | one per directory | Rebuilds paths from `parent` |
| `meta(key, value)` | | See below |

`build` loads everything with journaling and fsync off, builds the indexes, fsyncs, then renames `<fs>.db.new` → `<fs>.db`, so searches never see a half-written file. On a signal, a writer error, or when free space on `--db-dir` drops below `--min-free-pct` (default 35%), the new file is deleted. SQLite sort temp files go into `--db-dir` too, not `/tmp`.

Distinct names are deduplicated in a Go map during the load, which is the largest allocation (about 0.5 KiB RSS per distinct name). Expect 80–100 bytes per entry on disk. That makes SQLite a good fit for volumes up to tens of millions of entries. Use PostgreSQL for larger ones.

### Metadata

Both backends record `fs`, `fsid`, `host`, `prefix`, `started_at`, `scan_seconds`, `complete`, `journal_flushed`, exact counts (`entries` = `files` + `dirs` + `symlinks` + `hardlinks` + `special`), `missed_dirs`, `dup_dirs` and the error counts.

A directory renamed during the walk can be seen under both parents. `dirs` keeps the later sighting and counts it in `dup_dirs`, and the entries below it may appear under both paths. Hardlinks carry no owner and never match `--uid`.

## Build

```bash
git clone --recurse-submodules https://github.com/xorpaul/cephfs-index
make decode   # bin/cephfs-dentry-decode: static, pure Go
make indexd   # bin/cephfs-indexd: cgo + librados + bundled SQLite
make search   # bin/cephfs-search: cgo + bundled SQLite, no librados
make test
```

Building needs Go ≥ 1.25 and `librados-devel` for `indexd`. At runtime the binaries need only `librados.so.2` (indexd) and glibc ≥ 2.34, so a build on an EL9/EL10 host also runs on e.g. Debian 12. If you can't install the devel package, extract the `librados-devel`, `librados2`, `librdmacm`, `libibverbs` and `libnl3` RPMs with `rpm2cpio | cpio -idm` into a directory and run `make indexd SYSROOT=<dir>`.

Release builds use [go-build-release](https://github.com/xorpaul/go-build-release) (submodule), configured in `.build.cfg`.

## Usage

Run on a host with `/etc/ceph/ceph.conf` and a keyring that can read the metadata pool (and `mds` allow for `--flush-journal`).

```bash
# Measure first: a baseline at low concurrency, then ramp up while watching `ceph osd perf`
./cephfs-indexd probe --fs myfs --max-inflight 4 --latency-ceiling 0
./cephfs-indexd probe --fs myfs --max-inflight 32 --latency-ceiling 10ms --duration 5m

# SQLite index
./cephfs-indexd build --fs myfs --max-inflight 32
./cephfs-search --fs myfs --type dir '^node_modules$'     # ^literal: uses the name index
./cephfs-search --fs myfs 'id_rsa'                         # no literal prefix: parallel scan of all names

# PostgreSQL index
export CEPHFS_INDEX_PG_DSN="host=pg.example.org dbname=cephfs_index user=cephfs_index"
./cephfs-indexd build --fs myfs --flush-journal --max-inflight 64
./cephfs-search --uid 1000 '\.php$'                        # searches every indexed volume
```

Search patterns are Go RE2 regexes matched against the entry name, not the path. A pattern that starts with `^` and a literal uses the name index. Other patterns scan in parallel with a server-side `strpos` pre-filter on their required literals, and every candidate is re-checked in Go. Output columns: uid, size, mtime (unix), path.

`probe --match REGEX` prints matching paths without building an index, but it keeps every directory's (parent, name) in memory, so it is meant for small volumes or `--start-ino` subtrees.

## Validating the decoder

The object name is `<dir-ino-hex>.<frag-hex>`, where frag `00000000` is the unfragmented directory. Pick a long-lived directory on a client mount, decode a few of its dentries from RADOS, and compare them with `stat`:

```bash
# on a client
D=/mnt/cephfs/some/dir
printf '%x\n' "$(stat -c %i $D)"                    # e.g. 100031c8832

# on the admin host
O=100031c8832.00000000
rados -p cephfs_metadata listomapkeys $O | head -5    # pick NAME_head
rados -p cephfs_metadata getomapval $O NAME_head /tmp/v.bin
./cephfs-dentry-decode NAME_head /tmp/v.bin

# on the client: must match uid, gid, size, mtime (stat prints ino in decimal, the decoder in hex)
stat -c 'uid=%u gid=%g size=%s mtime=%Y ino=%i type=%F' $D/NAME
```

If `listomapkeys` returns ENOENT, the directory is fragmented. Decode its own dentry from its parent and use the objects listed in `frag_objects`.

For a full-field cross-check, strip the 19-byte dentry wrapper (8 bytes `first`, 1 byte marker `i`, a 6-byte struct header, 4 bytes alternate-name length when it is empty) and let Ceph decode it:

```bash
tail -c +20 /tmp/v.bin > /tmp/v.inodestore
ceph-dencoder type InodeStore import /tmp/v.inodestore decode dump_json
```

Also decode the root inode: `rados -p cephfs_metadata get 1.00000000.inode /tmp/root.bin && ./cephfs-dentry-decode --root /tmp/root.bin` should show type dir, ino 0x1 and `frag_objects`.

For end-to-end completeness, build an index of a small volume after `--flush-journal` and diff `cephfs-search --fs X '.' | cut -f4 | sort` against `find <mount> -mindepth 1 | sort`.

## Tests

`make test` runs the decoder, scan, SQLite round-trip, chunk routing and binary COPY encoder tests. An integration test against a live PostgreSQL runs when `CEPHFS_INDEX_TEST_DSN` is set; see `internal/pgindex/integration_test.go`.

## Roadmap

- systemd `cephfs-indexd@<fs>.service/.timer`, one volume at a time behind a global OSD limit.
- Incremental runs: skip subtrees whose `rstat.rctime` is older than the previous run minus a margin.
- Packaging.
