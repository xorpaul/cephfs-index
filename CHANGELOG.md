# Changelog

## v1.2.0 (2026-10-08)

### Added
- **Precomputed paths for chosen names (`--pg-named-paths`)** — the PostgreSQL build can precompute the full path of every entry with a given name (e.g. `mu-plugins`) into `<fs>.named_paths`, with the covered names in `<fs>.named_paths_names` and `meta.named_paths`. Exact-name searches for those names then skip path resolution, which needs one random read per directory level when `dirs` is not cached. A failure only logs a warning; the build still completes.
- **`cephfs-indexd named-paths`** — builds the same table on existing schemas (`--fs all` or a list), so names can be added without a rebuild. Each schema is swapped in one short transaction.

## v1.1.0 (2026-10-06)

### Added
- **ctime in index and search output** — `ctime` (inode change time) is now stored alongside `mtime` for every object in both the SQLite and PostgreSQL backends. `cephfs-search` includes it in results. The PostgreSQL covering name index is extended to include `ctime` so searches remain index-only.

## v1.0.0 (2026-10-01)

First public release.

### Added
- **`cephfs-indexd build`** — walks a CephFS volume by reading its metadata pool directly with librados, without mounting it. Dirfrags are read with bounded concurrency (`--max-inflight`, with an optional `--max-ops` rate limit), and in-flight reads are halved when p99 omap latency exceeds `--latency-ceiling`. The progress line shows entries/s, latency percentiles, pct and ETA against the volume's rstat entry count, `missed_dirs`, `enoent` and the COPY queue. `--flush-journal` runs `flush journal` on every active MDS rank once the build can proceed, so metadata still only in the journal is not missed. `--duration` stops the walk early and keeps it as `<fs>_partial`.
- **PostgreSQL backend (`--pg-dsn`)** — loads the walk into one schema per volume (create the database as SQL_ASCII, so any filename is accepted). The build writes to `<fs>_new` and swaps it in atomically at the end, so searches always see the previous complete index until then. An advisory lock stops concurrent builds of the same volume.
  - **Layout 2 (`meta.layout = 2`):** each of the `--pg-copy-workers` COPY streams writes its share of every `--pg-chunk-rows` chunk into its own UNLOGGED leaf table with `COPY … FREEZE`. Rows land frozen and all-visible, so no VACUUM pass over the data is needed. `entries` is partitioned by `part`, and the leaves attach without a validation scan.
  - **Background builders** (`--pg-index-builders`, `--pg-chunk-workers`) index each leaf while the scan continues. The name index is covering, `INCLUDE (parent, ino, type, uid, size, mtime)`. Each leaf's dir rows are upserted into `dirs`, keeping the last sighting of a dir renamed mid-walk.
  - **Post-load** adds a covering `dirs (ino) INCLUDE (parent, name)` index and one `VACUUM` of `dirs`, so name searches and path resolution are index-only scans. It reports `CREATE INDEX` and `VACUUM` progress.
- **SQLite backend** — one `<fs>.db` per volume in `--db-dir` (names, dirs, entries; atomic `.db.new` → rename). It stops and deletes the new file if free space drops below `--min-free-pct`.
- **Exact entry counts per type in `meta`** — both writers count every entry as it is written and store `files`, `symlinks`, `hardlinks` (remote dentries, the extra names of multiply-linked files) and `special` (device nodes, FIFOs, sockets) next to the existing `entries` and `dirs`. `entries` = `files` + `dirs` + `symlinks` + `hardlinks` + `special`. Readers can show these instead of estimating them from planner statistics, which undercount rare types.
- **`cephfs-search`** — finds entries by name (Go RE2 on the name, not the path) in either backend and prints uid, size, mtime and path. Filters: `--type` and `--uid`. `^literal` patterns use the name index; other patterns scan in parallel with a server-side `strpos` pre-filter on their required literals, and every candidate is re-checked in Go. Paths are resolved level by level with batched dirs queries. The PG backend reports query and path-resolution time separately.
- **`cephfs-indexd probe`** — walks a volume (or a subtree from `--start-ino`) without writing an index, to measure throughput, test `--localize-reads` and CRUSH placement, and optionally print matching names.
- **`cephfs-dentry-decode`** — decodes raw CephFS dentry omap values (pure Go, static).
- **Placement awareness** — resolves a pool's primary DC from its CRUSH rule and warns when a build reads cross-DC primaries, so each volume can be indexed from a host in its metadata-primary DC.
- **Tests** — scan, SQLite round trip, chunk routing, the binary COPY encoder, and an opt-in integration test against a live PostgreSQL (`CEPHFS_INDEX_TEST_DSN`).
