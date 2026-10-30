# Storage

The MVP uses [bbolt](https://github.com/etcd-io/bbolt), an embedded B+tree key-value store for Go. All data is in one file. The design is not validated yet. The first prototype must test it.

Other components use only the storage interface, so the engine can change later.

## Buckets

| Bucket | Key | Value | Purpose |
|---|---|---|---|
| `jobs` | `jid` | Job JSON | Job record |
| `queues/<queue>` | `seq` | `jid` | Waiting jobs, in FIFO order. One nested bucket for each queue. |
| `scheduled` | `time/jid` | None | Scheduled jobs |
| `retries` | `time/jid` | None | Retries, by next attempt time |
| `working` | `time/jid` | None | Reservations, by expiry time |
| `dead` | `time/jid` | None | Dead set, by death time |
| `counters` | Name | `uint64` | Size of each queue and set, and totals |
| `paused` | Queue | Marker | Paused queues |
| `uniques` | SHA-256 of type, queue, args | Expiry + `jid` | Unique locks |
| `unique_expiry` | `time/hash` | None | Sweep index for locks |
| `leases` | `queue NUL jid` | Expiry + `wid` | Throttle leases |
| `batches` | `bid` | JSON | Batch status, callbacks, and child counts |
| `batch_expiry` | `time/bid` | None | Sweep index for batches |
| `tracks` | `jid` | JSON | Job progress |
| `track_expiry` | `time/jid` | None | Sweep index for tracks |
| `meta` | Name | Value | Metadata, such as the schema version |

`seq` comes from `Bucket.NextSequence`. `seq` and `time` are big-endian `uint64` values, so key order equals time order.

Do not use `Bucket.Stats()` for sizes, because it walks pages. Each transaction updates `counters`, so the web UI reads a size in constant time.

## Atomic changes

Each state change is one transaction, and a commit syncs to disk. Batch counters, callbacks, tracks, and locks change in the same transaction as the job. Example for `FETCH`:

```text
delete queues/<queue>/<seq>
put    working/<expiry>/<jid>
put    jobs/<jid> (with the reservation data)
update counters
```

## Write throughput

bbolt has one writer. One commit loop puts all waiting operations in one transaction, and operations that arrive during a commit form the next group.

Do not use `db.Batch`. Its timer starts a new batch during a sync, so the writes split into many small batches with one sync each. In a test, this limited fetch and ack to 192 jobs per second, and the commit loop increased this to more than 1,000 on the same load.

An operation must validate before it writes. An expected error (`ErrDuplicateJob`, `ErrNotReserved`) does not roll back the group. Any other error rolls back the group and fails all its operations.

Single-connection throughput is limited by disk sync time. See Measurements.

## Read rules

A long read transaction blocks reuse of pages, and the file then grows under write load.

- Web UI lists use short, paged reads. A page opens a new transaction.
- Do not hold a read transaction during network I/O.
- Set `InitialMmapSize`, because a read transaction can block a write during an `mmap` remap.

## Memory

The OS page cache holds the mapped file. This memory can be reclaimed, but it appears in RSS. Measure both total RSS and anonymous memory. The memory target in `SPEC.md` applies to anonymous memory.

## File size

The file does not shrink when jobs complete. Free pages are reused. Offline compaction recovers disk space after a large backlog.

## Schema version

`meta/schema_version` holds the data format. The server refuses a file from a newer version. Add migrations in `prepareSchema`, and increase `schemaVersion` when the format changes.

## Recovery at boot

1. Open the file. bbolt uses the last valid meta page, so no log replay is necessary.
2. Start the background loops. Expired reservations then go to retry.

## Validation tests

- Maximum push, fetch, and ack rate with sync on, against the target in `SPEC.md`.
- Backlog of 10 million jobs: memory, latency, and file size.
- Long run at near-zero queue depth, with web UI reads: file growth.
- `kill -9` under load: no confirmed job is lost.

## Measurements

Apple M5 Pro, local NVMe, macOS, full sync on, loopback TCP, small jobs. These are one development machine, not a production benchmark.

| Connections for each side | Push (jobs/s) | Fetch and ack (jobs/s) |
|---|---|---|
| 1 | 108 | Not measured |
| 32 | 2,209 | 1,091 |
| 256 | 14,316 | 6,464 |

After the unique, throttle, and expiry features, fetch and ack with 256 connections measured 3,818 to 4,439 jobs/s in 4 runs. The cause of this drop is not investigated.

Backlog of 2 million jobs (before those features): 15,175 push jobs/s, 1.3 GB file, 283 MB RSS, 20 MB physical footprint, and 5,056 fetch and ack jobs/s at full backlog.

The target of 10 million jobs is not measured.

## Fallback

If bbolt does not reach the throughput target after tuning, replace it behind the storage interface. The candidates are Pebble and an own segmented log.
