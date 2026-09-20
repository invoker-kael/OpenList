# Synology Cloud Sync write-back mode

This fork adds a WebDAV write-back path intended for **one-way Synology Cloud Sync uploads**, including Cloud Sync client-side encrypted jobs.

## Design

Cloud Sync is treated as the authoritative source. The opaque object received by WebDAV is committed to a local spool before the HTTP PUT succeeds. A persistent MySQL/GORM row then provides canonical WebDAV metadata while a background worker uploads the payload through the normal OpenList storage driver.

The backing provider's modification time is deliberately not exposed back to Cloud Sync for objects tracked by write-back. This avoids repeated uploads caused by eventual-consistency windows or provider-side timestamp changes.

The stable WebDAV view is:

- path and object type
- actual encrypted payload size received by PUT
- canonical modification time
- generation-specific ETag

The remote provider's mtime is informational only.

## State machine

```text
PUT
 -> local fsync + atomic rename
 -> QUEUED
 -> UPLOADING
 -> VERIFYING
 -> COMPLETED
```

A successful PUT means the payload is durable in the local spool and its metadata is committed to the OpenList database. It does **not** mean the backing cloud has already finished uploading.

Failed remote uploads retry in the background. An interrupted `UPLOADING` row is deliberately re-queued from the durable spool after restart. This can duplicate a provider upload after a crash, but it avoids treating an older same-sized encrypted object as proof that the newest generation arrived. The remote write contract is therefore **at-least-once across crashes**, with the local canonical generation remaining authoritative to Cloud Sync.

## MySQL

The write-back queue and canonical metadata use the normal OpenList GORM database. With a MySQL deployment no SQLite side database is created.

Paths are stored as text, while SHA-256 path keys are indexed. This avoids MySQL `utf8mb4` index-length problems for long WebDAV paths.

## Configuration

Default configuration in this fork:

```json
"webdav_writeback": {
  "enabled": true,
  "spool_dir": "writeback",
  "reserve_free_space_mb": 20480,
  "workers": 4,
  "retry_initial_seconds": 30,
  "retry_max_seconds": 1800,
  "verify_interval_seconds": 5,
  "verify_attempts": 60,
  "completed_cache_ttl_minutes": 30
}
```

Relative `spool_dir` paths are resolved against the OpenList working directory. Put the spool on durable local storage; after WebDAV returns success it is the authoritative copy until remote verification completes.

For large Cloud Sync jobs, using SSD/NVMe for the spool is recommended. When free space would fall below the reserve, PUT fails instead of acknowledging data that cannot be durably staged.

## Semantics

- Repeated PROPFIND calls use canonical metadata instead of provider mtime/size.
- GET/HEAD use the local payload while it is cached and fall back to the backing provider after cleanup.
- A newer PUT increments the generation and invalidates an older in-flight upload. If the old upload finishes later at the same path, it is not deleted; the queued newer generation overwrites it next. Old remote paths are only cleaned when the object was moved or deleted.
- DELETE creates an immediate WebDAV tombstone and removes the provider object asynchronously.
- MOVE of a pending file updates the queued destination without requiring the provider object to exist first.
- A successful provider MOVE updates canonical metadata for tracked descendants.

This mode is intentionally optimized for **NAS -> OpenList -> cloud**. It does not attempt to propagate provider-side edits back to Synology Cloud Sync.


## Failure model

The write-back layer intentionally favors source correctness over avoiding duplicate provider traffic:

- A WebDAV success is returned only after the payload and canonical row are durable locally.
- Provider timestamps are never used as the Cloud Sync source-of-truth.
- Same-size remote objects are not trusted after an ambiguous process crash; the spool payload is uploaded again.
- A superseded upload to the same path is never followed by an eager delete, preventing an old worker from erasing the path while a newer generation is waiting.
- Duplicate provider writes can occur after a crash. For one-way encrypted backup this is preferable to silently accepting the wrong generation.
