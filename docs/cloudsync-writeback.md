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
  "cloudsync_settle_millis": 2000,
  "cloudsync_placeholder_millis": 10000,
  "directory_grace_seconds": 60,
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

- Repeated PROPFIND calls use canonical metadata instead of provider mtime/size. Cloud Sync can immediately re-list a just-written object without seeing the provider's temporary size=0 / missing state.
- Cloud Sync zero-length placeholder PUTs and the following real PUT are coalesced by a short settle window. The provider worker never starts an older generation while another PUT for the same path is still being received.
- PUT returns 201 for a newly tracked path and 204 for a later tracked overwrite, while the canonical generation/ETag changes immediately.
- GET/HEAD use the local payload while it is cached and fall back to the backing provider after cleanup. After the local completed cache has expired, a successful provider directory listing that no longer contains the object removes the stale canonical row so Cloud Sync can see the loss and upload it again.
- A newer PUT increments the generation and invalidates an older in-flight upload. If the old upload finishes later at the same path, it is not deleted; the queued newer generation overwrites it next. Old remote paths are only cleaned when the object was moved or deleted.
- DELETE creates an immediate WebDAV tombstone and removes the provider object asynchronously.
- MKCOL is shadowed in MySQL and returns after the logical directory is durable, instead of waiting for the backing provider. Directory jobs are prioritized ahead of files, and child uploads use short retries while the provider catches up.
- Completed directory shadows remain visible for a configurable grace period so Cloud Sync's per-directory PROPFIND scans do not observe a transient missing parent.
- MOVE of a pending file updates the queued destination without requiring the provider object to exist first.
- A successful provider MOVE updates canonical metadata for tracked descendants.

This mode is intentionally optimized for **NAS -> OpenList -> cloud**. It does not attempt to propagate provider-side edits back to Synology Cloud Sync.


## Failure model

The write-back layer intentionally favors source correctness over avoiding duplicate provider traffic:

- A WebDAV success is returned only after the payload and canonical row are durable locally.
- Provider timestamps are never used as the Cloud Sync source-of-truth.
- Same-size remote objects are not trusted after an ambiguous process crash; the spool payload is uploaded again.
- During normal post-upload verification, 115 SHA-1 is compared with the persisted encrypted payload SHA-1 whenever available; size equality alone is not enough to complete the generation.
- A superseded upload to the same path is never followed by an eager delete, preventing an old worker from erasing the path while a newer generation is waiting.
- Duplicate provider writes can occur after a crash. For one-way encrypted backup this is preferable to silently accepting the wrong generation.
- Client-level retries of the exact same encrypted payload are coalesced while the durable spool is still present, reducing duplicate provider traffic without weakening remote-loss recovery.


### Remote-loss reconciliation

Canonical metadata is retained after the local completed spool cache is released so provider-side mtime changes do not cause upload loops. That metadata is not allowed to hide a genuinely missing remote object forever.

During a successful provider directory listing, if a tracked object is `COMPLETED`, its spool payload has already been released, and the provider no longer lists that name, the canonical row is removed. The next Cloud Sync scan then observes the object as missing and uploads the source again.

A failed provider listing never triggers this cleanup, so a temporary network/provider outage cannot turn into a mass re-upload.


## Cloud Sync compatibility profile

This mode is intentionally shaped around Synology Cloud Sync's WebDAV behavior rather than generic bidirectional WebDAV semantics.

### Upload sequence

A typical supported sequence is:

```text
PROPFIND parent
MKCOL directory (optional)
PUT file (may be a zero-byte placeholder)
PUT file (encrypted payload)
PROPFIND file / parent immediately
```

The WebDAV-visible result is committed locally before any slow 115/OpenList provider upload is required to finish. Cloud Sync therefore sees the newest canonical type, encrypted payload size, modification time and generation ETag throughout the provider consistency window.

While the encrypted PUT body is written to the durable spool, write-back also computes the payload SHA-1 in the same sequential pass and persists it with the generation. The 115 Open worker supplies that hash through the FileStreamer metadata, so `Open115.Put()` can skip its otherwise-required `CacheFullAndHash()` full-file reread. This reduces spool I/O for large encrypted files and makes 115 rapid-upload negotiation start sooner.

The default `cloudsync_settle_millis=2000` delays normal provider dispatch briefly after each PUT. A zero-byte PUT uses the longer `cloudsync_placeholder_millis=10000` window because Synology Cloud Sync can create a zero-length placeholder and send the real encrypted payload in a subsequent request. A later PUT to the same path supersedes the previous generation, avoiding an unnecessary empty-object upload before the real payload.

An exact duplicate PUT is idempotently coalesced when the current generation still has its durable local spool and the encrypted payload size/SHA-1 are identical. The new request body is fully received and fsynced first, but its temporary generation is discarded instead of incrementing the canonical ETag or causing another 115 upload. If the old spool has already been released, the PUT is **not** coalesced; it is accepted as a new generation so a genuine remote-loss recovery can restore the provider object.

### Directory consistency

Cloud Sync walks WebDAV as a directory-listing provider and can create a directory and immediately operate below it. With write-back enabled, MKCOL creates a canonical directory shadow first and the backing storage mkdir runs asynchronously.

Directories are dispatched before files. A child directory/file whose canonical parent is still pending waits locally instead of racing the provider. After the parent mkdir API succeeds, any residual provider-visibility gap is handled with short retries. A completed directory shadow remains authoritative for the full `directory_grace_seconds` window (60 seconds by default), even if a parent listing starts showing the directory earlier, so an inconsistent single-path lookup cannot make the directory disappear from Cloud Sync.

### Encrypted one-way jobs

The encrypted payload is opaque to OpenList. The canonical size is the exact number of bytes accepted by the WebDAV PUT, not the NAS source-file size and not a temporarily stale provider size. Provider hash and provider mtime are never promoted into the Cloud Sync-facing canonical view.

This is specifically intended for one-way NAS -> WebDAV/OpenList -> cloud jobs. Provider-side changes are not treated as authoritative source edits.


### Conditional retries

PUT honors `If-Match` and `If-None-Match` and returns HTTP 412 when the entity-tag condition fails. To keep the normal Cloud Sync upload path fast, OpenList only performs a backing-provider lookup for this check when one of those headers is actually present. Canonical write-back metadata is used first whenever available.


### 115 verification fallback

The background upload verifier does not trust a single 115 object lookup by itself. If the object lookup is missing or returns incomplete metadata, the worker force-refreshes the parent directory. For 115 Open, verification now requires the exact encrypted payload size **and** the persisted payload SHA-1 returned by `Obj.GetHash()`; a same-size stale generation is therefore not accepted as the newly uploaded object, and a temporary 115 response with an empty SHA-1 is treated as incomplete metadata rather than a successful verification. Providers whose storage driver genuinely does not expose a content hash retain the size-based fallback. This prevents 115's post-upload metadata consistency window from turning either a successful upload into an unnecessary retry or an older same-size object into a false completion.


## Cloud Sync rename, copy and delete compatibility

Cloud Sync can issue WebDAV operations before an asynchronously uploaded 115 object is directly visible. Write-back therefore treats the local canonical state as authoritative for these operations as well:

- `MOVE` can relocate a file directly from the durable spool while it is queued, uploading, verifying, or recently completed with its cache still present.
- A just-created/pending directory tree can also MOVE before 115 exposes it, provided every live file in that canonical subtree still has a durable local spool payload. A 20-file burst therefore moves as one canonical tree instead of waiting for per-file provider visibility.
- An older completed directory, or any subtree with a live file whose spool has already been released, deliberately falls back to the provider MOVE path rather than guessing that the local tree is complete.
- A pending `MOVE` always leaves an immediate canonical tombstone at the source path, so an already-visible 115 object cannot reappear in Cloud Sync while the destination upload is still pending.
- The moved destination reuses the normal Cloud Sync settle window before provider dispatch, reducing churn when Cloud Sync performs rapid temp-name/final-name sequences.
- `COPY` can create a second canonical generation from the same immutable spool payload without waiting for 115 to expose the source file.
- A new destination directory can also be COPYed entirely from canonical state when the source tree is locally authoritative. Live file generations share the same immutable spool payload and are queued independently, so Cloud Sync gets an immediate WebDAV result without waiting for synchronous per-file 115 copies.
- `Depth: 0` pending-directory COPY only clones the collection row while that collection is still locally authoritative; an expired completed collection first reconciles with the provider. Recursive COPY requires every live file in the source subtree to retain a durable spool. Existing destination trees deliberately stay on the provider fallback path to avoid merging unknown provider-only children.
- A destination overwrite returns HTTP 204; a new destination returns HTTP 201; `Overwrite: F` returns HTTP 412 when the destination already exists.
- COPY/MOVE preflight provider-only destinations too. A target that exists on 115 but has no canonical row is not treated as a new local target; overwrite is delegated to the provider path.
- Before provider fallback overwrites a tracked destination, the destination canonical subtree must be fully `COMPLETED`. Pending, uploading, verifying, failed or deleting state returns HTTP 503 with `Retry-After: 2` so Cloud Sync retries instead of racing an older worker. This readiness check is non-destructive: if the provider operation fails, the previous canonical destination remains intact.
- Provider COPY with a different destination basename writes directly to the exact WebDAV target. It no longer uses `dstDir/srcName` as an intermediate path, so an unrelated same-name object in the destination directory is never touched. Same-name cross-directory COPY keeps the provider-native fast path.
- `Depth: 0` collection COPY remains shallow on the exact-path fallback.
- After a successful provider COPY, source canonical generations are transactionally mirrored to the destination. Completed generations get a fresh consistency grace, pending generations keep their shared spool and remain queued, and stale destination-only children become tombstones.
- If a fallback COPY source is provider-only and has no write-back rows, OpenList still captures the source metadata before the provider call and creates a fresh `COMPLETED` destination root shadow after success. The same fallback-root rule applies to provider-only MOVE, preventing an old destination canonical row from masking the newly copied/moved object.
- Provider MOVE metadata reconciliation now locks source and destination subtrees in one transaction. Exact path collisions merge into the destination generation, destination-only stale children become tombstones, and old source rows remain tombstones when needed to invalidate in-flight workers.
- Freshly remapped `COMPLETED` metadata, including files whose local spool has already expired, receives the same short consistency grace used for directory shadows. This prevents an immediate 115 NotFound/stale listing after MOVE from being treated as a real remote deletion.
- Shared COPY spool payloads are reference-counted in MySQL before physical cleanup, and active readers use an in-process reference count so one worker cannot make another worker's payload appear idle.
- `DELETE` tombstones the whole tracked subtree. If descendants are tracked but the parent row is not, a synthetic directory tombstone is created so the provider-side tree is still removed.
- A PUT below a canonical deleted/non-directory parent is rejected with HTTP 409 rather than creating an orphaned child generation.
- Background child jobs also stop at a canonical deleted/non-directory parent, covering races where the parent changes after the child PUT was already committed.

### Delete/recreate generation race

A Cloud Sync delete can be followed almost immediately by a PUT that recreates the same path. Provider deletion is generation-checked both before and after the remote remove. If an older delete overlaps a newer generation that has already completed, the newest payload is re-queued from its local spool so the stale delete cannot become the final remote state.

## Direct read reconciliation

After the completed local cache expires, direct `GET`, `HEAD` and single-resource `PROPFIND` perform conservative remote-loss reconciliation.

A 115 single-object NotFound or type mismatch is never trusted alone. OpenList force-refreshes the parent listing and only drops canonical metadata when the exact-name parent listing confirms the object is absent or has the wrong resource type. Transient 115 lookup inconsistencies therefore remain hidden from Cloud Sync, while genuinely removed remote objects eventually become visible as missing and are uploaded again by the one-way job.

`PROPPATCH` is also canonical-aware, so a just-written object does not become temporarily unpatchable merely because the backing provider has not exposed it yet.

## 115 Open driver hardening

This fork also hardens the 115 Open driver under the write-back workload:

- `Get()` falls back to the parent listing when a non-directory object has suspicious `size <= 0` or an invalid modification time, covering the post-upload incomplete-metadata window seen by Cloud Sync.
- failed or canceled OSS multipart uploads explicitly call `AbortMultipartUpload` before retry, avoiding accumulation of unfinished multipart sessions.
