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

Client-visible canonical state and provider replication state are deliberately separate:

```text
PUT body
 -> RECEIVING lease in MySQL
 -> local fsync + atomic rename
 -> canonical ACKED + DurableAt in MySQL
 -> HTTP 201/204 to Cloud Sync

provider replication (background only)
 -> QUEUED
 -> UPLOADING
 -> VERIFYING
 -> COMPLETED / FAILED+RETRY
```

A successful PUT means the complete payload is durable in the local spool and the canonical `ACKED` identity is committed to MySQL. It does **not** mean the backing cloud has already finished uploading. Provider queue transitions and retry failures therefore cannot change the size/mtime/ETag that Cloud Sync sees for that ACKed generation.

After the complete PUT body is fsynced and atomically installed in the spool, the canonical MySQL commit is intentionally detached from HTTP request cancellation. A late Cloud Sync timeout/disconnect can therefore lose the response, but it cannot make OpenList discard an already complete large payload; the next retry observes/coalesces against the durable canonical generation. An incomplete body is never committed. When the request has a declared length and OpenList has received exactly that many bytes, a terminal connection-cancellation error after the final byte is treated as end-of-body rather than as truncation; cancellation before the declared length remains a hard failure.

Failed remote uploads retry in the background. An interrupted `UPLOADING` row is deliberately re-queued from the durable spool after restart. This can duplicate a provider upload after a crash, but it avoids treating an older same-sized encrypted object as proof that the newest generation arrived. The remote write contract is therefore **at-least-once across crashes**, with the local canonical generation remaining authoritative to Cloud Sync.

## MySQL

The write-back queue and canonical metadata use the normal OpenList GORM database. With a MySQL deployment no SQLite side database is created.

Paths are stored as text, while SHA-256 path keys are indexed. This avoids MySQL `utf8mb4` index-length problems for long WebDAV paths.

Same-path PUT publication is also fenced in MySQL. Each WebDAV path has one small receive-fence row with a monotonically increasing start sequence and the last sequence that successfully published canonical state. A PUT receives its sequence before the body is accepted; after the complete payload is durable, canonical publication locks that fence and cannot move behind a newer committed PUT. This replaces process-local ordering as the correctness authority, so overlapping large-file retries converge the same way after restart or across multiple OpenList instances.

The same fence now carries a crash-expiring `RECEIVING` lease and active-receiver count. A long PUT refreshes that lease periodically while streaming. Provider workers consult the durable lease as well as the local fast-path map, so another OpenList instance cannot start uploading an older queued generation while the NAS is still sending a newer large file. A crashed receiver stops blocking automatically when its lease expires. This is coordination only: OpenList never returns success for a partial body.

The same fence is advanced by canonical MKCOL and DELETE mutations. Directory DELETE advances every already-known descendant fence in the same MySQL transaction as the tombstones. Therefore an older PUT that started before a later delete cannot finish late and resurrect the deleted path, including when the PUT and DELETE were handled by different OpenList instances.

MOVE and COPY use the same ordering domain. MOVE advances both source and destination fence trees before canonical metadata changes; COPY advances only its destination tree. Multi-root mutations lock fence roots in deterministic path order, so opposite-direction operations do not introduce a new A-to-B/B-to-A lock-order cycle. Provider-backed MOVE/COPY metadata reconciliation applies the same fence update after the durable provider-operation intent has protected the paths.

The queue table also has composite indexes on `(state, retry_at)` for the two-second worker dispatch scan and `(state, completed_at)` for completed-spool cleanup. They are declared in the GORM model, so the normal OpenList MySQL `AutoMigrate` path creates them automatically on upgrade; no separate SQLite database or manual migration is required. The hot dispatch scan selects only `id`, completed-cache cleanup selects only the four fields it actually consumes, and provider-intent protection scans select only routing/fence columns instead of large recovery snapshots and TEXT errors. This avoids repeated transfer/allocation of cold columns on large MySQL tables. Subtree mutation queries also use an escaped `root/%` LIKE pattern (`~` escape) instead of `root%`, so encrypted names containing `_`/`%` cannot expand into SQL wildcards and sibling prefixes such as `/a` versus `/ab` are excluded before row locking.

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

- Repeated PROPFIND calls use the ACKed canonical metadata instead of provider mtime/size or replication state. Cloud Sync can immediately re-list a just-written object without seeing the provider's temporary size=0 / missing state.
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
- Delete completion is also eventually-consistency aware: MySQL tombstones remain authoritative until two refreshed provider views agree that the name is absent.
- Duplicate provider writes can occur after a crash. For one-way encrypted backup this is preferable to silently accepting the wrong generation.
- Client-level retries of the exact same encrypted payload are coalesced while the durable spool is still present, reducing duplicate provider traffic without weakening remote-loss recovery.
- Overlapping unconditional PUT retries for the exact same WebDAV path share one temporary WebDAV lock lease in write-back mode. The underlying lock remains held until the last overlapping request exits, so DELETE/MOVE/explicit LOCK semantics remain protected without turning a long-file retry into `423 Locked`. Requests with an explicit `If` lock condition keep the standard lock path.


### Remote-loss reconciliation

Canonical metadata is retained after the local completed spool cache is released so provider-side mtime changes do not cause upload loops. That metadata is not allowed to hide a genuinely missing remote object forever.

A **single** 115 absence, type mismatch, size mismatch or SHA-1 mismatch is no longer allowed to evict a completed file's canonical identity. OpenList records the first divergent provider observation in the existing completed-row verification fields and keeps returning the NAS-authoritative size/mtime/ETag to Cloud Sync. After the configured verification interval, a normal cached directory listing still cannot evict the row: exactly one request claims the confirmation under the MySQL row lock, advances `retry_at` first, and then performs a separate `Refresh: true` parent listing. Concurrent PROPFIND/GET/HEAD scans therefore share the same cooling window instead of all force-refreshing 115. Direct reads also arm the first divergence without refreshing the parent immediately. An exact provider match clears the pending divergence evidence, while incomplete fresh 115 metadata remains inconclusive and keeps the canonical identity until the next verification interval. This prevents stale list cache, transient post-upload NotFound, and missing-hash windows from turning into a provider refresh storm or another PUT/upload loop.

For single-resource GET/HEAD/PROPFIND after the completed spool has been released, a confirmed provider match refreshes the existing generation-scoped `remote_verified_at` evidence. Repeated direct checks inside the configured verify interval use that evidence instead of issuing another provider `fs.Get`; a generation change, armed divergence, retry state, or expired interval bypasses the cooldown immediately.

115 large-file verification is size-aware. Files larger than 20 MiB use 115's multi-part path, so their conclusive remote-metadata divergence window is extended to at least `retry_max_seconds` (30 minutes by default) before one repair upload is allowed. If that repaired generation is still divergent, OpenList keeps the durable spool and probes at the long retry cadence instead of issuing unlimited provider reuploads. Cloud Sync continues to see the same canonical generation throughout.

During successful provider directory listings, if a tracked object is `COMPLETED`, its spool payload has already been released, and the provider continues to omit or disagree with that object across the separated confirmation window, the canonical row is removed. The next Cloud Sync scan then observes the confirmed remote divergence and repairs it from the NAS.

Post-upload verification now separates **match**, **conclusive divergence**, and **inconclusive provider evidence**. Only a successful refreshed parent listing that proves absence, or refreshed size/SHA-1 metadata that proves different content, consumes the retry budget that can eventually schedule another provider upload. A direct 115 NotFound followed by a failed refresh, a provider/network error, or a same-size 115 object whose SHA-1 is temporarily missing stays in `VERIFYING` and cannot consume the re-upload budget. Inconclusive checks use a slower bounded polling cadence (30 seconds with the default configuration) rather than the five-second normal visibility cadence. This prevents an unavailable 115 hash or transient API failure from turning a successful encrypted upload into an infinite upload loop.

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


### LOCK-before-PUT compatibility

When a WebDAV client locks a path that does not exist yet, write-back now exposes an RFC4918-style **lock-null resource** immediately. The placeholder is a canonical zero-byte file visible to PROPFIND/HEAD/GET, but it is never queued to 115 and never consumes a spool payload. A subsequent PUT upgrades the same canonical row into the normal durable upload generation, so LOCK-before-PUT does not create a stray zero-byte cloud object.

Finite lock-null resources expire with their lock timeout. UNLOCK removes an untouched placeholder immediately. Because the current WebDAV lock manager is process-local, all remaining lock-null rows are discarded on OpenList restart before upload recovery begins; a stale lock shadow therefore cannot survive after its token has been lost. LOCK on an unmapped child still requires an existing collection parent and returns 409 when the parent does not exist.

### Conditional retries

PUT honors `If-Match` and `If-None-Match` and returns HTTP 412 when the entity-tag condition fails. To keep the normal Cloud Sync upload path fast, OpenList only performs a backing-provider lookup for this check when one of those headers is actually present. Canonical write-back metadata is used first whenever available.


### 115 verification fallback

The background upload verifier does not trust a single 115 object lookup by itself. If the object lookup is missing or returns incomplete metadata, the worker force-refreshes the parent directory. A direct NotFound or mismatch is never allowed to consume repair/re-upload attempts when that refreshed listing itself fails. For 115 Open, verification requires the exact encrypted payload size **and** the persisted payload SHA-1 returned by `Obj.GetHash()`; a same-size stale generation is therefore not accepted as the newly uploaded object. At the same time, a same-size object with a temporarily unavailable SHA-1 is explicitly **inconclusive**, not failed: OpenList preserves the canonical generation and durable spool and continues low-frequency verification without ever promoting that evidence to another upload. Providers whose storage driver genuinely does not expose a content hash retain the size-based fallback. This prevents 115's post-upload metadata consistency window from turning either a successful upload into an unnecessary retry loop or an older same-size object into a false completion.


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
- Provider COPY with a different destination basename writes directly to the exact WebDAV target. It no longer uses `dstDir/srcName` as an intermediate path, so an unrelated same-name object in the destination directory is never touched. Same-name cross-directory COPY keeps the provider-native fast path. For overwrite fallback, destination removal tolerates an already-missing object on Cloud Sync retry, and 115 must remain absent across two provider observations separated by a short (maximum two-second) consistency fence before the final name is reused. If the old destination is still visible, COPY returns 503 with `Retry-After: 2` instead of racing the stale-name window.
- `Depth: 0` collection COPY remains shallow on the exact-path fallback.
- After a successful provider COPY, source canonical generations are transactionally mirrored to the destination. Completed generations get a fresh consistency grace and pending generations keep their shared spool. Destination-only canonical children whose corresponding source content is unknown are **not** blindly tombstoned: their old spool is released, their generation is refreshed, and size/SHA-1 reconciliation decides after the grace whether the new provider object is the same, changed, or absent.
- If a fallback COPY source is provider-only and has no write-back rows, OpenList still captures the source metadata before the provider call and creates a fresh `COMPLETED` destination root shadow after success. The same fallback-root rule applies to provider-only MOVE, preventing an old destination canonical row from masking the newly copied/moved object.
- The MOVE fallback-root rule also covers partially retained canonical trees: if child rows still exist but the source root row has already expired, the captured provider source metadata recreates `/dst` as a fresh completed root shadow before the moved children are exposed.
- Provider MOVE metadata reconciliation locks source and destination subtrees in one transaction. The destination is always created or merged as a separate canonical generation, while every moved source generation stays behind as a tombstone until normal provider-absence verification confirms the old 115 path is gone. If the source was provider-only, or its canonical root already expired while children remain, OpenList synthesizes a source-root tombstone from the captured provider metadata. This prevents 115's post-MOVE stale listing from making the old Cloud Sync path briefly reappear. Destination-only rows with unknown source correspondence enter a fresh reconciliation grace instead of being deleted blindly, preventing provider-only children from being accidentally removed after the move.
- Provider fallback MOVE no longer uses `dstDir/srcName` as an intermediate path when the destination basename changes. The source is first renamed to a UUID staging name, moved server-side, then renamed to the exact final basename; failed intermediate steps best-effort roll the staged object back to its original path.
- For `Overwrite: T`, the provider destination is removed before the source is moved. A retry is allowed to see provider NotFound from the already-completed removal instead of turning it into HTTP 500. On 115, two absence observations separated by the short consistency fence are required before the old final name is reused; if the destination is still visible, WebDAV returns 503 and leaves the source untouched so Cloud Sync can retry. Canonical COPY/MOVE reconciliation is only applied after HTTP 201/204 provider success; 403/409/412/503 responses leave the existing MySQL view unchanged. After provider success, the canonical MySQL reconciliation transaction is retried immediately, then after 50 ms and 200 ms, reducing ambiguous provider-success/database-failure responses from transient MySQL errors.
- Freshly created or merged destination `COMPLETED` metadata, including files whose local spool has already expired, receives the same short consistency grace used for directory shadows. This prevents an immediate 115 NotFound/stale listing after MOVE from being treated as a real remote deletion.
- Shared COPY spool payloads are reference-counted in MySQL before physical cleanup, and active readers use an in-process reference count so one worker cannot make another worker's payload appear idle.
- `DELETE` tombstones the whole tracked subtree. If descendants are tracked but the parent row is not, a synthetic directory tombstone is created so the provider-side tree is still removed. Provider-only objects are staged directly as tombstones in one MySQL transaction; if a concurrent PUT/MKCOL creates canonical state during that handoff, DELETE returns a retryable 503 instead of consuming the newer generation.
- A tombstone is not released merely because provider `Remove()` returned success/NotFound. OpenList force-refreshes the parent and requires two consecutive absence confirmations separated by the verify interval. Provider/listing errors or a still-visible same-name resource reset the confirmation count and keep the path hidden from Cloud Sync. If the parent itself is confirmed NotFound, that counts as descendant absence; this lets child tombstones drain after a directory-tree delete instead of retrying forever.
- Once a directory tombstone itself passes both absence confirmations, all still-`DELETED` canonical descendants are removed in the same locked MySQL transaction. Recreated/queued descendants are excluded, shared spool paths are released only after the last database reference disappears, and thousands of child rows no longer each need their own two provider probes.
- A PUT below a canonical deleted/non-directory parent is rejected with HTTP 409 rather than creating an orphaned child generation.
- Background child jobs also stop at a canonical deleted/non-directory parent, covering races where the parent changes after the child PUT was already committed.

### Delete/recreate generation race

A Cloud Sync delete can be followed almost immediately by a PUT that recreates the same path. Provider deletion is generation-checked both before and after the remote remove. If an older delete overlaps a newer generation that has already completed, the newest payload is re-queued from its local spool so the stale delete cannot become the final remote state.

### Durable provider COPY/MOVE intent

Provider-side COPY/MOVE has a second ambiguity window: the cloud mutation can succeed while the following MySQL metadata transaction fails. The operation is now fenced by a durable MySQL intent with a deterministic SHA-256 operation key and a `prepared -> started -> applied` lifecycle. OpenList refuses to call the provider unless the `started` marker is durable first.

On a Cloud Sync retry, OpenList checks the existing intent before destructive overwrite handling. File operations compare the destination against the captured source size/SHA-1, and MOVE additionally confirms whether the provider source disappeared. A recovered operation reconciles canonical metadata without issuing COPY/MOVE again. Ambiguous 115 views return HTTP 503 with `Retry-After: 2` rather than deleting an already-successful destination. Directory COPY is intentionally conservative: an unmarked, partially visible tree is treated as inconclusive instead of assuming the copy completed.

This specifically prevents the failure sequence where a successful 115 MOVE is followed by a MySQL error, then the retry sees the destination and deletes it before discovering that the provider source has already moved.

The intent also acts as a path fence while unresolved. PUT, DELETE, MKCOL and PROPPATCH under the source/destination tree return HTTP 503 with `Retry-After: 2` instead of creating a newer canonical generation that an older metadata reconcile could overwrite. Other COPY/MOVE requests are fenced when their source/destination trees overlap; independent COPY operations may still share the same stable source.

A `STARTED` operation is never declared "not applied" from one provider observation. OpenList requires two consistent observations separated by the configured verification interval (capped at two seconds) before allowing the cloud mutation to be retried. On process startup and during periodic maintenance, durable intents are scanned automatically: abandoned `PREPARED` rows are retired, already-converged metadata rows are cleaned up, and provider-confirmed operations are reconciled without waiting for Synology to repeat the request. Ambiguous directory-tree recovery remains fenced rather than being guessed.

Source identity now includes provider object ID plus canonical generation/ETag when available. If the original source generation has been superseded before metadata recovery, file operations may rebuild only the destination root from the durable snapshot; directory operations stay blocked because replaying a stale tree could touch newer descendants.

Directory provider operations now persist a deterministic SHA-256 tree fingerprint before the cloud mutation. Entries are sorted by relative path and include type, encrypted size and provider SHA-1. Hash requirements are resolved per file path rather than once at the root, so nested storage mounts do not inherit the wrong provider policy. The fingerprint respects WebDAV COPY depth.

The source fingerprint now follows the same view as the actual COPY implementation. Native recursive same-name provider COPY fingerprints the provider tree, while renamed/Depth:0/exact-tree COPY fingerprints the canonical write-back overlay that `copyExactTree()` will actually traverse. This prevents pending locally committed children from making a successful COPY look permanently inconsistent during recovery. For 115 directory MOVE, a visible destination object ID is additionally checked against the captured source object ID before the tree is accepted as the moved source.

A provider COPY that returns an ambiguous error is persisted as `FAILED`. Mutation state is now reported explicitly by `copyFiles()` / `moveFiles()`; HTTP status alone is not used to guess whether the provider was touched. This matters for overwrite: a 503 can occur after the old destination was already removed, so it must remain a durable recovery operation rather than being treated as a harmless preflight failure.

If a failed COPY destination nevertheless matches the captured file/tree identity it is recovered as success. Otherwise cleanup is identity-fenced. At failure time OpenList records whether the destination was visible and its provider object ID. On 115 Open, automatic partial-destination removal is allowed only when the current object ID is the same object observed at failure time; a replacement object, a missing ID, or an object that appeared only later remains fenced instead of being deleted. If the destination is already absent, OpenList confirms 115 absence across the short consistency window and retires the intent for a clean Cloud Sync retry. MOVE keeps its stricter ambiguous-error behavior because its source may already have been relocated.

Provider-intent maintenance also runs on an independent five-second cadence instead of waiting for the ten-minute completed-spool cleanup. APPLIED and abandoned PREPARED rows are prioritized, while STARTED rows rotate by `last_checked_at`; a few permanently ambiguous operations therefore cannot starve newer recoverable intents.

The recovery fence now also captures the destination's pre-operation provider object ID and canonical generation. A pre-existing canonical directory is never treated as proof that metadata reconciliation completed: the destination generation must advance after the provider mutation. While an intent is unresolved, canonical rows under both source and destination are protected from normal completed-shadow expiry, so the metadata evidence needed for reconciliation cannot disappear underneath the recovery worker.

FAILED COPY cleanup stores a failure-time destination snapshot rather than only an object ID. For files this includes type, encrypted size and SHA-1; for directories it includes the full provider-tree fingerprint. On 115 Open, cleanup requires both the same provider object ID and the same failure snapshot before deletion. Even an in-place content change that keeps the same provider ID therefore stops automatic cleanup. FAILED `not_applied` observations use the same two-observation consistency fence as STARTED operations, and maintenance will not recheck an intent more frequently than the confirmation interval.

Transitioning an intent back to STARTED or forward to APPLIED clears stale recovery/failure evidence, preventing a prior failed attempt from influencing a later generation of the same deterministic operation key.

## Direct read reconciliation

After the completed local cache expires, direct `GET`, `HEAD` and single-resource `PROPFIND` perform conservative remote-loss reconciliation.

A 115 single-object NotFound, type mismatch, size mismatch, empty hash, or other incomplete metadata is never trusted alone. A single-object lookup may prove an exact match, but any apparent mismatch first forces a refreshed exact-name parent listing before canonical metadata can be dropped. After the consistency grace, completed files whose local spool is gone are content-checked with provider mtime ignored. For 115 Open, both encrypted size and SHA-1 are required to prove identity from the refreshed listing; if the listing still omits SHA-1, the result is treated as inconclusive and canonical metadata is retained for a later retry. A confirmed missing object or real size/SHA-1 change becomes visible to one-way Cloud Sync so the NAS can repair it, while transient 115 metadata gaps and timestamp-only changes stay hidden.

Canonical `HEAD` responses also pin `Content-Length` to the MySQL generation size. If a completed object has to fall back to transparent provider proxying after its local spool expires, OpenList restores the canonical `ETag`, `Last-Modified`, `Content-Type` and `Content-Length` after copying provider headers. This keeps `PROPFIND` and `HEAD` from presenting two different identities for the same encrypted payload during provider metadata drift.

Canonical `GET`/`HEAD` conditional requests are evaluated before any 115 request. `If-Match`, `If-None-Match`, `If-Modified-Since` and `If-Unmodified-Since` therefore use the MySQL generation ETag/mtime rather than provider validators. After evaluation those headers are removed before proxying so 115 cannot re-decide the request with a different ETag. `If-Range` is evaluated against the canonical generation as well: a match preserves the byte range, while a mismatch intentionally downgrades to a full-body response. Non-range canonical GET/HEAD responses pin `Content-Length` to the canonical encrypted size. Conditional `PUT` now uses the same canonical identity: `If-Match`, `If-Unmodified-Since` and `If-None-Match` are evaluated in RFC order against the current MySQL generation before Cloud Sync upload data is committed, preventing a provider ETag/mtime drift from turning a retry into an overwrite of the wrong generation.

### Change identity for one-way encrypted upload

Cloud Sync change detection is intentionally content-first. The opaque encrypted payload size plus SHA-1 identifies content; canonical generation identifies the visible version. Provider mtime is never allowed to manufacture a new generation. A duplicate PUT with the same size/SHA-1 is coalesced, while same-size content with a different SHA-1 creates a new generation and is uploaded. For same-content duplicate PUTs, the canonical generation and ETag stay unchanged but OpenList accepts only mtime/ctime fields the client explicitly supplied, plus the latest MIME metadata. An omitted create-time therefore cannot be synthesized from mtime and accidentally replace the existing canonical ctime. This lets Synology's next comparison converge on the NAS-side timestamp without trusting or copying 115's drifting provider mtime. If the completed generation's local spool has already expired, a same-content PUT revives that same canonical generation with the newly received spool and enters `VERIFYING` instead of manufacturing another version. OpenList gives 115 the full verification window first: if size/SHA-1 still match, no provider upload occurs; if the remote object is missing or changed, the same durable spool is queued to repair 115. This keeps NAS/Cloud Sync authoritative without turning timestamp drift or duplicate client retries into duplicate 115 uploads. 115 is treated as the durable backing store and post-upload verification source, not as the authority for front-end change detection.

Write-back request parsing keeps timestamp presence separate from timestamp value. Only an explicitly supplied `X-OC-Mtime` or `X-OC-Ctime` may update an existing canonical generation; the normal synchronous WebDAV fallback to the current time / mtime is not used for duplicate Cloud Sync PUTs. Successful write-back PUT responses return the canonical `ETag` and `Last-Modified`, and acknowledge an accepted explicit `X-OC-Mtime` with `X-OC-MTime: accepted`.

### Concurrent upload admission and spool reservation

Cloud Sync can run many uploads concurrently, so write-back admission treats spool free space as a shared resource rather than letting each PUT inspect disk space independently. A PUT with a known `Content-Length` reserves its complete remaining payload before the request body is accepted. The reservation shrinks as bytes are physically written, which avoids double-counting bytes that are already reflected in filesystem free-space statistics while preserving capacity for every other admitted upload.

Chunked or otherwise unknown-length PUTs use a rolling reservation window. The default window is 64 MiB and can be changed with `webdav_writeback.incoming_reservation_chunk_mb`. Before the receiver can outgrow its current window it atomically reserves the next chunk against all other active PUTs. Every 64 MiB of physical writes also revalidates the configured free-space floor, so unrelated processes consuming the spool filesystem cannot silently push OpenList through `reserve_free_space_mb`.

Admission failures are typed as spool-capacity errors. WebDAV returns `507 Insufficient Storage` plus `Retry-After` (default 5 seconds, configurable through `webdav_writeback.admission_retry_seconds`) instead of returning a generic 500 or relying on error-string matching. When admission fails before or during a large PUT, the handler stops draining the remaining request body and closes the request connection, allowing Cloud Sync to retry later instead of needlessly transmitting the rest of an encrypted multi-gigabyte payload.

Reservations are released on success, short body, oversized body, disconnect, fsync/rename failure and MySQL failure. Unknown-length reservations retain only their current rolling window; interrupted `.part` files continue to be removed by the existing startup orphan cleanup. This provides bounded, concurrency-safe local staging without changing the durable canonical/115 verification semantics.

Same-path overlapping PUTs are also ordered by request start instead of body completion time. This matters when Cloud Sync retries a large encrypted object while an earlier request is still draining: if the later-started PUT successfully commits first, the older receiver discards its newly fsynced spool instead of creating a newer canonical generation merely because it finished later. If the later request fails before its canonical transaction commits, the older durable receiver remains eligible to commit. The ordering fence is process-local because only requests that are simultaneously alive can race; restart recovery continues to use the persisted canonical generation and spool state.

Successful provider verification is persisted with the canonical row as `remote_object_id`, `remote_sha1`, `remote_generation` and `remote_verified_at`. For 115 Open, the object id is the verified 115 file id and the provider SHA-1 is captured only after the exact generation passes the size/SHA-1 gate. The evidence is strictly generation-scoped: every PUT/MKCOL/delete/COPY/MOVE transition that creates a new canonical generation clears the previous remote object id/hash/timestamp before the row is saved, so stale 115 identity cannot leak into a newer Cloud Sync version.

If 115 visibility is slower than the configured verification window, a verifying generation is eventually re-queued. Before uploading the payload again, retry workers perform one fresh remote verification. If the correct size/SHA-1 has appeared in 115 during the backoff, the generation is completed from that evidence without sending the same encrypted payload a second time. The same rule covers process/container restarts: rows left in `UPLOADING` resume in `VERIFYING`, so the normal multi-attempt 115 consistency window runs before any retransmission. If the provider PUT succeeded but the following MySQL transition to `VERIFYING` fails transiently, OpenList also best-effort restores `VERIFYING` instead of leaving the row stranded in `UPLOADING`. This specifically reduces duplicate large encrypted uploads while retaining the same integrity gate.

`PROPPATCH` is also canonical-aware, so a just-written object does not become temporarily unpatchable merely because the backing provider has not exposed it yet.

## 115 Open driver hardening

This fork also hardens the 115 Open driver under the write-back workload:

- `Get()` falls back to the parent listing when a non-directory object has suspicious `size <= 0` or an invalid modification time, covering the post-upload incomplete-metadata window seen by Cloud Sync.
- failed or canceled OSS multipart uploads explicitly call `AbortMultipartUpload` before retry, avoiding accumulation of unfinished multipart sessions.

### Branch validation policy

The focused WebDAV write-back workflow is intentionally not triggered by ordinary pushes to `feature/cloudsync-writeback`. During compatibility work, related changes can accumulate without repeatedly canceling/rerunning the same CI. Validation remains available through `workflow_dispatch`, and pull requests targeting `main` still run the focused write-back suite automatically.
