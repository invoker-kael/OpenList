# Cloud Sync Write-back Operations Guide

This guide is for operating WebDAV Durable Write-back with **one-way Synology Cloud Sync** jobs.

## Core rule

**`needs_cloudsync_rehydrate` is the only normal write-back recovery state that requires Cloud Sync or operator intervention. All other normal write-back recovery states are designed to self-heal.**

Infrastructure failures are outside this rule. If the database service is unavailable, the spool filesystem is full or damaged, provider credentials are invalid, or the provider/network path is broken, repair that infrastructure first.

## 1. Component requirements

### Database

**SQLite is not supported for WebDAV Durable Write-back.**

Use a server database with transactional row-level locking semantics:

- **MySQL: validated and recommended**
- **PostgreSQL: supported as an OpenList server-database target; run the write-back test suite against the exact deployment before production use**
- **SQLite / sqlite3: unsupported**

Do not enable Durable Write-back on the default SQLite database merely because OpenList itself starts successfully.

The consistency model depends on database transactions for:

- same-path receive fencing;
- generation ordering and publication;
- durable receive leases;
- admission reservations and backlog accounting;
- provider COPY/MOVE intent recovery;
- multi-worker and multi-instance coordination;
- restart-safe state transitions.

These paths rely on server-database transaction and row-locking behavior. SQLite's file/process locking model is not treated as equivalent for this component.

### Durable spool

The configured `spool_dir` must be on durable local storage. SSD or NVMe is recommended for sustained Cloud Sync workloads.

The spool is not a disposable temporary directory while a generation is pending. After OpenList acknowledges a PUT, the spool may be the only complete payload available for automatic provider retry.

### Cloud Sync mode

This implementation is designed for **one-way upload from Synology Cloud Sync to WebDAV/OpenList**.

Do not assume identical recovery behavior for bidirectional or download jobs unless those modes have been separately tested.

### Provider

The provider may be 115 or another OpenList storage backend reached through the normal storage-driver path.

Provider failures are background replication failures. They do not invalidate a successfully acknowledged Cloud Sync generation while the durable spool remains available.

## 2. Why Cloud Sync can show "Completed" while OpenList is still working

Durable Write-back separates client success from provider replication success:

```text
Cloud Sync PUT
    |
    v
OpenList receives the complete request body
    |
    v
fsync + durable local spool
    |
    v
canonical metadata committed to the server database
    |
    v
HTTP 201/204 returned to Cloud Sync
    |
    +------ Cloud Sync may show "Completed"
    |
    v
OpenList background worker
    |
    +--> provider upload
    +--> provider verification
    +--> automatic retry/recovery when needed
```

A successful WebDAV PUT means the complete payload has been durably accepted locally and the canonical path, size, mtime, and ETag have been committed.

It does **not** mean the backing provider has already completed upload and verification.

This separation is intentional. It prevents a slow or eventually consistent provider from forcing Cloud Sync to retransmit large files unnecessarily.

## 3. State and action matrix

| State / recovery state | Meaning | Automatic recovery | Operator action |
|---|---|---:|---|
| `queued` | Waiting for a provider worker | Yes | None |
| `uploading` | Provider upload in progress | Yes | None |
| `verifying` | Checking provider state for the current generation | Yes | None |
| `waiting_provider_verification` | Provider evidence is currently inconclusive; OpenList will retry with fresh evidence | Yes | None |
| `restart_recovery` | An interrupted generation is being recovered after OpenList restart | Yes | None |
| ordinary provider error | Upload/list/token/network work failed but durable payload or recovery evidence still exists | Yes | Normally none |
| `missing_spool` | Local spool is missing; OpenList first checks whether the provider already contains the correct generation | Yes, initially | Do not restart Cloud Sync yet |
| `completed` | Current generation has verified provider evidence | Completed | None |
| normal `deleted` | Normal delete or tombstone lifecycle | Yes | None |
| **`needs_cloudsync_rehydrate`** | OpenList has no usable local payload and fresh provider evidence confirms the correct object cannot be recovered | **No** | **Stop/disable and then start/enable the Cloud Sync task to force a fresh reconciliation scan and re-upload** |

A non-empty `last_error` is **not automatically a manual-action signal**.

The recovery classification is more important than a raw error string. Most provider failures should remain inside OpenList's automatic retry loop.

## 4. Normal automatic recovery

The expected self-healing path is:

```text
queued
  |
  v
uploading
  |
  +---- transient provider failure
  |           |
  |           v
  |      queued + retry_at
  |           |
  |           +---- automatic retry
  |
  v
verifying
  |
  +---- provider evidence inconclusive
  |           |
  |           v
  | waiting_provider_verification
  |           |
  |           +---- fresh verification
  |
  v
completed
```

Examples that should normally self-heal without touching Cloud Sync:

- temporary 115/API/network failures;
- upload-token failures;
- provider PUT failure while the durable spool remains available;
- OpenList restart during `uploading`;
- temporarily stale or incomplete provider metadata;
- first observation of a provider divergence;
- verification retry windows;
- remote list/get inconsistencies that have not reached a conclusive loss decision.

Do not stop/start Cloud Sync merely to make these states move faster.

## 5. `missing_spool`: let OpenList recover first

`missing_spool` does **not** immediately mean the file is lost.

It means the local cached payload is no longer available, so OpenList must determine whether the provider already contains the correct generation.

```text
missing_spool
     |
     v
fresh provider verification
     |
     +--> correct provider object exists
     |          |
     |          v
     |       completed
     |
     +--> evidence still inconclusive
     |          |
     |          v
     |   wait and verify again
     |
     +--> provider conclusively missing/divergent
                |
                v
      needs_cloudsync_rehydrate
```

**Operator action while the state is only `missing_spool`: none.**

Restarting Cloud Sync at this stage can create an unnecessary duplicate PUT even though the provider may already contain the correct object.

## 6. `needs_cloudsync_rehydrate`: manual recovery

OpenList reaches `needs_cloudsync_rehydrate` only after the normal automatic recovery path can no longer reconstruct the acknowledged generation:

```text
Cloud Sync previously received success
        |
        v
local durable spool is no longer usable
        |
        v
fresh provider verification is conclusively missing/divergent
        |
        v
OpenList has no payload left to upload by itself
        |
        v
canonical state is exposed as deleted
        |
        v
needs_cloudsync_rehydrate
```

At this point OpenList intentionally makes the object absent from the canonical WebDAV view so that a fresh Cloud Sync reconciliation can rediscover the local source file and PUT it again.

### Required operator procedure

1. Confirm the recovery state is exactly `needs_cloudsync_rehydrate`.
2. Do **not** delete the Cloud Sync task and do not unlink/recreate the entire job.
3. Stop or disable the affected Cloud Sync task completely.
4. Wait until the task is fully stopped and no transfer is active.
5. Start or enable the same Cloud Sync task again.
6. Allow Cloud Sync to perform a fresh reconciliation scan.
7. Confirm the affected path produces a new PUT.
8. Confirm the PUT is followed by PROPFIND verification.
9. Confirm the new OpenList generation progresses through `queued -> uploading -> verifying -> completed`.

### Expected WebDAV trace

A successful rehydrate should broadly look like:

```text
PROPFIND parent (Depth: 1)
    |
    +--> affected object is absent from the canonical WebDAV view
    |
    v
Cloud Sync detects a local-only file
    |
    v
PUT affected-file
    |
    v
HTTP 201/204
    |
    v
PROPFIND affected-file (Depth: 0)
    |
    v
canonical object visible again
```

If Cloud Sync repeatedly sees the object as missing through PROPFIND but never issues a PUT, capture the trace before modifying OpenList or database state manually.

## 7. Normal `deleted` versus `needs_cloudsync_rehydrate`

Do not use `state=deleted` alone to decide that Cloud Sync must be restarted.

Normal DELETE and MOVE operations also use tombstones or deleted state.

The manual-action signal is the recovery classification:

```text
deleted + normal delete lifecycle
    -> automatic / no operator action

deleted + recovery_state=needs_cloudsync_rehydrate
    -> Cloud Sync operator action required
```

The admin UI and API should therefore prioritize Recovery State instead of expecting an operator to infer the required action from the low-level provider state.

## 8. Provider error handling

When OpenList still has a durable payload, provider errors belong to OpenList:

```text
provider PUT error
    |
    v
persist last_error + retry_count + retry_at
    |
    v
automatic retry / verification
    |
    v
completed
```

Examples include:

- 115 upload-initialization errors;
- sign-check or range errors;
- upload-token errors;
- multipart upload failures;
- remote verification failures;
- temporary rate-limit or network failures.

As long as the spool is still available, these errors should not require Cloud Sync to retransmit the entire file.

The current build also logs provider retry context and worker panic stacks to the OpenList log for diagnosis.

Useful filter:

```bash
docker logs op --since 2h 2>&1 | grep -E \
'write-back worker panic|write-back provider retry|write-back delete retry|115 Open'
```

## 9. Restart behavior

Restarting OpenList is not normally a reason to restart Cloud Sync.

Expected behavior after OpenList restart:

- interrupted `uploading` work is recovered from durable state;
- stale receive leases expire and are reclaimed;
- queued payloads return to provider workers;
- verification resumes;
- acknowledged canonical metadata remains stable for Cloud Sync.

Only if recovery ultimately becomes `needs_cloudsync_rehydrate` should the Cloud Sync task itself be stop/started.

## 10. What not to do

Avoid these actions unless performing deliberate incident recovery:

- do not manually delete spool files for queued/uploading/verifying generations;
- do not clear server-database write-back rows just to make the UI look clean;
- do not restart Cloud Sync for every provider retry;
- do not treat the first divergence observation as confirmed data loss;
- do not delete/recreate a Cloud Sync task merely to trigger rehydrate;
- do not use SQLite for Durable Write-back;
- do not directly modify canonical state while a receive or provider operation is active.

## 11. Infrastructure incidents

The following are not ordinary write-back recovery states and may require manual infrastructure work:

- MySQL/PostgreSQL unavailable or unhealthy;
- spool filesystem full, read-only, corrupted, or missing;
- provider credentials expired or revoked;
- provider storage disabled;
- persistent DNS/routing/TLS failure;
- incorrect mount/storage configuration;
- repeated process crashes unrelated to one object;
- database migration or index failures.

After infrastructure is repaired, allow OpenList to run its normal recovery first. Restart Cloud Sync only for rows that actually settle into `needs_cloudsync_rehydrate`.

## 12. Quick decision tree

```text
See an abnormal write-back item
        |
        v
Is recovery_state = needs_cloudsync_rehydrate?
        |
       YES ------------------------------+
        |                                |
        v                                |
Stop/disable Cloud Sync task             |
Start/enable the same task               |
Wait for fresh scan                      |
Confirm new PUT + PROPFIND               |
Confirm COMPLETED                        |
                                         |
       NO                                |
        |                                |
        v                                |
Is infrastructure unhealthy?             |
   |                                     |
 YES -> repair DB/disk/network/provider  |
   |      then let OpenList recover      |
   |                                     |
  NO                                     |
   |                                     |
   v                                     |
Do not touch Cloud Sync                   |
Let OpenList retry/verify automatically --+
```

## 13. Operational summary

- Cloud Sync owns the source copy.
- OpenList owns durable acceptance, provider upload, retry, and verification.
- MySQL is the validated and recommended database.
- PostgreSQL is the other supported server-database target; validate the exact deployment before production use.
- SQLite is unsupported for Durable Write-back.
- Do not restart Cloud Sync for ordinary provider errors.
- Do not restart Cloud Sync for `missing_spool` while OpenList is still verifying the provider.
- Restart the affected Cloud Sync task only for `needs_cloudsync_rehydrate`, by fully stopping/disabling and then starting/enabling the same task to force a fresh scan.
