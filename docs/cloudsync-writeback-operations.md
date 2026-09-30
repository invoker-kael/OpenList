# Cloud Sync Write-back Operations Guide

> [!WARNING]
> **This feature is only for one-way upload/backup jobs. Bidirectional Cloud Sync is unsupported.**
>
> Do not enable two-way synchronization against a Durable Write-back path. The design assumes Cloud Sync is the source of truth and the provider is a background replica. Remote-side changes are not intended to be merged back into Cloud Sync and may be masked, conflict with canonical state, or be overwritten by a later upload.

This guide covers day-to-day operation of WebDAV Durable Write-back with **one-way Synology Cloud Sync** jobs. Most recovery is automatic; the sections below focus on what the UI means and when operator action is genuinely useful.

> **Development note:** This feature was developed with AI assistance and validated through human testing.

## Core rule

For normal operation, there is one simple rule:

**Only `needs_cloudsync_rehydrate` requires Cloud Sync or operator intervention. Other normal write-back recovery states are designed to recover automatically.**

Infrastructure failures are the exception. If MySQL is unavailable, the spool filesystem is full or damaged, credentials have expired, or the provider/network path is broken, fix that underlying issue first and then let OpenList resume recovery.


## 1. Component requirements

### Database

**Durable Write-back in this fork is MySQL-only.**

- MySQL: supported and validated.
- SQLite / sqlite3: unsupported.
- Other database backends: outside the supported Durable Write-back path for this fork.

### Durable spool

Use reliable persistent local storage. SSD/NVMe is recommended for sustained Cloud Sync uploads. Do not manually delete spool payloads for active generations.

### Cloud Sync mode

This design targets **one-way Synology Cloud Sync upload/backup to WebDAV/OpenList**, including client-side encrypted jobs. **Bidirectional sync is not supported.** If the same provider path is also modified from the remote side, treat those changes as outside the supported consistency model.

### Provider

115 Open is the primary validated provider. Other OpenList drivers can use the same flow, but provider verification capability may differ.

## 2. Why Cloud Sync can be “done” while OpenList is still working

Durable Write-back still separates client success from provider replication, but a normal successful provider PUT **no longer schedules LIST/GET/hash verification**:

```text
Cloud Sync PUT
    |
    v
OpenList receives the full request body
    |
    v
fsync + durable local spool
    |
    v
canonical metadata committed to MySQL
    |
    v
HTTP 201/204 returned to Cloud Sync
    |
    v
OpenList uploads to the provider in the background
    |
    +--> PUT succeeds -> COMPLETED
    |
    +--> PUT fails -> retry_at/backoff -> direct PUT retry
```

Cloud Sync completion still means the object is durably accepted by OpenList, not that 115 has already finished. The normal provider lifecycle is now only queue -> upload -> complete/retry, so routine verification work cannot accumulate and starve uploads.

Remote checks remain only for uncertain recovery cases such as a process restart during UPLOADING, a missing durable spool, or MOVE/DELETE recovery that must determine the remote outcome.

## 3. User-facing states and progress

### Active tasks

| User status | Meaning | Action |
|---|---|---|
| **Receiving** | Cloud Sync is sending the PUT body | None |
| **Syncing** | OpenList is queueing, uploading, verifying, or auto-recovering the provider replica | None |
| **Waiting for Cloud Sync re-upload** | The old generation cannot safely converge and needs a fresh Cloud Sync PUT | Stop/disable, then start/enable the same Cloud Sync task |
| **Deleted** | Delete lifecycle | None |

The Active status filter is an Excel-style table-header dropdown. Internal states remain available in Advanced information.

The **Progress** column shows real progress only:
- Receiving = Cloud Sync bytes received / declared PUT size.
- Uploading = native OpenList provider upload progress.
- Queueing / verification = no fake percentage.

### History

History is per-generation audit data. Its normal final statuses are only:

- **Completed** — includes normal completion and recovered completion.
- **Waiting for Cloud Sync re-upload** — old generation unresolved, waiting for a newer same-path PUT.
- **Deleted** — completed delete outcome.

Transient states such as receiving/uploading/verifying are not History final statuses.

## 4. Normal automatic recovery

Ordinary provider failures now use “fail, back off, and re-upload” without entering a verification queue first:

```text
queued
  |
  v
uploading
  |
  +---- PUT succeeds ----> completed
  |
  +---- PUT fails
          |
          v
      queued + retry_at
          |
          +---- direct re-upload after backoff
```

This covers temporary 115/API/network failures, upload-token failures, rate limiting, and ordinary 5xx responses. As long as the durable spool still exists, OpenList owns the retry and Cloud Sync does not need to resend the file.

Recovery checks are reserved for uncertain outcomes where OpenList cannot safely infer the provider state, such as a crash while UPLOADING or a missing local spool.

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


## 6. Final Cloud Sync re-upload recovery

When OpenList has exhausted safe automatic provider recovery, the generation moves to the final Cloud Sync re-upload path.

The canonical Durable ACK remains visible. OpenList does **not** hide/delete the object simply to force a rescan.

### Required operator procedure

1. Fully stop/disable the affected Cloud Sync task.
2. Wait until transfers stop.
3. Start/enable the same task.
4. Let Cloud Sync perform a fresh reconciliation scan.
5. Confirm a new PUT is issued for the affected path.
6. Confirm the newer generation reaches Completed.

A newer same-path PUT supersedes the old incident by generation/ACK ordering. Its encrypted payload size or SHA-1 does not need to equal the old generation.

There is no OpenList manual retransmit button.

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

As long as OpenList still has the durable payload, provider errors go directly back to upload retry:

```text
provider PUT error
    |
    v
persist last_error + retry_count + retry_at
    |
    v
wait for backoff
    |
    v
PUT again
    |
    +--> success -> completed
    +--> failure -> next backoff
```

Provider/WAF responses such as 405, 429, and 5xx still use a cooldown, but once the cooldown expires OpenList re-uploads directly instead of probing first. Provider probes are reserved for uncertain restart or missing-spool recovery.

Useful log filter:

```bash
docker logs op --since 2h 2>&1 | grep -E \
'write-back worker panic|write-back provider retry|115 Open'
```

## 9. Restart behavior

A normal OpenList restart should be transparent to Cloud Sync in most cases.

After restart, OpenList handles the two interrupted stages differently:

- a PUT interrupted before Durable ACK is not resumable from its partial body; its receive lease expires within about one minute while a live PUT renews the lease every 10 seconds, and Cloud Sync may retry that file later after continuing other work;
- an already-ACKed file interrupted during provider upload is recovered entirely by OpenList from the durable spool and does not depend on Cloud Sync retransmission;
- restart-interrupted provider uploads first use fresh verification with a bounded roughly one-minute window, then automatically return to provider upload when the provider is still conclusively divergent and the spool is intact;
- queued payloads return to provider workers;
- acknowledged canonical metadata remains stable for Cloud Sync.

Only if recovery ultimately becomes `needs_cloudsync_rehydrate` should the Cloud Sync task itself be stop/started.

## 10. Recommended operating boundaries

The safest approach is to let OpenList finish its own recovery whenever it still has enough durable state to do so.

In normal operation:

- keep spool files for queued/uploading/verifying generations intact;
- keep write-back database rows unless you are performing a deliberate clean test reset;
- leave Cloud Sync running during ordinary provider retries and verification;
- treat the first divergence observation as evidence to verify, not immediate proof of data loss;
- stop/start the existing Cloud Sync task only when the UI reaches the final re-upload state;
- use MySQL for Durable Write-back;
- avoid manually changing canonical state while a receive or provider operation is active.

## 11. Infrastructure incidents

The following are not ordinary write-back recovery states and may require manual infrastructure work:

- MySQL unavailable or unhealthy;
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
- OpenList owns durable acceptance, provider upload, retry, verification, and recovery.
- Durable Write-back in this fork is MySQL-only.
- Normal provider errors stay inside OpenList automatic recovery.
- Active shows the simplified Cloud Sync lifecycle and real receive/upload progress.
- History shows only meaningful final outcomes; recovered completion is simply Completed.
- The final fallback keeps canonical ACK visible and waits for a fresh same-path Cloud Sync PUT.

## Remote hash difference and final recovery

`remote_hash_mismatch` is a confirmed provider-divergence cause, not a manual retransmit state.

OpenList requires stable fresh provider evidence before accepting the mismatch. If the durable spool is still available, OpenList performs at most one automatic provider repair upload and verifies again. A fresh provider pre-check can skip that upload if the provider has already converged.

If the same generation still cannot converge after that single automatic repair, OpenList stops provider retry loops and transitions the item to `waiting_cloudsync_reupload` with `cloud_sync_reupload_required=true`. The root cause remains `remote_hash_mismatch` for diagnostics, but the operator action is only:

1. fully stop/disable the affected Cloud Sync task;
2. start/enable the same task;
3. allow a fresh reconciliation scan;
4. confirm Cloud Sync issues a new PUT for the affected path;
5. verify the new generation reaches `completed`.

Once OpenList accepts a newer PUT for the same path, that newer generation supersedes the old recovery incident. Active shows the current receive/sync progress, while History keeps the old incident as audit data and presents the normal user-facing final outcome as **Completed** after the newer generation converges. Recovery correlation does not require the new payload SHA-1 or size to equal the failed generation, because Cloud Sync encryption or a legitimate local edit can change the bytes; same-path generation/ACK ordering is authoritative.

There is no manual re-upload action in OpenList. When OpenList can no longer safely converge the provider replica, Cloud Sync is the source-of-truth recovery path.
