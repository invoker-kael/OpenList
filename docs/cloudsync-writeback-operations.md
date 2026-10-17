# Cloud Sync Write-back Operations

> This mode supports one-way Cloud Sync upload and backup. Do not use it for bidirectional synchronization.

## Normal flow

1. OpenList receives the complete WebDAV `PUT`, writes it durably to the local spool, and acknowledges Cloud Sync.
2. Before uploading to 115, OpenList performs a fresh parent `LIST`. If any same-name object has the same size and SHA-1, the task completes without another upload.
3. Otherwise OpenList uploads the spool payload, then confirms it with another fresh `LIST` and the same size + SHA-1 check.
4. A matching object completes the task. Timeouts, rate limits, WAF responses, API errors, and missing SHA-1 remain **Processing**; they are not evidence that a file is absent.
5. Completed objects receive low-frequency fresh checks. A match keeps them completed. Only three successful fresh checks with no match mark one failed; provider/API errors do not count.

Cloud Sync remains the source copy. OpenList's local spool is the durable handoff until provider confirmation. Keep the spool on persistent local storage and do not manually remove files used by active tasks.

## Overview statuses

Overview shows only these four task states:

| Status | Meaning |
|---|---|
| Receiving | Cloud Sync is sending the PUT body. |
| Processing | The payload is durable and OpenList is checking or writing it to 115. |
| Completed | 115 has matching content evidence. |
| Failed | OpenList exposes the path as missing so Cloud Sync can upload it again. |

Internal queue, upload, retry, and verification details do not create additional Overview statuses.

## Full rebuild

The rebuild request returns promptly and runs in the background. A current PUT finishes first. During rebuild, `GET`, `HEAD`, and `PROPFIND` continue; new writes receive `503` with `Retry-After` so Cloud Sync can retry.

After the active PUTs and worker stop, rebuild clears OpenList's writeback objects, history, receive fences, reservations, provider-operation records, and local `.part` / `.data` spool files, including orphans. It clears in-memory provider caches and starts the worker again. It never deletes or changes files on 115.

## Database and diagnostics

Write-back state uses the configured MySQL database. Admin API access requires OpenList authentication. Diagnose row counts and spool references before considering database repair; do not clear writeback rows or spool files while OpenList is active.
