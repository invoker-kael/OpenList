# Cloud Sync Write-back Operations

> This mode supports one-way Cloud Sync upload and backup. Do not use it for bidirectional synchronization.

## Normal flow

1. OpenList receives the complete WebDAV `PUT`, writes it durably to the local spool, and acknowledges Cloud Sync.
2. Before uploading to 115, OpenList checks a recent MySQL remote-index snapshot for an exact same-name, size and SHA-1 match. If none is available, it performs a fresh parent `LIST`. An exact match completes without another upload; cached misses never prove that a remote file is absent.
3. Otherwise OpenList uploads the local spool. A successful provider API response completes the task; normal uploads do not wait for a second `LIST`.
4. Timeouts, rate limits, WAF responses, and API errors keep the durable spool in **Processing** and retry with backoff. Before a retry, OpenList performs a fresh `LIST` to catch an upload accepted by 115 whose response was lost. Provider/API errors never mean that the file is absent.
5. Completed objects are checked only at a low frequency after the local cache is released. Any same-name size + SHA-1 match keeps the object completed. API errors or missing SHA-1 do not count as missing; three consecutive successful fresh LISTs without a match are required before Cloud Sync sees the path as missing and resends it.

A repeated PUT with the same size and SHA-1 coalesces into the existing generation, including after completion. Different content starts a new generation. This lets Cloud Sync resend without creating another task or transmitting the same payload to 115 again.

Cloud Sync remains the source copy. OpenList's local spool is the durable handoff until 115 accepts the upload. Keep the spool on persistent local storage and do not manually remove files used by active tasks.

The Overview remote-cache button refreshes only parent directories already tracked by Writeback. Fresh LIST concurrency is capped by the Writeback setting, while the selected 115 storage's `LimitRate` controls the actual request pace, including each page of large listings. Lower the storage's `LimitRate` for WAF-sensitive accounts.

## Overview statuses

Overview shows only these four task states:

| Status | Meaning |
|---|---|
| Receiving | Cloud Sync is sending the PUT body. |
| Processing | The payload is durable and OpenList is checking or writing it to 115. |
| Completed | 115 accepted the upload, or a fresh listing found exact matching content. |
| Failed | OpenList confirmed that the acknowledged payload is unavailable and exposes the path for Cloud Sync to resend. |

Internal queue, upload, retry, and verification details do not create additional Overview statuses.

Per-path Advanced details show coalesced duplicate client PUTs, 115 upload attempts, and bytes reported by the provider progress callback on attempts after the first.

## Full rebuild

The rebuild request returns promptly and runs in the background. A current PUT finishes first. During rebuild, `GET`, `HEAD`, and `PROPFIND` continue; new writes receive `503` with `Retry-After` so Cloud Sync can retry.

After the active PUTs, remote-index refresh and worker stop, rebuild clears OpenList's writeback objects, history, receive fences, reservations, provider-operation records, cached remote-index snapshots, and local `.part` / `.data` spool files, including orphans. It clears in-memory provider caches and starts the worker again. It never deletes or changes files on 115.

## Database and diagnostics

Write-back state uses the configured MySQL database. Admin API access requires OpenList authentication. Diagnose row counts and spool references before considering database repair; do not clear writeback rows or spool files while OpenList is active.
