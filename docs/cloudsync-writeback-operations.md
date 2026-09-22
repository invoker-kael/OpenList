# Cloud Sync Write-back Operations Guide / Cloud Sync 写回运维指南

This is the operator-facing guide for the WebDAV Durable Write-back mode used with **one-way Synology Cloud Sync** jobs.

本文档面向实际运维，说明 WebDAV Durable Write-back 与**单向 Synology Cloud Sync** 配合时，各状态代表什么、哪些状态会自动恢复、什么时候需要人工操作，以及如何验证恢复已经完成。

> **Core rule / 核心规则**
>
> **`needs_cloudsync_rehydrate` is the only normal write-back recovery state that requires Cloud Sync/operator intervention. All other normal write-back recovery states are designed to self-heal.**
>
> **`needs_cloudsync_rehydrate` 是正常写回恢复流程中唯一需要 Cloud Sync / 人工介入的状态。其他正常写回恢复状态都应由 OpenList 自动恢复。**

Infrastructure failures are outside this rule. A failed MySQL/PostgreSQL service, a full or failed spool disk, invalid provider credentials, a broken network path, or a disabled/misconfigured storage backend must be repaired as infrastructure incidents first.

上述规则不包含基础设施故障。MySQL/PostgreSQL 不可用、spool 磁盘满或损坏、远端存储凭据失效、网络中断、存储后端被禁用或配置错误，都必须先按基础设施故障处理。

---

## 1. Component requirements / 组件要求

### Database / 数据库

**SQLite is explicitly unsupported for WebDAV Durable Write-back.**

**WebDAV Durable Write-back 明确不支持 SQLite。**

Use a server database that provides transactional row-level locking semantics:

- **MySQL: validated and recommended / 已验证并推荐**
- **PostgreSQL: supported OpenList server-database target; run the write-back test suite for the exact deployment before production use / OpenList 支持的服务端数据库目标，生产前应针对实际版本运行 write-back 测试**
- **SQLite / sqlite3: unsupported / 不支持**

Do **not** enable this component on OpenList's default SQLite database merely because the application starts successfully.

不要因为 OpenList 本身能够使用默认 SQLite 启动，就认为 Durable Write-back 也适用。

The write-back consistency model uses database transactions for:

- same-path receive fencing;
- generation ordering and publication;
- durable receive leases;
- admission reservations and backlog control;
- provider COPY/MOVE intent recovery;
- multi-worker and multi-instance coordination;
- restart-safe state transitions.

写回一致性依赖数据库事务来完成：

- 同路径接收 fence；
- generation 顺序与发布；
- durable receive lease；
- admission reservation 与 backlog 控制；
- provider COPY/MOVE intent 恢复；
- 多 worker / 多实例协调；
- 重启后的状态恢复。

These paths rely on server-database row locking and transaction behavior. SQLite's file/process locking model is not treated as equivalent for this component.

这些路径依赖服务端数据库的行级锁和事务语义。SQLite 的文件/进程锁模型不视为本组件的等价实现。

### Durable spool / 本地持久 spool

The configured `spool_dir` must be on durable local storage. SSD/NVMe is preferred for sustained Cloud Sync workloads.

配置的 `spool_dir` 必须位于可靠的本地持久存储上。持续的大量 Cloud Sync 上传建议使用 SSD/NVMe。

The spool is not a disposable temporary directory while a generation is pending. After OpenList ACKs the PUT, the spool may be the only complete copy available for automatic provider retry.

当 generation 尚未完成远端验证时，spool 不是可以随意删除的临时目录。OpenList 对 PUT 返回成功后，spool 可能是后台自动重试时唯一完整的数据副本。

### Cloud Sync mode / Cloud Sync 模式

This implementation is designed for **one-way upload from Synology Cloud Sync to WebDAV/OpenList**.

本实现针对 **Synology Cloud Sync → WebDAV/OpenList 的单向上传**。

Do not assume the same recovery semantics for bidirectional or download jobs unless they have been separately tested.

双向同步或下载任务不能默认套用本文的恢复语义，除非已经单独验证。

### Provider / 远端存储

The provider may be 115 or another OpenList storage backend reached through the normal OpenList driver path. Provider failures are background replication failures and do not automatically invalidate a successfully ACKed Cloud Sync generation while the durable spool is still available.

远端可以是 115，也可以是通过 OpenList 正常 storage driver 路径访问的其他后端。只要 durable spool 仍可用，provider 失败属于后台复制失败，不会自动否定已经 ACK 给 Cloud Sync 的 generation。

---

## 2. Why Cloud Sync can show “Completed” while OpenList is still working / 为什么 Cloud Sync 显示“完成”，OpenList 仍可能继续上传

Durable Write-back separates **client success** from **provider replication success**:

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
canonical metadata committed to MySQL/PostgreSQL
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

A successful WebDAV PUT therefore means:

- the complete payload was durably accepted locally;
- the canonical path/size/mtime/ETag is committed;
- Cloud Sync no longer needs to retransmit merely because the provider is slow.

It does **not** mean the backing provider has already completed upload and verification.

因此 WebDAV PUT 成功表示：

- 文件已完整、持久地落到本地 spool；
- canonical path/size/mtime/ETag 已提交；
- 不需要因为远端较慢就让 Cloud Sync 重复发送整个文件。

它**不代表**远端存储已经完成上传和验证。

This separation is intentional and is the reason large-file Cloud Sync jobs can converge without waiting for provider propagation on every PUT.

---

## 3. State and action matrix / 状态与操作矩阵

| State / Recovery state | Meaning / 含义 | Automatic recovery / 自动恢复 | Operator action / 人工操作 |
|---|---|---:|---|
| `queued` | Waiting for a provider worker / 等待后台 worker | Yes / 是 | None / 无 |
| `uploading` | Provider upload in progress / 正在上传远端 | Yes / 是 | None / 无 |
| `verifying` | Checking provider state for the current generation / 正在验证当前 generation 的远端状态 | Yes / 是 | None / 无 |
| `waiting_provider_verification` | Current provider evidence is inconclusive; OpenList will retry with fresh evidence / 当前远端证据不足，OpenList 会等待并重新获取新证据 | Yes / 是 | None / 无 |
| `restart_recovery` | An interrupted generation is being recovered after OpenList restart / OpenList 重启后恢复中断 generation | Yes / 是 | None / 无 |
| ordinary provider error / 普通 provider error | Upload/list/token/network operation failed but durable payload/recovery evidence still exists / 远端 upload/list/token/网络失败，但本地 payload 或恢复证据仍存在 | Yes / 是 | Normally none / 通常无需操作 |
| `missing_spool` | Local spool is missing; OpenList first checks whether the provider already contains the correct generation / 本地 spool 缺失；OpenList 会先确认远端是否已经存在正确 generation | Yes, initially / 初始阶段是 | **Do not restart Cloud Sync yet / 暂时不要重启 Cloud Sync** |
| `completed` | Current generation has verified provider evidence / 当前 generation 已获得远端验证证据 | Completed / 已完成 | None / 无 |
| normal `deleted` | Normal delete/tombstone lifecycle / 正常删除或 tombstone 生命周期 | Yes / 是 | None / 无 |
| **`needs_cloudsync_rehydrate`** | OpenList no longer has a usable payload and fresh provider evidence confirms the correct object cannot be recovered / OpenList 已无可用 payload，且刷新后的远端证据确认正确对象无法恢复 | **No / 否** | **Stop/disable then start/enable the Cloud Sync task to force a fresh reconciliation scan and re-upload / 彻底停止/停用后重新启动/启用 Cloud Sync 任务，强制重新扫描和上传** |

### Important distinction / 重要区别

A non-empty `last_error` is **not automatically a manual-action signal**.

`last_error` 非空**不等于必须人工处理**。

The operator should use the recovery classification and the availability of the durable spool/provider evidence. Most provider failures are expected to remain entirely inside OpenList's automatic retry loop.

运维判断应以 recovery state 和 durable spool/provider evidence 为准。绝大多数 provider 错误都应该留在 OpenList 自己的自动重试闭环中。

---

## 4. Normal automatic recovery / 正常自动恢复

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
- upload token failures;
- failed provider PUT while the durable spool remains available;
- OpenList restart during `uploading`;
- temporarily stale or incomplete provider metadata;
- first observation of a provider divergence;
- verification retry windows;
- remote list/get inconsistencies that have not yet reached a conclusive loss decision.

以下情况正常都不需要操作 Cloud Sync：

- 115/API/网络临时失败；
- upload token 获取失败；
- durable spool 仍存在时的 provider PUT 失败；
- `uploading` 时 OpenList 重启；
- provider metadata 暂时陈旧或不完整；
- 第一次发现远端 divergence；
- verification 等待/重试窗口；
- 尚未形成明确丢失结论的远端 list/get 不一致。

Do not stop/start Cloud Sync merely to make these states move faster.

不要为了让这些状态“快一点”而反复停止/启动 Cloud Sync。

---

## 5. `missing_spool`: wait for OpenList first / `missing_spool`：先让 OpenList 自己恢复

`missing_spool` does **not** immediately mean data is lost.

`missing_spool` **不等于文件已经丢失**。

It means the local cached payload is no longer available, so OpenList must determine whether the provider already contains the correct generation.

它表示本地缓存 payload 已不可用，因此 OpenList 必须先判断远端是否已经存在正确 generation。

Expected flow:

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

**Operator action while state is only `missing_spool`: none.**

**当状态还只是 `missing_spool` 时：不要操作 Cloud Sync。**

Restarting Cloud Sync here can create a redundant PUT even though the provider may already contain the correct object.

此时过早重启 Cloud Sync，可能在远端其实已经存在正确对象的情况下制造重复 PUT。

---

## 6. `needs_cloudsync_rehydrate`: the manual recovery state / 唯一需要人工介入的正常恢复状态

OpenList enters `needs_cloudsync_rehydrate` only after the normal automatic recovery path can no longer reconstruct the acknowledged generation:

```text
Cloud Sync previously received success
        |
        v
local durable spool is no longer usable
        |
        v
fresh provider verification is conclusively divergent/missing
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

At this point OpenList intentionally makes the object absent from the WebDAV canonical view so that a fresh Cloud Sync reconciliation can rediscover the local source file and PUT it again.

到这个阶段，OpenList 会主动让该对象从 WebDAV canonical view 中消失，使 Cloud Sync 在下一次完整 reconciliation 时看到：

```text
Synology local source: file exists
WebDAV/OpenList:       file missing
```

然后由 Cloud Sync 重新执行 PUT。

### Required operator procedure / 必须执行的人工步骤

1. **Confirm the recovery state is exactly `needs_cloudsync_rehydrate`.**  
   **确认 Recovery State 确实是 `needs_cloudsync_rehydrate`。**

2. **Do not delete the Cloud Sync task and do not unlink/recreate the entire job.**  
   **不要删除 Cloud Sync 任务，也不要直接 unlink 后重建整个任务。**

3. **Stop/disable the affected Cloud Sync task completely. Do not rely on waiting for its normal background scan.**  
   **彻底停止/停用对应的 Cloud Sync 任务，不要只等它后台什么时候自行扫描。**

4. Wait until the task is fully stopped and no transfer is active.  
   等待任务完全停止，确认没有正在进行的传输。

5. **Start/enable the same Cloud Sync task again.**  
   **重新启动/启用同一个 Cloud Sync 任务。**

6. Let Cloud Sync perform a fresh reconciliation scan.  
   等待 Cloud Sync 重新扫描本地与 WebDAV 状态。

7. Verify that the affected path produces a new PUT and is followed by PROPFIND verification.  
   确认受影响路径重新出现 PUT，并在之后看到 PROPFIND 验证。

8. Verify the new OpenList generation proceeds through `queued -> uploading -> verifying -> completed`.  
   确认新的 OpenList generation 进入 `queued -> uploading -> verifying -> completed`。

### Expected WebDAV trace / 预期 WebDAV trace

A successful rehydrate should look broadly like:

```text
PROPFIND parent (Depth: 1)
    |
    +--> affected object is absent from the canonical WebDAV view
    |
    v
Cloud Sync detects local-only file
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

If Cloud Sync performs repeated PROPFINDs, sees the file as missing, but never issues a PUT, capture the trace before modifying OpenList state manually.

如果 Cloud Sync 已经多次 PROPFIND 并明确看到远端缺失，但始终没有重新 PUT，请先保留 trace，不要手工修改 OpenList/MySQL 状态。

---

## 7. Normal `deleted` vs `needs_cloudsync_rehydrate` / 正常 deleted 与 rehydrate 的区别

Do not use `state=deleted` alone to decide that Cloud Sync must be restarted.

不能只看到 `state=deleted` 就判断要重启 Cloud Sync。

A normal DELETE/MOVE operation also uses tombstone/deleted state. The manual-action signal is the **recovery classification**:

```text
deleted + normal delete lifecycle
    -> automatic/no action

deleted + recovery_state=needs_cloudsync_rehydrate
    -> Cloud Sync operator action required
```

后台 UI/接口应优先显示 Recovery State，而不是要求运维人员仅凭底层 provider state 猜测。

---

## 8. Provider error handling / Provider 错误处理

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

- 115 upload initialization errors;
- sign-check/range errors;
- upload token errors;
- multipart upload failures;
- remote verification failures;
- temporary rate limit or network failures.

只要 spool 还存在，这些错误不应该要求 Cloud Sync 重传整个文件。

The current build also logs write-back provider retry context and worker panic stacks to the OpenList log so a provider-side failure can be diagnosed without turning every retry into operator action.

当前版本还会把 provider retry 上下文和 worker panic stack 写入 OpenList 日志，便于定位问题，而不是把每次后台重试都变成人工操作。

---

## 9. Restart behavior / OpenList 重启行为

OpenList restart is not normally a reason to restart Cloud Sync.

OpenList 重启通常**不需要**同时重启 Cloud Sync。

Expected behavior:

- `uploading` work is recovered from durable state;
- stale receive leases expire/recover;
- queued payloads return to the provider worker;
- verification resumes;
- canonical ACKed metadata remains stable for Cloud Sync.

预期行为：

- 中断的 `uploading` 从 durable state 恢复；
- stale receive lease 被回收；
- queued payload 重新进入 worker；
- verification 继续；
- 已 ACK 的 canonical metadata 对 Cloud Sync 保持稳定。

Only when recovery ultimately becomes `needs_cloudsync_rehydrate` should the Cloud Sync task itself be stop/started.

只有最终进入 `needs_cloudsync_rehydrate` 时，才需要对 Cloud Sync 任务执行停止/启动。

---

## 10. What not to do / 不要这样做

Avoid these actions unless performing deliberate incident recovery:

- do not manually delete spool files for queued/uploading/verifying generations;
- do not clear MySQL/PostgreSQL write-back rows merely to make the UI look clean;
- do not restart Cloud Sync for every provider retry;
- do not treat the first divergence observation as confirmed data loss;
- do not delete/recreate a Cloud Sync task merely to trigger rehydrate;
- do not use SQLite for Durable Write-back;
- do not directly modify canonical state while a receive or provider operation is active.

除非明确执行事故恢复，否则不要：

- 手工删除 queued/uploading/verifying generation 的 spool；
- 为了“页面干净”直接清理 MySQL/PostgreSQL 的 write-back 行；
- 每次 provider retry 都去重启 Cloud Sync；
- 第一次发现 divergence 就认定数据丢失；
- 为触发 rehydrate 而删除/重建整个 Cloud Sync 任务；
- 使用 SQLite 承载 Durable Write-back；
- 在 receive/provider operation 正在运行时手改 canonical state。

---

## 11. Monitoring and diagnostics / 监控与诊断

Useful container log filter:

```bash
docker logs op --since 2h 2>&1 | grep -E \
'write-back worker panic|write-back provider retry|write-back delete retry|115 Open'
```

For a worker panic, the log should include the task ID and stack trace.

如果 worker 出现 panic，日志应包含 task ID 和 stack trace。

For provider retries, the log includes context such as:

```text
id
path
generation
state
size
retry
error
```

The admin monitor should be interpreted in this order:

```text
Recovery State
    |
    +--> needs_cloudsync_rehydrate
    |       -> operator action
    |
    +--> missing_spool / restart_recovery / waiting_provider_verification
    |       -> automatic recovery
    |
    +--> empty
            -> inspect provider state + last_error only if needed
```

后台判断优先级应为：

```text
Recovery State
    |
    +--> needs_cloudsync_rehydrate
    |       -> 人工操作
    |
    +--> missing_spool / restart_recovery / waiting_provider_verification
    |       -> 自动恢复
    |
    +--> 空
            -> 有需要时再结合 provider state + last_error 排查
```

---

## 12. Infrastructure incidents / 基础设施故障

The following are not ordinary write-back recovery states and can require manual infrastructure work:

- MySQL/PostgreSQL unavailable or unhealthy;
- spool filesystem full, read-only, corrupted, or missing;
- provider credentials expired/revoked;
- provider storage disabled;
- persistent DNS/routing/TLS failure;
- incorrect mount/storage configuration;
- repeated process crashes unrelated to one object;
- database migration/index failures.

以下不是普通状态机自动恢复范围，需要先处理基础设施：

- MySQL/PostgreSQL 不可用或异常；
- spool 文件系统满、只读、损坏或丢失；
- provider 凭据过期/撤销；
- provider storage 被禁用；
- 持续 DNS/路由/TLS 故障；
- mount/storage 配置错误；
- 非单一对象导致的持续进程崩溃；
- 数据库 migration/index 失败。

After infrastructure is repaired, allow OpenList to run its normal recovery first. Restart Cloud Sync only for rows that actually settle into `needs_cloudsync_rehydrate`.

基础设施恢复后，先让 OpenList 自己跑恢复流程。只有真正最终落到 `needs_cloudsync_rehydrate` 的对象才需要重启对应 Cloud Sync 任务。

---

## 13. Quick decision tree / 快速判断

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

中文：

```text
发现 Write-back 异常
        |
        v
Recovery State 是否为 needs_cloudsync_rehydrate？
        |
       是 --------------------------------+
        |                                 |
        v                                 |
彻底停止/停用 Cloud Sync 任务             |
重新启动/启用同一个任务                    |
等待重新扫描                              |
确认出现新的 PUT + PROPFIND               |
确认最终 COMPLETED                        |
                                          |
       否                                 |
        |                                 |
        v                                 |
基础设施是否异常？                         |
   |                                      |
  是 -> 修复数据库/磁盘/网络/provider      |
   |      然后让 OpenList 自动恢复         |
   |                                      |
  否                                      |
   |                                      |
   v                                      |
不要操作 Cloud Sync                       |
等待 OpenList 自动 retry/verify -----------+
```

---

## 14. Operational summary / 运维总结

For normal write-back operation:

- **Cloud Sync owns the source copy.**
- **OpenList owns durable acceptance, provider upload, retry, and verification.**
- **MySQL is the validated/recommended database; PostgreSQL is the other server-database target. SQLite is not supported for this component.**
- **Do not restart Cloud Sync for ordinary provider errors.**
- **Do not restart Cloud Sync for `missing_spool` while OpenList is still verifying the provider.**
- **Restart the affected Cloud Sync task only for `needs_cloudsync_rehydrate`, by fully stopping/disabling and then starting/enabling the same task to force a fresh scan.**

正常运维原则：

- **Cloud Sync 负责保留源文件。**
- **OpenList 负责 durable 接收、远端上传、重试和验证。**
- **数据库推荐并已验证 MySQL；PostgreSQL 是另一个服务端数据库目标。SQLite 不适用于本组件。**
- **普通 provider 错误不要重启 Cloud Sync。**
- **`missing_spool` 仍在远端验证阶段时不要重启 Cloud Sync。**
- **只有 `needs_cloudsync_rehydrate` 才需要人工完整停止/停用并重新启动/启用对应 Cloud Sync 任务，以强制重新扫描。**
