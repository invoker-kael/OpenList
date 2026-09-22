# Cloud Sync Write-back 运维指南

本文档用于运维 **Synology Cloud Sync 单向上传**场景下的 WebDAV Durable Write-back。

## 核心规则

**`needs_cloudsync_rehydrate` 是正常写回恢复流程中唯一需要 Cloud Sync 或人工介入的状态。其他正常写回恢复状态都应由 OpenList 自动恢复。**

基础设施故障不属于这条规则。如果数据库不可用、spool 文件系统已满或损坏、远端存储凭据失效、网络链路异常，应先修复基础设施。

## 1. 组件要求

### 数据库

**WebDAV Durable Write-back 明确不支持 SQLite。**

必须使用具备事务和行级锁语义的服务端数据库：

- **MySQL：已验证并推荐**
- **PostgreSQL：属于 OpenList 支持的服务端数据库目标；生产使用前应针对实际部署版本运行 write-back 测试**
- **SQLite / sqlite3：不支持**

不要因为 OpenList 本身可以使用默认 SQLite 正常启动，就认为 Durable Write-back 也适用。

写回一致性模型依赖数据库事务完成：

- 同路径接收 fence；
- generation 顺序与发布；
- durable receive lease；
- admission reservation 与 backlog 统计；
- provider COPY/MOVE intent 恢复；
- 多 worker 和多实例协调；
- 重启后的安全状态恢复。

这些路径依赖服务端数据库的事务和行级锁行为。SQLite 的文件/进程锁模型不视为本组件的等价实现。

### Durable spool

配置的 `spool_dir` 必须位于可靠的本地持久存储上。持续大量 Cloud Sync 上传建议使用 SSD 或 NVMe。

当 generation 尚未完成时，spool 不是可以随意删除的临时目录。OpenList 已经对 PUT 返回成功后，spool 可能是后台自动重试时唯一完整的数据副本。

### Cloud Sync 模式

本实现针对 **Synology Cloud Sync 到 WebDAV/OpenList 的单向上传**。

双向同步或下载任务不能默认套用本文的恢复语义，除非已经单独验证。

### 远端存储

远端可以是 115，也可以是通过 OpenList 正常 storage driver 路径访问的其他后端。

只要 durable spool 仍可用，provider 失败属于后台复制失败，不会自动否定已经成功 ACK 给 Cloud Sync 的 generation。

## 2. 为什么 Cloud Sync 显示“完成”，OpenList 仍可能继续工作

Durable Write-back 将客户端成功和远端复制成功分开：

```text
Cloud Sync PUT
    |
    v
OpenList 完整接收请求体
    |
    v
fsync + 本地持久 spool
    |
    v
canonical metadata 提交到服务端数据库
    |
    v
HTTP 201/204 返回给 Cloud Sync
    |
    +------ Cloud Sync 此时可能显示“完成”
    |
    v
OpenList 后台 worker
    |
    +--> 上传到 provider
    +--> provider 验证
    +--> 必要时自动重试和恢复
```

WebDAV PUT 成功表示：

- 文件已经完整、持久地落到本地；
- canonical path、size、mtime、ETag 已经提交；
- 不需要因为远端速度慢而让 Cloud Sync 重发整个文件。

它**不表示**远端 provider 已经完成上传和验证。

这是有意设计，用来避免大文件因为远端最终一致性或速度问题产生重复上传。

## 3. 状态与操作矩阵

| 状态 / Recovery State | 含义 | 自动恢复 | 人工操作 |
|---|---|---:|---|
| `queued` | 等待 provider worker | 是 | 无 |
| `uploading` | 正在上传远端 | 是 | 无 |
| `verifying` | 正在验证当前 generation 的远端状态 | 是 | 无 |
| `waiting_provider_verification` | 当前 provider 证据不足，OpenList 会等待后重新获取新证据 | 是 | 无 |
| `restart_recovery` | OpenList 重启后正在恢复中断 generation | 是 | 无 |
| 普通 provider error | upload/list/token/网络操作失败，但 durable payload 或恢复证据仍存在 | 是 | 通常无需操作 |
| `missing_spool` | 本地 spool 缺失；OpenList 会先确认远端是否已经存在正确 generation | 初始阶段是 | 暂时不要重启 Cloud Sync |
| `completed` | 当前 generation 已获得远端验证证据 | 已完成 | 无 |
| 正常 `deleted` | 正常删除或 tombstone 生命周期 | 是 | 无 |
| **`needs_cloudsync_rehydrate`** | OpenList 已无可用本地 payload，且刷新后的 provider 证据确认正确对象无法恢复 | **否** | **彻底停止/停用后重新启动/启用对应 Cloud Sync 任务，强制重新扫描并上传** |

`last_error` 非空**不等于必须人工处理**。

运维判断应优先看 Recovery State。绝大多数 provider 错误都应该留在 OpenList 自己的自动重试闭环中。

## 4. 正常自动恢复

正常自愈路径：

```text
queued
  |
  v
uploading
  |
  +---- provider 临时失败
  |           |
  |           v
  |      queued + retry_at
  |           |
  |           +---- 自动重试
  |
  v
verifying
  |
  +---- provider 证据不足
  |           |
  |           v
  | waiting_provider_verification
  |           |
  |           +---- 重新获取新证据
  |
  v
completed
```

以下情况正常都不需要操作 Cloud Sync：

- 115/API/网络临时失败；
- upload token 获取失败；
- durable spool 仍存在时的 provider PUT 失败；
- `uploading` 期间 OpenList 重启；
- provider metadata 暂时陈旧或不完整；
- 第一次发现 provider divergence；
- verification 等待或重试窗口；
- 尚未形成明确丢失结论的远端 list/get 不一致。

不要为了让状态“快一点”而反复停止或启动 Cloud Sync。

## 5. `missing_spool`：先让 OpenList 自己恢复

`missing_spool` **不等于文件已经丢失**。

它表示本地缓存 payload 已不可用，因此 OpenList 必须先判断远端是否已经存在正确 generation。

```text
missing_spool
     |
     v
刷新 provider 并验证
     |
     +--> 远端存在正确对象
     |          |
     |          v
     |       completed
     |
     +--> 证据仍不足
     |          |
     |          v
     |   等待后继续验证
     |
     +--> 明确确认远端缺失或不一致
                |
                v
      needs_cloudsync_rehydrate
```

**当状态还只是 `missing_spool` 时，不需要人工操作。**

此时过早重启 Cloud Sync，可能在远端其实已经存在正确对象的情况下制造一次重复 PUT。

## 6. `needs_cloudsync_rehydrate`：唯一需要人工介入的正常恢复状态

只有在正常自动恢复已经无法重建先前 ACK 的 generation 时，才会进入 `needs_cloudsync_rehydrate`：

```text
Cloud Sync 之前已经收到成功响应
        |
        v
本地 durable spool 已不可用
        |
        v
刷新后的 provider 证据明确显示缺失或不一致
        |
        v
OpenList 已经没有 payload 可以自行重新上传
        |
        v
canonical state 对 WebDAV 表现为 deleted
        |
        v
needs_cloudsync_rehydrate
```

此时 OpenList 会有意让该对象从 canonical WebDAV view 中消失，让 Cloud Sync 在下一次完整 reconciliation 时看到本地有文件、WebDAV 端没有文件，从而重新执行 PUT。

### 必须执行的人工步骤

1. 确认 Recovery State 确实是 `needs_cloudsync_rehydrate`。
2. **不要删除 Cloud Sync 任务，也不要 unlink 后重建整个任务。**
3. 彻底停止或停用对应的 Cloud Sync 任务。
4. 等待任务完全停止，确认没有正在进行的传输。
5. 重新启动或启用**同一个** Cloud Sync 任务。
6. 等待 Cloud Sync 做一次新的 reconciliation 扫描。
7. 确认受影响路径重新出现 PUT。
8. 确认 PUT 之后出现 PROPFIND 验证。
9. 确认新的 OpenList generation 按 `queued -> uploading -> verifying -> completed` 收敛。

### 预期 WebDAV trace

正常 rehydrate 大致应表现为：

```text
PROPFIND 父目录（Depth: 1）
    |
    +--> 受影响对象已经从 canonical WebDAV view 消失
    |
    v
Cloud Sync 发现本地存在、远端不存在
    |
    v
PUT 受影响文件
    |
    v
HTTP 201/204
    |
    v
PROPFIND 文件（Depth: 0）
    |
    v
canonical 对象重新出现
```

如果 Cloud Sync 已经多次通过 PROPFIND 明确看到文件缺失，但始终没有重新 PUT，请先保留 trace，不要手工修改 OpenList 或数据库状态。

## 7. 正常 `deleted` 与 `needs_cloudsync_rehydrate` 的区别

不能只看到 `state=deleted` 就判断要重启 Cloud Sync。

正常 DELETE 和 MOVE 也会使用 tombstone 或 deleted 状态。

真正的人工操作信号是 Recovery State：

```text
deleted + 正常删除生命周期
    -> 自动处理，不需要人工操作

deleted + recovery_state=needs_cloudsync_rehydrate
    -> 需要 Cloud Sync 人工操作
```

后台 UI 和 API 应优先展示 Recovery State，而不是要求运维人员仅凭底层 provider state 猜测下一步动作。

## 8. Provider 错误处理

只要 OpenList 仍保留 durable payload，provider 错误就应该由 OpenList 负责：

```text
provider PUT error
    |
    v
持久化 last_error + retry_count + retry_at
    |
    v
自动重试或验证
    |
    v
completed
```

常见例子：

- 115 upload init 失败；
- sign-check 或 range 错误；
- upload token 错误；
- multipart upload 失败；
- remote verification 失败；
- 临时 rate limit 或网络错误。

只要 spool 仍存在，这些错误都不应该要求 Cloud Sync 重新发送整个文件。

当前版本也会把 provider retry 上下文和 worker panic stack 写入 OpenList 日志，方便定位问题。

常用日志过滤：

```bash
docker logs op --since 2h 2>&1 | grep -E \
'write-back worker panic|write-back provider retry|write-back delete retry|115 Open'
```

## 9. OpenList 重启行为

OpenList 重启通常**不需要**同时重启 Cloud Sync。

重启后预期行为：

- 中断的 `uploading` 从 durable state 恢复；
- stale receive lease 被回收；
- queued payload 重新进入 provider worker；
- verification 继续；
- 已 ACK 的 canonical metadata 对 Cloud Sync 保持稳定。

只有最终恢复状态变成 `needs_cloudsync_rehydrate` 时，才需要停止并重新启动 Cloud Sync 任务。

## 10. 不要这样做

除非明确执行事故恢复，否则不要：

- 手工删除 queued/uploading/verifying generation 的 spool；
- 为了“页面干净”直接清理服务端数据库中的 write-back 行；
- 每次 provider retry 都去重启 Cloud Sync；
- 第一次发现 divergence 就认定数据丢失；
- 为触发 rehydrate 而删除或重建整个 Cloud Sync 任务；
- 使用 SQLite 承载 Durable Write-back；
- 在 receive 或 provider operation 正在运行时手工修改 canonical state。

## 11. 基础设施故障

以下情况不是普通 write-back recovery state，需要先处理基础设施：

- MySQL/PostgreSQL 不可用或异常；
- spool 文件系统已满、只读、损坏或丢失；
- provider 凭据过期或撤销；
- provider storage 被禁用；
- 持续 DNS/路由/TLS 故障；
- mount/storage 配置错误；
- 与单一对象无关的持续进程崩溃；
- 数据库 migration 或 index 失败。

基础设施恢复后，先让 OpenList 自己跑正常恢复。只有真正最终落到 `needs_cloudsync_rehydrate` 的对象才需要重启对应 Cloud Sync 任务。

## 12. 快速判断

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

## 13. 运维总结

- Cloud Sync 负责保留源文件。
- OpenList 负责 durable 接收、provider 上传、重试和验证。
- MySQL 是当前已验证并推荐的数据库。
- PostgreSQL 是另一个服务端数据库目标，生产使用前应验证实际部署版本。
- SQLite 不支持 Durable Write-back。
- 普通 provider 错误不要重启 Cloud Sync。
- `missing_spool` 仍在验证 provider 时不要重启 Cloud Sync。
- 只有 `needs_cloudsync_rehydrate` 才需要人工完整停止/停用并重新启动/启用对应 Cloud Sync 任务，以强制重新扫描。
