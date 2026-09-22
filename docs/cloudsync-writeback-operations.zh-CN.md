# Cloud Sync Write-back 运维指南

本文档用于运维 **Synology Cloud Sync 单向上传**场景下的 WebDAV Durable Write-back。

> **开发说明：** 本功能由 AI 辅助开发，并经过人工测试验证。

## 核心规则

**`needs_cloudsync_rehydrate` 是正常写回恢复流程中唯一需要 Cloud Sync 或人工介入的状态。其他正常写回恢复状态都应由 OpenList 自动恢复。**

基础设施故障不属于这条规则。如果数据库不可用、spool 文件系统已满或损坏、远端存储凭据失效、网络链路异常，应先修复基础设施。


## 1. 组件要求

### 数据库

**本 fork 的 Durable Write-back 仅支持 MySQL。**

- MySQL：已支持并经过验证。
- SQLite / sqlite3：不支持。
- 其他数据库后端：不属于当前 Durable Write-back 支持范围。

### Durable spool

必须使用可靠的本地持久存储。持续大量上传建议使用 SSD/NVMe。活动 generation 的 spool 不应手工删除。

### Cloud Sync 模式

本设计针对 **Synology Cloud Sync -> WebDAV/OpenList 单向上传**，包括 Cloud Sync 客户端加密任务。

### 远端存储

115 Open 是当前主要验证对象。其他 OpenList storage driver 可以走同一 Write-back 流程，但远端验证能力可能不同。

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


## 3. 用户看到的状态与进度

### 活动任务

| 用户状态 | 含义 | 操作 |
|---|---|---|
| **接收中** | Cloud Sync 正在把 PUT 数据发送给 OpenList | 无 |
| **后台同步中** | OpenList 已持久化数据，正在排队、上传、验证或自动恢复 provider 副本 | 无 |
| **等待 Cloud Sync 重传** | 旧 generation 已无法安全自动收敛，需要 Cloud Sync 重新 PUT | 完整停止/停用后重新启动/启用同一个 Cloud Sync 任务 |
| **已删除** | 删除生命周期 | 无 |

活动任务状态筛选放在表格标题中，采用 Excel 风格下拉。内部的 `queued/uploading/verifying`、重试、恢复类型、Hash、resolution reason 等仍保留在“高级信息”。

**进度**列只显示真实进度：
- 接收中：Cloud Sync 已发送字节 / PUT 声明大小。
- Provider 上传中：OpenList storage driver 原生上传进度回调。
- 排队、验证阶段：不伪造百分比。

### 历史记录

历史记录是 generation 级审计，不是实时任务页。普通用户看到的“最终状态”只保留：

- **已完成**：正常完成和恢复后完成统一归为已完成，不再单独显示 `recovered`。
- **等待 Cloud Sync 重传**：旧 generation 尚未解决，等待同路径的新 PUT。
- **已删除**：删除已经完成。

接收中、上传中、验证中、自动恢复中不再作为历史记录“最终状态”。详细内部状态仍可在“高级信息”查看。

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


## 6. 最终 Cloud Sync 重传恢复

当 OpenList 已经耗尽安全的 Provider 自动恢复路径时，旧 generation 进入最终的 Cloud Sync 重传等待状态。

**canonical Durable ACK 会继续对 WebDAV 可见。OpenList 不再通过隐藏/删除 canonical 对象来强迫 Cloud Sync 重扫。**

### 必须执行的人工步骤

1. 完整停止或停用受影响的 Cloud Sync 任务。
2. 确认传输停止。
3. 重新启动或启用同一个 Cloud Sync 任务。
4. 等待 fresh reconciliation scan。
5. 确认异常路径出现新的 PUT。
6. 确认新 generation 最终进入“已完成”。

新的同路径 PUT 会按 generation/ACK 顺序接管旧 incident。由于 Cloud Sync 加密或合法本地变更，新 payload 的大小或 SHA-1 不需要和旧 generation 相同。

OpenList 不提供“手动重传”按钮。

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

- Cloud Sync 保留源文件。
- OpenList 负责 durable 接收、Provider 上传、重试、验证和恢复。
- 本 fork 的 Durable Write-back 仅支持 MySQL。
- 普通 Provider 错误继续由 OpenList 自动恢复。
- 活动任务只展示简化后的 Cloud Sync 生命周期，并显示真实接收/上传进度。
- 历史记录只展示有意义的最终结果；恢复成功统一显示“已完成”。
- 最终 fallback 保持 canonical ACK 可见，等待 Cloud Sync 对同一路径发起新的 PUT。

## 远端 Hash 差异与最终恢复

`remote_hash_mismatch` 表示已经确认的 Provider 差异原因，不再作为“人工重传”状态。

OpenList 必须先取得连续、稳定的 fresh provider evidence 才会确认 Hash mismatch。如果本地 durable spool 仍存在，OpenList 最多自动执行 **1 次 Provider repair upload**，随后重新验证；如果 Provider 已经自行收敛，repair 前的 fresh pre-check 会直接跳过不必要的重复上传。

如果同一 generation 在这一次自动 repair 后仍然无法收敛，OpenList 不再继续 Provider 重试或无限低频验证，而是进入 `waiting_cloudsync_reupload`，并设置 `cloud_sync_reupload_required=true`。后台仍保留 `remote_hash_mismatch` 作为根因，但最终运维动作只有：

1. 完整停止/停用对应 Cloud Sync 任务；
2. 重新启动/启用同一个任务；
3. 等待 fresh reconciliation scan；
4. 确认 Cloud Sync 对异常路径重新发起 PUT；
5. 确认新 generation 最终进入 `completed`。

只要 OpenList 接收到同一路径的更新 PUT，新 generation 就会接管旧 recovery incident。后台/History 应随新 generation 依次显示 `reupload_receiving`、`reupload_received`、`reupload_uploading`、`reupload_verifying`，最终变为 `recovered`。这里不再要求新 payload 的 SHA-1 或大小必须与失败 generation 完全一致，因为 Cloud Sync 加密重新生成或本地文件的正常变更都可能改变实际字节；恢复判断以同一路径的 generation/ACK 顺序为准。旧 incident 仍保留在 History 作为审计记录，但不再属于待人工处理状态。

OpenList 不提供“手动重传”动作。当 OpenList 已无法安全地让 Provider 副本收敛时，最终恢复数据来源必须回到 Cloud Sync。
