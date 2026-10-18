# Cloud Sync 写回运维

> 本模式只支持 Cloud Sync 单向上传和备份，不支持双向同步。

## 正常流程

1. OpenList 收到完整 WebDAV `PUT` 后，先把文件持久写入本地 spool，再向 Cloud Sync 确认。
2. 上传 115 前，OpenList 先检查 MySQL 中未过期的远端索引；若没有同名、Size、SHA1 完全匹配项，再执行父目录 fresh `LIST`。精确匹配会直接完成，不再上传；缓存未命中不会被当作远端文件不存在。
3. 如果没有匹配项，OpenList 上传本地 spool。115 API 成功响应后任务直接完成；正常上传不再等待第二次 `LIST`。
4. 超时、限流、WAF 和 API 错误会保留 spool，并在**处理中**退避重试。每次重试前 fresh `LIST`，检查 115 是否已接收文件但响应丢失。Provider/API 错误不能当作文件不存在。
5. 本地缓存释放后，已完成文件才会进入低频 Fresh LIST 巡检。任意同名 Size+SHA1 匹配都会保持完成；API 错误或缺少 SHA1 不计为缺失；连续三次成功 Fresh LIST 都没有匹配，才会向 Cloud Sync 暴露缺失路径并等待重传。

相同路径、Size 和 SHA1 的重复 PUT 会合并到原有代次，包括已经完成的任务，不会新建任务或再次上传 115。内容不同才建立新代次。因此 Cloud Sync 可以全量重传，而相同内容不会造成重复 115 上传。

Cloud Sync 是源数据。115 接受上传前，本地 spool 是持久交接副本。请将 spool 放在持久本地存储中，不要手工删除活动任务使用的文件。

Overview 的远端缓存按钮只刷新 Writeback 已记录的父目录。Fresh LIST 并发由 Writeback 设置限制，实际请求频率遵循所选 115 存储的 `LimitRate`，包括大目录的每一页；WAF 敏感时请降低存储后端的 `LimitRate`。

## Overview 状态

Overview 只显示以下四种任务状态：

| 状态 | 含义 |
|---|---|
| 接收中 | Cloud Sync 正在发送 PUT。 |
| 处理中 | 文件已持久接收，OpenList 正在检查或写入 115。 |
| 已完成 | 115 已接受上传，或 fresh LIST 找到完全匹配的内容。 |
| 失败 | 已确认本地持久 payload 不可用，OpenList 将路径暴露为缺失并让 Cloud Sync 重传。 |

队列、上传、重试和验证等内部细节不会成为额外的 Overview 状态。

每个路径的“高级信息”会显示合并的客户端重复 PUT 次数、115 上传尝试次数，以及第二次及后续尝试中由 Provider 进度回调报告的字节数。

## 全量重建

重建请求会快速返回，并在后台执行。当前 PUT 会先完成。重建期间 `GET`、`HEAD`、`PROPFIND` 仍可用；新的写入会收到带 `Retry-After` 的 `503`，由 Cloud Sync 稍后重试。

当前 PUT、远端索引刷新和 worker 停止后，重建会清除 OpenList 自己的写回对象、历史、接收 fence、reservation、Provider 操作记录、MySQL 远端索引快照，以及本地 `.part` / `.data` spool 文件（包括孤儿文件）。随后清空内存 Provider 缓存并重新启动 worker。整个流程不会删除或修改 115 上的文件。

## 数据库与诊断

写回状态存放在 OpenList 配置的 MySQL 数据库中。管理 API 需要 OpenList 认证。考虑数据库修复前先核对行数和 spool 引用；OpenList 运行期间不要清空写回行或手工删除 spool 文件。
