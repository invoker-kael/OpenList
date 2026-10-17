# Cloud Sync 写回运维

> 本模式只支持 Cloud Sync 单向上传和备份，不支持双向同步。

## 正常流程

1. OpenList 收到完整 WebDAV `PUT` 后，先把文件持久写入本地 spool，再向 Cloud Sync 确认。
2. 上传 115 前，OpenList 对父目录执行 fresh `LIST`。任意同名对象只要 Size 和 SHA1 都匹配，就直接完成，不再上传。
3. 如果没有匹配项，OpenList 上传本地 spool，再通过 fresh `LIST` 和相同的 Size + SHA1 规则确认。
4. 找到匹配内容后任务完成。超时、限流、WAF、API 错误或暂时没有 SHA1 都保持**处理中**，不能当作文件不存在。
5. 已完成文件会低频 fresh 检查。存在匹配项就保持完成；只有连续三次成功的 fresh 检查都没有匹配项，才标记失败。Provider/API 错误不计入这三次。

Cloud Sync 是源数据。115 确认前，本地 spool 是持久交接副本。请将 spool 放在持久本地存储中，不要手工删除活动任务使用的文件。

## Overview 状态

Overview 只显示以下四种任务状态：

| 状态 | 含义 |
|---|---|
| 接收中 | Cloud Sync 正在发送 PUT。 |
| 处理中 | 文件已持久接收，OpenList 正在检查或写入 115。 |
| 已完成 | 已确认 115 存在匹配内容。 |
| 失败 | OpenList 将路径暴露为不存在，让 Cloud Sync 重新上传。 |

队列、上传、重试和验证等内部细节不会成为额外的 Overview 状态。

## 全量重建

重建请求会快速返回，并在后台执行。当前 PUT 会先完成。重建期间 `GET`、`HEAD`、`PROPFIND` 仍可用；新的写入会收到带 `Retry-After` 的 `503`，由 Cloud Sync 稍后重试。

当前 PUT 和 worker 停止后，重建会清除 OpenList 自己的写回对象、历史、接收 fence、reservation、Provider 操作记录，以及本地 `.part` / `.data` spool 文件（包括孤儿文件）。随后清空内存 Provider 缓存并重新启动 worker。整个流程不会删除或修改 115 上的文件。

## 数据库与诊断

写回状态存放在 OpenList 配置的 MySQL 数据库中。管理 API 需要 OpenList 认证。考虑数据库修复前先核对行数和 spool 引用；OpenList 运行期间不要清空写回行或手工删除 spool 文件。
