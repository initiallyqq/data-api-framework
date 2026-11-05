# data-api-framework

数据服务（交易日历 `trade-calendar-service`、代码映射 `code-mapping-service`）共用的 Go 模块。
每个服务把 DolphinDB 的表同步成本地 SQLite 副本，用 HTTP 提供一个查询，另有一个 MCP 适配器把这个查询转成一个工具。

```bash
go get github.com/initiallyqq/data-api-framework@v0.2.0
```

## 包

| 包 | 内容 |
| --- | --- |
| `problem` | RFC 9457 错误体和错误码；服务启动时设置 `problem.TypeBase` |
| `endpoint` | 查询参数、返回字段、JSON、HTTP 服务和健康检查 |
| `timezone` | 北京时间（固定 UTC+8），上游表的时区 |
| `upstream` | 读 DolphinDB 上游表 |
| `replica` | SQLite 副本和同步 |
| `service` | 数据服务程序骨架 |
| `adapter` | MCP 适配器程序骨架 |
| `buildinfo` | 构建时注入的版本号：`-ldflags "-X github.com/initiallyqq/data-api-framework/buildinfo.Version=..."` |

## 测试

测试全部在 `test/` 下。`test/harness` 不是测试，是服务仓端到端测试用的工具：编译服务的二进制、建临时 DolphinDB 库、启动服务。
连 DolphinDB 的测试在没设 `DATA_API_E2E_DOLPHINDB_ADDRESS` 时跳过：

```bash
docker run -d -p 127.0.0.1:18848:8848 registry.cn-hangzhou.aliyuncs.com/dolphindbit/dolphindb:v2.00.12.11
DATA_API_E2E_DOLPHINDB_ADDRESS=127.0.0.1:18848 go test ./...
```

## 发布

改动提交后打 tag（`vX.Y.Z`），再到两个服务仓升级：

```bash
go get github.com/initiallyqq/data-api-framework@vX.Y.Z
go mod tidy
```

两个服务要用同一个版本。改动还没打 tag 时，在服务仓旁边建一个不提交的 `go.work`（`go work init . ../data-api-framework`）在本地联调。

本仓是公开仓，`go get` 走模块代理即可，不需要凭据。若在没法访问代理的环境里要直连 GitHub，再配：

```bash
go env -w GOPRIVATE=github.com/initiallyqq/data-api-framework
git config --global url."git@github.com:".insteadOf https://github.com/
```
