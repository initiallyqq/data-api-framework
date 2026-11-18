# data-api-framework

data-api 的数据服务共用的 Go 模块：把上游 DolphinDB 表同步成本地 SQLite 副本，用 HTTP 提供一个查询，再由 MCP 适配器把它转成一个工具。

## 目录

- [背景](#背景)
- [安装](#安装)
- [用法](#用法)
  - [一个服务](#一个服务)
  - [一个 MCP 适配器](#一个-mcp-适配器)
- [各包](#各包)
  - [problem](#problem)
  - [endpoint](#endpoint)
  - [timezone](#timezone)
  - [upstream](#upstream)
  - [replica](#replica)
  - [service](#service)
  - [adapter](#adapter)
  - [buildinfo](#buildinfo)
  - [test/harness](#testharness)
- [测试](#测试)
- [发布](#发布)
- [维护者](#维护者)
- [许可证](#许可证)

## 背景

data-api 下的数据服务形状相同：一组上游 DolphinDB 表变成一份本地 SQLite 副本，副本对外是一个只读 HTTP 查询，同一个查询再包成一个 MCP 工具。交易日历（`trade-calendar-service`）与代码映射（`code-mapping-service`）两个服务因此共用这个模块；各服务仓库只写自己的数据形状——行类型、副本里的表、DolphinDB 查询、HTTP 端点的语义、MCP 工具的参数。

其余行为由模块统一提供：按间隔把上游整表取回、在一个事务里整体替换副本、校验并备份副本、把同步状态与数据新鲜度做成健康响应、把失败写成 RFC 9457 错误体、按 MCP 协议版本组织工具结果。

## 安装

```sh
go get github.com/initiallyqq/data-api-framework@vX.Y.Z
```

把 `vX.Y.Z` 换成要用的版本。当前可用的版本用 `go list -m -versions github.com/initiallyqq/data-api-framework` 查，或看仓库的 tags 页：<https://github.com/initiallyqq/data-api-framework/tags>。模块要求 Go 1.25 或更新（`go.mod` 的 `go` 行）。

## 用法

### 一个服务

服务二进制的 `main` 只有一个 `service.Spec` 和一次 `Main` 调用。下面是一个交易日历服务的最小形态；`layout`（副本里怎么存一行、怎么读回来）在服务自己的包里实现 `replica.Layout`。

```go
package main

import (
	"context"
	"net/http"
	"os"

	"github.com/initiallyqq/data-api-framework/replica"
	"github.com/initiallyqq/data-api-framework/service"
	"github.com/initiallyqq/data-api-framework/upstream"
)

// Row 是副本里的一行，layout 实现 replica.Layout[Row]。
type Row struct {
	Exchange string `json:"exchange"`
	Date     string `json:"calendar_date"`
	IsOpen   bool   `json:"is_open"`
}

var spec = service.Spec[Row]{
	Name:              "trade-calendar-service",
	Noun:              "calendar",
	DefaultListenAddr: ":8091",
	Open: func(files replica.Files) (*replica.Store[Row], error) {
		return replica.Open(files, 3, layout{})
	},
	Fetch: func(ctx context.Context, source upstream.Source) ([]Row, error) {
		session, err := upstream.Connect(ctx, source)
		if err != nil {
			return nil, err
		}
		defer session.Close()
		return upstream.Select[Row](ctx, session, upstream.Query{
			Table:   "trade_calendar_xshg",
			Columns: "calendar_date, is_open",
		})
	},
	Register: func(server *service.Server, store *replica.Store[Row]) {
		server.Query("/v1/api/trade-calendar", func(r *http.Request) (any, error) {
			rows := []Row{}
			err := store.Read(r.Context(), func(q replica.Queryer) error {
				result, err := q.QueryContext(r.Context(),
					"SELECT calendar_date, is_open FROM trade_calendar_xshg")
				if err != nil {
					return err
				}
				defer result.Close()
				for result.Next() {
					var row Row
					if err := result.Scan(&row.Date, &row.IsOpen); err != nil {
						return err
					}
					rows = append(rows, row)
				}
				return result.Err()
			})
			return rows, err
		})
	},
}

func main() { spec.Main(os.Args[1:]) }
```

`Main` 接的命令行是 `[--config <path>]`，默认取 `<Name>.yaml`；配置文件里的键见 [service](#service)。上游地址、凭据文件、副本路径、备份路径都由这个文件给。

### 一个 MCP 适配器

适配器不读副本也不读凭据：每次工具调用都转发成上面那个服务的一个 HTTP 请求。

```go
package main

import (
	"context"
	"net/url"
	"os"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/initiallyqq/data-api-framework/adapter"
	"github.com/initiallyqq/data-api-framework/endpoint"
	"github.com/initiallyqq/data-api-framework/problem"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const toolName = "get_cfi_trade_calendar"

var spec = adapter.Spec{
	Name:              "trade-calendar-mcp",
	Noun:              "calendar",
	DefaultListenAddr: ":8093",
	DefaultServiceURL: "http://data-api-trade-calendar:8091",
	Tool:              toolName,
	Narrow:            "narrow the date range or name fewer exchanges",
	Register: func(server *mcp.Server, client *adapter.Client) error {
		schema, err := adapter.InputSchema(
			[]endpoint.Parameter{
				{Name: "exchange", Required: true},
				{Name: "begin_date"},
				{Name: "end_date"},
			},
			map[string]*jsonschema.Schema{
				"exchange":   adapter.OneOrManySchema("交易所代码，如 XSHG", jsonschema.Ptr(32)),
				"begin_date": {Type: "string", Pattern: `^([0-9]{4}-[0-9]{2}-[0-9]{2})?$`},
				"end_date":   {Type: "string", Pattern: `^([0-9]{4}-[0-9]{2}-[0-9]{2})?$`},
			})
		if err != nil {
			return err
		}
		mcp.AddTool(server, &mcp.Tool{
			Name:        toolName,
			Description: "交易日历：交易所代码与日期区间内的每一个自然日，及其开市/休市记录。",
			InputSchema: schema,
			Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true, IdempotentHint: true},
		}, func(ctx context.Context, _ *mcp.CallToolRequest, args arguments) (*mcp.CallToolResult, any, error) {
			return client.Call(ctx, "/v1/api/trade-calendar", url.Values{
				"exchange":   {strings.ToUpper(strings.Join(args.Exchange, ","))},
				"begin_date": {args.BeginDate},
				"end_date":   {args.EndDate},
			}), nil, nil
		})
		return nil
	},
	Smoke: func(today string) adapter.SmokeTest {
		return adapter.SmokeTest{
			Good:    map[string]any{"exchange": "XSHG", "end_date": today},
			Bad:     map[string]any{"exchange": "NO_SUCH_EXCHANGE"},
			BadCode: problem.ExchangeNotFound,
		}
	},
}

type arguments struct {
	Exchange  adapter.OneOrMany `json:"exchange"`
	BeginDate string            `json:"begin_date"`
	EndDate   string            `json:"end_date"`
}

func main() { spec.Main(os.Args[1:]) }
```

## 各包

下面每个小节先说明这个包管什么，再用一张表列出它的公开名字（类型的方法写成 `类型.方法`）。

### problem

RFC 9457 错误体。一个 `Code` 定死了 HTTP 状态与标题；`Details` 就是写出去的那个 JSON 对象，字段 `type`、`title`、`status`、`code`、`detail`，另有可选的 `parameter`、`values`、`available`、`request_id`。`MarshalJSON` 额外输出 `error` 与 `message`，两者都等于 `detail`，供只认这两个字段的调用方。`Write` 写出响应，回显请求的 `X-Request-ID`，并设 `Cache-Control: no-store`；`WriteMethodNotAllowed` 带 `Allow` 头；`Parse` 读服务返回的问题体，适配器用它把服务端的错误原样交给工具调用方。

| 码 | 状态 | 何时 |
| --- | --- | --- |
| `MissingParameter` | 400 | 必填参数没给 |
| `InvalidParameter` | 400 | 参数值解析不了或不合法 |
| `UnknownParameter` | 400 | 参数名不在这个端点接受的名字里 |
| `ResponseTooLarge` | 400 | 答案超过上限，请求要收窄 |
| `Unauthenticated` | 401 | 管理员 token 缺失或不匹配 |
| `OriginNotAllowed` | 403 | `Origin` 不在白名单 |
| `RouteNotFound` | 404 | 路径不存在 |
| `CategoryNotFound` | 404 | 代码映射没有这个类别 |
| `ExchangeNotFound` | 404 | 交易日历没有这个交易所 |
| `MethodNotAllowed` | 405 | HTTP 方法不对 |
| `SyncInProgress` | 409 | 已有同步在排队或运行 |
| `RequestTooLarge` | 413 | 请求体超过 1 MiB |
| `RateLimited` | 429 | 并发工具调用超过 8 |
| `InternalError` | 500 | 本地数据库操作失败 |
| `UpstreamError` | 502 | 上游返回了无法处理的东西 |
| `DataUnavailable` | 503 | 副本里还没有数据 |
| `ServiceUnavailable` | 503 | 同步不可用 |
| `UpstreamTimeout` | 504 | 上游没在超时内回答 |

其余公开名字 12 个：

| 名字 | 说明 |
| --- | --- |
| `MediaType` | 错误体的媒体类型 `application/problem+json` |
| `TypeBase` | `type` 字段的前缀，与码拼成 RFC 9457 的类型 URI；各服务在 `init()` 里设成自己公开地址下的一个 URI |
| `Code` | 码的类型 |
| `Details` | 错误体 |
| `Details.Error` | 让 `Details` 实现 `error` |
| `Details.ForParameter` | 填入 `parameter`、`values`、`available`，返回自己 |
| `Details.MarshalJSON` | 输出错误体，附带 `error` 与 `message` |
| `New` | 由码与 `detail` 造一个错误体 |
| `NotFound` | 造一个「某个参数值不存在」的错误体，带上可用取值 |
| `Parse` | 解析一份错误体 |
| `Write` | 写出错误响应 |
| `WriteMethodNotAllowed` | 写 405，带 `Allow` 头 |

### endpoint

HTTP 端点的公共部分：参数的声明与检查、启动与关闭、健康检查命令、JSON 写出。

| 名字 | 说明 |
| --- | --- |
| `Parameter` | 一个查询参数：`Name`、`Required` |
| `ParameterNames` | 参数名列表，用来拼说明文字 |
| `Field` | 一个结果字段：`Name`、`Comment`，注释进 MCP 工具说明 |
| `FieldList` | 把字段拼成 `名称 (注释), ...` 的一段话 |
| `CheckParameters` | 检查请求里的参数名都在声明内，多了就是 `UnknownParameter` |
| `ListParameter` | 读可重复的参数：`?x=a&x=b` 与逗号连写都认，去空白、去重、保序 |
| `Server` | 造 `*http.Server`：给 handler 与读写超时，`ReadHeaderTimeout` 15 秒、`IdleTimeout` 60 秒 |
| `Run` | 服务到 ctx 结束，然后 10 秒内优雅关闭；启动时打一行 `<name> listening on <addr>` |
| `Healthcheck` | 二进制自己的 `healthcheck [-url] [-timeout]`：返回 2xx 才算过，默认超时 5 秒 |
| `WriteJSON` | 写一个 JSON 响应 |

### timezone

| 名字 | 说明 |
| --- | --- |
| `Beijing` | 北京时间，固定 UTC+8 的时区（不随夏令时变化） |

### upstream

连 DolphinDB、取表、跑一条查询并把每行解码成结构体。日期解析按北京时间。

| 名字 | 说明 |
| --- | --- |
| `Source` | 上游连接参数：`Address`、`CredentialsFile`、`Database`、`Timeout` |
| `Connect` | 建连接；`Database` 必须是 `dfs://` 加库名（字母、数字、`_ - .`），`Timeout` 为零取 5 分钟 |
| `Session` | 一个连接 |
| `Session.Close` | 关闭连接 |
| `Session.Tables` | 库里的表名，按名字排序 |
| `Query` | 一条查询：`Table`、`Columns`、可选的 `Where` |
| `Select` | 跑查询，把每行解码成 `T` 返回 |
| `ParseDate` | 解析日期，接受 `2026.09.30` 与 `2026-01-02`，输出 `YYYY-MM-DD` |
| `ParseDateTime` | 解析日期时间，`T` 或 `-` 分隔、可带毫秒，按 `timezone.Beijing` 解释 |

凭据文件是 `Source.CredentialsFile` 指向的那个文件，逐行一条：

- 空行与 `#` 开头的行跳过；`export ` 前缀可有可无。
- 每行按第一个 `=` 切成名字与值，只认 `DDB_USER` 与 `DDB_PASSWORD`，两个都必须有值。
- 值可以加引号：单引号内原样，双引号按 Go 字符串转义解释。不加引号的值里出现空格、制表符、`&`、`|`、反引号、`$` 或小括号，连接就失败并报 `unquoted value contains shell syntax`——值里含 shell 元字符时必须加引号。

### replica

本地副本：打开与迁移、整表替换、读事务、状态与摘要、备份。副本是一个 SQLite 文件，`sync_state` 表记着内容摘要、行数、上次成功同步时间与数据变化时间。

| 名字 | 说明 |
| --- | --- |
| `Open` | 打开副本；建目录（0700）、设 WAL 与 `synchronous=full`、`MaxOpenConns` 8，文件权限 0600 |
| `Files` | 一对路径：`Path`（副本）与 `BackupPath`（备份） |
| `Layout` | 服务的副本形状：`Key`（一行的标识）、`Replace`（整表替换）、`Load`（读回全部行） |
| `Store` | 打开后的副本，泛型于行类型 |
| `Store.Migrate` | 建 `sync_state`；文件是别的版本写的就拒绝启动 |
| `Store.Ping` | `Ping` 加 `PRAGMA quick_check`，检查过不了就是错 |
| `Store.State` | 当前状态，按行重算摘要核对 |
| `Store.Read` | 只读事务里读一次；还没有数据时返回 `ErrNoData` |
| `Store.Synchronize` | 用一次取回的行替换副本，并在需要时做备份 |
| `Store.CheckBackup` | 备份是否与给定摘要一致 |
| `Store.Close` | 关闭 |
| `State` | 状态：`HasData`、`Digest`、`Rows`、`LastSourceSuccessAt`、`DataChangedAt` |
| `SyncResult` | 一次同步的结果：`Changed`、`Rows`、`BackupError` |
| `ErrNoData` | 副本还没有数据 |
| `ErrSyncInProgress` | 已有同步在排队或运行 |
| `Queryer` | 只读事务里可用的查询接口 |
| `Monitor` | 同步与存储的错误状态，供健康响应读取 |
| `Monitor.Begin` | 记一次尝试开始 |
| `Monitor.Success` | 记一次成功及其时间 |
| `Monitor.Failure` | 记一次失败及其错误 |
| `Monitor.RestoreSuccess` | 启动时把上次成功时间恢复进来 |
| `Monitor.SetStorageError` | 记副本或备份的错误 |
| `Monitor.Snapshot` | 取一份当前状态 |
| `Snapshot` | 状态快照：`LastAttemptAt`、`LastSuccessAt`、`LastError`、`StorageError`、`InProgress` |
| `Syncer` | 同步循环：取数、写入、按间隔重复 |
| `Syncer.Sync` | 同步一次，失败时区分「读上游」与「写副本」 |
| `Syncer.RequestSync` | 请一次立即同步；已有同步时返回 `ErrSyncInProgress` |
| `Syncer.Run` | 循环到 ctx 结束；单次同步最多 30 分钟 |

同步规则：

- 启动时先同步一次。副本里已有数据时，首次同步失败只打日志、继续用现有副本服务；副本为空时失败就退出。
- 成功后等 `syncInterval`，失败后等 `syncRetryInterval`，再试下一次。
- 一次同步是整表替换：`Layout.Replace` 写完后内容才对外可见，读者看到的是完整的旧内容或完整的新内容。
- 上游返回空行、或同一 `Key` 出现两次，都拒绝，副本保持原样。
- 内容没变时只更新上次成功同步时间，不重写副本、不做备份。
- 备份在内容变化或备份与摘要不一致时才做：先写 `<BackupPath>.tmp`，校验摘要后再改名。

日志行：

```text
<name> listening on <addr>
<name> synchronization completed (changed=%t rows=%d)
<name> synchronization failed: <err>
<name> backup failed: <err>
initial synchronization completed (changed=%t rows=%d)
initial synchronization failed; serving existing SQLite state: <err>
```

### service

一个服务的全部行为：读配置、打开副本、跑同步、路由、健康响应、管理员鉴权。

配置是 YAML，键固定（未知键拒绝启动），零值取默认：

| 键 | 默认 | 说明 |
| --- | --- | --- |
| `listenAddr` | 取 `Spec.DefaultListenAddr` | 监听地址 |
| `databasePath` | 必填 | 副本文件 |
| `backupPath` | 必填 | 备份文件，不能与 `databasePath` 相同 |
| `adminTokenFile` | 必填 | 管理员 token 文件 |
| `requestTimeout` | 30s | 一个查询的超时 |
| `dolphinDBAddress` | 必填 | 上游地址 |
| `dolphinDBCredentialsFile` | 必填 | 上游凭据文件，格式见 [upstream](#upstream) |
| `dolphinDBDatabase` | 必填 | 上游库名 |
| `syncInterval` | 1h | 两次成功同步之间的间隔 |
| `syncRetryInterval` | 5m | 一次失败之后的等待 |
| `maxStaleness` | 48h | 数据超过这么久算过期 |

| 名字 | 说明 |
| --- | --- |
| `Spec` | 服务的形状：`Name`、`Noun`、`DefaultListenAddr`、`Open`、`Fetch`、`Register` |
| `Spec.Handler` | 由配置与副本造出 `http.Handler` |
| `Spec.Main` | 二进制的入口：`healthcheck` 子命令、`--config`、启动与信号处理 |
| `Config` | 上面那张表的配置 |
| `Config.Validate` | 检查必填项与路径冲突，填默认值 |
| `LoadConfig` | 读并校验 YAML 配置 |
| `Server` | 路由与鉴权 |
| `Server.Query` | 注册一个 GET 端点，`answer` 的返回值写成 200 的 JSON |
| `Replica` | 交给 `Handler` 的一组东西：`Store`、`Monitor`、`Syncer` |

路由：

| 方法与路径 | 说明 |
| --- | --- |
| `GET /healthz` | 公共健康响应，不含错误文本 |
| `GET /admin/v1/healthz` | 同一份响应，另带 `lastError` 等细节 |
| `POST /admin/v1/sync` | 请求一次立即同步，接受后返回 202 与 `{"status": "accepted"}` |

管理员路由要 `Authorization: Bearer <token>`，token 从 `adminTokenFile` 读，文件为空则服务拒绝启动；比较按 SHA-256 常数时间做。

健康响应（时间都是 RFC3339Nano、UTC）：

```json
{
  "status": "healthy",
  "checkedAt": "2026-01-05T01:02:03.456789Z",
  "database": {"status": "healthy"},
  "dolphinDB": {
    "status": "healthy",
    "inProgress": false,
    "lastAttemptAt": "2026-01-05T01:00:00Z",
    "lastSuccessAt": "2026-01-05T01:00:00Z"
  },
  "dataFreshness": {
    "status": "fresh",
    "ageSeconds": 90,
    "maxStalenessSeconds": 172800,
    "lastSuccessfulSyncAt": "2026-01-05T01:00:00Z"
  }
}
```

| 字段 | 取值 |
| --- | --- |
| `status` | `healthy`、`degraded`、`unhealthy` |
| `database.status` | `healthy`、`degraded`、`unavailable` |
| `dolphinDB.status` | `unknown`、`syncing`、`failed`、`healthy` |
| `dataFreshness.status` | `unknown`、`fresh`、`stale` |

- 副本 `Ping` 不过：`status` 为 `unhealthy`、`database.status` 为 `unavailable`、HTTP 503。
- 副本或备份出错（例如备份与摘要不一致）：`status` 为 `degraded`、`database.status` 为 `degraded`，仍是 200。
- 从未成功同步过：`unhealthy`、503。
- 数据年龄超过 `maxStaleness`：`unhealthy`、`dataFreshness.status` 为 `stale`、503。
- 同步状态不是 `healthy` 而整体仍是 `healthy`：降为 `degraded`。

### adapter

把服务的 HTTP 查询转成一个 MCP 工具：工具参数从 `endpoint.Parameter` 生成 JSON Schema，每次调用转发成一个 GET，服务的答案或错误体原样作为工具结果。

| 名字 | 说明 |
| --- | --- |
| `Spec` | 适配器的形状：`Name`、`Noun`、`DefaultListenAddr`、`DefaultServiceURL`、`Tool`、`Narrow`、`Register`、`Smoke` |
| `Spec.Handler` | 造出 `http.Handler`：`/healthz` 与 `/mcp` |
| `Spec.Main` | 二进制的入口：`healthcheck`、`smoke`、监听相关 flag |
| `Spec.Run` | 解析 flag、校验配置、跑服务到 ctx 结束 |
| `Config` | `ListenAddr`、`ServiceURL`、`RequestTimeout`、`AllowedOrigins` |
| `Client` | 转发给服务的客户端 |
| `Client.Call` | 一次 GET，结果或错误体转成 `mcp.CallToolResult` |
| `ToolError` | 把一份错误体做成 `isError` 的工具结果 |
| `InputSchema` | 由参数表与每个参数的 schema 生成工具的 `InputSchema`；有参数缺 schema 就启动失败 |
| `OneOrMany` | 接受一个字符串或字符串数组的参数类型 |
| `OneOrMany.UnmarshalJSON` | 解析成字符串数组 |
| `OneOrManySchema` | 上面的类型的 schema：不列取值（取值是数据的答案） |
| `SmokeTest` | 冒烟用的两个调用：`Good` 必须返回非空数组，`Bad` 必须被拒且码等于 `BadCode` |
| `DefaultTimeout` | 30 秒 |
| `MaxRequestBytes` | 1 MiB |
| `MaxResponseBytes` | 16 MiB |
| `MaxConcurrency` | 8 |

限制：

- `--timeout` 默认 30 秒，大于 30 秒按 30 秒算；到时间没答完就是 `UpstreamTimeout`。
- 请求体上限 1 MiB，超过是 `RequestTooLarge`。
- 工具结果上限 16 MiB（含 1024 字节信封余量），超过是 `ResponseTooLarge`，并带上 `Narrow` 那句「怎么要得更少」。
- 同时进行的工具调用最多 8 个，第 9 个得到 `RateLimited` 与 `Retry-After: 1`。
- 服务地址必须是 http 或 https、带主机名、不带 query 与 fragment；`--allowed-origins` 的每一项是 `scheme://host[:port]`，不在名单里的 `Origin` 得到 `OriginNotAllowed`。

MCP 协议版本：

- 2026-07-28 及以后：结果的 `content` 里有 `text`（一份 JSON），`structuredContent` 里是同一个 JSON 对象。
- 2026-07-28 之前（`2024-11-05`、`2025-03-26`、`2025-06-18`、`2025-11-25` 这些修订，Python SDK 1.x / 2.x 用的就是它们）：`structuredContent` 只在是 JSON 对象时给；数组会从 `structuredContent` 去掉，同一份 JSON 仍在 `text` 里（SEP-2106）。只认 `structuredContent` 的客户端因此拿不到非对象结果，但不会因为类型不对而报错。

适配器的命令行：

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `--listen-addr` | `Spec.DefaultListenAddr` | 监听地址 |
| `--<noun>-url` | `Spec.DefaultServiceURL` | 要转发的服务地址 |
| `--timeout` | 30s | 一个工具调用的超时 |
| `--allowed-origins` | 空 | 逗号分隔的 Origin 白名单 |

`healthcheck` 探本机 `/healthz`；`smoke` 连 `/mcp`，建一次 MCP 会话，确认工具列表正好是那一个工具、`Good` 调用返回非空数组、`Bad` 调用是工具错误且码为 `BadCode`，其中的日期取北京时间的今天。

### buildinfo

| 名字 | 说明 |
| --- | --- |
| `Version` | 版本号，默认 `dev`，发布构建时用 `-ldflags "-X github.com/initiallyqq/data-api-framework/buildinfo.Version=<版本>"` 注入 |

### test/harness

服务仓端到端测试用的工具：起真实 DolphinDB、造 scratch 库、编译二进制、起服务与适配器、走一遍 HTTP 与 MCP。唯一的替身是数据。

| 名字 | 说明 |
| --- | --- |
| `AdminToken` | 测试用的固定管理员 token |
| `DolphinDB` | 一个 DolphinDB 实例：`Address`、`Database` |
| `DolphinDB.Run` | 跑一段 DolphinDB 脚本 |
| `ScratchDatabase` | 建一个一次性库；`DATA_API_E2E_DOLPHINDB_ADDRESS` 没设就跳过测试 |
| `Build` | 编译仓库里的 `cmd/<name>`，`GOCOVERDIR` 有值时带覆盖率编译 |
| `FreeAddress` | 找一个空闲端口 |
| `Process` | 一个跑起来的进程 |
| `Process.Write` | 写它的标准输入 |
| `Process.Output` | 取它到目前为止的输出 |
| `Process.Stop` | 发 SIGTERM，15 秒后退回 Kill |
| `Start` | 起一个进程 |
| `WaitFor` | 等某件事成立，例如健康检查通过 |
| `WaitHealthy` | 等一个地址的健康检查通过 |
| `Get` | 发一个 GET，返回状态码与响应体 |
| `WriteFile` | 写一个测试用文件 |
| `Service` | 跑起来的服务：`URL`、`Process` |
| `Service.Sync` | 请求一次同步并要求 202 |
| `Upstream` | 一个 DolphinDB 实例与它的地址 |
| `StartService` | 写凭据、token、配置，起服务二进制 |
| `StartAdapter` | 起适配器二进制并连上它的 `/mcp` |
| `Call` | 调一次工具，返回文本与 `isError` |

## 测试

```sh
gofmt -l .
go vet ./...
go test ./...
```

`go test ./...` 含用真实 DolphinDB 的端到端测试，需要一个可连的实例：

```sh
docker run -d -p 127.0.0.1:18848:8848 registry.cn-hangzhou.aliyuncs.com/dolphindbit/dolphindb:v2.00.12.11
export DATA_API_E2E_DOLPHINDB_ADDRESS=127.0.0.1:18848
```

没设 `DATA_API_E2E_DOLPHINDB_ADDRESS` 时，需要它的测试跳过。CI 在 `.github/workflows/test.yml` 里起同一个 DolphinDB 镜像作为服务容器，跑：

- `gofmt -l .` 必须为空；
- `go vet ./...`；
- `go test ./...`；
- `golangci-lint`（版本 v2.15.0，配置在 `.golangci.yml`）：启用 `errcheck`、`govet`、`ineffassign`、`staticcheck`、`unconvert`、`unused`，格式检查用 `gofmt` 与 `goimports`。

## 发布

打一个 `vX.Y.Z` 的 tag 并推到 GitHub 即可，模块路径就是仓库地址，不需要额外步骤：

```sh
git tag vX.Y.Z
git push origin vX.Y.Z
```

tag 名就是模块版本号，以 `v` 开头、按语义化版本递增；推 tag 由仓库维护者做。

消费方：

```sh
go get github.com/initiallyqq/data-api-framework@vX.Y.Z
go mod tidy
```

两个数据服务在 `data-api` 同一个 module 里，用的是同一个版本。

同时改多个仓库时用 `go.work` 把本仓库与消费方放在一起联调，避免反复发布版本。在消费方仓库的根目录执行，`go.work` 不提交：

```sh
go work init . ../data-api-framework
```

本仓库是公开仓，`go get` 走模块代理即可，不需要凭据。没有模块代理的环境里让 Go 直接走 Git，并把仓库地址指到可用的镜像：

```sh
export GOPRIVATE=github.com/initiallyqq/*
git config --global url."ssh://git@<git-host>/initiallyqq/".insteadOf "https://github.com/initiallyqq/"
```

升级框架版本后，用到它的服务要一起重新构建并滚动部署：副本的 schema 版本、健康响应、同步行为都随框架版本走，只升其中一个二进制会让新旧行为同时在线。

## 维护者

initiallyqq

## 许可证

待确认
