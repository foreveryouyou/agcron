# agcron

`agcron` 是一个**分布式、基于 Redis 的定时任务库**。它在 [gocron](https://github.com/go-co-op/gocron) 之上，叠加了一层 **Redis 领导者选举（leader election）** 和 **共享任务存储（job store）**，使得一个由多个进程组成的集群里，每个被调度的任务在任意时刻都**只会在 Leader 实例上执行一次**——其余实例保持热备，Leader 宕机时自动接管，从而保证高可用与无重复执行。

## 核心特性

- **Leader 选举**：基于 Redis `SET NX` + 心跳续期的分布式锁，集群中只有一个实例执行任务，其余实例热备。
- **共享任务存储**：任务定义存放在共享存储（默认 Redis Hash，可替换为 MySQL / 内存等），所有实例收敛到同一任务集。
- **运行时热更新**：通过 Admin API 增删改任务或启停任务，一次写入即对全集群生效（调度器 reconciler 周期对齐）。
- **两种任务类型**：
  - `func`：注册的 Go 函数任务。
  - `http`：对外发起 HTTP 请求任务。
- **可插拔存储**：`jobstore.Store` 是接口，默认 Redis 实现，另含 MySQL 实现示例，可自行扩展。
- **Admin HTTP 控制面**：查看状态 / 列出任务 / 创建 / 删除 / 启停任务。

## 架构

```text
                ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
   Redis  ─────►│ instance A   │   │ instance B   │   │ instance C   │
 (leader lock)  │  (LEADER)    │   │  (follower)  │   │  (follower)  │
                │ scheduler    │   │ scheduler    │   │ scheduler    │
                │ executor     │   │ executor     │   │ executor     │
                └──────┬───────┘   └──────┬───────┘   └──────┬───────┘
                       └─────────┬───────┴────────┬─────────┘
                              shared job store (Redis / MySQL / ...)
```

- **elector**：Redis 领导者选举，实现 gocron 的 `Elector` 接口。
- **scheduler**：封装 gocron，内置 reconciler 轮询存储并增删改本地调度。
- **executor**：执行任务（Go 函数或 HTTP）。
- **jobstore**：任务定义持久化接口与 Redis / MySQL 实现。
- **admin**：轻量 HTTP 控制面。

## 安装

```bash
go get github.com/foreveryouyou/agcron
```

要求 Go 1.24+，且有一个可用的 Redis 实例（用于领导者选举与默认任务存储）。

## 快速开始

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/foreveryouyou/agcron"
	"github.com/foreveryouyou/agcron/executor"
	"github.com/foreveryouyou/agcron/jobstore"
)

func main() {
	eng, err := agcron.New(context.Background(), agcron.Config{
		RedisAddr:  "localhost:6379",
		InstanceID: "node-1", // 每个进程必须唯一，默认取主机名
		ElectorTTL: 10 * time.Second,
		Reconcile:  5 * time.Second,
		AdminAddr:  ":8080", // 非空时启动 Admin HTTP 服务
		Funcs: executor.FuncRegistry{
			"sayHello": func(ctx context.Context, j jobstore.JobDef) error {
				log.Printf("job %q executed", j.Name)
				return nil
			},
		},
		Seed: []jobstore.JobDef{
			{
				ID:          "job-hello",
				Name:        "say-hello",
				Type:        jobstore.JobTypeFunc,
				Schedule:    "*/10 * * * * *", // 6 段 cron（含秒）
				WithSeconds: true,
				Enabled:     true,
				Func:        "sayHello",
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	eng.Start()
	defer eng.Stop()

	// 阻塞运行...
	select {}
}
```

启动多个实例（不同的 `InstanceID` 指向同一 Redis），即可看到只有 Leader 实例执行任务；关闭 Leader 后，follower 会自动接管。

## 配置项（agcron.Config）

| 字段         | 说明                                                         |
| ------------ | ------------------------------------------------------------ |
| `RedisAddr`  | Redis 地址，如 `localhost:6379`                              |
| `RedisPass`  | Redis 密码，可为空                                           |
| `InstanceID` | 进程唯一标识，默认取主机名                                   |
| `ElectorKey` | 领导者选举使用的 Redis key，默认 `cron:leader`               |
| `ElectorTTL` | 领导者锁 TTL，`<=0` 表示 10s                                 |
| `Reconcile`  | reconciler 轮询间隔，`<=0` 表示 5s                           |
| `Store`      | 任务存储，默认使用 Redis 实现；可传入自定义 `jobstore.Store` |
| `Funcs`      | `executor.FuncRegistry`，按名称注册的 Go 函数任务            |
| `Seed`       | 仅当存储为空时写入的初始化任务                               |
| `AdminAddr`  | 非空时在该地址启动 Admin HTTP 服务                           |

### Engine 主要方法

| 方法                     | 说明                                             |
| ------------------------ | ------------------------------------------------ |
| `Start()`                | 启动调度器，并按需启动 Admin 服务                |
| `Stop()`                 | 停止调度器、释放领导锁、关闭 Redis               |
| `Store()`                | 返回底层任务存储，便于直接读写                   |
| `Mux()`                  | 返回 Admin HTTP handler，便于挂载到自己的 server |
| `RegisterFunc(name, fn)` | 按名称注册 / 替换 Go 函数任务                    |
| `IsLeader()`             | 返回当前实例是否持有领导权                       |

## 任务定义（jobstore.JobDef）

```go
type JobDef struct {
	ID          string          // 任务唯一 ID
	Name        string          // 展示名
	Type        JobType         // "func" 或 "http"
	Schedule    string          // cron 表达式
	WithSeconds bool            // true => 6 段 cron（含秒），false => 5 段
	Enabled     bool            // 是否启用
	Func        string          // Type=="func" 时对应的函数名
	HTTP        HTTPConfig      // Type=="http" 时的请求配置
}
```

### func 类型示例

```go
jobstore.JobDef{
	ID: "job-hello", Name: "say-hello",
	Type: jobstore.JobTypeFunc,
	Schedule: "0 0 * * *", WithSeconds: false,
	Enabled: true, Func: "sayHello",
}
```

### http 类型示例

```go
jobstore.JobDef{
	ID: "job-http", Name: "ping-echo",
	Type: jobstore.JobTypeHTTP,
	Schedule: "*/15 * * * * *", WithSeconds: true,
	Enabled: true,
	HTTP: jobstore.HTTPConfig{
		Method: "POST",
		URL:    "http://localhost:8080/echo",
		Body:   `{"from":"agcron"}`,
	},
}
```

## Admin HTTP API

当 `Config.AdminAddr` 非空时，自动暴露以下端点：

| 方法     | 路径                | 说明                                               |
| -------- | ------------------- | -------------------------------------------------- |
| `GET`    | `/status`           | 返回当前实例、是否 Leader、全部任务                |
| `GET`    | `/jobs`             | 列出全部任务                                       |
| `POST`   | `/jobs`             | 创建 / 覆盖一个任务（body 为 `JobDef`，需含 `id`） |
| `GET`    | `/jobs/{id}`        | 获取单个任务                                       |
| `DELETE` | `/jobs/{id}`        | 删除任务                                           |
| `POST`   | `/jobs/{id}/pause`  | 暂停任务（`Enabled=false`）                        |
| `POST`   | `/jobs/{id}/resume` | 恢复任务（`Enabled=true`）                         |
| `POST`   | `/echo`             | 示例 HTTP 任务的自带回显目标                       |

> 任意实例收到写入请求后写入共享存储，reconciler 会在各实例上周期对齐，因此**单机写入即可收敛整个集群**。

示例：

```bash
# 查看状态
curl http://localhost:8080/status

# 新增一个任务
curl -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"id":"job-x","name":"demo","type":"func","schedule":"*/10 * * * * *","with_seconds":true,"enabled":true,"func":"sayHello"}'

# 暂停任务
curl -X POST http://localhost:8080/jobs/job-x/pause
```

也可以把 Admin 挂载到自己的 HTTP server：

```go
http.Handle("/agcron/", http.StripPrefix("/agcron", eng.Mux()))
http.ListenAndServe(":8080", nil)
```

## 自定义存储（MySQL 示例）

`jobstore.Store` 是接口，默认实现为 Redis。你也可以替换为其他后端——例如内置的 MySQL 实现：

```go
import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/foreveryouyou/agcron/jobstore/mysqlstore"
)

db, _ := sql.Open("mysql", "user:pass@tcp(localhost:3306)/cron?parseTime=true")
store := mysqlstore.New(db, "cron_jobs")
store.Migrate(ctx) // 首次创建表

eng, _ := agcron.New(ctx, agcron.Config{
	RedisAddr: "localhost:6379", // 领导者选举仍用 Redis
	Store:     store,            // 任务定义改用 MySQL
	// ... 其余同上
})
```

> 注意：领导者选举始终依赖 Redis，自定义 `Store` 仅替换任务定义的存储后端。

## 运行示例

仓库 `examples/` 下提供了两个可直接运行的示例：

- `examples/server`：基于默认 Redis 存储的分布式定时任务引擎。
- `examples/mysql`：将任务存储替换为 MySQL 的示例。

使用 Docker Compose 一键拉起 Redis + 3 个实例：

```bash
docker-compose up --build
```

随后分别访问各实例的 Admin 接口（宿主映射端口 `8081` / `8082` / `8083`）观察 Leader 选举与任务执行情况。

手动运行单实例：

```bash
cd examples/server
INSTANCE_ID=cron-a REDIS_ADDR=localhost:6379 go run .
```

## 工作原理简述

1. 每个实例启动时创建一个 `RedisElector`，通过 `SET NX` 竞选领导权并周期性续期；只有持有锁的实例为 Leader。
2. `Scheduler` 内置 reconciler，按 `Reconcile` 间隔从共享存储 `List` 全部任务，并与本地 gocron 收敛（新增 / 更新 / 删除）。
3. gocron 通过 `Elector` 接口判断当前是否 Leader，仅在 Leader 上真正触发 `Executor.Run`。
4. `Executor.Run` 重新从存储读取任务定义（因此启停立即生效），按 `Type` 执行 Go 函数或发起 HTTP 请求。
5. 任意实例通过 Admin API 写入存储后，所有实例在下一个 reconciler 周期对齐，实现集群一致。

## 许可证

详见仓库 LICENSE。
