# polling-transport-go

面向变更订阅的统一传输层。客户端可以通过**轮询（poll）**、**长轮询（long-poll）**
和**长连接事件流（stream）**三种方式订阅同一份有序变更序列，并在断线或慢消费之后
仍按游标正确续传。

## 特性

- **单一有序变更源**：三种传输方式从同一个 gap-free 的连续前缀读取变更，
  任一时刻各订阅者看到的游标视图完全一致，不同方式之间没有可见的语义偏差。
- **按游标续传**：客户端携带上次处理位置重连，已确认的变更不丢失、不重复。
- **乱序与重复归一化**：变更携带单调递增的序号，乱序到达先入缓冲区、补齐空洞后
  再推进；重复序号直接丢弃。
- **长轮询挂起而非空转**：无新变更时长轮询挂起等待，有新变更或超时上限时立即
  返回。日志前沿通过**广播代次通道（generation channel）**唤醒所有等待者，
  每次追加的唤醒开销为 O(1)，与挂起请求数量无关。
- **慢订阅者背压**：每个订阅者拥有独立的有界缓冲；单个订阅者变慢只阻塞自己，
  不影响其他订阅者。缓冲超过写超时上限时，该订阅者进入**可重新同步状态**，
  客户端携带最后游标重连即可续传。
- **优雅关闭与排水**：关闭时所有挂起的长轮询被唤醒（`ErrShutdown`），事件流收到
  终止事件（`EventClosed`），后台任务全部退出，无 goroutine 或连接泄漏。
- **并发安全**：所有共享状态在多订阅者并发访问下安全，通过 `-race` 检测。
- **可测试时钟**：内置真实时钟与内存假时钟（`FakeClock`），长轮询超时、心跳、
  背压等时间相关测试可确定性重复。

## 核心概念

### 变更日志（ChangeLog）

```go
type Change struct {
    Seq   int64  // 全局单调递增序号
    Key   string
    Value []byte
    Time  time.Time
}
```

- `Append` 接收变更：序号已存在则丢弃（去重）；序号按 `1,2,3...` 连续排列，
  乱序到达时先缓存，空洞补齐后前沿（frontier）推进。
- `Read(afterSeq, limit)` 只返回序号严格大于 `afterSeq` 的连续前缀中的变更，
  因此读者永远看不到空洞。
- `WaitChan(afterSeq)` 返回一个通道：当前沿已超过 `afterSeq` 时立即关闭，否则
  在前沿推进时关闭。所有等待者共享同一个广播通道，单次关闭唤醒全部。

### 三种传输

| 方式 | 接口 | 语义 |
| --- | --- | --- |
| 轮询 | `Poll(ctx, id, afterSeq, limit)` | 立即返回当前可用变更，无变更时返回空 |
| 长轮询 | `LongPoll(ctx, id, afterSeq, limit, timeout)` | 有变更立即返回；否则挂起直到有变更或超时，超时返回空 |
| 事件流 | `Stream(ctx, id, afterSeq)` | 返回 `<-chan Event`，持续推送变更，空闲时发心跳 |

三种方式返回的变更顺序、游标含义完全相同：`next` 为本次最后一条变更的序号，
客户端下次请求携带该游标。

### 游标一致性与续传

- 游标 = 客户端已处理的最后一条变更序号。服务端只推送序号严格大于游标的变更。
- 事件流通过 `Ack(ctx, id, seq)` 确认已处理位置；重连时服务端取
  `max(客户端传入游标, 已确认位点, 已投递位点)` 作为起点，因此即使客户端传入
  过期游标，已确认的变更也不会重复投递。
- 日志保留全部历史变更，按游标重连可以从任意位置恢复。

### 背压与重新同步

- 每个事件流订阅者有一个容量为 `BufferSize` 的有界缓冲通道。
- 泵（pump）向通道发送事件：缓冲满时阻塞（对该订阅者施加背压，不影响他人），
  阻塞超过写超时（`LongPollTimeout`）则进入重新同步状态：排空缓冲、发送
  `EventResync` 标记并关闭流。
- 客户端收到 `EventResync` 后，用最后处理的游标重新调用 `Stream` 即可续传，
  缓冲中丢弃的变更仍在日志中，会被重新投递。

### 优雅关闭

```go
err := server.Shutdown(ctx)
```

- 关闭后新请求返回 `ErrShutdown`。
- 挂起中的长轮询立即返回 `ErrShutdown`。
- 打开中的事件流收到 `EventClosed` 后关闭。
- 所有后台 goroutine 退出后 `Shutdown` 返回（或 ctx 超时）。

## 配置

```go
type Config struct {
    PollInterval      time.Duration // 轮询间隔建议值，必须 > 0
    LongPollTimeout   time.Duration // 长轮询最大挂起时间，必须 > 0
    BufferSize        int           // 单订阅者缓冲上限，必须 > 0
    MaxSubscriptions  int           // 最大并发订阅数，必须 > 0
    HeartbeatInterval time.Duration // 事件流空闲心跳间隔，必须 > 0
}
```

使用 `DefaultConfig()` 获取默认配置；`New` 会调用 `Validate`，非法配置返回
`ErrInvalidConfig` 及可读错误信息。额外约束：`HeartbeatInterval` 不应超过
`LongPollTimeout`。

## 测试

```bash
go test ./... -race
```

- `log_test.go`：追加、去重、乱序归一、空洞补齐唤醒、游标读取。
- `server_test.go`：轮询、长轮询（立即返回/挂起/超时/关闭唤醒）、事件流、
  心跳、背压与重新同步、断线重连、已确认位点不重复、最大订阅数、配置校验、
  优雅关闭排水、多订阅者并发、100 个长轮询同时唤醒。

时间相关测试使用 `FakeClock`（内存时钟）：只有调用 `Advance` 才会推进时间，
因此长轮询超时、心跳、写超时等行为可确定性复现，不依赖真实时间。

## 接口速览

```go
cfg := transport.DefaultConfig()
srv, err := transport.New(cfg)

// 追加变更
srv.Log().Append(transport.Change{Seq: 1, Key: "order", Value: []byte("...")})

// 轮询
changes, next, err := srv.Poll(ctx, "poller-1", cursor, 100)

// 长轮询
changes, next, err := srv.LongPoll(ctx, "lp-1", cursor, 100, 30*time.Second)

// 事件流
ch, err := srv.Stream(ctx, "stream-1", cursor)
for ev := range ch {
    switch ev.Type {
    case transport.EventChange:
        // 处理 ev.Change
        srv.Ack(ctx, "stream-1", ev.Cursor)
    case transport.EventHeartbeat:
        // 心跳
    case transport.EventResync:
        // 用最后游标重连
    case transport.EventClosed:
        // 服务端关闭
    }
}

// 退订 / 关闭
srv.Unsubscribe(ctx, "stream-1")
srv.Shutdown(ctx)
```
