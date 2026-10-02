# polling-transport-go

面向变更订阅的统一传输层。客户端可以通过 **轮询（Poll）**、**长轮询（LongPoll）**
和 **长连接事件流（Subscribe）** 三种方式订阅同一份有序变更序列，
断线或慢消费后可按游标正确续传。

## 快速开始

```go
hub, err := transport.NewHub(transport.DefaultConfig())
if err != nil { log.Fatal(err) }
defer hub.Close()

// 写入底层变更（允许乱序、重复到达，内部自动归一化）
hub.Publish(transport.Change{Seq: 1, Key: "user/1", Value: []byte(`{"name":"a"}`)})

// 方式一：轮询
changes, head, _ := hub.Poll(0, 100)

// 方式二：长轮询（无变更时挂起，直到新变更、超时或关闭）
changes, cursor, _ := hub.LongPoll(ctx, cursor, 100)

// 方式三：长连接事件流
sub, _ := hub.Subscribe(cursor)
for ev := range sub.C {
    switch ev.Kind {
    case transport.EventChange:    // 处理变更，确认游标 = ev.Ch.Seq
    case transport.EventHeartbeat: // 保活
    case transport.EventResync:    // 缓冲溢出，需重同步（见下）
    case transport.EventClosed:    // 服务端关闭
    }
}
```

## 一致性语义

- **单一有序序列**：所有变更携带全局单调递增序号（从 1 开始）。
  三种订阅方式读取同一份日志，任一时刻各订阅者看到的游标视图一致，
  不存在方式之间的语义偏差。
- **归一化**：底层变更允许乱序、重复到达。重复变更被丢弃；
  乱序变更被暂存，只有连续无空洞的前缀对订阅者可见。
- **按游标续传**：游标 = 客户端已确认消费的最大序号。携带游标重连后，
  序号大于游标的变更会被完整投递——不丢失、不重复（at-least-once 到游标，
  游标之后精确一次）。
- **背压与重同步**：每个事件流订阅者有独立的有界缓冲（`MaxBufferSize`），
  慢订阅者不会阻塞其他订阅者。缓冲溢出时采用 *drain-and-resync* 策略：
  丢弃该订阅者未消费的缓冲，投递 `EventResync` 信号并关闭通道。
  客户端随后携带最后确认游标重同步：若存量超过缓冲上限，
  先通过 `Poll`/`LongPoll` 分页补齐，再在最新游标处恢复事件流订阅。
- **长轮询**：无新变更时挂起等待（代际通道唤醒，大量并发挂起时唤醒开销 O(1)/
  请求），有新变更立即返回；达到 `LongPollTimeout` 返回空结果与原游标。
- **优雅关闭**：`Close` 唤醒所有挂起的长轮询（返回 `ErrClosed`），
  向所有订阅者投递 `EventClosed` 终止信号并关闭通道，随后等待后台
  心跳任务退出，不遗留后台任务或悬挂连接。
- **并发安全**：所有共享状态均可多订阅者并发访问，`-race` 下无数据竞争。

## 配置项

| 字段 | 含义 | 默认值 | 约束 |
|---|---|---|---|
| `PollInterval` | 给客户端的轮询间隔提示 | 1s | > 0 |
| `LongPollTimeout` | 长轮询单次挂起上限 | 30s | > 0 |
| `MaxBufferSize` | 单订阅者缓冲上限（条） | 1024 | > 0 |
| `MaxSubscribers` | 最大并发订阅数 | 1024 | > 0 |
| `HeartbeatInterval` | 事件流心跳间隔 | 15s | > 0 且 < `LongPollTimeout` |

非法配置会被 `Config.Validate` / `NewHub` 拒绝并返回可读的中文错误。

## 测试

```sh
go test ./transport/ -race -count=1
```

覆盖：并发订阅、按游标重连、乱序与重复投递归一化、背压与重同步、
长轮询挂起/超时、心跳与优雅关闭。测试使用内存时钟（`fakeClock`）
与内存通道，保证可重复。
