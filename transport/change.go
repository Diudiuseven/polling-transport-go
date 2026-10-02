package transport

// Cursor 标识订阅者在有序变更序列中的位置。
// 值为已确认消费的最后一条变更的序号；0 表示从头开始。
type Cursor uint64

// Change 是一条有序的变更记录。
type Change struct {
	Seq   uint64 // 全局单调递增序号，由变更源分配
	Key   string
	Value []byte
}

// Event 是投递给订阅者的元素：可能是变更、心跳或终止/重同步信号。
type Event struct {
	Kind  EventKind
	Ch    Change
	Cause string // Kind 为 EventResync / EventClosed 时的可读原因
}

type EventKind int

const (
	EventChange    EventKind = iota // 正常变更
	EventHeartbeat                  // 心跳，保活
	EventResync                     // 订阅者落后过多，需携带最后游标重新订阅
	EventClosed                     // 服务端关闭，明确终止信号
)
