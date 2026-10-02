package transport

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrClosed 表示传输层已关闭。
var ErrClosed = errors.New("transport: hub 已关闭")

// ErrTooManySubscribers 表示并发订阅数达到上限。
var ErrTooManySubscribers = errors.New("transport: 并发订阅数已达上限")

// timer 抽象便于测试使用内存时钟。
type timer interface {
	C() <-chan time.Time
	Stop() bool
}

type clock interface {
	NewTimer(d time.Duration) timer
	Now() time.Time
}

type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time { return r.t.C }
func (r realTimer) Stop() bool          { return r.t.Stop() }

type realClock struct{}

func (realClock) NewTimer(d time.Duration) timer { return realTimer{time.NewTimer(d)} }
func (realClock) Now() time.Time                 { return time.Now() }

// Subscription 是一个长连接事件流订阅句柄。
type Subscription struct {
	C    <-chan Event // 事件流；终止时关闭
	hub  *Hub
	ch   chan Event
	done chan struct{} // 终止后关闭
	err  error         // 终止原因（正常关闭时为 nil）

	cursor Cursor // 已投递的最大序号，仅由 hub 在锁内维护
}

// Err 返回订阅终止原因；通道正常因服务端关闭而结束时为 nil。
func (s *Subscription) Err() error {
	<-s.done
	return s.err
}

// Hub 是订阅中枢：三种订阅方式共享同一份 changeLog，
// 负责扇出、慢订阅者背压、长轮询挂起唤醒与优雅关闭。
type Hub struct {
	cfg Config
	log *changeLog
	clk clock

	mu       sync.Mutex
	closed   bool
	waitGen  chan struct{} // 每次可见前缀推进或关闭时关闭并替换，用于唤醒长轮询
	subs     map[*Subscription]struct{}
	lastSend map[*Subscription]time.Time // 各订阅最近一次投递时间（心跳用）

	wg sync.WaitGroup // 后台心跳任务
}

// NewHub 创建订阅中枢，配置非法时返回错误。
func NewHub(cfg Config) (*Hub, error) {
	return newHubWithClock(cfg, realClock{})
}

func newHubWithClock(cfg Config, clk clock) (*Hub, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	h := &Hub{
		cfg:      cfg,
		log:      newChangeLog(),
		clk:      clk,
		waitGen:  make(chan struct{}),
		subs:     make(map[*Subscription]struct{}),
		lastSend: make(map[*Subscription]time.Time),
	}
	h.wg.Add(1)
	go h.heartbeatLoop()
	return h, nil
}

// Publish 写入一条底层变更（允许乱序/重复，内部归一化）。
func (h *Hub) Publish(c Change) error {
	head, advanced := h.log.append(c)
	if !advanced {
		return nil
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return ErrClosed
	}
	// 唤醒所有挂起的长轮询（O(1)：关闭代际通道）。
	close(h.waitGen)
	h.waitGen = make(chan struct{})
	// 扇出到事件流订阅者。
	now := h.clk.Now()
	for s := range h.subs {
		h.pushLocked(s, h.log.read(afterOf(s, head), 0), now)
	}
	h.mu.Unlock()
	return nil
}

// afterOf 返回订阅者尚未接收的起始游标。调用方须持有 h.mu。
func afterOf(s *Subscription, head Cursor) Cursor {
	return s.cursor
}

// Poll 立即读取游标之后的变更（简单轮询），返回变更与最新游标。
func (h *Hub) Poll(after Cursor, limit int) ([]Change, Cursor, error) {
	if h.isClosed() {
		return nil, after, ErrClosed
	}
	return h.log.read(after, limit), h.log.head(), nil
}

// LongPoll 挂起等待新变更，直到有新变更、超时、上下文取消或关闭。
// 超时返回空切片与原游标，属正常结果。
func (h *Hub) LongPoll(ctx context.Context, after Cursor, limit int) ([]Change, Cursor, error) {
	deadline := h.clk.NewTimer(h.cfg.LongPollTimeout)
	defer deadline.Stop()
	for {
		// 先取代际再读日志，避免与 Publish 竞争丢唤醒。
		h.mu.Lock()
		if h.closed {
			h.mu.Unlock()
			return nil, after, ErrClosed
		}
		gen := h.waitGen
		h.mu.Unlock()

		if changes := h.log.read(after, limit); len(changes) > 0 {
			return changes, Cursor(changes[len(changes)-1].Seq), nil
		}

		select {
		case <-gen:
			// 有新变更或关闭，重新检查。
		case <-ctx.Done():
			return nil, after, ctx.Err()
		case <-deadline.C():
			return nil, after, nil
		}
	}
}

// Subscribe 建立长连接事件流订阅。先补发游标之后的存量变更，
// 随后实时投递新变更；缓冲满时订阅转入 EventResync 终止。
func (h *Hub) Subscribe(after Cursor) (*Subscription, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, ErrClosed
	}
	if len(h.subs) >= h.cfg.MaxSubscribers {
		return nil, ErrTooManySubscribers
	}
	ch := make(chan Event, h.cfg.MaxBufferSize)
	s := &Subscription{hub: h, ch: ch, done: make(chan struct{})}
	s.C = ch
	s.cursor = after
	h.subs[s] = struct{}{}
	h.lastSend[s] = h.clk.Now()
	// 补发存量（锁内，保证与实时扇出不交错）。
	h.pushLocked(s, h.log.read(after, 0), h.clk.Now())
	return s, nil
}

// Close 优雅关闭：唤醒挂起的长轮询，向所有订阅者发送终止信号，
// 并等待后台任务退出。
func (h *Hub) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return ErrClosed
	}
	h.closed = true
	close(h.waitGen) // 唤醒长轮询，使其看到 closed 状态
	for s := range h.subs {
		h.terminateLocked(s, Event{Kind: EventClosed, Cause: "服务端关闭"}, nil)
	}
	h.mu.Unlock()
	h.wg.Wait()
	return nil
}

// Config 返回中枢配置（含给客户端的轮询间隔提示）。
func (h *Hub) Config() Config { return h.cfg }

func (h *Hub) isClosed() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closed
}

// pushLocked 将变更追加到订阅缓冲，并推进订阅游标。
// 缓冲满时按背压策略终止订阅并投递 EventResync。调用方须持有 h.mu。
func (h *Hub) pushLocked(s *Subscription, changes []Change, now time.Time) {
	if _, ok := h.subs[s]; !ok {
		return // 已终止
	}
	for _, c := range changes {
		select {
		case s.ch <- Event{Kind: EventChange, Ch: c}:
			s.cursor = Cursor(c.Seq)
		default:
			h.terminateLocked(s, Event{Kind: EventResync,
				Cause: "订阅缓冲已满，请携带最后游标重新订阅"}, errBufferOverflow)
			return
		}
	}
	if len(changes) > 0 {
		h.lastSend[s] = now
	}
}

var errBufferOverflow = errors.New("transport: 订阅缓冲溢出")

// terminateLocked 终止订阅：保证终止信号可达（必要时清空缓冲腾位），
// 随后关闭通道。调用方须持有 h.mu。
func (h *Hub) terminateLocked(s *Subscription, sig Event, err error) {
	if _, ok := h.subs[s]; !ok {
		return
	}
	delete(h.subs, s)
	delete(h.lastSend, s)
	select {
	case s.ch <- sig:
	default:
		// 缓冲满：排空后投递终止信号，保证订阅者收到明确结束原因。
		for len(s.ch) > 0 {
			<-s.ch
		}
		s.ch <- sig
	}
	close(s.ch)
	s.err = err
	close(s.done)
}

// heartbeatLoop 周期性地向空闲订阅者发送心跳，关闭时退出。
func (h *Hub) heartbeatLoop() {
	defer h.wg.Done()
	for {
		t := h.clk.NewTimer(h.cfg.HeartbeatInterval)
		h.mu.Lock()
		if h.closed {
			h.mu.Unlock()
			t.Stop()
			return
		}
		gen := h.waitGen
		h.mu.Unlock()
		select {
		case <-t.C():
		case <-gen:
			// 有活动，重新评估（也避免关闭时无法退出）。
			t.Stop()
			continue
		}
		now := h.clk.Now()
		h.mu.Lock()
		for s := range h.subs {
			if now.Sub(h.lastSend[s]) < h.cfg.HeartbeatInterval {
				continue
			}
			select {
			case s.ch <- Event{Kind: EventHeartbeat}:
				h.lastSend[s] = now
			default:
				// 缓冲满时心跳可丢弃，背压由 pushLocked 处理。
			}
		}
		h.mu.Unlock()
	}
}
