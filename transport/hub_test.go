package transport

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func testConfig() Config {
	return Config{
		PollInterval:      10 * time.Millisecond,
		LongPollTimeout:   30 * time.Second,
		MaxBufferSize:     16,
		MaxSubscribers:    8,
		HeartbeatInterval: 10 * time.Second,
	}
}

func publishSeq(t *testing.T, h *Hub, seqs ...uint64) {
	t.Helper()
	for _, s := range seqs {
		if err := h.Publish(Change{Seq: s, Key: "k"}); err != nil {
			t.Fatalf("Publish(%d): %v", s, err)
		}
	}
}

func seqsOf(changes []Change) []uint64 {
	out := make([]uint64, len(changes))
	for i, c := range changes {
		out[i] = c.Seq
	}
	return out
}

// 三种订阅方式读取同一份序列，游标视图必须一致。
func TestThreeModesConsistentView(t *testing.T) {
	h, err := newHubWithClock(testConfig(), newFakeClock())
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	// 含乱序与重复的底层投递。
	publishSeq(t, h, 2, 1, 3, 3, 5, 4, 6, 7, 7, 8, 9, 10)
	want := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	// 1) 轮询
	got, cur, err := h.Poll(0, 0)
	if err != nil || cur != 10 {
		t.Fatalf("Poll: cur=%d err=%v", cur, err)
	}
	if !reflect.DeepEqual(seqsOf(got), want) {
		t.Fatalf("Poll seqs=%v", seqsOf(got))
	}

	// 2) 长轮询（已有数据应立即返回）
	got, cur, err = h.LongPoll(context.Background(), 0, 0)
	if err != nil || cur != 10 {
		t.Fatalf("LongPoll: cur=%d err=%v", cur, err)
	}
	if !reflect.DeepEqual(seqsOf(got), want) {
		t.Fatalf("LongPoll seqs=%v", seqsOf(got))
	}

	// 3) 事件流
	sub, err := h.Subscribe(0)
	if err != nil {
		t.Fatal(err)
	}
	var stream []uint64
	for i := 0; i < 10; i++ {
		ev := <-sub.C
		if ev.Kind != EventChange {
			t.Fatalf("event kind=%v", ev.Kind)
		}
		stream = append(stream, ev.Ch.Seq)
	}
	if !reflect.DeepEqual(stream, want) {
		t.Fatalf("stream seqs=%v", stream)
	}
}

// 按游标续传：不丢失也不重复。
func TestResumeByCursor(t *testing.T) {
	h, err := newHubWithClock(testConfig(), newFakeClock())
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	publishSeq(t, h, 1, 2, 3)

	got, cur, _ := h.Poll(0, 2) // 只取前两条，模拟客户端处理中断
	if len(got) != 2 || cur != 3 {
		t.Fatalf("got=%v cur=%d", seqsOf(got), cur)
	}
	clientCursor := Cursor(got[len(got)-1].Seq) // 客户端确认到 2

	publishSeq(t, h, 4, 5)
	got, cur, _ = h.Poll(clientCursor, 0)
	if cur != 5 || !reflect.DeepEqual(seqsOf(got), []uint64{3, 4, 5}) {
		t.Fatalf("resume got=%v cur=%d", seqsOf(got), cur)
	}
}

// 长轮询挂起：无变更时等待，新变更到达立即唤醒。
func TestLongPollSuspendAndWake(t *testing.T) {
	h, err := newHubWithClock(testConfig(), newFakeClock())
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	type res struct {
		changes []Change
		err     error
	}
	done := make(chan res, 1)
	go func() {
		cs, _, err := h.LongPoll(context.Background(), 0, 0)
		done <- res{cs, err}
	}()

	// 挂起期间不应返回。
	select {
	case r := <-done:
		t.Fatalf("长轮询不应空转返回: %+v", r)
	case <-time.After(50 * time.Millisecond):
	}

	publishSeq(t, h, 1)
	select {
	case r := <-done:
		if r.err != nil || len(r.changes) != 1 || r.changes[0].Seq != 1 {
			t.Fatalf("res=%+v", r)
		}
	case <-time.After(time.Second):
		t.Fatal("新变更后长轮询未被唤醒")
	}
}

// 长轮询达到超时上限返回空结果（内存时钟）。
func TestLongPollTimeout(t *testing.T) {
	clk := newFakeClock()
	h, err := newHubWithClock(testConfig(), clk)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	done := make(chan error, 1)
	go func() {
		cs, cur, err := h.LongPoll(context.Background(), 7, 0)
		if err == nil && (len(cs) != 0 || cur != 7) {
			t.Errorf("超时应返回空结果与原游标, got %v %d", cs, cur)
		}
		done <- err
	}()
	clk.waitForTimers(2) // 长轮询超时定时器 + 心跳定时器均就绪
	clk.Advance(31 * time.Second)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("超时返回 err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("超时后长轮询未返回")
	}
}

// 事件流心跳：空闲超过心跳间隔收到 EventHeartbeat（内存时钟）。
func TestStreamHeartbeat(t *testing.T) {
	clk := newFakeClock()
	h, err := newHubWithClock(testConfig(), clk)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	sub, err := h.Subscribe(0)
	if err != nil {
		t.Fatal(err)
	}
	clk.waitForTimers(1) // 等待心跳循环注册定时器
	clk.Advance(11 * time.Second)
	select {
	case ev := <-sub.C:
		if ev.Kind != EventHeartbeat {
			t.Fatalf("kind=%v, want heartbeat", ev.Kind)
		}
	case <-time.After(time.Second):
		t.Fatal("未收到心跳")
	}
}

func TestMaxSubscribers(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubscribers = 1
	h, err := newHubWithClock(cfg, newFakeClock())
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if _, err := h.Subscribe(0); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Subscribe(0); err != ErrTooManySubscribers {
		t.Fatalf("err=%v, want ErrTooManySubscribers", err)
	}
}

func TestConfigValidate(t *testing.T) {
	valid := testConfig()
	if err := valid.Validate(); err != nil {
		t.Fatalf("合法配置被拒绝: %v", err)
	}
	cases := []func(*Config){
		func(c *Config) { c.PollInterval = 0 },
		func(c *Config) { c.LongPollTimeout = -time.Second },
		func(c *Config) { c.MaxBufferSize = 0 },
		func(c *Config) { c.MaxSubscribers = -1 },
		func(c *Config) { c.HeartbeatInterval = 0 },
		func(c *Config) { c.HeartbeatInterval = c.LongPollTimeout },
	}
	for i, mutate := range cases {
		c := valid
		mutate(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("case %d: 非法配置未被拒绝", i)
		}
	}
	if _, err := NewHub(Config{}); err == nil {
		t.Error("零值配置应被 NewHub 拒绝")
	}
}
