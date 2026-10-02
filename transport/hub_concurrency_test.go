package transport

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

// 背压：慢订阅者缓冲满后收到 EventResync 并进入可重同步状态，
// 且不阻塞其他订阅者。
func TestBackpressureSlowSubscriber(t *testing.T) {
	cfg := testConfig()
	cfg.MaxBufferSize = 4
	h, err := newHubWithClock(cfg, newFakeClock())
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	slow, err := h.Subscribe(0)
	if err != nil {
		t.Fatal(err)
	}
	fast, err := h.Subscribe(0)
	if err != nil {
		t.Fatal(err)
	}
	// 快订阅者持续消费，验证其不被慢订阅者阻塞。
	fastSeqs := make(chan uint64, 16)
	go func() {
		for ev := range fast.C {
			if ev.Kind == EventChange {
				fastSeqs <- ev.Ch.Seq
			}
		}
		close(fastSeqs)
	}()

	// 逐条发布并确认快订阅者及时收到（不被慢订阅者阻塞）。
	for want := uint64(1); want <= 10; want++ {
		publishSeq(t, h, want)
		select {
		case got := <-fastSeqs:
			if got != want {
				t.Fatalf("fast: got seq=%d, want %d", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("快订阅者被慢订阅者阻塞, 停在 seq=%d", want)
		}
	}

	// 慢订阅者：缓冲溢出策略为丢弃未消费缓冲并投递重同步信号，
	// 随后通道关闭。已确认游标之后的变更通过重订阅补齐。
	var seqs []uint64
	var gotResync bool
	for ev := range slow.C {
		switch ev.Kind {
		case EventChange:
			seqs = append(seqs, ev.Ch.Seq)
		case EventResync:
			gotResync = true
		}
	}
	if !gotResync {
		t.Fatalf("slow: 应收到重同步信号, seqs=%v", seqs)
	}
	if err := slow.Err(); err == nil {
		t.Fatal("慢订阅者终止后 Err 应非空")
	}

	// 慢订阅者重同步：存量超过缓冲上限，先用轮询分页补齐，
	// 再在最新游标处恢复事件流订阅。
	last := Cursor(0)
	if len(seqs) > 0 {
		last = Cursor(seqs[len(seqs)-1])
	}
	backlog, head, err := h.Poll(last, 0)
	if err != nil {
		t.Fatal(err)
	}
	if head != 10 || uint64(len(backlog)) != 10-uint64(last) {
		t.Fatalf("backlog=%d head=%d", len(backlog), head)
	}
	resub, err := h.Subscribe(head)
	if err != nil {
		t.Fatal(err)
	}
	publishSeq(t, h, 11)
	ev := <-resub.C
	if ev.Kind != EventChange || ev.Ch.Seq != 11 {
		t.Fatalf("重同步后应继续收到实时变更, got %+v", ev)
	}

}

// 优雅关闭：挂起的长轮询被正常结束，订阅者收到明确终止信号。
func TestGracefulClose(t *testing.T) {
	h, err := newHubWithClock(testConfig(), newFakeClock())
	if err != nil {
		t.Fatal(err)
	}
	publishSeq(t, h, 1)
	sub, err := h.Subscribe(0)
	if err != nil {
		t.Fatal(err)
	}

	lpDone := make(chan error, 1)
	go func() {
		_, _, err := h.LongPoll(context.Background(), 1, 0) // 无新变更，挂起
		lpDone <- err
	}()
	time.Sleep(20 * time.Millisecond) // 确保长轮询已挂起

	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-lpDone:
		if err != ErrClosed {
			t.Fatalf("挂起的长轮询应收到 ErrClosed, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("关闭后长轮询未结束")
	}

	// 订阅者收到：存量变更 + 明确终止信号，随后通道关闭。
	ev1 := <-sub.C
	if ev1.Kind != EventChange {
		t.Fatalf("ev1=%+v", ev1)
	}
	ev2 := <-sub.C
	if ev2.Kind != EventClosed {
		t.Fatalf("应收到 EventClosed, got %+v", ev2)
	}
	if _, ok := <-sub.C; ok {
		t.Fatal("终止信号后通道应关闭")
	}
	if err := sub.Err(); err != nil {
		t.Fatalf("正常关闭 Err 应为 nil, got %v", err)
	}

	// 关闭后所有操作报错，重复关闭幂等报错。
	if err := h.Close(); err != ErrClosed {
		t.Fatalf("重复关闭应返回 ErrClosed, got %v", err)
	}
	if _, _, err := h.Poll(0, 0); err != ErrClosed {
		t.Fatalf("Poll: %v", err)
	}
	if _, _, err := h.LongPoll(context.Background(), 0, 0); err != ErrClosed {
		t.Fatalf("LongPoll: %v", err)
	}
	if _, err := h.Subscribe(0); err != ErrClosed {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := h.Publish(Change{Seq: 2}); err != ErrClosed {
		t.Fatalf("Publish: %v", err)
	}
}

// 高并发压力：多发布者乱序写入，多订阅者并发消费，
// 所有订阅者必须看到一致的有序序列；关闭后不得遗留后台任务。
func TestConcurrentStress(t *testing.T) {
	cfg := testConfig()
	cfg.MaxBufferSize = 2048
	cfg.MaxSubscribers = 64
	h, err := newHubWithClock(cfg, realClock{})
	if err != nil {
		t.Fatal(err)
	}
	before := runtime.NumGoroutine()

	const publishers = 4
	const perPublisher = 250
	const total = publishers * perPublisher
	const subscribers = 8

	var wg sync.WaitGroup
	results := make([][]uint64, subscribers)
	// 订阅者：持续消费直到终止信号。
	for i := 0; i < subscribers; i++ {
		sub, err := h.Subscribe(0)
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func(idx int, s *Subscription) {
			defer wg.Done()
			for ev := range s.C {
				if ev.Kind == EventChange {
					results[idx] = append(results[idx], ev.Ch.Seq)
				}
			}
		}(i, sub)
	}
	// 长轮询者：游标单调前进直到关闭。
	var lpWg sync.WaitGroup
	for i := 0; i < 4; i++ {
		lpWg.Add(1)
		go func() {
			defer lpWg.Done()
			cur := Cursor(0)
			for {
				cs, next, err := h.LongPoll(context.Background(), cur, 100)
				if err == ErrClosed {
					return
				}
				if err != nil {
					return
				}
				if next < cur {
					t.Error("长轮询游标回退")
					return
				}
				cur = next
				_ = cs
			}
		}()
	}
	// 发布者：各自负责一段序号区间，并发写入形成乱序。
	var pubWg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		pubWg.Add(1)
		go func(p int) {
			defer pubWg.Done()
			for i := 1; i <= perPublisher; i++ {
				if err := h.Publish(Change{Seq: uint64(p*perPublisher + i)}); err != nil {
					return
				}
			}
		}(p)
	}
	pubWg.Wait()
	if head := h.log.head(); head != total {
		t.Fatalf("head=%d, want %d", head, total)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	lpWg.Wait()

	// 每个订阅者必须看到完整且一致的 1..total。
	for i, seqs := range results {
		if len(seqs) != total {
			t.Fatalf("sub %d 收到 %d 条, want %d", i, len(seqs), total)
		}
		for j, s := range seqs {
			if s != uint64(j+1) {
				t.Fatalf("sub %d 第 %d 条 seq=%d", i, j, s)
			}
		}
	}
	// 关闭后不得遗留后台任务。
	for i := 0; i < 100; i++ {
		if runtime.NumGoroutine() <= before {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("疑似后台任务泄漏: before=%d after=%d", before, runtime.NumGoroutine())
}
