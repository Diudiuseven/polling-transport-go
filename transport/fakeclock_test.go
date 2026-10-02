package transport

import (
	"sync"
	"time"
)

// fakeClock 是内存时钟：时间只在 Advance 被调用时前进，保证测试可重复。
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers map[*fakeTimer]struct{}
}

type fakeTimer struct {
	ch   chan time.Time
	fire time.Time
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }
func (t *fakeTimer) Stop() bool {
	select {
	case <-t.ch:
		return false
	default:
		return true
	}
}

func newFakeClock() *fakeClock {
	return &fakeClock{
		now:    time.Unix(0, 0),
		timers: make(map[*fakeTimer]struct{}),
	}
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) NewTimer(d time.Duration) timer {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := &fakeTimer{ch: make(chan time.Time, 1), fire: f.now.Add(d)}
	f.timers[t] = struct{}{}
	return t
}

// Advance 推进时钟并触发所有到期的定时器。
func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
	for t := range f.timers {
		if !f.now.Before(t.fire) {
			select {
			case t.ch <- f.now:
			default:
			}
			delete(f.timers, t)
		}
	}
}

// waitForTimers 自旋直到时钟注册了至少 n 个定时器，用于消除测试时序竞争。
func (f *fakeClock) waitForTimers(n int) {
	for i := 0; i < 10000; i++ {
		f.mu.Lock()
		got := len(f.timers)
		f.mu.Unlock()
		if got >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	panic("fakeClock: 等待定时器超时")
}
