package transport

import (
	"sync"
	"time"
)

// Clock abstracts time so tests can use an in-memory deterministic clock.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// After returns a channel that fires after d.
	After(d time.Duration) <-chan time.Time
	// Ticker returns a periodic ticker.
	Ticker(d time.Duration) Ticker
	// Timer returns a one-shot timer that can be reset.
	Timer(d time.Duration) Timer
}

// Timer is a resettable one-shot timer.
type Timer interface {
	// C returns the timer channel.
	C() <-chan time.Time
	// Reset resets the timer to fire after d.
	Reset(d time.Duration)
	// Stop stops the timer.
	Stop()
}

// Ticker is a periodic timer.
type Ticker interface {
	// C returns the tick channel.
	C() <-chan time.Time
	// Stop stops the ticker.
	Stop()
}

// RealClock uses the real system clock.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

func (RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

func (RealClock) Ticker(d time.Duration) Ticker { return &realTicker{t: time.NewTicker(d)} }

func (RealClock) Timer(d time.Duration) Timer {
	t := time.NewTimer(d)
	return &realTimer{t: t}
}

type realTicker struct {
	t *time.Ticker
}

func (t *realTicker) C() <-chan time.Time { return t.t.C }
func (t *realTicker) Stop()               { t.t.Stop() }

type realTimer struct {
	t *time.Timer
}

func (t *realTimer) C() <-chan time.Time { return t.t.C }
func (t *realTimer) Reset(d time.Duration) {
	t.t.Stop()
	select {
	case <-t.t.C:
	default:
	}
	t.t.Reset(d)
}
func (t *realTimer) Stop() { t.t.Stop() }

// FakeClock is an in-memory clock for deterministic tests. Time only
// advances when Advance is called; all due timers and tickers fire in
// deadline order.
type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	timers  []*fakeTimer
	tickers []*fakeTicker
}

type fakeTimer struct {
	deadline time.Time
	ch       chan time.Time
	stopped  bool
}

type fakeTicker struct {
	interval time.Duration
	next     time.Time
	ch       chan time.Time
	stopped  bool
}

// NewFakeClock creates a fake clock starting at the given time.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

// Now returns the fake current time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After returns a channel that fires once when the clock has advanced past d.
func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{deadline: c.now.Add(d), ch: make(chan time.Time, 1)}
	c.timers = append(c.timers, t)
	return t.ch
}

// Ticker returns a fake periodic ticker.
func (c *FakeClock) Ticker(d time.Duration) Ticker {
	c.mu.Lock()
	defer c.mu.Unlock()
	tk := &fakeTicker{interval: d, next: c.now.Add(d), ch: make(chan time.Time, 1)}
	c.tickers = append(c.tickers, tk)
	return &fakeTickerHandle{c: c, tk: tk}
}

// Timer returns a fake one-shot timer.
func (c *FakeClock) Timer(d time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{deadline: c.now.Add(d), ch: make(chan time.Time, 1)}
	c.timers = append(c.timers, t)
	return &fakeTimerHandle{c: c, t: t}
}

// Advance moves the clock forward by d, firing all timers and tickers whose
// deadline has passed, in deadline order.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	target := c.now.Add(d)
	// Prune stopped timers and tickers.
	alive := c.timers[:0]
	for _, t := range c.timers {
		if !t.stopped {
			alive = append(alive, t)
		}
	}
	c.timers = alive
	aliveT := c.tickers[:0]
	for _, tk := range c.tickers {
		if !tk.stopped {
			aliveT = append(aliveT, tk)
		}
	}
	c.tickers = aliveT
	for {
		earliest := target.Add(time.Nanosecond)
		timerIdx, tickerIdx := -1, -1
		for i, t := range c.timers {
			if !t.deadline.After(target) && t.deadline.Before(earliest) {
				earliest, timerIdx = t.deadline, i
			}
		}
		for i, tk := range c.tickers {
			if tk.stopped {
				continue
			}
			if !tk.next.After(target) && tk.next.Before(earliest) {
				earliest, tickerIdx = tk.next, i
			}
		}
		if timerIdx == -1 && tickerIdx == -1 {
			break
		}
		c.now = earliest
		if timerIdx >= 0 {
			t := c.timers[timerIdx]
			t.ch <- earliest
			c.timers = append(c.timers[:timerIdx], c.timers[timerIdx+1:]...)
		} else {
			tk := c.tickers[tickerIdx]
			select {
			case tk.ch <- earliest:
			default:
			}
			tk.next = tk.next.Add(tk.interval)
		}
	}
	c.now = target
}

// PendingTimers returns the number of active (not stopped) timers currently
// registered.
func (c *FakeClock) PendingTimers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, t := range c.timers {
		if !t.stopped {
			n++
		}
	}
	return n
}

// PendingTickers returns the number of active tickers currently registered.
func (c *FakeClock) PendingTickers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, tk := range c.tickers {
		if !tk.stopped {
			n++
		}
	}
	return n
}

type fakeTickerHandle struct {
	c  *FakeClock
	tk *fakeTicker
}

type fakeTimerHandle struct {
	c *FakeClock
	t *fakeTimer
}

func (h *fakeTimerHandle) C() <-chan time.Time { return h.t.ch }

func (h *fakeTimerHandle) Stop() {
	h.c.mu.Lock()
	defer h.c.mu.Unlock()
	h.t.stopped = true
}

func (h *fakeTimerHandle) Reset(d time.Duration) {
	h.c.mu.Lock()
	defer h.c.mu.Unlock()
	h.t.deadline = h.c.now.Add(d)
	h.t.stopped = false
	select {
	case <-h.t.ch:
	default:
	}
}

func (h *fakeTickerHandle) C() <-chan time.Time { return h.tk.ch }

func (h *fakeTickerHandle) Stop() {
	h.c.mu.Lock()
	defer h.c.mu.Unlock()
	h.tk.stopped = true
}
