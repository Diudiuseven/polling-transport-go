package transport

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

func testConfig() Config {
	return Config{
		PollInterval:      10 * time.Millisecond,
		LongPollTimeout:   5 * time.Second,
		BufferSize:        64,
		MaxSubscriptions:  100,
		HeartbeatInterval: 1 * time.Second,
	}
}

func newTestServer(t *testing.T, cfg Config) (*Server, *FakeClock) {
	t.Helper()
	clk := NewFakeClock(time.Unix(1000, 0))
	s, err := New(cfg, WithClock(clk))
	if err != nil {
		t.Fatal(err)
	}
	return s, clk
}

// waitForPending blocks until the fake clock has at least one pending timer.
func waitForPending(clk *FakeClock) {
	for clk.PendingTimers() == 0 {
		runtime.Gosched()
	}
}

// readSeqs reads exactly n change events. It returns early with ErrResync
// on a resync marker or ErrShutdown on a closed stream.
func readSeqs(ch <-chan Event, n int, timeout time.Duration) ([]int64, error) {
	var seqs []int64
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for len(seqs) < n {
		select {
		case ev, ok := <-ch:
			if !ok {
				return seqs, ErrShutdown
			}
			switch ev.Type {
			case EventChange:
				seqs = append(seqs, ev.Change.Seq)
			case EventResync:
				return seqs, ErrResync
			case EventClosed:
				return seqs, ErrShutdown
			}
		case <-timer.C:
			return seqs, context.DeadlineExceeded
		}
	}
	return seqs, nil
}

// awaitResync reads events until a resync marker (or channel close).
func awaitResync(ch <-chan Event, timeout time.Duration) ([]int64, error) {
	var seqs []int64
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return seqs, ErrResync
			}
			switch ev.Type {
			case EventChange:
				seqs = append(seqs, ev.Change.Seq)
			case EventResync:
				return seqs, ErrResync
			case EventClosed:
				return seqs, ErrShutdown
			}
		case <-timer.C:
			return seqs, context.DeadlineExceeded
		}
	}
}

func TestConfig_Validate(t *testing.T) {
	valid := testConfig()
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	bad := []Config{
		{0, 5 * time.Second, 64, 100, time.Second},
		{time.Millisecond, 0, 64, 100, time.Second},
		{time.Millisecond, 5 * time.Second, 0, 100, time.Second},
		{time.Millisecond, 5 * time.Second, 64, 0, time.Second},
		{time.Millisecond, 5 * time.Second, 64, 100, 0},
		{time.Millisecond, time.Second, 64, 100, 2 * time.Second}, // heartbeat > longpoll
	}
	for i, c := range bad {
		if err := c.Validate(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("bad config %d: err=%v, want ErrInvalidConfig", i, err)
		}
		if _, err := New(c); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("bad config %d: New err=%v, want ErrInvalidConfig", i, err)
		}
	}
}

func TestServer_Poll(t *testing.T) {
	s, _ := newTestServer(t, testConfig())
	defer s.Shutdown(context.Background())

	s.Log().Append(mkChange(1, "a"), mkChange(2, "b"))
	ctx := context.Background()

	changes, next, err := s.Poll(ctx, "p1", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 2 || next != 2 {
		t.Fatalf("got %d changes next=%d, want 2/2", len(changes), next)
	}

	// No new changes: empty.
	changes, next, err = s.Poll(ctx, "p1", next, 10)
	if err != nil || len(changes) != 0 || next != 2 {
		t.Fatalf("got %d changes next=%d err=%v, want 0/2/nil", len(changes), next, err)
	}

	// Out-of-order + duplicate append is normalized before polling.
	s.Log().Append(mkChange(4, "d"), mkChange(3, "c"), mkChange(3, "c"))
	changes, next, err = s.Poll(ctx, "p1", 2, 10)
	if err != nil || len(changes) != 2 || next != 4 {
		t.Fatalf("got %+v next=%d err=%v, want seq 3,4 / 4", changes, next, err)
	}
}

func TestServer_LongPoll_ReturnsImmediately(t *testing.T) {
	s, _ := newTestServer(t, testConfig())
	defer s.Shutdown(context.Background())
	s.Log().Append(mkChange(1, "a"))

	changes, next, err := s.LongPoll(context.Background(), "lp1", 0, 10, time.Minute)
	if err != nil || len(changes) != 1 || next != 1 {
		t.Fatalf("got %d changes next=%d err=%v", len(changes), next, err)
	}
}

func TestServer_LongPoll_WaitsAndReturns(t *testing.T) {
	s, clk := newTestServer(t, testConfig())
	defer s.Shutdown(context.Background())
	ctx := context.Background()

	s.Log().Append(mkChange(1, "a"))
	_, next, err := s.LongPoll(ctx, "lp1", 0, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		changes []Change
		next    int64
		err     error
	}
	done := make(chan result, 1)
	go func() {
		c, n, e := s.LongPoll(ctx, "lp1", next, 10, time.Minute)
		done <- result{c, n, e}
	}()
	waitForPending(clk)

	select {
	case <-done:
		t.Fatal("long-poll should be suspended while no changes")
	default:
	}

	s.Log().Append(mkChange(2, "b"))
	select {
	case r := <-done:
		if r.err != nil || len(r.changes) != 1 || r.next != 2 {
			t.Fatalf("got %+v, want 1 change / next 2", r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("long-poll did not wake after append")
	}
}

func TestServer_LongPoll_Timeout(t *testing.T) {
	s, clk := newTestServer(t, testConfig())
	defer s.Shutdown(context.Background())

	done := make(chan error, 1)
	go func() {
		changes, next, err := s.LongPoll(context.Background(), "lp1", 0, 10, 5*time.Second)
		if err != nil {
			done <- err
			return
		}
		if len(changes) != 0 || next != 0 {
			done <- errors.New("expected empty result on timeout")
			return
		}
		done <- nil
	}()
	waitForPending(clk)
	clk.Advance(5*time.Second + time.Millisecond)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("long-poll did not return after timeout")
	}
}

func TestServer_LongPoll_Shutdown(t *testing.T) {
	s, clk := newTestServer(t, testConfig())

	done := make(chan error, 1)
	go func() {
		_, _, err := s.LongPoll(context.Background(), "lp1", 0, 10, time.Minute)
		done <- err
	}()
	waitForPending(clk)

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- s.Shutdown(context.Background()) }()

	select {
	case err := <-done:
		if !errors.Is(err, ErrShutdown) {
			t.Fatalf("long-poll err=%v, want ErrShutdown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("long-poll not woken on shutdown")
	}
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("shutdown err=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not complete")
	}
}

func TestServer_Stream(t *testing.T) {
	s, clk := newTestServer(t, testConfig())
	defer s.Shutdown(context.Background())
	ctx := context.Background()

	ch, err := s.Stream(ctx, "st1", 0)
	if err != nil {
		t.Fatal(err)
	}
	s.Log().Append(mkChange(1, "a"), mkChange(2, "b"))

	seqs, err := readSeqs(ch, 2, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(seqs) != 2 || seqs[0] != 1 || seqs[1] != 2 {
		t.Fatalf("got seqs %v, want [1 2]", seqs)
	}

	// Heartbeat on idle stream.
	clk.Advance(time.Second + time.Millisecond)
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("stream closed")
		}
		if ev.Type != EventHeartbeat {
			t.Fatalf("event type=%v, want heartbeat", ev.Type)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no heartbeat after idle interval")
	}
}

func TestServer_Stream_BackpressureResync(t *testing.T) {
	cfg := testConfig()
	cfg.BufferSize = 2
	cfg.LongPollTimeout = 5 * time.Second
	s, clk := newTestServer(t, cfg)
	defer s.Shutdown(context.Background())
	ctx := context.Background()

	ch, err := s.Stream(ctx, "slow", 0)
	if err != nil {
		t.Fatal(err)
	}
	// Append many changes without reading: the pump fills its buffer and
	// blocks on the slow send. Wait until the buffer is full (cursor at 2)
	// and the write timer is pending, so the advance fires the blocked send.
	for i := int64(1); i <= 10; i++ {
		s.Log().Append(mkChange(i, "k"))
	}
	for {
		sub := s.Subscription("slow")
		if sub != nil && sub.Cursor() >= 2 && clk.PendingTimers() > 0 {
			break
		}
		runtime.Gosched()
	}

	// Advance past the write timeout: the slow subscriber is resynced.
	clk.Advance(5*time.Second + time.Millisecond)
	seqs, err := awaitResync(ch, 2*time.Second)
	if !errors.Is(err, ErrResync) {
		t.Fatalf("err=%v, want ErrResync", err)
	}
	_ = seqs

	// Reconnect with the last delivered cursor (2): no loss, no duplicate.
	ch2, err := s.Stream(ctx, "slow", 2)
	if err != nil {
		t.Fatal(err)
	}
	seqs2, err := readSeqs(ch2, 8, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	want := []int64{3, 4, 5, 6, 7, 8, 9, 10}
	if len(seqs2) != len(want) {
		t.Fatalf("after resync got %v, want %v", seqs2, want)
	}
	for i := range want {
		if seqs2[i] != want[i] {
			t.Fatalf("after resync got %v, want %v", seqs2, want)
		}
	}
}

func TestServer_ReconnectAckedCursor(t *testing.T) {
	s, _ := newTestServer(t, testConfig())
	defer s.Shutdown(context.Background())
	ctx, cancel := context.WithCancel(context.Background())

	ch, err := s.Stream(ctx, "st1", 0)
	if err != nil {
		t.Fatal(err)
	}
	s.Log().Append(mkChange(1, "a"), mkChange(2, "b"), mkChange(3, "c"))
	seqs, err := readSeqs(ch, 3, time.Second)
	if err != nil || len(seqs) != 3 {
		t.Fatalf("seqs=%v err=%v", seqs, err)
	}
	// Acknowledge up to 3, then simulate a network drop: cancel the
	// client context without unsubscribing.
	if err := s.Ack(ctx, "st1", 3); err != nil {
		t.Fatal(err)
	}
	cancel()

	// Reconnect: the server remembers the acknowledged cursor, so even a
	// client passing a stale cursor gets no replay of confirmed changes.
	s.Log().Append(mkChange(4, "d"))
	ctx2 := context.Background()
	ch2, err := s.Stream(ctx2, "st1", 0)
	if err != nil {
		t.Fatal(err)
	}
	seqs2, err := readSeqs(ch2, 1, time.Second)
	if err != nil || len(seqs2) != 1 || seqs2[0] != 4 {
		t.Fatalf("after reconnect got %v err=%v, want [4]", seqs2, err)
	}
}

func TestServer_MaxSubscriptions(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubscriptions = 2
	s, _ := newTestServer(t, cfg)
	defer s.Shutdown(context.Background())
	ctx := context.Background()

	if _, err := s.Stream(ctx, "s1", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Stream(ctx, "s2", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Stream(ctx, "s3", 0); !errors.Is(err, ErrTooManySubscriptions) {
		t.Fatalf("err=%v, want ErrTooManySubscriptions", err)
	}
	if st := s.Stats(); st.Subscriptions != 2 {
		t.Fatalf("stats=%+v, want 2 subscriptions", st)
	}
}

func TestServer_ConcurrentSubscriptions(t *testing.T) {
	cfg := testConfig()
	cfg.BufferSize = 256
	s, _ := newTestServer(t, cfg)
	defer s.Shutdown(context.Background())
	ctx := context.Background()

	const total = 100
	// Append concurrently with duplicates and out-of-order arrivals.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(1); i <= total; i++ {
			s.Log().Append(mkChange(i, "k"))
			if i%7 == 0 {
				s.Log().Append(mkChange(i, "k")) // duplicate
			}
			if i%11 == 0 && i > 1 {
				s.Log().Append(mkChange(i-1, "k")) // duplicate of earlier
			}
			runtime.Gosched()
		}
	}()

	const perMode = 6
	results := make(chan []int64, perMode*3)
	runPoller := func(id string) {
		defer wg.Done()
		var got []int64
		cursor := int64(0)
		for len(got) < total {
			changes, next, err := s.Poll(ctx, id, cursor, 7)
			if err != nil {
				t.Error(err)
				return
			}
			for _, c := range changes {
				got = append(got, c.Seq)
			}
			cursor = next
			if len(changes) == 0 {
				runtime.Gosched()
			}
		}
		results <- got
	}
	runLongPoller := func(id string) {
		defer wg.Done()
		var got []int64
		cursor := int64(0)
		for len(got) < total {
			changes, next, err := s.LongPoll(ctx, id, cursor, 5, 2*time.Second)
			if err != nil {
				t.Error(err)
				return
			}
			for _, c := range changes {
				got = append(got, c.Seq)
			}
			cursor = next
		}
		results <- got
	}
	runStreamer := func(id string) {
		defer wg.Done()
		ch, err := s.Stream(ctx, id, 0)
		if err != nil {
			t.Error(err)
			return
		}
		got, err := readSeqs(ch, total, 5*time.Second)
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Error(err)
			return
		}
		results <- got
	}

	for i := 0; i < perMode; i++ {
		wg.Add(3)
		go runPoller("poll-" + string(rune('a'+i)))
		go runLongPoller("lp-" + string(rune('a'+i)))
		go runStreamer("st-" + string(rune('a'+i)))
	}
	wg.Wait()
	close(results)

	want := make([]int64, total)
	for i := range want {
		want[i] = int64(i + 1)
	}
	for got := range results {
		if len(got) != total {
			t.Fatalf("got %d seqs, want %d", len(got), total)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("seqs mismatch at %d: got %d want %d", i, got[i], want[i])
			}
		}
	}
}

func TestServer_ShutdownDrainsStreams(t *testing.T) {
	s, _ := newTestServer(t, testConfig())
	ctx := context.Background()

	ch, err := s.Stream(ctx, "st1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	ev, ok := <-ch
	if !ok {
		t.Fatal("stream closed without EventClosed")
	}
	if ev.Type != EventClosed {
		t.Fatalf("event type=%v, want EventClosed", ev.Type)
	}
	// Second shutdown is a no-op.
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestServer_ManyLongPollersWakeTogether(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubscriptions = 200
	s, clk := newTestServer(t, cfg)
	defer s.Shutdown(context.Background())
	ctx := context.Background()

	const n = 100
	var wg sync.WaitGroup
	results := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := "lp-" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26))
			changes, next, err := s.LongPoll(ctx, id, 0, 10, time.Minute)
			if err != nil {
				results <- err
				return
			}
			if len(changes) != 1 || next != 1 {
				results <- fmt.Errorf("got %d changes next=%d, want 1/1", len(changes), next)
				return
			}
			results <- nil
		}(i)
	}
	// Wait until all long-pollers are suspended.
	for {
		st := s.Stats()
		if st.Subscriptions >= n {
			break
		}
		runtime.Gosched()
	}
	// Let all pollers register their timers, then append one change.
	for clk.PendingTimers() < n {
		runtime.Gosched()
	}
	s.Log().Append(mkChange(1, "a"))
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
}
