package transport

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

func waitForStats(s *Server, want int) {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s.Stats().Subscriptions == want {
			return
		}
		runtime.Gosched()
	}
}

// TestCapacity_OneShotRequestsReleaseSlots is the deterministic reproduction
// of the capacity leak: thousands of completed poll and long-poll requests
// with distinct ids must not retain subscription slots. After the burst the
// active subscription count returns to zero and a brand-new stream (whose id
// was never seen) is accepted well beyond the old limit.
func TestCapacity_OneShotRequestsReleaseSlots(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubscriptions = 16
	cfg.BufferSize = 4
	s, clk := newTestServer(t, cfg)
	defer s.Shutdown(context.Background())
	ctx := context.Background()

	s.Log().Append(mkChange(1, "k"))

	const burst = 2000
	// Phase 1: a long-running stream occupies one real slot throughout.
	live, err := s.Stream(ctx, "live", 0)
	if err != nil {
		t.Fatal(err)
	}

	// Phase 2: many one-shot poll requests with unique ids.
	for i := 0; i < burst; i++ {
		id := fmt.Sprintf("poll-%d", i)
		if _, _, err := s.Poll(ctx, id, 0, 4); err != nil {
			t.Fatalf("poll %d: %v", i, err)
		}
	}
	waitForStats(s, 1)
	if got := s.Stats().Subscriptions; got != 1 {
		t.Fatalf("after poll burst active=%d, want 1", got)
	}

	// Phase 3: many one-shot long-poll requests with unique ids. They have
	// data immediately available, so they return without suspending.
	for i := 0; i < burst; i++ {
		id := fmt.Sprintf("lp-%d", i)
		changes, _, err := s.LongPoll(ctx, id, 0, 4, time.Minute)
		if err != nil {
			t.Fatalf("longpoll %d: %v", i, err)
		}
		if len(changes) != 1 {
			t.Fatalf("longpoll %d got %d changes", i, len(changes))
		}
	}
	waitForStats(s, 1)
	if got := s.Stats().Subscriptions; got != 1 {
		t.Fatalf("after longpoll burst active=%d, want 1 (only the live stream)", got)
	}

	// Historical ids never come back: new unique subscriptions are accepted.
	for i := 0; i < cfg.MaxSubscriptions-1; i++ {
		id := fmt.Sprintf("new-%d", i)
		if _, err := s.Stream(ctx, id, 0); err != nil {
			t.Fatalf("accept new stream %d after burst: %v", i, err)
		}
	}
	if got := s.Stats().Subscriptions; got != cfg.MaxSubscriptions {
		t.Fatalf("active=%d, want at capacity %d", got, cfg.MaxSubscriptions)
	}
	// And the real limit is still enforced once genuinely full.
	if _, err := s.Stream(ctx, "over-capacity", 0); err == nil {
		t.Fatal("expected ErrTooManySubscriptions at real capacity")
	}

	// Releasing one real slot makes room again.
	if err := s.Unsubscribe(ctx, "new-0"); err != nil {
		t.Fatal(err)
	}
	ch, err := s.Stream(ctx, "after-release", 0)
	if err != nil {
		t.Fatalf("expected slot freed by unsubscribe: %v", err)
	}
	_ = ch
	_ = live
	_ = clk
}

// TestCapacity_SuspendedLongPollsCountWhileInFlight verifies the limit
// applies to genuinely active (suspended) long-polls, and that all their
// resources are released when they return or are cancelled.
func TestCapacity_SuspendedLongPollsCountWhileInFlight(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubscriptions = 4
	s, clk := newTestServer(t, cfg)
	defer s.Shutdown(context.Background())

	errCh := make(chan error, cfg.MaxSubscriptions)
	var wg sync.WaitGroup
	for i := 0; i < cfg.MaxSubscriptions; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, err := s.LongPoll(context.Background(), fmt.Sprintf("lp-%d", i), 0, 4, time.Minute)
			errCh <- err
		}(i)
	}
	// Wait until all four are suspended.
	waitForStats(s, cfg.MaxSubscriptions)
	// A fifth active request must be rejected while they are genuinely in
	// flight.
	if _, _, err := s.Poll(context.Background(), "extra", 0, 4); err == nil {
		t.Fatal("expected rejection while long-polls are suspended")
	}
	// Let them time out via the in-memory clock.
	for clk.PendingTimers() < cfg.MaxSubscriptions {
		runtime.Gosched()
	}
	clk.Advance(time.Minute + time.Millisecond)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("suspended long-poll: %v", err)
		}
	}
	waitForStats(s, 0)
	if got := s.Stats().Subscriptions; got != 0 {
		t.Fatalf("active=%d after suspended batch returned, want 0", got)
	}
}

// TestCapacity_ProgressIsSwept verifies the auxiliary ack-progress table
// cannot grow without bound either: it is bounded by the in-memory clock
// idle sweep and never blocks new subscriptions.
func TestCapacity_ProgressIsSwept(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubscriptions = 4
	s, clk := newTestServer(t, cfg)
	defer s.Shutdown(context.Background())
	ctx := context.Background()

	const burst = 200
	for i := 0; i < burst; i++ {
		id := fmt.Sprintf("p-%d", i)
		if _, _, err := s.Poll(ctx, id, 0, 4); err != nil {
			t.Fatalf("poll %d: %v", i, err)
		}
	}
	s.mu.Lock()
	progressBefore := len(s.progress)
	s.mu.Unlock()
	if progressBefore != burst {
		t.Fatalf("progress entries=%d, want %d retained for stale-cursor resume", progressBefore, burst)
	}

	// Advance well beyond the idle TTL and issue one more request to trigger
	// the amortized sweep. Every prior entry is idle, so it must be forgotten.
	clk.Advance(progressIdleTTL + time.Minute)
	if _, _, err := s.Poll(ctx, "trigger", 0, 4); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	progressAfter := len(s.progress)
	active := len(s.subs)
	s.mu.Unlock()
	if active != 0 {
		t.Fatalf("active slots=%d, want 0", active)
	}
	if progressAfter >= progressBefore {
		t.Fatalf("progress entries=%d after idle sweep, was %d", progressAfter, progressBefore)
	}
}
