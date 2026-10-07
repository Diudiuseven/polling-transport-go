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

// runPollClient consumes the whole log via Poll with the given id, retrying
// from its own processed cursor and tolerating capacity rejection by backing
// off. It returns the deduplicated ordered sequence it observed.
func runPollClient(s *Server, id string, total int64, slow <-chan struct{}) []int64 {
	seen := make(map[int64]struct{})
	var ordered []int64
	cursor := int64(0)
	ctx := context.Background()
	for cursor < total {
		changes, next, err := s.Poll(ctx, id, cursor, 3)
		if errors.Is(err, ErrTooManySubscriptions) || errors.Is(err, ErrShutdown) {
			runtime.Gosched()
			continue
		}
		if err != nil {
			return ordered
		}
		for _, c := range changes {
			if _, dup := seen[c.Seq]; !dup {
				seen[c.Seq] = struct{}{}
				ordered = append(ordered, c.Seq)
			}
			cursor = c.Seq
		}
		if next > cursor {
			cursor = next
		}
		if len(changes) == 0 {
			runtime.Gosched()
		}
		if slow != nil {
			select {
			case <-slow:
			default:
			}
		}
	}
	return ordered
}

func runLongPollClient(s *Server, id string, total int64) []int64 {
	seen := make(map[int64]struct{})
	var ordered []int64
	cursor := int64(0)
	ctx := context.Background()
	for cursor < total {
		changes, _, err := s.LongPoll(ctx, id, cursor, 3, time.Second)
		if err != nil {
			runtime.Gosched()
			continue
		}
		for _, c := range changes {
			if _, dup := seen[c.Seq]; !dup {
				seen[c.Seq] = struct{}{}
				ordered = append(ordered, c.Seq)
			}
			if c.Seq > cursor {
				cursor = c.Seq
			}
		}
	}
	return ordered
}

// runStreamClient consumes via Stream and simulates a flaky, sometimes slow
// subscriber: it reconnects repeatedly, always carrying the cursor it has
// actually processed, and on resync markers reconnects the same way. The
// returned sequence must contain every seq 1..total exactly once.
func runStreamClient(t *testing.T, s *Server, id string, total int64, stallEvery, reconnectEvery int) []int64 {
	t.Helper()
	seen := make(map[int64]struct{})
	var ordered []int64
	processed := int64(0)

	for processed < total {
		ctx, cancel := context.WithCancel(context.Background())
		ch, err := s.Stream(ctx, id, processed)
		if errors.Is(err, ErrTooManySubscriptions) {
			cancel()
			runtime.Gosched()
			continue
		}
		if err != nil {
			cancel()
			t.Errorf("%s: stream: %v", id, err)
			return ordered
		}
		sinceReconnect := 0
		closed := false
		for processed < total {
			select {
			case ev, ok := <-ch:
				if !ok {
					closed = true
					break
				}
				switch ev.Type {
				case EventChange:
					if ev.Change.Seq <= processed {
						// Re-delivery of an unacknowledged change we already
						// processed in an earlier connection is permitted
						// (at-least-once); it must never move us backwards and
						// we must not count it twice.
						continue
					}
					if ev.Change.Seq != processed+1 {
						t.Errorf("%s: hole: got %d after %d", id, ev.Change.Seq, processed)
						cancel()
						return ordered
					}
					seen[ev.Change.Seq] = struct{}{}
					ordered = append(ordered, ev.Change.Seq)
					processed = ev.Change.Seq
					sinceReconnect++
					// Simulate occasional slowness and flaky disconnects.
					if stallEvery > 0 && processed%int64(stallEvery) == 0 {
						time.Sleep(2 * time.Millisecond)
					}
					if reconnectEvery > 0 && sinceReconnect >= reconnectEvery {
						cancel()
						closed = true
					}
				case EventResync:
					cancel()
					closed = true
				case EventHeartbeat:
				case EventClosed:
					closed = true
				}
			case <-time.After(2 * time.Second):
				t.Errorf("%s: stuck at %d (frontier %d)", id, processed, s.Log().Frontier())
				cancel()
				return ordered
			}
			if closed {
				break
			}
		}
		cancel()
		if !closed {
			// Defensive: ensure goroutine/channel teardown before reattach.
			runtime.Gosched()
		}
		waitForSlotGone(s, id)
	}
	if int64(len(ordered)) != total {
		t.Errorf("%s: got %d unique seqs, want %d", id, len(ordered), total)
	}
	return ordered
}

// TestConcurrent_InterleavedSubscribers exercises many subscribers on all
// three transports while changes are appended concurrently, with slow stream
// consumers repeatedly disconnecting mid-backlog and reconnecting from their
// true positions. Every subscriber must observe 1..total exactly once with no
// hole; no data race may be reported; active slots must return to zero and no
// background goroutines may survive shutdown.
func TestConcurrent_InterleavedSubscribers(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubscriptions = 24
	cfg.BufferSize = 4
	cfg.LongPollTimeout = 2 * time.Second
	cfg.HeartbeatInterval = time.Second
	s, _ := newTestServer(t, cfg)

	const total = int64(400)
	const pollers = 4
	const longPollers = 4
	const streamers = 8

	var wg sync.WaitGroup
	errDone := make(chan struct{})
	var failOnce sync.Once
	fail := func() { failOnce.Do(func() { close(errDone) }) }

	// Producer.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(1); i <= total; i++ {
			s.Log().Append(mkChange(i, "k"))
			if i%23 == 0 {
				time.Sleep(time.Millisecond)
			}
		}
	}()

	for i := 0; i < pollers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got := runPollClient(s, fmt.Sprintf("poll-%d", i), total, nil)
			if int64(len(got)) != total {
				t.Errorf("poller %d got %d", i, len(got))
				fail()
			}
		}(i)
	}
	for i := 0; i < longPollers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got := runLongPollClient(s, fmt.Sprintf("lp-%d", i), total)
			if int64(len(got)) != total {
				t.Errorf("longpoller %d got %d", i, len(got))
				fail()
			}
		}(i)
	}
	for i := 0; i < streamers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Alternate profiles: some slow, some reconnect frequently.
			stall := 0
			reconnect := 0
			switch i % 3 {
			case 0:
				stall = 5 // slow consumer
			case 1:
				reconnect = 7 // flaky network
			case 2:
				stall = 3
				reconnect = 11
			}
			got := runStreamClient(t, s, fmt.Sprintf("st-%d", i), total, stall, reconnect)
			if int64(len(got)) != total {
				fail()
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-errDone:
		t.Fatal("subscriber reported an error")
	case <-time.After(60 * time.Second):
		t.Fatal("concurrent test timed out")
	}

	// All stream slots should be gone; shut down and require full drain with
	// no leaked goroutines (wg must reach zero promptly).
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := s.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown did not drain: %v", err)
	}
	if st := s.Stats(); st.Subscriptions != 0 {
		t.Fatalf("active slots after shutdown=%d, want 0", st.Subscriptions)
	}
}
