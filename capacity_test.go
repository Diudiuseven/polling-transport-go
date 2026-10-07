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

// awaitSubscriptions blocks until the server reports exactly n
// subscriptions, failing the test if that does not happen promptly.
func awaitSubscriptions(t *testing.T, s *Server, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for s.Stats().Subscriptions != n {
		if time.Now().After(deadline) {
			t.Fatalf("subscriptions=%d, want %d", s.Stats().Subscriptions, n)
		}
		runtime.Gosched()
	}
}

// TestServer_OneShotRequestsDoNotLeakSubscriptions reproduces the capacity
// leak: finished poll/long-poll requests must not keep occupying
// subscription slots. After a large number of one-shot requests the
// occupancy must return to zero and the full capacity must remain available
// for real subscriptions.
func TestServer_OneShotRequestsDoNotLeakSubscriptions(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubscriptions = 8
	s, _ := newTestServer(t, cfg)
	defer s.Shutdown(context.Background())
	ctx := context.Background()

	s.Log().Append(mkChange(1, "a"))

	// A long run of one-shot requests, each with a fresh client id. Every
	// request returns immediately because a change is available.
	for i := 0; i < 500; i++ {
		id := fmt.Sprintf("one-shot-%d", i)
		if _, _, err := s.Poll(ctx, id, 0, 10); err != nil {
			t.Fatalf("poll %d: %v", i, err)
		}
		if _, _, err := s.LongPoll(ctx, id+"-lp", 0, 10, time.Minute); err != nil {
			t.Fatalf("long-poll %d: %v", i, err)
		}
	}

	// No slot may be retained by finished requests.
	awaitSubscriptions(t, s, 0)

	// The whole capacity is still available for real subscriptions.
	chans := make([]<-chan Event, 0, cfg.MaxSubscriptions)
	for i := 0; i < cfg.MaxSubscriptions; i++ {
		ch, err := s.Stream(ctx, fmt.Sprintf("st-%d", i), 0)
		if err != nil {
			t.Fatalf("stream %d: %v", i, err)
		}
		chans = append(chans, ch)
	}
	if _, err := s.Stream(ctx, "st-overflow", 0); !errors.Is(err, ErrTooManySubscriptions) {
		t.Fatalf("err=%v, want ErrTooManySubscriptions", err)
	}
	_ = chans
}

// TestServer_SuspendedLongPollsCountAndRelease verifies that in-flight
// long-polls count against the capacity while suspended, and release their
// slots as soon as they return (here: by timeout).
func TestServer_SuspendedLongPollsCountAndRelease(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubscriptions = 5
	s, clk := newTestServer(t, cfg)
	defer s.Shutdown(context.Background())
	ctx := context.Background()

	const n = 5
	done := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			_, _, err := s.LongPoll(ctx, fmt.Sprintf("lp-%d", i), 0, 10, 5*time.Second)
			done <- err
		}(i)
	}
	// All five are suspended and occupy the whole capacity.
	awaitSubscriptions(t, s, n)
	for clk.PendingTimers() < n {
		runtime.Gosched()
	}

	// A sixth concurrent request is rejected: the cap still applies to real
	// in-flight subscriptions.
	if _, _, err := s.LongPoll(ctx, "lp-overflow", 0, 10, time.Second); !errors.Is(err, ErrTooManySubscriptions) {
		t.Fatalf("err=%v, want ErrTooManySubscriptions", err)
	}

	// Time out all five: slots are released as the requests return.
	clk.Advance(5*time.Second + time.Millisecond)
	for i := 0; i < n; i++ {
		if err := <-done; err != nil {
			t.Fatalf("long-poll err=%v", err)
		}
	}
	awaitSubscriptions(t, s, 0)

	// New requests are accepted again (a change is available so the
	// long-poll returns immediately instead of suspending).
	s.Log().Append(mkChange(1, "a"))
	if _, _, err := s.LongPoll(ctx, "lp-new", 0, 10, time.Second); err != nil {
		t.Fatalf("after release: %v", err)
	}
}

// TestServer_ConcurrentOneShotFloodNoLeak hammers the server with concurrent
// one-shot requests and reconnecting streams; occupancy must stabilize at
// the number of truly active subscriptions.
func TestServer_ConcurrentOneShotFloodNoLeak(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubscriptions = 64
	cfg.BufferSize = 16
	s, _ := newTestServer(t, cfg)
	defer s.Shutdown(context.Background())
	ctx := context.Background()

	const changes = 200
	var wg sync.WaitGroup

	// Appender: trickles changes in while subscribers work.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(1); i <= changes; i++ {
			s.Log().Append(mkChange(i, "k"))
			if i%3 == 0 {
				runtime.Gosched()
			}
		}
	}()

	// One-shot flood: pollers and long-pollers with fresh ids per request.
	errs := make(chan error, 128)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				id := fmt.Sprintf("flood-%d-%d", g, i)
				if _, _, err := s.Poll(ctx, id, 0, 5); err != nil {
					errs <- fmt.Errorf("poll: %w", err)
					return
				}
				if _, _, err := s.LongPoll(ctx, id+"-lp", 0, 5, 10*time.Millisecond); err != nil {
					errs <- fmt.Errorf("long-poll: %w", err)
					return
				}
			}
		}(g)
	}

	// Reconnecting stream readers: each reads the full sequence, dropping
	// and re-establishing its stream with the last processed cursor.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			id := fmt.Sprintf("reader-%d", r)
			processed := int64(0)
			for processed < changes {
				rctx, cancel := context.WithCancel(ctx)
				ch, err := s.Stream(rctx, id, processed)
				if err != nil {
					cancel()
					errs <- fmt.Errorf("stream: %w", err)
					return
				}
				// Read a few events, then simulate a connection drop.
				n := 0
				for ev := range ch {
					if ev.Type != EventChange {
						continue
					}
					if ev.Change.Seq != processed+1 {
						errs <- fmt.Errorf("%s: hole: got seq %d after %d", id, ev.Change.Seq, processed)
						cancel()
						return
					}
					processed = ev.Change.Seq
					n++
					if n >= 7 || processed >= changes {
						break
					}
				}
				cancel()
			}
			if err := s.Unsubscribe(ctx, id); err != nil {
				errs <- fmt.Errorf("unsubscribe: %w", err)
			}
		}(r)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	// Only truly active subscriptions may remain: all readers unsubscribed
	// and every one-shot request released its slot.
	awaitSubscriptions(t, s, 0)
}
