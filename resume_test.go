package transport

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"
)

// waitForCursor blocks until the subscription's delivered cursor reaches at
// least seq and the pump has a pending write timer (i.e. it is blocked on a
// full buffer).
func waitForCursor(s *Server, clk *FakeClock, id string, seq int64) {
	for {
		sub := s.Subscription(id)
		if sub != nil && sub.Cursor() >= seq && clk.PendingTimers() > 0 {
			return
		}
		runtime.Gosched()
	}
}

// TestServer_ResumeDisconnectWithBacklog reproduces the lost-changes bug: a
// slow stream subscriber disconnects while the server has already buffered
// (delivered-but-unprocessed) changes beyond the client's real position.
// Reconnecting with the last processed cursor must resume exactly after it:
// no hole, no replay of acknowledged changes.
func TestServer_ResumeDisconnectWithBacklog(t *testing.T) {
	cfg := testConfig()
	cfg.BufferSize = 4
	s, clk := newTestServer(t, cfg)
	defer s.Shutdown(context.Background())
	ctx, cancel := context.WithCancel(context.Background())

	ch, err := s.Stream(ctx, "slow", 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 10; i++ {
		s.Log().Append(mkChange(i, "k"))
	}
	// The pump fills the buffer (4 events) and blocks on the 5th send.
	waitForCursor(s, clk, "slow", 4)

	// The client processes only the first two events and acknowledges them,
	// then the connection drops without unsubscribing.
	seqs, err := readSeqs(ch, 2, 2*time.Second)
	if err != nil || len(seqs) != 2 || seqs[0] != 1 || seqs[1] != 2 {
		t.Fatalf("seqs=%v err=%v, want [1 2]", seqs, err)
	}
	if err := s.Ack(ctx, "slow", 2); err != nil {
		t.Fatal(err)
	}
	cancel()

	// Reconnect with the last processed cursor (2). The server had delivered
	// up to >=4 into the old buffer, but those were never processed: they
	// must be redelivered. Acknowledged changes (1,2) must not repeat.
	ch2, err := s.Stream(context.Background(), "slow", 2)
	if err != nil {
		t.Fatal(err)
	}
	got, err := readSeqs(ch2, 8, 2*time.Second)
	if err != nil {
		t.Fatalf("err=%v got=%v", err, got)
	}
	want := []int64{3, 4, 5, 6, 7, 8, 9, 10}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("after reconnect got %v, want %v", got, want)
	}
}

// TestServer_ResumeResyncDrainedBacklog covers the resync variant: the slow
// subscriber's buffered events are drained by the server when it enters the
// resync state, so the client never saw them. Reconnecting with the last
// processed cursor (0) must replay everything from there.
func TestServer_ResumeResyncDrainedBacklog(t *testing.T) {
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
	for i := int64(1); i <= 10; i++ {
		s.Log().Append(mkChange(i, "k"))
	}
	// Pump delivered 1,2 into the buffer and is blocked on the 3rd send.
	waitForCursor(s, clk, "slow", 2)

	// Write timeout: the server drains the buffered (never read) events and
	// marks the subscription for resync.
	clk.Advance(5*time.Second + time.Millisecond)
	if _, err := awaitResync(ch, 2*time.Second); !errors.Is(err, ErrResync) {
		t.Fatalf("err=%v, want ErrResync", err)
	}

	// The client processed nothing (the drained events were lost to it).
	// Reconnecting from 0 must deliver the full contiguous sequence.
	ch2, err := s.Stream(ctx, "slow", 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := readSeqs(ch2, 10, 2*time.Second)
	if err != nil {
		t.Fatalf("err=%v got=%v", err, got)
	}
	for i := int64(1); i <= 10; i++ {
		if got[i-1] != i {
			t.Fatalf("after resync got %v, want [1..10]", got)
		}
	}
}

// TestServer_ResumeConsistencyAcrossModes verifies that all three transports
// resume from exactly the client-supplied cursor with identical results.
func TestServer_ResumeConsistencyAcrossModes(t *testing.T) {
	s, _ := newTestServer(t, testConfig())
	defer s.Shutdown(context.Background())
	ctx := context.Background()

	for i := int64(1); i <= 5; i++ {
		s.Log().Append(mkChange(i, "k"))
	}
	want := []int64{3, 4, 5}

	changes, next, err := s.Poll(ctx, "p", 2, 10)
	if err != nil || next != 5 || len(changes) != 3 {
		t.Fatalf("poll: changes=%v next=%d err=%v", changes, next, err)
	}
	for i, c := range changes {
		if c.Seq != want[i] {
			t.Fatalf("poll: got %v, want %v", changes, want)
		}
	}

	changes, next, err = s.LongPoll(ctx, "lp", 2, 10, time.Minute)
	if err != nil || next != 5 || len(changes) != 3 {
		t.Fatalf("longpoll: changes=%v next=%d err=%v", changes, next, err)
	}
	for i, c := range changes {
		if c.Seq != want[i] {
			t.Fatalf("longpoll: got %v, want %v", changes, want)
		}
	}

	ch, err := s.Stream(ctx, "st", 2)
	if err != nil {
		t.Fatal(err)
	}
	got, err := readSeqs(ch, 3, 2*time.Second)
	if err != nil || fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("stream: got %v err=%v, want %v", got, err, want)
	}
}
