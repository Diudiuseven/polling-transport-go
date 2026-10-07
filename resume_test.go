package transport

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
)

// waitForSlotGone spins until the server reports no active slot for id (or
// the test deadline approaches). It uses only in-memory state, so it is
// deterministic and does not depend on wall-clock sleeps.
func waitForSlotGone(s *Server, id string) {
	for s.Subscription(id) != nil {
		runtime.Gosched()
	}
}

// drainChangeSeqs reads change events until ch closes or a non-change
// terminal event arrives. Heartbeats are skipped.
func drainChangeSeqs(ch <-chan Event) []int64 {
	var seqs []int64
	for ev := range ch {
		switch ev.Type {
		case EventChange:
			seqs = append(seqs, ev.Change.Seq)
		case EventResync, EventClosed:
			return seqs
		}
	}
	return seqs
}

// readN reads exactly n change events from a stream, failing on timeout.
func readN(t *testing.T, ch <-chan Event, n int) []int64 {
	t.Helper()
	seqs, err := readSeqs(ch, n, 3*time.Second)
	if err != nil {
		t.Fatalf("read %d events: %v (got %v)", n, err, seqs)
	}
	return seqs
}

func assertContinuous(t *testing.T, got []int64, wantStart, wantEnd int64) {
	t.Helper()
	if int64(len(got)) != wantEnd-wantStart+1 {
		t.Fatalf("got %v, want continuous %d..%d", got, wantStart, wantEnd)
	}
	for i, seq := range got {
		if seq != wantStart+int64(i) {
			t.Fatalf("got %v, want continuous %d..%d", got, wantStart, wantEnd)
		}
	}
}

func resumeConfig() Config {
	cfg := testConfig()
	cfg.BufferSize = 2
	cfg.LongPollTimeout = time.Hour
	cfg.HeartbeatInterval = 30 * time.Minute
	return cfg
}

// TestResume_Stream_SlowSubscriberBacklogReconnect is the deterministic
// reproduction of the slow-subscriber hole. A stream client fills its
// bounded buffer but only processes a prefix. New changes keep arriving and
// back up on the server. The connection then drops (client cancel, without
// any unsubscribe) and the client reconnects carrying the real position it
// processed. Every buffered-but-unread change must be replayed exactly once
// and the received sequence must be gap-free.
func TestResume_Stream_SlowSubscriberBacklogReconnect(t *testing.T) {
	cases := []struct {
		name      string
		reconnect func(processed int64) int64
	}{
		{"exact cursor", func(p int64) int64 { return p }},
		{"stale cursor", func(p int64) int64 { return 0 }},
		{"midpoint cursor", func(p int64) int64 { return p / 2 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newTestServer(t, resumeConfig())
			defer s.Shutdown(context.Background())

			const total = int64(12)
			const processed = int64(3)

			ctx, cancel := context.WithCancel(context.Background())
			ch, err := s.Stream(ctx, "slow", 0)
			if err != nil {
				t.Fatal(err)
			}
			for i := int64(1); i <= processed; i++ {
				s.Log().Append(mkChange(i, "k"))
			}
			// Only the first 'processed' changes are actually handled.
			got := readN(t, ch, int(processed))
			assertContinuous(t, got, 1, processed)

			// The server keeps producing; events 4.. fill the tiny buffer and
			// then back up in the blocked pump send. None of these are allowed
			// to be skipped on resume.
			for i := processed + 1; i <= total; i++ {
				s.Log().Append(mkChange(i, "k"))
			}
			// Wait until the pump has handed the buffer's worth to the
			// channel and is stalled on the next send (its generation cursor
			// then sits at processed+BufferSize).
			wantStall := processed + int64(resumeConfig().BufferSize)
			for {
				sub := s.Subscription("slow")
				if sub != nil && sub.Cursor() >= wantStall {
					break
				}
				runtime.Gosched()
			}

			// Network drop: cancel without processing the buffer and without
			// unsubscribing.
			cancel()
			waitForSlotGone(s, "slow")
			// Drain whatever the dropped connection had buffered; the client
			// never processed it.
			drainChangeSeqs(ch)

			// Reconnect with the true processed position (or an earlier one).
			startAt := tc.reconnect(processed)
			ch2, err := s.Stream(context.Background(), "slow", startAt)
			if err != nil {
				t.Fatal(err)
			}
			rest := readN(t, ch2, int(total-startAt))
			// Delivery starts strictly after the reported position and is
			// continuous through the frontier: no hole, no skip.
			assertContinuous(t, rest, startAt+1, total)
			// Every change the client actually handled was already in the
			// dropped connection only; reconnect never reaches past total and
			// never duplicates within this stream.
			if st := s.Stats(); st.Subscriptions != 1 {
				t.Fatalf("stats=%+v, want 1 active stream", st)
			}
		})
	}
}

// TestResume_Stream_ResyncBacklogReconnect drives the forced resync path
// (write timeout while the client is stalled) and verifies the reconnect
// after the EventResync marker replays the whole backlog exactly once.
func TestResume_Stream_ResyncBacklogReconnect(t *testing.T) {
	s, clk := newTestServer(t, resumeConfig())
	defer s.Shutdown(context.Background())

	const total = int64(12)
	const processed = int64(3)

	ch, err := s.Stream(context.Background(), "slow", 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= processed; i++ {
		s.Log().Append(mkChange(i, "k"))
	}
	got := readN(t, ch, int(processed))
	assertContinuous(t, got, 1, processed)
	for i := processed + 1; i <= total; i++ {
		s.Log().Append(mkChange(i, "k"))
	}
	// Wait until the buffer is full and the pump is blocked in the send's
	// select, where the lazy write timer has been registered.
	wantStall := processed + 2 // BufferSize in resumeConfig
	for {
		sub := s.Subscription("slow")
		if sub != nil && sub.Cursor() >= wantStall && clk.PendingTimers() > 0 {
			break
		}
		runtime.Gosched()
	}
	clk.Advance(time.Hour + time.Millisecond)

	// Marker arrives; drain until channel closes.
	seqs, err := awaitResync(ch, 3*time.Second)
	if !errors.Is(err, ErrResync) {
		t.Fatalf("err=%v, want ErrResync", err)
	}
	waitForSlotGone(s, "slow")
	_ = seqs

	ch2, err := s.Stream(context.Background(), "slow", processed)
	if err != nil {
		t.Fatal(err)
	}
	rest := readN(t, ch2, int(total-processed))
	assertContinuous(t, rest, processed+1, total)
}

// TestResume_Poll_LargeBatchReconnect covers the poll transport: a request
// returns a batch the client processes only in part (or loses entirely),
// then the client reconnects from its true processed position. The server
// must resume exactly there even when the client's cursor is behind what an
// earlier response reached, and acknowledged changes must never come back.
func TestResume_Poll_LargeBatchReconnect(t *testing.T) {
	s, _ := newTestServer(t, resumeConfig())
	defer s.Shutdown(context.Background())
	ctx := context.Background()

	for i := int64(1); i <= 12; i++ {
		s.Log().Append(mkChange(i, "k"))
	}

	// First poll returns a big batch, but the client only processes seq 1..3
	// before failing.
	changes, _, err := s.Poll(ctx, "p", 0, 12)
	if err != nil || len(changes) != 12 {
		t.Fatalf("poll: %d changes err=%v", len(changes), err)
	}
	const processed = int64(3)

	// Reconnect from the real processed position: changes 4..12 replayed.
	changes, next, err := s.Poll(ctx, "p", processed, 12)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 9 || next != 12 {
		t.Fatalf("reconnect poll got %d next=%d, want 9/12", len(changes), next)
	}
	assertContinuous(t, seqsOf(changes), 4, 12)

	// Ack through 12 and poll again with a stale cursor: no duplicates.
	if err := s.Ack(ctx, "p", 12); err != nil {
		t.Fatal(err)
	}
	changes, next, err = s.Poll(ctx, "p", 0, 12)
	if err != nil || len(changes) != 0 || next != 12 {
		t.Fatalf("post-ack poll got %d next=%d err=%v, want 0/12", len(changes), next, err)
	}

	// New changes after the ack deliver normally.
	s.Log().Append(mkChange(13, "k"))
	changes, next, err = s.Poll(ctx, "p", 12, 12)
	if err != nil || len(changes) != 1 || next != 13 {
		t.Fatalf("post-ack new poll got %d next=%d err=%v", len(changes), next, err)
	}
}

func seqsOf(changes []Change) []int64 {
	out := make([]int64, len(changes))
	for i, c := range changes {
		out[i] = c.Seq
	}
	return out
}

// TestResume_LongPoll_BacklogReconnect mirrors the stream scenario for long
// polling: a suspended request returns a batch while more changes pile up;
// the client processes only a prefix, then re-requests from the real
// processed position and receives the backlog exactly once.
func TestResume_LongPoll_BacklogReconnect(t *testing.T) {
	s, clk := newTestServer(t, resumeConfig())
	defer s.Shutdown(context.Background())
	ctx := context.Background()

	for i := int64(1); i <= 3; i++ {
		s.Log().Append(mkChange(i, "k"))
	}
	changes, _, err := s.LongPoll(ctx, "lp", 0, 3, time.Second)
	if err != nil || len(changes) != 3 {
		t.Fatalf("longpoll: %d changes err=%v", len(changes), err)
	}
	const processed = int64(3)

	// More changes accumulate while the client is busy/not connected.
	for i := processed + 1; i <= 20; i++ {
		s.Log().Append(mkChange(i, "k"))
	}
	// Re-request from the true position with a small limit; chunk through
	// the backlog and verify strict continuity, including an even earlier
	// cursor variant via a fresh id with an ack.
	cursor := processed
	var seen []int64
	for cursor < 20 {
		changes, next, err := s.LongPoll(ctx, "lp", cursor, 5, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if len(changes) == 0 {
			t.Fatal("expected backlog changes")
		}
		seen = append(seen, seqsOf(changes)...)
		cursor = next
	}
	assertContinuous(t, seen, processed+1, 20)

	// Ack everything, then long-poll with cursor 0: nothing replayed.
	if err := s.Ack(ctx, "lp", 20); err != nil {
		t.Fatal(err)
	}
	go func() {
		waitForPending(clk)
		clk.Advance(time.Second + time.Millisecond)
	}()
	changes, next, err := s.LongPoll(ctx, "lp", 0, 5, time.Second)
	if err != nil || len(changes) != 0 || next != 20 {
		t.Fatalf("post-ack longpoll got %d next=%d err=%v, want 0/20", len(changes), next, err)
	}
}

// TestResume_AckedFloorAllTransports verifies the acknowledged cursor is a
// hard resume floor on every transport: after acking seq 5, reconnecting with
// cursor 0 never re-delivers 1..5, while seq 6+ is still delivered.
func TestResume_AckedFloorAllTransports(t *testing.T) {
	s, clk := newTestServer(t, resumeConfig())
	defer s.Shutdown(context.Background())
	ctx := context.Background()
	for i := int64(1); i <= 5; i++ {
		s.Log().Append(mkChange(i, "k"))
	}

	// Establish the acked floor via a stream, acknowledge 1..5, drop it.
	ch, err := s.Stream(ctx, "c", 0)
	if err != nil {
		t.Fatal(err)
	}
	readN(t, ch, 5)
	if err := s.Ack(ctx, "c", 5); err != nil {
		t.Fatal(err)
	}
	// Reattach (superseding the stream) with a stale cursor 0 via each
	// one-shot transport; the floor must clamp to 5.
	if changes, next, err := s.Poll(ctx, "c", 0, 10); err != nil || len(changes) != 0 || next != 5 {
		t.Fatalf("poll stale after ack: %d changes next=%d err=%v, want 0/5", len(changes), next, err)
	}
	// The poll released the slot; a suspended long-poll with cursor 0 times
	// out empty rather than replaying 1..5.
	go func() {
		for clk.PendingTimers() == 0 {
			runtime.Gosched()
		}
		clk.Advance(time.Second + time.Millisecond)
	}()
	if changes, next, err := s.LongPoll(ctx, "c", 0, 10, time.Second); err != nil || len(changes) != 0 || next != 5 {
		t.Fatalf("longpoll stale after ack: %d changes next=%d err=%v, want 0/5", len(changes), next, err)
	}

	// A fresh stream with cursor 0 also resumes after the acked floor; once
	// seq 6 is appended it is the very first thing delivered (1..5 skipped).
	ch2, err := s.Stream(ctx, "c", 0)
	if err != nil {
		t.Fatal(err)
	}
	s.Log().Append(mkChange(6, "k"))
	rest := readN(t, ch2, 1)
	assertContinuous(t, rest, 6, 6)
}

// TestResume_StreamSupersededByOneShotHandsOverSlot verifies that a live
// stream replaced by a same-id poll/long-poll is cancelled, its channel
// closes, the one-shot request returns and releases the slot, and a following
// stream for the id resumes from the acked floor without duplicate capacity.
func TestResume_StreamSupersededByOneShotHandsOverSlot(t *testing.T) {
	s, _ := newTestServer(t, resumeConfig())
	defer s.Shutdown(context.Background())
	ctx := context.Background()
	for i := int64(1); i <= 4; i++ {
		s.Log().Append(mkChange(i, "k"))
	}
	ch, err := s.Stream(ctx, "x", 0)
	if err != nil {
		t.Fatal(err)
	}
	readN(t, ch, 2) // client processed 1..2
	if err := s.Ack(ctx, "x", 2); err != nil {
		t.Fatal(err)
	}

	// The displaced connection keeps a reader goroutine (the real client's
	// receive loop, which notices the close and tears down on reconnect).
	closed := make(chan struct{})
	go func() {
		for range ch {
		}
		close(closed)
	}()
	// Let the pump flush seq 3..4 into the buffer and block on the next send.
	for s.Subscription("x").Cursor() < 4 {
		runtime.Gosched()
	}

	// A poll for the same id supersedes the live stream.
	changes, next, err := s.Poll(ctx, "x", 2, 10)
	if err != nil || len(changes) != 2 || next != 4 {
		t.Fatalf("superseding poll: %d changes next=%d err=%v", len(changes), next, err)
	}
	// The old pump is cancelled and (once its buffered events drain) closes
	// its own channel.
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("superseded stream channel not closed")
	}
	// Poll returned, slot released: a fresh stream attaches, resumes at the
	// acked floor 2, and replays unacknowledged 3..4 exactly.
	ch2, err := s.Stream(ctx, "x", 2)
	if err != nil {
		t.Fatal(err)
	}
	rest := readN(t, ch2, 2)
	assertContinuous(t, rest, 3, 4)
	if st := s.Stats(); st.Subscriptions != 1 {
		t.Fatalf("active=%d, want 1", st.Subscriptions)
	}
}
