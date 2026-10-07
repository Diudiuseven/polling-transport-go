package transport

import (
	"context"
	"errors"
	"sync"
	"time"
)

// streamBatchSize is the maximum number of changes pulled from the log
// per stream read iteration.
const streamBatchSize = 64

// progressIdleTTL bounds how long server-side acknowledgement progress is
// remembered for an id without an active connection or any activity. It is
// an internal hygiene limit, not a user-visible configuration: progress
// entries only exist to let reconnecting clients use a stale cursor without
// re-receiving acknowledged changes.
const progressIdleTTL = 10 * time.Minute

// progressSoftFactor sets the progress-table size (as a multiple of
// MaxSubscriptions) above which idle progress entries are swept.
const progressSoftFactor = 4

// subProgress is the lightweight, capacity-free memory kept for a subscriber
// id after its active connection is gone. It never counts against
// MaxSubscriptions: subscription slots only track live connections.
type subProgress struct {
	acked int64 // last position acknowledged by the subscriber
	last  time.Time
}

// Subscription is an active subscriber connection (in-flight request or
// live stream) of the change log.
type Subscription struct {
	srv    *Server
	id     string
	mode   Mode
	cursor int64 // last position delivered on this connection generation
	resync bool
	// gen is the connection generation. Every (re)attach for the same id
	// increments it; a superseded goroutine must not touch slot state once
	// its generation is stale.
	gen    uint64
	cancel context.CancelFunc
	last   time.Time
}

// ID returns the subscription id.
func (s *Subscription) ID() string { return s.id }

// Mode returns the subscription mode.
func (s *Subscription) Mode() Mode { return s.mode }

// Cursor returns the subscription's last delivered cursor.
func (s *Subscription) Cursor() int64 {
	s.srv.mu.Lock()
	defer s.srv.mu.Unlock()
	return s.cursor
}

// Acked returns the last acknowledged cursor remembered for the
// subscription's id, even if the acknowledgement was recorded by a previous
// connection generation.
func (s *Subscription) Acked() int64 {
	s.srv.mu.Lock()
	defer s.srv.mu.Unlock()
	return s.srv.ackedLocked(s.id)
}

// Server is the unified transport server. It serves poll, long-poll and
// event-stream subscribers from a single ordered change log.
type Server struct {
	cfg   Config
	log   *ChangeLog
	clock Clock

	mu sync.Mutex
	// subs holds active connection slots (live streams and in-flight
	// one-shot requests). Its size never exceeds MaxSubscriptions.
	subs map[string]*Subscription
	// progress remembers acknowledged cursors for ids that have no active
	// connection slot. It is bounded by idle sweeping and never counts
	// against MaxSubscriptions.
	progress     map[string]*subProgress
	shut         chan struct{}
	shuttingDown bool
	wg           sync.WaitGroup
}

// Option configures a Server.
type Option func(*Server)

// WithClock overrides the clock used for timeouts and heartbeats.
func WithClock(clk Clock) Option {
	return func(s *Server) {
		if clk != nil {
			s.clock = clk
		}
	}
}

// New creates a server with the given configuration.
func New(cfg Config, opts ...Option) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	s := &Server{
		cfg:      cfg,
		log:      NewChangeLog(),
		clock:    RealClock{},
		subs:     make(map[string]*Subscription),
		progress: make(map[string]*subProgress),
		shut:     make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// Log returns the underlying change log used to append changes.
func (s *Server) Log() *ChangeLog { return s.log }

// Stats is a point-in-time view of server state.
type Stats struct {
	Subscriptions int
	Resync        int
	Frontier      int64
}

// Stats returns a point-in-time snapshot of server state.
func (s *Server) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := Stats{Frontier: s.log.Frontier()}
	for _, sub := range s.subs {
		st.Subscriptions++
		if sub.resync {
			st.Resync++
		}
	}
	return st
}

// Subscription returns the subscription with the given id, or nil if absent.
func (s *Server) Subscription(id string) *Subscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.subs[id]
}

// ackedLocked returns the highest acknowledged cursor recorded for id,
// whether the acknowledgement came from an active slot's stored progress or
// from progress left behind by a previous connection.
func (s *Server) ackedLocked(id string) int64 {
	if p, ok := s.progress[id]; ok {
		return p.acked
	}
	return 0
}

// rememberAckLocked records an acknowledgement for id and touches its
// progress activity time.
func (s *Server) rememberAckLocked(id string, seq int64) {
	p := s.progress[id]
	if p == nil {
		p = &subProgress{}
		s.progress[id] = p
	}
	if seq > p.acked {
		p.acked = seq
	}
	p.last = s.clock.Now()
}

// releaseLocked ends the current connection generation for id. If the slot
// still belongs to that generation it is removed (freeing the subscription
// capacity) and its acknowledgement progress is retained out-of-band so a
// later reconnect can clamp a stale cursor. A superseded slot is left
// untouched for the newer generation.
func (s *Server) releaseLocked(id string, gen uint64) {
	sub := s.subs[id]
	if sub == nil || sub.gen != gen {
		return
	}
	delete(s.subs, id)
	p := s.progress[id]
	if p == nil {
		p = &subProgress{}
		s.progress[id] = p
	}
	// Only explicit Acks advance the resume floor. Delivery that was merely
	// buffered or handed out (but not acknowledged) stays replayable, so the
	// release only refreshes activity time.
	p.last = s.clock.Now()
}

// sweepProgressLocked bounds the progress table: once it grows well past the
// active subscription capacity, entries that have been idle (no attach and
// no ack) beyond progressIdleTL are forgotten. Active slots are never
// removed. Sweeping is amortized into request registration, so there is no
// background goroutine or timer to leak.
func (s *Server) sweepProgressLocked() {
	soft := s.cfg.MaxSubscriptions * progressSoftFactor
	if len(s.progress) <= soft {
		return
	}
	cutoff := s.clock.Now().Add(-progressIdleTTL)
	for id, p := range s.progress {
		if _, active := s.subs[id]; active {
			continue
		}
		if p.last.Before(cutoff) {
			delete(s.progress, id)
		}
	}
}

// beginLocked attaches a new connection generation for id and returns the
// slot, the generation tag the caller must present on release, and the
// effective resume start position. Resume semantics are identical for every
// transport: delivery starts strictly after max(client cursor, server
// remembered ack). Changes already acknowledged are never re-delivered;
// everything delivered but not yet acknowledged (including events buffered
// for a slow subscriber whose connection dropped) is replayed. The
// per-connection delivery cursor is reset to the start, so a stale
// delivery position from a superseded connection can never skip changes.
//
// ephemeral marks one-shot poll/long-poll requests, which release the slot
// when the request returns even if no newer generation superseded it.
func (s *Server) beginLocked(id string, mode Mode, afterSeq int64, ephemeral bool, cancel context.CancelFunc) (*Subscription, uint64, int64, error) {
	if s.shuttingDown {
		return nil, 0, afterSeq, ErrShutdown
	}
	// A same-id request supersedes whatever connection generation is active:
	// a replaced stream pump or suspended long-poll is cancelled.
	if existing, ok := s.subs[id]; ok {
		if existing.cancel != nil {
			existing.cancel()
		}
		// Preserve acknowledgements recorded by the outgoing generation.
		// Stream delivery that was merely buffered (possibly unread) must
		// not be treated as acknowledged; that is handled by the caller
		// updating progress only on explicit Ack.
		existing.resync = false
		existing.mode = mode
		existing.gen++
		start := afterSeq
		if a := s.ackedLocked(id); a > start {
			start = a
		}
		existing.cursor = start
		// Store the cancel func for every generation: it lets a later
		// same-id request interrupt a suspended long-poll and lets Shutdown
		// wake in-flight requests (nil for non-blocking polls).
		existing.cancel = cancel
		existing.last = s.clock.Now()
		if p := s.progress[id]; p != nil {
			p.last = s.clock.Now()
		}
		return existing, existing.gen, start, nil
	}
	if len(s.subs) >= s.cfg.MaxSubscriptions {
		return nil, 0, afterSeq, ErrTooManySubscriptions
	}
	sub := &Subscription{
		srv:    s,
		id:     id,
		mode:   mode,
		cursor: afterSeq,
		gen:    1,
		last:   s.clock.Now(),
	}
	if !ephemeral {
		sub.cancel = cancel
	}
	s.subs[id] = sub
	if a := s.ackedLocked(id); a > sub.cursor {
		sub.cursor = a
	}
	s.sweepProgressLocked()
	return sub, sub.gen, sub.cursor, nil
}

// Subscribe registers (or reactivates) a persistent subscription slot. The
// slot is retained across the call; use Stream to serve an event stream on
// it and Unsubscribe to release it.
func (s *Server) Subscribe(ctx context.Context, id string, mode Mode, afterSeq int64) (*Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, _, _, err := s.beginLocked(id, mode, afterSeq, false, nil)
	return sub, err
}

// Unsubscribe removes the subscription, forgets its resume progress and
// releases all its resources.
func (s *Server) Unsubscribe(ctx context.Context, id string) error {
	s.mu.Lock()
	sub, ok := s.subs[id]
	if !ok {
		s.mu.Unlock()
		return ErrSubscriptionNotFound
	}
	delete(s.subs, id)
	delete(s.progress, id)
	if sub.cancel != nil {
		sub.cancel()
	}
	s.mu.Unlock()
	return nil
}

// advanceCursorLocked moves the connection delivery cursor forward and
// touches activity time. Caller must hold s.mu.
func (s *Server) advanceCursorLocked(sub *Subscription, cursor int64) {
	if cursor > sub.cursor {
		sub.cursor = cursor
	}
	sub.last = s.clock.Now()
}

// waitForChanges suspends until changes are available after afterSeq, the
// timeout elapses, the context is cancelled or the server shuts down.
// A timeout returns an empty slice with no error.
func (s *Server) waitForChanges(ctx context.Context, afterSeq int64, limit int, timeout time.Duration) ([]Change, error) {
	timer := s.clock.After(timeout)
	for {
		changes, err := s.log.Read(afterSeq, limit)
		if err != nil {
			return nil, err
		}
		if len(changes) > 0 {
			return changes, nil
		}
		select {
		case <-s.log.WaitChan(afterSeq):
			// frontier advanced (or already had): re-read
		case <-timer:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.shut:
			return nil, ErrShutdown
		}
	}
}

// Poll reads up to limit changes after the given cursor without waiting.
// It returns the changes and the next cursor to use. The request occupies a
// subscription slot only while in flight and releases it on return.
func (s *Server) Poll(ctx context.Context, id string, afterSeq int64, limit int) ([]Change, int64, error) {
	s.mu.Lock()
	sub, gen, start, err := s.beginLocked(id, ModePoll, afterSeq, true, nil)
	s.mu.Unlock()
	if err != nil {
		return nil, afterSeq, err
	}

	s.wg.Add(1)
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		s.releaseLocked(id, gen)
		s.mu.Unlock()
	}()

	changes, err := s.log.Read(start, limit)
	if err != nil {
		return nil, start, err
	}
	next := start
	if len(changes) > 0 {
		next = changes[len(changes)-1].Seq
	}
	s.mu.Lock()
	if s.subs[id] == sub {
		s.advanceCursorLocked(sub, next)
	}
	s.mu.Unlock()
	return changes, next, nil
}

// LongPoll reads up to limit changes after the given cursor, suspending
// until changes are available or the timeout elapses. A timeout returns an
// empty slice with no error. The request occupies a subscription slot only
// while in flight and releases it on return; a same-id reconnect while
// suspended cancels the stale request.
func (s *Server) LongPoll(ctx context.Context, id string, afterSeq int64, limit int, timeout time.Duration) ([]Change, int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.mu.Lock()
	sub, gen, start, err := s.beginLocked(id, ModeLongPoll, afterSeq, true, cancel)
	s.mu.Unlock()
	if err != nil {
		return nil, afterSeq, err
	}

	s.wg.Add(1)
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		s.releaseLocked(id, gen)
		s.mu.Unlock()
	}()

	if timeout <= 0 {
		timeout = s.cfg.LongPollTimeout
	}
	changes, err := s.waitForChanges(ctx, start, limit, timeout)
	if err != nil {
		// A same-id reconnect cancels the stale suspended request; report
		// it as cancelled to that stale caller. Server shutdown, which also
		// closes s.shut, reports ErrShutdown instead.
		if errors.Is(err, context.Canceled) {
			s.mu.Lock()
			down := s.shuttingDown
			superseded := false
			if cur := s.subs[id]; cur != nil && cur.gen != gen {
				superseded = true
			}
			s.mu.Unlock()
			if down && !superseded {
				err = ErrShutdown
			}
		}
		return nil, start, err
	}
	next := start
	if len(changes) > 0 {
		next = changes[len(changes)-1].Seq
	}
	s.mu.Lock()
	if s.subs[id] == sub {
		s.advanceCursorLocked(sub, next)
	}
	s.mu.Unlock()
	return changes, next, nil
}

// Stream opens a long-lived event stream starting after the given cursor.
// A same-id stream (or any other request for that id) supersedes the
// previous connection generation; the old pump is cancelled and must hand
// the slot over rather than close or release it.
func (s *Server) Stream(ctx context.Context, id string, afterSeq int64) (<-chan Event, error) {
	streamCtx, cancel := context.WithCancel(ctx)

	s.mu.Lock()
	sub, gen, start, err := s.beginLocked(id, ModeStream, afterSeq, false, cancel)
	if err != nil {
		s.mu.Unlock()
		cancel()
		return nil, err
	}
	ch := make(chan Event, s.cfg.BufferSize)
	s.mu.Unlock()

	s.wg.Add(1)
	go s.pump(sub, gen, start, ch, streamCtx)
	return ch, nil
}

// enterResync enqueues the resync marker that terminates the given
// connection generation. It does not move the cursor: the client reconnects
// with its true processed position, and resume always starts after
// max(client cursor, acked). Buffered events are drained first so the marker
// is deliverable; drained events remain in the log and are replayed on
// reconnect. The caller (the owning pump) closes the channel afterwards, so
// this function never closes it itself and therefore cannot double-close
// while racing cancellation.
func (s *Server) enterResync(sub *Subscription, gen uint64, ch chan Event, cause error) {
	s.mu.Lock()
	cursor := int64(0)
	current := s.subs[sub.id] == sub && sub.gen == gen
	if current {
		sub.resync = true
		cursor = sub.cursor
	}
	s.mu.Unlock()

	if !current {
		// A newer generation owns this id; do not inject anything into a
		// channel the successor is tearing down.
		return
	}

	for {
		select {
		case <-ch:
		case <-s.shut:
			return
		default:
			goto delivered
		}
	}
delivered:
	select {
	case ch <- Event{Type: EventResync, Err: cause, Cursor: cursor}:
	case <-s.shut:
	}
}

// pump is the per-stream background goroutine. It pulls changes from the
// log and pushes them to the subscriber's bounded buffer. Sends block when
// the buffer is full, applying backpressure to this subscriber only; if the
// subscriber stays blocked past the write timeout it is resynchronized.
//
// The pump advances only its own connection-generation cursor (the last
// event actually handed to the channel). That cursor is never used to skip
// changes on reconnect: resume starts from the client's reported position
// clamped by the acked cursor, so events buffered-but-unread when the
// connection dropped are replayed instead of forming a hole.
func (s *Server) pump(sub *Subscription, gen uint64, after int64, ch chan Event, ctx context.Context) {
	defer s.wg.Done()

	hb := s.clock.Ticker(s.cfg.HeartbeatInterval)
	defer hb.Stop()

	// closeOnce serializes every terminal channel action: a blocked send
	// racing with cancellation must not double-close the channel.
	var closeOnce sync.Once
	closeWith := func(ev *Event) {
		closeOnce.Do(func() {
			if ev != nil {
				select {
				case ch <- *ev:
				default:
				}
			}
			close(ch)
		})
	}

	// stale reports whether this pump has been superseded by a newer
	// connection generation for the same id.
	stale := func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		cur := s.subs[sub.id]
		return cur != sub || cur.gen != gen
	}

	// finish is the terminal action. A superseded pump neither owns the
	// current slot nor the successor's channel; it only closes its own
	// (already replaced) channel so its client's range loop terminates.
	finish := func(sendClosed bool) {
		if stale() {
			closeWith(nil)
			return
		}
		if sendClosed {
			closeWith(&Event{Type: EventClosed})
		} else {
			closeWith(nil)
		}
		s.mu.Lock()
		s.releaseLocked(sub.id, gen)
		s.mu.Unlock()
	}

	// send delivers one event, blocking until there is room, the write
	// timeout elapses, the client goes away or the server shuts down.
	// The timer is created lazily so idle pumps hold no pending timer.
	send := func(ev Event) bool {
		timer := s.clock.Timer(s.cfg.LongPollTimeout)
		defer timer.Stop()
		select {
		case ch <- ev:
			return true
		case <-ctx.Done():
			finish(true)
			return false
		case <-s.shut:
			finish(true)
			return false
		case <-timer.C():
			s.enterResync(sub, gen, ch, ErrSlowConsumer)
			// Resync terminates this generation: close the channel and drop
			// the slot so a client reconnecting with its true processed
			// cursor re-attaches and gets the backlog replayed.
			closeWith(nil)
			s.mu.Lock()
			s.releaseLocked(sub.id, gen)
			s.mu.Unlock()
			return false
		}
	}

	for {
		if stale() {
			return
		}
		changes, err := s.log.Read(after, streamBatchSize)
		if err != nil {
			s.enterResync(sub, gen, ch, err)
			closeWith(nil)
			s.mu.Lock()
			s.releaseLocked(sub.id, gen)
			s.mu.Unlock()
			return
		}
		if len(changes) > 0 {
			for _, c := range changes {
				if !send(Event{Type: EventChange, Change: c, Cursor: c.Seq}) {
					return
				}
				after = c.Seq
				s.mu.Lock()
				if cur := s.subs[sub.id]; cur == sub && cur.gen == gen {
					s.advanceCursorLocked(sub, after)
				}
				s.mu.Unlock()
			}
			continue
		}

		select {
		case <-s.log.WaitChan(after):
			// frontier advanced: loop and re-read
		case <-hb.C():
			if !send(Event{Type: EventHeartbeat}) {
				return
			}
		case <-ctx.Done():
			finish(true)
			return
		case <-s.shut:
			finish(true)
			return
		}
	}
}

// Ack acknowledges all changes up to and including seq for the subscriber.
// Acknowledgements are recorded against the id (whether or not a connection
// is currently attached) and clamp every later resume for that id, so an
// acknowledged change is never re-delivered on any transport.
func (s *Server) Ack(ctx context.Context, id string, seq int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.shuttingDown {
		return ErrShutdown
	}
	if seq > s.log.Frontier() {
		return ErrCursorAhead
	}
	if seq < 0 {
		return ErrBadCursor
	}
	s.rememberAckLocked(id, seq)
	if sub := s.subs[id]; sub != nil && seq > sub.cursor {
		sub.cursor = seq
	}
	return nil
}

// Shutdown gracefully drains the server: pending long-polls are woken with
// ErrShutdown, streams receive an EventClosed, and all background tasks
// finish before it returns (or ctx is done).
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.shuttingDown {
		s.mu.Unlock()
		return nil
	}
	s.shuttingDown = true
	close(s.shut)
	for _, sub := range s.subs {
		if sub.cancel != nil {
			sub.cancel()
		}
	}
	s.mu.Unlock()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
