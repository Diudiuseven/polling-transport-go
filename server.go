package transport

import (
	"context"
	"sync"
	"time"
)

// streamBatchSize is the maximum number of changes pulled from the log
// per stream read iteration.
const streamBatchSize = 64

// Subscription is a registered subscriber of the change log.
type Subscription struct {
	srv    *Server
	id     string
	mode   Mode
	cursor int64 // last position delivered to the subscriber
	acked  int64 // last position acknowledged by the subscriber
	resync bool
	send   chan Event
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

// Acked returns the subscription's last acknowledged cursor.
func (s *Subscription) Acked() int64 {
	s.srv.mu.Lock()
	defer s.srv.mu.Unlock()
	return s.acked
}

// Server is the unified transport server. It serves poll, long-poll and
// event-stream subscribers from a single ordered change log.
type Server struct {
	cfg   Config
	log   *ChangeLog
	clock Clock

	mu           sync.Mutex
	subs         map[string]*Subscription
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
		cfg:   cfg,
		log:   NewChangeLog(),
		clock: RealClock{},
		subs:  make(map[string]*Subscription),
		shut:  make(chan struct{}),
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

// registerLocked creates or supersedes a subscription. Caller must hold s.mu.
func (s *Server) registerLocked(id string, mode Mode, afterSeq int64) (*Subscription, error) {
	if s.shuttingDown {
		return nil, ErrShutdown
	}
	if existing, ok := s.subs[id]; ok {
		if existing.cancel != nil {
			existing.cancel()
		}
		existing.resync = false
		existing.mode = mode
		start := afterSeq
		if existing.acked > start {
			start = existing.acked
		}
		if existing.cursor > start {
			start = existing.cursor
		}
		existing.cursor = start
		existing.send = nil
		existing.cancel = nil
		existing.last = s.clock.Now()
		return existing, nil
	}
	if len(s.subs) >= s.cfg.MaxSubscriptions {
		return nil, ErrTooManySubscriptions
	}
	sub := &Subscription{srv: s, id: id, mode: mode, cursor: afterSeq, last: s.clock.Now()}
	s.subs[id] = sub
	return sub, nil
}

// trackLocked returns the active subscription for id, creating it on first
// use. It does not supersede an existing stream. Caller must hold s.mu.
func (s *Server) trackLocked(id string, mode Mode, afterSeq int64) (*Subscription, error) {
	if s.shuttingDown {
		return nil, ErrShutdown
	}
	if sub, ok := s.subs[id]; ok {
		if sub.resync {
			return nil, ErrResync
		}
		if afterSeq > sub.cursor {
			sub.cursor = afterSeq
		}
		sub.mode = mode
		sub.last = s.clock.Now()
		return sub, nil
	}
	if len(s.subs) >= s.cfg.MaxSubscriptions {
		return nil, ErrTooManySubscriptions
	}
	sub := &Subscription{srv: s, id: id, mode: mode, cursor: afterSeq, last: s.clock.Now()}
	s.subs[id] = sub
	return sub, nil
}

// Subscribe registers (or reactivates) a subscription. If a subscription
// with the same id already exists, it is superseded (reconnect semantics)
// and the new subscription starts at max(afterSeq, last acknowledged).
func (s *Server) Subscribe(ctx context.Context, id string, mode Mode, afterSeq int64) (*Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.registerLocked(id, mode, afterSeq)
}

// Unsubscribe removes a subscription and releases its resources.
func (s *Server) Unsubscribe(ctx context.Context, id string) error {
	s.mu.Lock()
	sub, ok := s.subs[id]
	if !ok {
		s.mu.Unlock()
		return ErrSubscriptionNotFound
	}
	delete(s.subs, id)
	if sub.cancel != nil {
		sub.cancel()
	}
	s.mu.Unlock()
	return nil
}

// touch updates the subscription cursor and last-active time.
func (s *Server) touch(sub *Subscription, cursor int64) {
	s.mu.Lock()
	if cursor > sub.cursor {
		sub.cursor = cursor
	}
	sub.last = s.clock.Now()
	s.mu.Unlock()
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
// It returns the changes and the next cursor to use.
func (s *Server) Poll(ctx context.Context, id string, afterSeq int64, limit int) ([]Change, int64, error) {
	s.mu.Lock()
	sub, err := s.trackLocked(id, ModePoll, afterSeq)
	s.mu.Unlock()
	if err != nil {
		return nil, afterSeq, err
	}

	s.wg.Add(1)
	defer s.wg.Done()

	changes, err := s.log.Read(afterSeq, limit)
	if err != nil {
		return nil, afterSeq, err
	}
	next := afterSeq
	if len(changes) > 0 {
		next = changes[len(changes)-1].Seq
	}
	s.touch(sub, next)
	return changes, next, nil
}

// LongPoll reads up to limit changes after the given cursor, suspending
// until changes are available or the timeout elapses. A timeout returns
// an empty slice with no error.
func (s *Server) LongPoll(ctx context.Context, id string, afterSeq int64, limit int, timeout time.Duration) ([]Change, int64, error) {
	s.mu.Lock()
	sub, err := s.trackLocked(id, ModeLongPoll, afterSeq)
	s.mu.Unlock()
	if err != nil {
		return nil, afterSeq, err
	}

	s.wg.Add(1)
	defer s.wg.Done()

	if timeout <= 0 {
		timeout = s.cfg.LongPollTimeout
	}
	changes, err := s.waitForChanges(ctx, afterSeq, limit, timeout)
	if err != nil {
		return nil, afterSeq, err
	}
	next := afterSeq
	if len(changes) > 0 {
		next = changes[len(changes)-1].Seq
	}
	s.touch(sub, next)
	return changes, next, nil
}

// Stream opens a long-lived event stream starting after the given cursor.
func (s *Server) Stream(ctx context.Context, id string, afterSeq int64) (<-chan Event, error) {
	streamCtx, cancel := context.WithCancel(ctx)

	s.mu.Lock()
	if s.shuttingDown {
		s.mu.Unlock()
		cancel()
		return nil, ErrShutdown
	}
	sub, err := s.registerLocked(id, ModeStream, afterSeq)
	if err != nil {
		s.mu.Unlock()
		cancel()
		return nil, err
	}
	ch := make(chan Event, s.cfg.BufferSize)
	sub.send = ch
	sub.cancel = cancel
	s.mu.Unlock()

	s.wg.Add(1)
	go s.pump(sub, ch, streamCtx)
	return ch, nil
}

// enterResync marks the subscriber as requiring resynchronization and
// closes its event channel. The subscriber reconnects with its last cursor
// and resumes from the log. Buffered events are drained first so the
// resync marker can always be delivered; the marker is informational.
func (s *Server) enterResync(sub *Subscription, ch chan Event, cause error) {
	s.mu.Lock()
	sub.resync = true
	cursor := sub.cursor
	s.mu.Unlock()

	// Drain buffered events so the resync marker always has room. The
	// drained events remain in the log and are replayed on reconnect.
	for {
		select {
		case <-ch:
		default:
			goto delivered
		}
	}
delivered:
	ch <- Event{Type: EventResync, Err: cause, Cursor: cursor}
	close(ch)
}

// sendClosed delivers a terminal EventClosed and closes the channel.
func (s *Server) sendClosed(ch chan Event) {
	select {
	case ch <- Event{Type: EventClosed}:
	default:
	}
	close(ch)
}

// pump is the per-stream background goroutine. It pulls changes from the
// log and pushes them to the subscriber's bounded buffer. Sends block when
// the buffer is full, applying backpressure to this subscriber only; if the
// subscriber stays blocked past the write timeout it is resynchronized.
func (s *Server) pump(sub *Subscription, ch chan Event, ctx context.Context) {
	defer s.wg.Done()

	hb := s.clock.Ticker(s.cfg.HeartbeatInterval)
	defer hb.Stop()

	after := sub.Cursor()

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
			s.sendClosed(ch)
			return false
		case <-s.shut:
			s.sendClosed(ch)
			return false
		case <-timer.C():
			s.enterResync(sub, ch, ErrSlowConsumer)
			return false
		}
	}

	for {
		changes, err := s.log.Read(after, streamBatchSize)
		if err != nil {
			s.enterResync(sub, ch, err)
			return
		}
		if len(changes) > 0 {
			for _, c := range changes {
				if !send(Event{Type: EventChange, Change: c, Cursor: c.Seq}) {
					return
				}
				after = c.Seq
				s.mu.Lock()
				sub.cursor = after
				sub.last = s.clock.Now()
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
			s.sendClosed(ch)
			return
		case <-s.shut:
			s.sendClosed(ch)
			return
		}
	}
}

// Ack acknowledges all changes up to and including seq for the subscription.
func (s *Server) Ack(ctx context.Context, id string, seq int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.shuttingDown {
		return ErrShutdown
	}
	sub, ok := s.subs[id]
	if !ok {
		return ErrSubscriptionNotFound
	}
	if seq > s.log.Frontier() {
		return ErrCursorAhead
	}
	if seq > sub.acked {
		sub.acked = seq
	}
	if seq > sub.cursor {
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
