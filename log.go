package transport

import "sync"

// defaultReadLimit caps the number of changes returned by a single read
// when the caller does not specify a limit.
const defaultReadLimit = 100

// ChangeLog is the single ordered source of truth for changes.
//
// Changes are identified by a monotonically increasing sequence number.
// Out-of-order appends are buffered until gaps are filled; duplicate
// appends (same sequence number) are ignored. Readers observe a gap-free
// contiguous prefix: every subscriber sees the same ordered view.
type ChangeLog struct {
	mu       sync.RWMutex
	changes  map[int64]Change
	frontier int64
	// gen is closed and replaced whenever the frontier advances. Waiters
	// hold a reference to the current generation channel, so a single
	// close wakes every waiting goroutine at O(1) cost.
	gen chan struct{}
}

// NewChangeLog creates an empty change log.
func NewChangeLog() *ChangeLog {
	return &ChangeLog{
		changes: make(map[int64]Change),
		gen:     make(chan struct{}),
	}
}

// Append adds changes to the log. Out-of-order changes are buffered until
// earlier gaps are filled; duplicate sequence numbers are ignored. It
// returns the number of changes added and the number of duplicates dropped.
func (l *ChangeLog) Append(changes ...Change) (added, duplicates int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	advanced := false
	for _, ch := range changes {
		if ch.Seq <= 0 {
			duplicates++
			continue
		}
		if _, ok := l.changes[ch.Seq]; ok {
			duplicates++
			continue
		}
		l.changes[ch.Seq] = ch
		added++
	}
	for {
		next := l.frontier + 1
		if _, ok := l.changes[next]; !ok {
			break
		}
		l.frontier = next
		advanced = true
	}
	if advanced {
		close(l.gen)
		l.gen = make(chan struct{})
	}
	return added, duplicates
}

// Frontier returns the highest contiguous sequence number in the log
// (0 when the log is empty).
func (l *ChangeLog) Frontier() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.frontier
}

// Read returns up to limit changes with sequence numbers strictly greater
// than afterSeq, in order. It only returns changes from the contiguous
// prefix, so callers never observe gaps.
func (l *ChangeLog) Read(afterSeq int64, limit int) ([]Change, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if afterSeq < 0 {
		return nil, ErrBadCursor
	}
	if afterSeq > l.frontier {
		return nil, ErrCursorAhead
	}
	if limit <= 0 {
		limit = defaultReadLimit
	}

	out := make([]Change, 0, limit)
	for seq := afterSeq + 1; len(out) < limit; seq++ {
		ch, ok := l.changes[seq]
		if !ok {
			break
		}
		out = append(out, ch)
	}
	return out, nil
}

// WaitChan returns a channel that is closed when the contiguous frontier
// advances past afterSeq. If it has already advanced, an already-closed
// channel is returned.
func (l *ChangeLog) WaitChan(afterSeq int64) <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.frontier > afterSeq {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return l.gen
}
