package transport

import (
	"errors"
	"time"
)

// Mode identifies the subscription transport mode.
type Mode int

const (
	// ModePoll is one-shot polling: the client issues a request and gets
	// the changes currently available.
	ModePoll Mode = iota
	// ModeLongPoll is long-polling: the request suspends until changes are
	// available or the wait times out.
	ModeLongPoll
	// ModeStream is a long-lived event stream (server-sent events style).
	ModeStream
)

func (m Mode) String() string {
	switch m {
	case ModePoll:
		return "poll"
	case ModeLongPoll:
		return "longpoll"
	case ModeStream:
		return "stream"
	default:
		return "unknown"
	}
}

// Change is a single ordered change in the unified change log.
// Seq is the globally unique, monotonically increasing cursor.
type Change struct {
	Seq   int64     `json:"seq"`
	Key   string    `json:"key"`
	Value []byte    `json:"value"`
	Time  time.Time `json:"time"`
}

// EventType identifies the kind of event delivered on a stream.
type EventType int

const (
	// EventChange carries a change from the log.
	EventChange EventType = iota
	// EventHeartbeat is emitted when the stream is idle.
	EventHeartbeat
	// EventResync signals that the subscriber fell behind and must
	// reconnect with its last acknowledged cursor.
	EventResync
	// EventClosed signals that the stream was terminated (e.g. shutdown).
	EventClosed
)

// Event is delivered to stream subscribers.
type Event struct {
	Type   EventType `json:"type"`
	Change Change    `json:"change,omitempty"`
	Cursor int64     `json:"cursor,omitempty"`
	Err    error     `json:"-"`
}

// Errors returned by the transport layer.
var (
	ErrBadCursor            = errors.New("transport: cursor must be non-negative")
	ErrCursorAhead          = errors.New("transport: cursor is ahead of the log frontier")
	ErrResync               = errors.New("transport: subscriber must resynchronize with its last cursor")
	ErrTooManySubscriptions = errors.New("transport: maximum number of subscriptions reached")
	ErrShutdown             = errors.New("transport: server is shutting down")
	ErrSlowConsumer         = errors.New("transport: subscriber is too slow, buffer overflow")
	ErrSubscriptionNotFound = errors.New("transport: subscription not found")
	ErrInvalidConfig        = errors.New("transport: invalid configuration")
)
