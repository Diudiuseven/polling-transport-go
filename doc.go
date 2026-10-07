// Package transport provides a unified change-subscription transport layer.
//
// Clients subscribe to the same ordered change log via three transports:
// one-shot polling, long-polling and long-lived event streams. All three
// read the same gap-free contiguous prefix of the log, so subscribers see
// identical cursor semantics regardless of transport.
//
// Changes carry a monotonically increasing sequence number. Out-of-order
// appends are buffered until gaps are filled and duplicate appends are
// ignored, which normalizes disordered and redelivered upstream changes.
//
// Subscribers resume by cursor: a client reconnects with the last position
// it has processed and the server continues from max(client cursor, last
// acknowledged). The delivered cursor is never a resume floor, so changes
// buffered but not yet processed (drained on resync or lost on disconnect)
// are redelivered, while acknowledged changes are never replayed. Slow
// stream subscribers overflow a bounded per-subscriber buffer and enter a
// resynchronization state; they reconnect with their last processed cursor
// and resume from the log without gaps.
//
// One-shot poll and long-poll requests occupy a subscription slot only
// while in flight and release it when they return, so the subscription
// count stays near the number of truly active subscribers; the configured
// maximum is enforced against those active subscriptions. Long-poll
// requests suspend until changes arrive or the timeout elapses; a single
// broadcast channel wakes all waiting requests at O(1) cost when the log
// frontier advances. Graceful shutdown drains pending requests and
// terminates streams without leaking goroutines.
package transport
