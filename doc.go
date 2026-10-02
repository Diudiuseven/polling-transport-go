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
// it has processed and the server continues from there without replaying
// acknowledged changes. Slow stream subscribers overflow a bounded per-
// subscriber buffer and enter a resynchronization state; they reconnect
// with their last cursor and resume from the log.
//
// Long-poll requests suspend until changes arrive or the timeout elapses;
// a single broadcast channel wakes all waiting requests at O(1) cost when
// the log frontier advances. Graceful shutdown drains pending requests and
// terminates streams without leaking goroutines.
package transport
