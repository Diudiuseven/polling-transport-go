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
// # Resume semantics
//
// A client resumes by passing the last position it has actually processed.
// For every transport the server starts delivery strictly after
// max(client cursor, server-remembered acked cursor). This gives identical
// behavior for poll, long-poll and stream:
//
//   - A change that was only delivered (pushed into a stream buffer, or
//     returned by a one-shot request) but neither processed nor acknowledged
//     is replayable. When a slow stream subscriber disconnects mid-backlog,
//     or is forced into resync, the changes it had not consumed are still in
//     the log and are delivered again on reconnect, so the received sequence
//     can never contain a hole.
//   - A change explicitly acknowledged via Ack is the resume floor and is
//     never delivered again on any transport, even if the client reconnects
//     with an older cursor.
//   - Delivery itself never advances the resume floor. In particular the
//     stream pump's delivery cursor is per-connection: it is reset on every
//     (re)attach and can never cause later changes to be skipped.
//
// Delivery of unacknowledged changes is therefore at-least-once across a
// dropped/reconnected connection; clients dedupe by sequence. Within a single
// connection each change is delivered once.
//
// # Backlog and resynchronization
//
// Each stream subscriber owns a bounded buffer (BufferSize). When it is full
// the pump blocks, applying backpressure to that subscriber only. If the block
// lasts longer than the write timeout (LongPollTimeout) the pump drains its
// buffer, emits an EventResync marker and closes the stream; it also releases
// the subscription slot. The client then reconnects with its true processed
// cursor and the full backlog (buffered-but-unread events included) is read
// back from the log. Resync only terminates the connection; it never skips or
// truncates changes.
//
// # Subscription lifecycle and capacity
//
// MaxSubscriptions bounds concurrently active connection slots: live streams
// and in-flight (including suspended) poll/long-poll requests. One-shot
// requests occupy a slot only while executing and always release it on
// return, so a long-running server handling arbitrarily many historical
// requests stays near the number of genuinely active subscribers instead of
// growing without bound; the limit still rejects genuinely over-capacity
// load. Acknowledgement cursors of departed connections are remembered
// out-of-band purely for stale-cursor clamping; that auxiliary table does not
// count against the limit and is swept after an internal idle period.
//
// A same-id request supersedes the previous connection generation: a replaced
// stream pump or a suspended long-poll is cancelled and hands over the slot
// without closing the successor's channel or leaking goroutines.
//
// Long-poll requests suspend until changes arrive or the timeout elapses;
// a single broadcast channel wakes all waiting requests at O(1) cost when
// the log frontier advances. Graceful shutdown drains pending requests and
// terminates streams without leaking goroutines.
package transport
