% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_await}
\alias{grpc_await}
\title{Wait for events belonging to one call}
\usage{
grpc_await(x, timeout_ms, max_events = 64L)
}
\arguments{
\item{x}{A \code{"grpc_call"}, \code{"grpc_stream"}, or
\code{"grpc_request"} object.}

\item{timeout_ms}{How long to wait for an event belonging to
\code{x}: \code{0} returns immediately, a positive value waits up to
that many milliseconds, \code{-1} waits indefinitely. Required, so
that a stalled peer cannot silently become an unbounded wait; the
call's own deadline is the other half of that guarantee.}

\item{max_events}{Maximum events to return in this batch.}
}
\value{
A list of events for \code{x} (possibly empty), in arrival
  order.
}
\description{
Like \code{\link{grpc_poll}}, but scoped to a single call: only that
call's events are returned, and the wait ends when one of them
arrives rather than when anything at all does. Events for other calls
stay queued in arrival order and are delivered by a later
\code{\link{grpc_poll}} or \code{grpc_await} on their own call.
}
\details{
This is the sequential-mode API, on both sides of the wire. It
removes the demultiplexing a shared client or server otherwise pushes
onto the caller: there is no way for a \code{grpc_await} loop to
splice another call's messages into this one's payload, or to mistake
another call's completion for this one's. The cost is that awaiting
one call means not looking at the others, so a second call's deadline
can pass unnoticed while this one is waited on. Drive genuinely
concurrent work with \code{\link{grpc_poll}} and dispatch on
\code{id}.

A call ends at its terminal event, so loop until that arrives: a
\code{"unary"} event for \code{\link{grpc_call}}, a
\code{"stream_status"} for \code{\link{grpc_stream}}. On a server
\code{"grpc_request"} the awaited events are the ones that follow the
request itself -- \code{"stream_msg"}, \code{"client_done"},
\code{"stream_writable"}, \code{"cancelled"} -- since the request
arrives from \code{\link{grpc_poll}} in the first place. A server
call has no terminal event of its own: it ends when the handler ends
it with \code{\link{grpc_reply}} or \code{\link{grpc_finish}}, so
\code{"client_done"} is what a client-streaming handler loops to.

Two different clocks are in play, and the words for them are not
self-distinguishing. \code{deadline_ms} on \code{\link{grpc_call}} or
\code{\link{grpc_stream}} bounds the RPC: when it expires the call
really is over, and the peer is told. \code{timeout_ms} here bounds
only this wait. Setting the wait shorter than the deadline is normal
and harmless; setting no deadline at all is what makes
\code{timeout_ms = -1} an unbounded wait. A server sees the client's
deadline as \code{deadline_ms} on the request event.

An empty result means the wait ended with nothing for this call. It
is not a failure and not an answer: an empty await leaves the call
exactly as it was, so await it again to keep waiting, and the worst
it costs is another trip round the loop. Because an empty result is
possible, index the batch only after checking it --
\code{grpc_await(call, timeout_ms = 1000)[[1]]} raises \code{subscript
out of bounds} on a slow peer, which reads like a bug in the caller
rather than the timeout it is.

Usually the wait ended because \code{timeout_ms} expired, but it also
returns early when a signal interrupts it, so an empty result is not
proof that \code{timeout_ms} of wall time passed. That early return is
deliberate — it is what lets R process a Ctrl-C instead of ignoring it
until the timeout runs out. It matters only if you are deriving
elapsed time from the number of empty awaits; use a clock for
deadlines, not a count.

}
\examples{
\dontrun{
## unary: keep waiting until the completion arrives. The call's own
## deadline_ms is what guarantees this loop ends.
call <- grpc_call(client, "/demo.Echo/Say", req, deadline_ms = 5000)
repeat {
  evs <- grpc_await(call, timeout_ms = 1000)
  if (length(evs)) break                       # empty just means "not yet"
}
evs[[1]]$status_name

## server stream: accumulate to the terminal status
s <- grpc_stream(client, "/demo.Big/List", deadline_ms = 15000)
grpc_writes_done(s)
out <- list()
repeat {
  evs <- grpc_await(s, timeout_ms = 1000)
  for (ev in evs) if (ev$kind == "stream_msg") out <- c(out, list(ev$response))
  if (any(vapply(evs, function(e) e$kind == "stream_status", TRUE))) break
}

## server handler: drain one client-streaming call without seeing any
## other call's messages
got <- list()
repeat {
  grpc_read(req)
  evs <- grpc_await(req, timeout_ms = 1000)
  for (ev in evs) if (ev$type == "stream_msg") got <- c(got, list(ev$request))
  if (any(vapply(evs, function(e) e$type == "client_done", TRUE))) break
}
grpc_finish(req)
}
}
