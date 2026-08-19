#' Receive completed events
#'
#' Drains up to \code{max_events} pending events from a client or server.
#'
#' For a \code{"grpc_client"}, each event is a completed call: a list
#' with \code{id}, \code{status} (integer, see
#' \code{\link{grpc_status_codes}}), \code{status_name}, \code{message},
#' \code{response} (raw vector, \code{NULL} unless the status is
#' \code{OK}), \code{initial_metadata}, and \code{trailing_metadata}.
#'
#' For a \code{"grpc_server"}, each event is either an incoming request
#' (class \code{"grpc_request"}: \code{type = "request"}, \code{id},
#' \code{method}, \code{request} raw vector, \code{metadata},
#' \code{deadline_ms}, answered via \code{\link{grpc_reply}}) or a
#' cancellation notice (\code{type = "cancelled"}, \code{id}) for a
#' request the peer abandoned before it was answered.
#'
#' An empty result means the wait ended with nothing queued at that
#' moment; it says nothing about outstanding work, which ends only when
#' its terminal event is delivered (\code{"unary"} for a call,
#' \code{"stream_status"} for a stream). Loop until that event rather
#' than treating an empty batch as the end of a call;
#' \code{\link{grpc_pending}} reports what is still in flight.
#'
#' An empty result does \emph{not} prove \code{timeout_ms} elapsed. The
#' wait also returns early when a signal interrupts it, which is
#' deliberate: handing control back to R is what lets an interrupt be
#' processed, where restarting the wait would swallow a Ctrl-C for the
#' rest of the timeout. Nothing changes for the ordinary caller — empty
#' still means "nothing yet, go round again" — but do not build a
#' deadline by counting empty returns and multiplying by
#' \code{timeout_ms}, because that arithmetic silently under-counts on
#' an interrupted wait. Read a clock instead.
#'
#' One queue serves the whole client or server, so a batch can mix
#' events from every call in flight, and they arrive in completion
#' order rather than the order the calls were started. Dispatch on
#' \code{id}, in both directions: the first event in a batch need not
#' belong to the call you just started, and not every
#' \code{"stream_msg"} in it belongs to the stream you are reading.
#' Taking \code{events[[1]]} as the answer to a unary call, and
#' accumulating every \code{"stream_msg"} into one stream's payload,
#' are the same assumption -- one queue per call -- and it does not
#' hold. This bites hardest after a stream is abandoned unread, since
#' its queued messages keep arriving; \code{\link{grpc_cancel}} bounds
#' how many more are produced but cannot recall events already queued.
#'
#' @param x A \code{"grpc_client"} or \code{"grpc_server"} object.
#' @param max_events Maximum events to return in this batch.
#' @param timeout_ms How long to wait if no event is ready: \code{0}
#'   returns immediately, a positive value waits up to that many
#'   milliseconds, \code{-1} waits indefinitely.
#' @return A list of events (possibly empty).
#' @examples
#' \dontrun{
#' events <- grpc_poll(client, max_events = 64L, timeout_ms = 100L)
#' for (ev in events) if (ev$status_name == "OK") handle(ev$response)
#' }
#' @export
grpc_poll <- function(x, max_events = 64L, timeout_ms = 0L) {
    UseMethod("grpc_poll")
}

#' Wait for events belonging to one call
#'
#' Like \code{\link{grpc_poll}}, but scoped to a single call: only that
#' call's events are returned, and the wait ends when one of them
#' arrives rather than when anything at all does. Events for other calls
#' stay queued in arrival order and are delivered by a later
#' \code{\link{grpc_poll}} or \code{grpc_await} on their own call.
#'
#' This is the sequential-mode API, on both sides of the wire. It
#' removes the demultiplexing a shared client or server otherwise pushes
#' onto the caller: there is no way for a \code{grpc_await} loop to
#' splice another call's messages into this one's payload, or to mistake
#' another call's completion for this one's. The cost is that awaiting
#' one call means not looking at the others, so a second call's deadline
#' can pass unnoticed while this one is waited on. Drive genuinely
#' concurrent work with \code{\link{grpc_poll}} and dispatch on
#' \code{id}.
#'
#' A call ends at its terminal event, so loop until that arrives: a
#' \code{"unary"} event for \code{\link{grpc_call}}, a
#' \code{"stream_status"} for \code{\link{grpc_stream}}. On a server
#' \code{"grpc_request"} the awaited events are the ones that follow the
#' request itself -- \code{"stream_msg"}, \code{"client_done"},
#' \code{"stream_writable"}, \code{"cancelled"} -- since the request
#' arrives from \code{\link{grpc_poll}} in the first place. A server
#' call has no terminal event of its own: it ends when the handler ends
#' it with \code{\link{grpc_reply}} or \code{\link{grpc_finish}}, so
#' \code{"client_done"} is what a client-streaming handler loops to.
#'
#' Two different clocks are in play, and the words for them are not
#' self-distinguishing. \code{deadline_ms} on \code{\link{grpc_call}} or
#' \code{\link{grpc_stream}} bounds the RPC: when it expires the call
#' really is over, and the peer is told. \code{timeout_ms} here bounds
#' only this wait. Setting the wait shorter than the deadline is normal
#' and harmless; setting no deadline at all is what makes
#' \code{timeout_ms = -1} an unbounded wait. A server sees the client's
#' deadline as \code{deadline_ms} on the request event.
#'
#' An empty result means the wait ended with nothing for this call. It
#' is not a failure and not an answer: an empty await leaves the call
#' exactly as it was, so await it again to keep waiting, and the worst
#' it costs is another trip round the loop. Because an empty result is
#' possible, index the batch only after checking it --
#' \code{grpc_await(call, timeout_ms = 1000)[[1]]} raises \code{subscript
#' out of bounds} on a slow peer, which reads like a bug in the caller
#' rather than the timeout it is.
#'
#' Usually the wait ended because \code{timeout_ms} expired, but it also
#' returns early when a signal interrupts it, so an empty result is not
#' proof that \code{timeout_ms} of wall time passed. That early return is
#' deliberate — it is what lets R process a Ctrl-C instead of ignoring it
#' until the timeout runs out. It matters only if you are deriving
#' elapsed time from the number of empty awaits; use a clock for
#' deadlines, not a count.
#'
#' @param x A \code{"grpc_call"}, \code{"grpc_stream"}, or
#'   \code{"grpc_request"} object.
#' @param timeout_ms How long to wait for an event belonging to
#'   \code{x}: \code{0} returns immediately, a positive value waits up to
#'   that many milliseconds, \code{-1} waits indefinitely. Required, so
#'   that a stalled peer cannot silently become an unbounded wait; the
#'   call's own deadline is the other half of that guarantee.
#' @param max_events Maximum events to return in this batch.
#' @return A list of events for \code{x} (possibly empty), in arrival
#'   order.
#' @examples
#' \dontrun{
#' ## unary: keep waiting until the completion arrives. The call's own
#' ## deadline_ms is what guarantees this loop ends.
#' call <- grpc_call(client, "/demo.Echo/Say", req, deadline_ms = 5000)
#' repeat {
#'   evs <- grpc_await(call, timeout_ms = 1000)
#'   if (length(evs)) break                       # empty just means "not yet"
#' }
#' evs[[1]]$status_name
#'
#' ## server stream: accumulate to the terminal status
#' s <- grpc_stream(client, "/demo.Big/List", deadline_ms = 15000)
#' grpc_writes_done(s)
#' out <- list()
#' repeat {
#'   evs <- grpc_await(s, timeout_ms = 1000)
#'   for (ev in evs) if (ev$kind == "stream_msg") out <- c(out, list(ev$response))
#'   if (any(vapply(evs, function(e) e$kind == "stream_status", TRUE))) break
#' }
#'
#' ## server handler: drain one client-streaming call without seeing any
#' ## other call's messages
#' got <- list()
#' repeat {
#'   grpc_read(req)
#'   evs <- grpc_await(req, timeout_ms = 1000)
#'   for (ev in evs) if (ev$type == "stream_msg") got <- c(got, list(ev$request))
#'   if (any(vapply(evs, function(e) e$type == "client_done", TRUE))) break
#' }
#' grpc_finish(req)
#' }
#' @export
grpc_await <- function(x, timeout_ms, max_events = 64L) {
    UseMethod("grpc_await")
}

#' Completion wakeup file descriptor
#'
#' Returns the object's eventfd. It is readable exactly while events are
#' queued, so an event loop can wake on it instead of polling, e.g. with
#' \code{later::later_fd()}. Do not read from this descriptor;
#' \code{\link{grpc_poll}} drains it and leaves it readable if it
#' returned a partial batch.
#'
#' @param x A \code{"grpc_client"} or \code{"grpc_server"} object.
#' @return Integer file descriptor.
#' @examples
#' \dontrun{
#' later::later_fd(function(ready) grpc_poll(client),
#'                 readfds = grpc_fd(client))
#' }
#' @export
grpc_fd <- function(x) {
    UseMethod("grpc_fd")
}

#' Number of pending operations
#'
#' For a client, calls started but not yet delivered by
#' \code{\link{grpc_poll}}. For a server, accepted calls not yet
#' completed.
#'
#' @param x A \code{"grpc_client"} or \code{"grpc_server"} object.
#' @return Integer count.
#' @examples
#' \dontrun{while (grpc_pending(client) > 0) grpc_poll(client, timeout_ms = 100L)}
#' @export
grpc_pending <- function(x) {
    UseMethod("grpc_pending")
}

#' Send a message on a stream
#'
#' Enqueues one outbound message on a bounded write queue. For a client
#' \code{"grpc_stream"}, this is the request direction; for a server
#' \code{"grpc_request"}, the response direction. Returns (invisibly)
#' \code{TRUE} if the message was queued, or \code{FALSE} if the queue
#' is full (backpressure: wait for the \code{"stream_writable"} event
#' and retry) or the stream can no longer accept writes.
#'
#' @param x A \code{"grpc_stream"} (client) or \code{"grpc_request"}
#'   (server) object.
#' @param msg Raw vector, or an \code{RProtoBuf} \code{Message} to
#'   serialize (validated against the method's \code{input_type} on a
#'   typed client stream).
#' @param ... Reserved.
#' @examples
#' \dontrun{while (!grpc_send(s, msg)) grpc_poll(client, timeout_ms = 100L)}
#' @export
grpc_send <- function(x, msg, ...) {
    UseMethod("grpc_send")
}

#' Cancel an in-flight call or stream
#'
#' Requests cancellation. The call or stream still completes through
#' \code{\link{grpc_poll}}, normally with status \code{CANCELLED}.
#' Cancelling something already completed is a no-op. On a server, a
#' \code{"grpc_request"} can be cancelled as the hard escalation when
#' even an abortive \code{\link{grpc_finish}} cannot get its status
#' past a peer that has stopped reading; the peer sees \code{CANCELLED}.
#'
#' For a \code{"grpc_request"} the (invisible) return is \code{TRUE} if
#' cancellation was requested on a live call and \code{FALSE} if the
#' call was already terminal. \code{FALSE} guarantees no further
#' messages can be delivered on the stream; it is \emph{not} a receipt
#' that any terminal status reached the peer.
#'
#' @param x A \code{"grpc_call"}, \code{"grpc_stream"}, or
#'   \code{"grpc_request"} object.
#' @examples
#' \dontrun{grpc_cancel(call)}
#' @export
grpc_cancel <- function(x) {
    UseMethod("grpc_cancel")
}

#' Shut down a client or server
#'
#' Cancels outstanding work, shuts down the completion queue, joins the
#' completion thread, and releases the transport. Undelivered events are
#' discarded. Closing twice is a no-op; the finalizer performs the same
#' shutdown if the object is garbage collected unclosed.
#'
#' @param x A \code{"grpc_client"} or \code{"grpc_server"} object.
#' @examples
#' \dontrun{grpc_close(client)}
#' @export
grpc_close <- function(x) {
    UseMethod("grpc_close")
}
