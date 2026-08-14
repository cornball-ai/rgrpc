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

#' Completion wakeup file descriptor
#'
#' Returns the object's eventfd. It becomes readable whenever an event is
#' queued, so an event loop can wake on it instead of polling, e.g. with
#' \code{later::later_fd()}. Do not read from this descriptor;
#' \code{\link{grpc_poll}} drains it.
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
