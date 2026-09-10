% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_cancel}
\alias{grpc_cancel}
\title{Cancel an in-flight call or stream}
\usage{
grpc_cancel(x)
}
\arguments{
\item{x}{A \code{"grpc_call"}, \code{"grpc_stream"}, or
\code{"grpc_request"} object.}
}
\value{
Invisibly. For a client \code{"grpc_call"} or
  \code{"grpc_stream"}, \code{NULL}: the function is called for its
  side effect, and the outcome arrives as the call's \code{CANCELLED}
  completion. For a server \code{"grpc_request"}, a logical scalar:
  \code{TRUE} if cancellation was requested on a live call,
  \code{FALSE} if the call was already terminal.
}
\description{
Requests cancellation. The call or stream still completes through
\code{\link{grpc_poll}}, normally with status \code{CANCELLED}.
Cancelling something already completed is a no-op. On a server, a
\code{"grpc_request"} can be cancelled as the hard escalation when
even an abortive \code{\link{grpc_finish}} cannot get its status
past a peer that has stopped reading; the peer sees \code{CANCELLED}.
}
\details{
For a \code{"grpc_request"} the (invisible) return is \code{TRUE} if
cancellation was requested on a live call and \code{FALSE} if the
call was already terminal. \code{FALSE} guarantees no further
messages can be delivered on the stream; it is \emph{not} a receipt
that any terminal status reached the peer.

}
\examples{
srv <- grpc_server("127.0.0.1:0")
cl <- grpc_client(sprintf("127.0.0.1:\%d", grpc_server_port(srv)))

## the next request event; the server has one queue for every call, so
## other events are stepped over, and 5 s of silence is an error
next_request <- function(srv) {
  repeat {
    evs <- grpc_poll(srv, timeout_ms = 5000L)
    if (!length(evs)) stop("no request within 5 s")
    for (ev in evs) if (ev$type == "request") return(ev)
  }
}

## a call the server holds without answering: cancel it instead of
## waiting for its deadline
call <- grpc_call(cl, "/demo.Echo/Say", raw(0), deadline_ms = 60000)
req <- next_request(srv)
grpc_cancel(call)
repeat {
  evs <- grpc_await(call, timeout_ms = 1000L)
  if (length(evs)) break
}
evs[[1]]$status_name

## the server hears of it as a "cancelled" event, and the request can no
## longer be answered
repeat {
  evs <- grpc_await(req, timeout_ms = 1000L)
  if (length(Filter(function(e) e$type == "cancelled", evs))) break
}
(grpc_reply(req, raw(0)))               # FALSE
(grpc_cancel(req))                      # FALSE: already terminal

grpc_close(cl)
grpc_close(srv)
}
