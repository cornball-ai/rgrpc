% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_finish}
\alias{grpc_finish}
\title{End a server stream}
\usage{
grpc_finish(request, status = 0L, message = "", metadata = NULL, drain = TRUE)
}
\arguments{
\item{request}{A \code{"grpc_request"} event from
\code{\link{grpc_poll}}.}

\item{status}{Integer status code or name from
\code{\link{grpc_status_codes}}.}

\item{message}{Optional error detail string for non-\code{OK} status.}

\item{metadata}{Optional named character vector sent as trailing
metadata.}

\item{drain}{If \code{TRUE} (default), queued messages are delivered
before the status; if \code{FALSE}, they are discarded and the
status is prioritized.}
}
\value{
Invisibly, a logical scalar: \code{TRUE} if the status was
  accepted for sending, \code{FALSE} if the call is no longer
  answerable.
}
\description{
Sends the terminal status for a streaming call after any messages
queued with \code{\link{grpc_send}} have drained. For unary replies
use \code{\link{grpc_reply}}, which sends a payload and the status in
one step. Returns (invisibly) \code{TRUE} if accepted, \code{FALSE}
if the call is no longer answerable.
}
\details{
With \code{drain = FALSE} the close is abortive: queued messages are
discarded and the terminal status goes out first. This is for
fencing — e.g. \code{ABORTED} on session replacement — where
delivering queued-but-stale messages to the peer would be wrong and
waiting behind them (potentially forever, if the peer has stopped
reading) delays the fence. One already-posted message cannot be
recalled, so the status can still wait for that single in-flight
write; if the peer's flow-control window is exhausted even that may
not complete, and \code{\link{grpc_cancel}} on the request is the
hard escalation (the peer then sees \code{CANCELLED} rather than
this status).

}
\examples{
srv <- grpc_server("127.0.0.1:0")
cl <- grpc_client(sprintf("127.0.0.1:\%d", grpc_server_port(srv)))

## server streaming: the client sends one request and half-closes
s <- grpc_stream(cl, "/demo.Echo/Watch", deadline_ms = 5000)
grpc_send(s, as.raw(7))
grpc_writes_done(s)

## server: three messages, then the status with trailing metadata
evs <- grpc_poll(srv, timeout_ms = 5000L)
req <- Filter(function(e) e$type == "request", evs)[[1]]
for (i in 1:3) grpc_send(req, as.raw(i))
grpc_finish(req, metadata = c("x-count" = "3"))

out <- list()
repeat {
  evs <- grpc_await(s, timeout_ms = 1000L)
  for (ev in evs) if (ev$kind == "stream_msg") out <- c(out, list(ev$response))
  st <- Filter(function(e) e$kind == "stream_status", evs)
  if (length(st)) break
}
length(out)
st[[1]]$status_name
st[[1]]$trailing_metadata

## an error status ends a stream without any payload
s <- grpc_stream(cl, "/demo.Echo/Watch", deadline_ms = 5000)
grpc_send(s, raw(1))
evs <- grpc_poll(srv, timeout_ms = 5000L)
req <- Filter(function(e) e$type == "request", evs)[[1]]
grpc_finish(req, status = "NOT_FOUND", message = "no such stream")
repeat {
  evs <- grpc_await(s, timeout_ms = 1000L)
  st <- Filter(function(e) e$kind == "stream_status", evs)
  if (length(st)) break
}
st[[1]]$status_name
st[[1]]$message

grpc_close(cl)
grpc_close(srv)
}
