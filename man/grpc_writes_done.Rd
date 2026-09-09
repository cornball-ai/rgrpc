% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_writes_done}
\alias{grpc_writes_done}
\title{Half-close a client stream}
\usage{
grpc_writes_done(stream)
}
\arguments{
\item{stream}{A \code{"grpc_stream"} object.}
}
\value{
Invisibly, a logical scalar: \code{TRUE} if the half-close was
  posted, \code{FALSE} if the stream was already half-closed or is
  over.
}
\description{
Signals that no further messages will be sent. Queued messages are
flushed first. Returns (invisibly) \code{FALSE} if already
half-closed.
}
\examples{
srv <- grpc_server("127.0.0.1:0")
cl <- grpc_client(sprintf("127.0.0.1:\%d", grpc_server_port(srv)))
s <- grpc_stream(cl, "/demo.Echo/Collect", deadline_ms = 5000)
grpc_send(s, as.raw(1))
(grpc_writes_done(s))                   # TRUE: request direction closed
(grpc_writes_done(s))                   # FALSE: already half-closed
(grpc_send(s, as.raw(2)))               # FALSE: no writes after the half-close

## the server reads past the first message and sees "client_done"
evs <- grpc_poll(srv, timeout_ms = 5000L)
req <- Filter(function(e) e$type == "request", evs)[[1]]
grpc_read(req)
repeat {
  evs <- grpc_await(req, timeout_ms = 1000L)
  if (length(Filter(function(e) e$type \%in\% c("client_done", "cancelled"), evs))) break
}
evs[[1]]$type
grpc_reply(req, raw(0))

grpc_close(cl)
grpc_close(srv)
}
