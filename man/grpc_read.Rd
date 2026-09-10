% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_read}
\alias{grpc_read}
\title{Pull the next inbound message on a server stream}
\usage{
grpc_read(request)
}
\arguments{
\item{request}{A \code{"grpc_request"} event from
\code{\link{grpc_poll}}.}
}
\value{
Invisibly, a logical scalar: \code{TRUE} if a read was posted,
  \code{FALSE} if one is already in flight or the call is over.
}
\description{
Posts one read on a client- or bidirectionally-streaming call. The
result arrives through \code{\link{grpc_poll}} as a
\code{"stream_msg"} event (with \code{request} bytes), or
\code{"client_done"} when the peer has half-closed. One read at a
time: returns (invisibly) \code{FALSE} if a read is already in flight
or the call is over.
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

s <- grpc_stream(cl, "/demo.Echo/Collect", deadline_ms = 5000)
for (i in 1:4) grpc_send(s, as.raw(i))
grpc_writes_done(s)

req <- next_request(srv)
got <- as.integer(req$request)          # the first message rides on the request
(grpc_read(req))                        # TRUE: one read is now in flight
(grpc_read(req))                        # FALSE: one read at a time
## each read yields one "stream_msg", or "client_done" at the end
repeat {
  evs <- grpc_await(req, timeout_ms = 1000L)
  for (ev in evs) if (ev$type == "stream_msg") got <- c(got, as.integer(ev$request))
  if (length(Filter(function(e) e$type \%in\% c("client_done", "cancelled"), evs))) break
  grpc_read(req)                        # post the next read
}
got
grpc_reply(req, as.raw(length(got)))

grpc_close(cl)
grpc_close(srv)
}
