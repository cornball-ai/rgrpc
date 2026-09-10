% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_send}
\alias{grpc_send}
\title{Send a message on a stream}
\usage{
grpc_send(x, msg, ...)
}
\arguments{
\item{x}{A \code{"grpc_stream"} (client) or \code{"grpc_request"}
(server) object.}

\item{msg}{Raw vector, or an \code{RProtoBuf} \code{Message} to
serialize (validated against the method's \code{input_type} on a
typed client stream).}

\item{...}{Reserved.}
}
\value{
Invisibly, a logical scalar: \code{TRUE} if the message was
  queued for sending, \code{FALSE} if the write queue is full or the
  stream no longer accepts writes (half-closed, finished, cancelled,
  or past its deadline).
}
\description{
Enqueues one outbound message on a bounded write queue. For a client
\code{"grpc_stream"}, this is the request direction; for a server
\code{"grpc_request"}, the response direction. Returns (invisibly)
\code{TRUE} if the message was queued, or \code{FALSE} if the queue
is full (backpressure: wait for the \code{"stream_writable"} event
and retry) or the stream can no longer accept writes.
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

## client streaming: queue five messages, then half-close
s <- grpc_stream(cl, "/demo.Echo/Collect", deadline_ms = 5000)
for (i in 1:5) grpc_send(s, as.raw(i))
grpc_writes_done(s)
(grpc_send(s, as.raw(6)))               # FALSE: no writes after the half-close

## server: count what arrives (the first message rides on the request
## event), then answer once, unary-style
req <- next_request(srv)
n <- 1L
repeat {
  grpc_read(req)
  evs <- grpc_await(req, timeout_ms = 1000L)
  n <- n + length(Filter(function(e) e$type == "stream_msg", evs))
  if (length(Filter(function(e) e$type \%in\% c("client_done", "cancelled"), evs))) break
}
grpc_reply(req, as.raw(n))

## client: the reply is one "stream_msg", followed by the "stream_status"
reply <- NULL
repeat {
  evs <- grpc_await(s, timeout_ms = 1000L)
  for (ev in evs) if (ev$kind == "stream_msg") reply <- ev$response
  if (length(Filter(function(e) e$kind == "stream_status", evs))) break
}
as.integer(reply)

## a full write queue makes grpc_send() return FALSE; wait for the
## "stream_writable" event (grpc_await() on the stream) and retry
grpc_close(cl)
grpc_close(srv)
}
