% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_stream}
\alias{grpc_stream}
\title{Open a streaming call}
\usage{
grpc_stream(
  client,
  method,
  deadline_ms = NULL,
  metadata = NULL,
  wait_for_ready = FALSE,
  read_buffer = 16L,
  write_buffer = 16L
)
}
\arguments{
\item{client}{A \code{"grpc_client"} object.}

\item{method}{Full method path, or a \code{"grpc_method"} object for
a typed stream (any streaming shape).}

\item{deadline_ms}{Optional deadline in milliseconds for the whole
stream.}

\item{metadata}{Optional named character vector of request metadata.}

\item{wait_for_ready}{If \code{TRUE}, wait for the channel to connect
instead of failing fast.}

\item{read_buffer}{Bound on undelivered inbound messages.}

\item{write_buffer}{Bound on queued outbound messages.}
}
\value{
An object of class \code{"grpc_stream"}: a list with the
  \code{client}, the stream \code{id} that its events will carry, and
  the \code{method} path. Send on it with \code{\link{grpc_send}},
  half-close with \code{\link{grpc_writes_done}}, and receive with
  \code{\link{grpc_await}} or \code{\link{grpc_poll}}.
}
\description{
Opens a client-, server-, or bidirectionally-streaming RPC. Messages
are sent with \code{\link{grpc_send}}, the request direction is
half-closed with \code{\link{grpc_writes_done}}, and everything
inbound arrives through \code{\link{grpc_poll}} on the client:
\code{"stream_msg"} events per message (with \code{response} bytes,
plus \code{response_message} decoded on a typed stream),
\code{"stream_writable"} when the send queue drains, and a final
\code{"stream_status"} with status and trailing metadata.
}
\details{
Inbound flow control is automatic and bounded: at most
\code{read_buffer} undelivered messages are held; beyond that the
stream stops reading until \code{\link{grpc_poll}} drains, and HTTP/2
backpressure propagates to the peer.

A stream runs until its \code{"stream_status"}, not until the caller
loses interest: dropping the returned object stops nothing, and the
stream's queued messages keep surfacing in \code{\link{grpc_poll}}
alongside later calls on the same client. Read every stream to its
status, or \code{\link{grpc_cancel}} the ones you are done with, and
dispatch events on \code{id} either way.

}
\examples{
srv <- grpc_server("127.0.0.1:0")
cl <- grpc_client(sprintf("127.0.0.1:\%d", grpc_server_port(srv)))

## client: three messages out, then half-close the request direction
s <- grpc_stream(cl, "/demo.Echo/Chat", deadline_ms = 5000)
for (i in 1:3) grpc_send(s, as.raw(i))
grpc_writes_done(s)

## server: the first message rides on the request event; read the rest,
## then echo each one back doubled and end the stream
evs <- grpc_poll(srv, timeout_ms = 5000L)
req <- Filter(function(e) e$type == "request", evs)[[1]]
got <- list(req$request)
repeat {
  grpc_read(req)
  evs <- grpc_await(req, timeout_ms = 1000L)
  for (ev in evs) if (ev$type == "stream_msg") got <- c(got, list(ev$request))
  if (length(Filter(function(e) e$type \%in\% c("client_done", "cancelled"), evs))) break
}
for (m in got) grpc_send(req, as.raw(as.integer(m) * 2L))
grpc_finish(req)

## client: responses arrive as "stream_msg" events up to the "stream_status"
out <- list()
repeat {
  evs <- grpc_await(s, timeout_ms = 1000L)
  for (ev in evs) if (ev$kind == "stream_msg") out <- c(out, list(ev$response))
  st <- Filter(function(e) e$kind == "stream_status", evs)
  if (length(st)) break
}
unlist(out)
st[[1]]$status_name

grpc_close(cl)
grpc_close(srv)
}
