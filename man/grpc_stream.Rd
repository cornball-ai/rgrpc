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
An object of class \code{"grpc_stream"} holding the stream id.
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

}
\examples{
\dontrun{
s <- grpc_stream(client, grpc_method(rt, "GetContainerEvents"),
                 RProtoBuf::P("runtime.v1.GetEventsRequest")$new())
}
}
