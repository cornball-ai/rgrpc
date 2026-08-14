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
\description{
Enqueues one outbound message on a bounded write queue. For a client
\code{"grpc_stream"}, this is the request direction; for a server
\code{"grpc_request"}, the response direction. Returns (invisibly)
\code{TRUE} if the message was queued, or \code{FALSE} if the queue
is full (backpressure: wait for the \code{"stream_writable"} event
and retry) or the stream can no longer accept writes.
}
\examples{
\dontrun{while (!grpc_send(s, msg)) grpc_poll(client, timeout_ms = 100L)}
}
