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
\description{
Posts one read on a client- or bidirectionally-streaming call. The
result arrives through \code{\link{grpc_poll}} as a
\code{"stream_msg"} event (with \code{request} bytes), or
\code{"client_done"} when the peer has half-closed. One read at a
time: returns (invisibly) \code{FALSE} if a read is already in flight
or the call is over.
}
\examples{
\dontrun{grpc_read(ev)}
}
