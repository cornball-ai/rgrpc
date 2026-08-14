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
\description{
Signals that no further messages will be sent. Queued messages are
flushed first. Returns (invisibly) \code{FALSE} if already
half-closed.
}
\examples{
\dontrun{grpc_writes_done(s)}
}
