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
\description{
Requests cancellation. The call or stream still completes through
\code{\link{grpc_poll}}, normally with status \code{CANCELLED}.
Cancelling something already completed is a no-op. On a server, a
\code{"grpc_request"} can be cancelled as the hard escalation when
even an abortive \code{\link{grpc_finish}} cannot get its status
past a peer that has stopped reading; the peer sees \code{CANCELLED}.
}
\examples{
\dontrun{grpc_cancel(call)}
}
