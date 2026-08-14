% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_cancel}
\alias{grpc_cancel}
\title{Cancel an in-flight call or stream}
\usage{
grpc_cancel(x)
}
\arguments{
\item{x}{A \code{"grpc_call"} or \code{"grpc_stream"} object.}
}
\description{
Requests cancellation. The call or stream still completes through
\code{\link{grpc_poll}}, normally with status \code{CANCELLED}.
Cancelling something already completed is a no-op.
}
\examples{
\dontrun{grpc_cancel(call)}
}
