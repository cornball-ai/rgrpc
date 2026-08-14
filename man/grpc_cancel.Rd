% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_cancel}
\alias{grpc_cancel}
\title{Cancel an in-flight call}
\usage{
grpc_cancel(call)
}
\arguments{
\item{call}{A \code{"grpc_call"} object.}
}
\description{
Requests cancellation. The call still completes through
\code{\link{grpc_poll}}, normally with status \code{CANCELLED}.
Cancelling a call that already completed is a no-op.
}
\examples{
\dontrun{grpc_cancel(call)}
}
