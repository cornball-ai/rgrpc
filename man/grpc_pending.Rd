% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_pending}
\alias{grpc_pending}
\title{Number of pending operations}
\usage{
grpc_pending(x)
}
\arguments{
\item{x}{A \code{"grpc_client"} or \code{"grpc_server"} object.}
}
\value{
Integer count.
}
\description{
For a client, calls started but not yet delivered by
\code{\link{grpc_poll}}. For a server, accepted calls not yet
completed.
}
\examples{
\dontrun{while (grpc_pending(client) > 0) grpc_poll(client, timeout_ms = 100L)}
}
