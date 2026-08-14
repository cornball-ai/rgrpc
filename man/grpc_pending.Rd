% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_pending}
\alias{grpc_pending}
\title{Number of in-flight calls}
\usage{
grpc_pending(client)
}
\arguments{
\item{client}{A \code{"grpc_client"} object.}
}
\value{
Integer count of calls started but not yet delivered by
  \code{\link{grpc_poll}}.
}
\description{
Number of in-flight calls
}
\examples{
\dontrun{while (grpc_pending(client) > 0) grpc_poll(client, timeout_ms = 100L)}
}
