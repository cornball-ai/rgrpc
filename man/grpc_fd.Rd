% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_fd}
\alias{grpc_fd}
\title{Completion wakeup file descriptor}
\usage{
grpc_fd(client)
}
\arguments{
\item{client}{A \code{"grpc_client"} object.}
}
\value{
Integer file descriptor.
}
\description{
Returns the client's eventfd. It becomes readable whenever a
completion is queued, so an event loop can wake on it instead of
polling, e.g. \code{later::later_fd(function(...) grpc_poll(client),
readfds = grpc_fd(client))}. Do not read from this descriptor;
\code{\link{grpc_poll}} drains it.
}
\examples{
\dontrun{
later::later_fd(function(ready) grpc_poll(client),
                readfds = grpc_fd(client))
}
}
