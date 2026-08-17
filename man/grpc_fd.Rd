% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_fd}
\alias{grpc_fd}
\title{Completion wakeup file descriptor}
\usage{
grpc_fd(x)
}
\arguments{
\item{x}{A \code{"grpc_client"} or \code{"grpc_server"} object.}
}
\value{
Integer file descriptor.
}
\description{
Returns the object's eventfd. It is readable exactly while events are
queued, so an event loop can wake on it instead of polling, e.g. with
\code{later::later_fd()}. Do not read from this descriptor;
\code{\link{grpc_poll}} drains it and leaves it readable if it
returned a partial batch.
}
\examples{
\dontrun{
later::later_fd(function(ready) grpc_poll(client),
                readfds = grpc_fd(client))
}
}
