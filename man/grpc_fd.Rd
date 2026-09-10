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
Integer scalar: the wake descriptor, valid until the object is
  closed.
}
\description{
Returns the object's wake descriptor: a pipe file descriptor on Unix,
a socket descriptor on Windows. It is readable exactly while events
are queued, so an event loop can wake on it instead of polling, e.g.
with \code{later::later_fd()}. Do not read from this descriptor;
\code{\link{grpc_poll}} drains it and leaves it readable if it
returned a partial batch.
}
\examples{
srv <- grpc_server("127.0.0.1:0")
cl <- grpc_client(sprintf("127.0.0.1:\%d", grpc_server_port(srv)))
grpc_fd(cl)
grpc_fd(srv)
## hand the descriptor to an event loop instead of polling, e.g.
##   later::later_fd(function(ready) grpc_poll(cl), readfds = grpc_fd(cl))
grpc_close(cl)
grpc_close(srv)
}
