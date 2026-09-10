% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_close}
\alias{grpc_close}
\title{Shut down a client or server}
\usage{
grpc_close(x)
}
\arguments{
\item{x}{A \code{"grpc_client"} or \code{"grpc_server"} object.}
}
\value{
No return value (\code{NULL}, invisibly); called for its side
  effect of shutting the object down.
}
\description{
Cancels outstanding work, shuts down the completion queue, joins the
completion thread, and releases the transport. Undelivered events are
discarded. Closing twice is a no-op; the finalizer performs the same
shutdown if the object is garbage collected unclosed.
}
\examples{
srv <- grpc_server("127.0.0.1:0")
cl <- grpc_client(sprintf("127.0.0.1:\%d", grpc_server_port(srv)))
grpc_close(cl)
grpc_close(srv)
grpc_close(srv)                         # closing twice is a no-op
}
