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
\description{
Cancels outstanding work, shuts down the completion queue, joins the
completion thread, and releases the transport. Undelivered events are
discarded. Closing twice is a no-op; the finalizer performs the same
shutdown if the object is garbage collected unclosed.
}
\examples{
\dontrun{grpc_close(client)}
}
