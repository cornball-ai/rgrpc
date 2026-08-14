% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_close}
\alias{grpc_close}
\title{Close a client}
\usage{
grpc_close(client)
}
\arguments{
\item{client}{A \code{"grpc_client"} object.}
}
\description{
Cancels in-flight calls, shuts down the completion queue, joins the
completion thread, and releases the channel. Undelivered completions
are discarded. Closing twice is a no-op; the finalizer performs the
same shutdown if a client is garbage collected unclosed.
}
\examples{
\dontrun{grpc_close(client)}
}
