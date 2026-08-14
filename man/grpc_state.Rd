% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_state}
\alias{grpc_state}
\title{Channel connectivity state}
\usage{
grpc_state(client)
}
\arguments{
\item{client}{A \code{"grpc_client"} object.}
}
\value{
One of \code{"IDLE"}, \code{"CONNECTING"}, \code{"READY"},
  \code{"TRANSIENT_FAILURE"}, \code{"SHUTDOWN"}.
}
\description{
Observes (without provoking a connection attempt) the client
channel's connectivity state.
}
\details{
For deeper transport diagnostics, gRPC's built-in tracing applies to
this package unchanged: set the \code{GRPC_TRACE} and
\code{GRPC_VERBOSITY} environment variables before the package loads,
e.g. \code{GRPC_TRACE=http,connectivity_state GRPC_VERBOSITY=debug}.

}
\examples{
\dontrun{grpc_state(cl)}
}
