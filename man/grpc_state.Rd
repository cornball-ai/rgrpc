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
A character scalar, one of \code{"IDLE"}, \code{"CONNECTING"},
  \code{"READY"}, \code{"TRANSIENT_FAILURE"}, \code{"SHUTDOWN"}: the
  channel's connectivity state at the moment of the call.
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
srv <- grpc_server("127.0.0.1:0")
cl <- grpc_client(sprintf("127.0.0.1:\%d", grpc_server_port(srv)))
grpc_state(cl)                          # "IDLE": channels connect on first use

## the next request event; the server has one queue for every call, so
## other events are stepped over, and 5 s of silence is an error
next_request <- function(srv) {
  repeat {
    evs <- grpc_poll(srv, timeout_ms = 5000L)
    if (!length(evs)) stop("no request within 5 s")
    for (ev in evs) if (ev$type == "request") return(ev)
  }
}

call <- grpc_call(cl, "/demo.Echo/Say", raw(0), deadline_ms = 5000)
req <- next_request(srv)
grpc_reply(req, raw(0))
repeat {
  evs <- grpc_await(call, timeout_ms = 1000L)
  if (length(evs)) break
}
grpc_state(cl)                          # "READY" once a call has gone through

grpc_close(cl)
grpc_close(srv)
}
