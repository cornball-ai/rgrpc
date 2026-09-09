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
Integer scalar: the number of in-flight operations, zero when
  nothing is outstanding.
}
\description{
For a client, calls started but not yet delivered by
\code{\link{grpc_poll}}. For a server, accepted calls not yet
completed.
}
\examples{
srv <- grpc_server("127.0.0.1:0")
cl <- grpc_client(sprintf("127.0.0.1:\%d", grpc_server_port(srv)))
grpc_pending(cl)                        # 0
call <- grpc_call(cl, "/demo.Echo/Say", raw(0), deadline_ms = 5000)
grpc_pending(cl)                        # 1: started, not yet completed

evs <- grpc_poll(srv, timeout_ms = 5000L)
req <- Filter(function(e) e$type == "request", evs)[[1]]
grpc_pending(srv)                       # 1: accepted, not yet answered
grpc_reply(req, raw(0))
repeat {
  evs <- grpc_await(call, timeout_ms = 1000L)
  if (length(evs)) break
}
grpc_pending(cl)                        # 0 again

grpc_close(cl)
grpc_close(srv)
}
