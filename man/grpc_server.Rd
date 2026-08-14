% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_server}
\alias{grpc_server}
\title{Create an asynchronous generic gRPC server}
\usage{
grpc_server(address = "127.0.0.1:0", accept_window = 8L, max_active = 256L)
}
\arguments{
\item{address}{Bind address, e.g. \code{"127.0.0.1:0"} for an
ephemeral TCP port (see \code{\link{grpc_server_port}}) or
\code{"unix:/path/to.sock"} for a unix-domain socket.}

\item{accept_window}{Outstanding accept slots (burst capacity).}

\item{max_active}{Bound on concurrently active calls.}
}
\value{
An object of class \code{"grpc_server"}.
}
\description{
Binds \code{address}, registers the generic asynchronous service (any
method name is accepted; there are no generated stubs), and starts the
server's completion machinery: a background thread that drains the
completion queue and signals an eventfd. Incoming requests are
received on the R main thread via \code{\link{grpc_poll}} and answered
with \code{\link{grpc_reply}}.
}
\details{
Backpressure: at most \code{accept_window} accept slots are kept
outstanding, and no new slot is posted while active calls plus
outstanding slots would exceed \code{max_active}. Excess incoming
calls queue in the transport until capacity frees up.

}
\examples{
\dontrun{
srv <- grpc_server("127.0.0.1:0")
events <- grpc_poll(srv, timeout_ms = 1000L)
for (ev in events) {
  if (ev$type == "request") grpc_reply(ev, handle(ev$request))
}
grpc_close(srv)
}
}
