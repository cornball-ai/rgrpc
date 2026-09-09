% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_server}
\alias{grpc_server}
\title{Create an asynchronous generic gRPC server}
\usage{
grpc_server(
  address = "127.0.0.1:0",
  credentials = NULL,
  accept_window = 8L,
  max_active = 256L,
  keepalive_ms = NULL,
  keepalive_timeout_ms = NULL,
  min_ping_interval_ms = NULL
)
}
\arguments{
\item{address}{Bind address, e.g. \code{"127.0.0.1:0"} for an
ephemeral TCP port (see \code{\link{grpc_server_port}}) or
\code{"unix:/path/to.sock"} for a unix-domain socket.}

\item{credentials}{\code{NULL} for a plaintext listener, or a
\code{\link{grpc_tls}} object (needs \code{cert_file} and
\code{key_file}; \code{require_client_cert = TRUE} for mTLS).}

\item{accept_window}{Outstanding accept slots (burst capacity).}

\item{max_active}{Bound on concurrently active calls.}

\item{keepalive_ms}{Interval of transport inactivity after which the
server pings a client. \code{NULL} (default) disables keepalive.}

\item{keepalive_timeout_ms}{Time to wait for a ping answer before the
connection is declared dead.}

\item{min_ping_interval_ms}{Minimum interval between client pings the
server tolerates without counting a ping strike.}
}
\value{
An object of class \code{"grpc_server"}: a list holding the
  native server handle (\code{ptr}) and the bind \code{address}. Pass
  it to \code{\link{grpc_poll}}, \code{\link{grpc_server_port}}, and
  \code{\link{grpc_close}}.
}
\description{
Binds \code{address}, registers the generic asynchronous service (any
method name is accepted; there are no generated stubs), and starts the
server's completion machinery: a background thread that drains the
completion queue and signals a wake descriptor. Incoming requests are
received on the R main thread via \code{\link{grpc_poll}} and answered
with \code{\link{grpc_reply}}.
}
\details{
Backpressure: at most \code{accept_window} accept slots are kept
outstanding, and no new slot is posted while active calls plus
outstanding slots would exceed \code{max_active}. Excess incoming
calls queue in the transport until capacity frees up.

Request events carry the transport \code{peer} address and, on a TLS
listener with \code{require_client_cert}, the verified
\code{peer_identity} (the client certificate's identity values).

Keepalive: \code{keepalive_ms}/\code{keepalive_timeout_ms} make the
server ping quiet clients, mirroring \code{\link{grpc_client}}.
\code{min_ping_interval_ms} is the tolerance for \emph{client} pings:
gRPC's server default allows one unsolicited ping per 5 minutes and
kills faster clients with a \code{too_many_pings} GOAWAY, so a
deployment where clients keep 10-second heartbeats must lower this to
at most the client ping interval.

}
\section{Forking}{
A server must not be used across a \code{fork()}, for the same reason
a client must not: the completion thread does not survive forking.
See the Forking section of \code{\link{grpc_client}}, which documents
the measured behaviour. Create the server in the process that will
serve it.

}
\examples{
srv <- grpc_server("127.0.0.1:0")
grpc_server_port(srv)

## a client in the same process makes one call
cl <- grpc_client(sprintf("127.0.0.1:\%d", grpc_server_port(srv)))
call <- grpc_call(cl, "/demo.Echo/Say", charToRaw("hello"), deadline_ms = 5000)

## the server sees it as a request event; there are no handlers to
## register, the method name arrives with the event
evs <- grpc_poll(srv, timeout_ms = 5000L)           # waits for the request
req <- Filter(function(e) e$type == "request", evs)[[1]]
req$method
grpc_reply(req, req$request)                        # echo it back

## the client collects the completion
repeat {
  evs <- grpc_await(call, timeout_ms = 1000L)
  if (length(evs)) break                            # empty means "not yet"
}
evs[[1]]$status_name
rawToChar(evs[[1]]$response)

grpc_close(cl)
grpc_close(srv)
}
