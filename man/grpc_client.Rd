% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_client}
\alias{grpc_client}
\title{Create an asynchronous gRPC client}
\usage{
grpc_client(
  target,
  credentials = NULL,
  keepalive_ms = NULL,
  keepalive_timeout_ms = NULL
)
}
\arguments{
\item{target}{Server address, e.g. \code{"localhost:50051"} or
\code{"unix:///run/containerd/containerd.sock"}.}

\item{credentials}{\code{NULL} for a plaintext channel, or a
\code{\link{grpc_tls}} object.}

\item{keepalive_ms}{Interval of transport inactivity after which an
HTTP/2 keepalive ping is sent. \code{NULL} (default) disables
keepalive.}

\item{keepalive_timeout_ms}{Time to wait for a ping answer before
the connection is declared dead.}
}
\value{
An object of class \code{"grpc_client"}.
}
\description{
Opens a channel to \code{target} and starts the client's completion
machinery: a background thread that drains the gRPC completion queue
and signals an eventfd. The background thread never calls the R API;
completions are received on the R main thread via \code{\link{grpc_poll}}.
}
\details{
Keepalive: with \code{keepalive_ms} set, the client pings the peer
every \code{keepalive_ms} of transport inactivity and declares the
connection dead \code{keepalive_timeout_ms} after an unanswered ping
(gRPC's default timeout is 20000). Pings are enabled without payload
data and without active calls, so a quiet connection is genuinely
watched. The server must tolerate the cadence: see
\code{min_ping_interval_ms} in \code{\link{grpc_server}} — gRPC's
server default kills clients that ping more often than every 5
minutes.

}
\examples{
\dontrun{
client <- grpc_client("localhost:50051")
call <- grpc_call(client, "/demo.Echo/Say", serialize(msg, NULL),
                  deadline_ms = 1000)
events <- grpc_poll(client, timeout_ms = 1000L)
grpc_close(client)
}
}
