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
and signals a wake descriptor. The background thread never calls the
R API;
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
\section{Forking}{
A client must not be used across a \code{fork()}. The background
completion thread does not survive forking, so in the child there is
nothing left to drain the completion queue: calls are posted and
never complete. Worse, using an inherited client in the child also
breaks it in the \emph{parent} — the parent's subsequent calls fail
with \code{DEADLINE_EXCEEDED}. Setting
\code{GRPC_ENABLE_FORK_SUPPORT=1} does not change any of this.

This matters because \code{parallel::mclapply} and much of the mirai
ecosystem fork. Measured behaviour (\code{tools/fork-probe.sh}):

\itemize{
  \item Child uses the parent's client: the call never completes. A
    wait with a timeout returns empty; a wait without one does not
    return. Set \code{deadline_ms} and you get
    \code{DEADLINE_EXCEEDED} instead of a hang.
  \item Parent afterwards: broken, \code{DEADLINE_EXCEEDED}.
  \item Forking alone, with the child never touching the client:
    harmless, the parent keeps working.
  \item Child opens its \emph{own} client after the fork: works
    normally.
}

So the rule is to open the client after forking, once per process,
and never to let a forked worker inherit one. Fork safety is a
non-goal; this is documented behaviour, not a defect to be fixed.

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
