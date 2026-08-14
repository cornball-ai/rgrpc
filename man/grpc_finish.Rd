% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_finish}
\alias{grpc_finish}
\title{End a server stream}
\usage{
grpc_finish(request, status = 0L, message = "", metadata = NULL, drain = TRUE)
}
\arguments{
\item{request}{A \code{"grpc_request"} event from
\code{\link{grpc_poll}}.}

\item{status}{Integer status code or name from
\code{\link{grpc_status_codes}}.}

\item{message}{Optional error detail string for non-\code{OK} status.}

\item{metadata}{Optional named character vector sent as trailing
metadata.}

\item{drain}{If \code{TRUE} (default), queued messages are delivered
before the status; if \code{FALSE}, they are discarded and the
status is prioritized.}
}
\description{
Sends the terminal status for a streaming call after any messages
queued with \code{\link{grpc_send}} have drained. For unary replies
use \code{\link{grpc_reply}}, which sends a payload and the status in
one step. Returns (invisibly) \code{TRUE} if accepted, \code{FALSE}
if the call is no longer answerable.
}
\details{
With \code{drain = FALSE} the close is abortive: queued messages are
discarded and the terminal status goes out first. This is for
fencing — e.g. \code{ABORTED} on session replacement — where
delivering queued-but-stale messages to the peer would be wrong and
waiting behind them (potentially forever, if the peer has stopped
reading) delays the fence. One already-posted message cannot be
recalled, so the status can still wait for that single in-flight
write; if the peer's flow-control window is exhausted even that may
not complete, and \code{\link{grpc_cancel}} on the request is the
hard escalation (the peer then sees \code{CANCELLED} rather than
this status).

}
\examples{
\dontrun{
for (m in msgs) grpc_send(ev, m)
grpc_finish(ev)
grpc_finish(ev, status = "ABORTED", message = "session replaced",
            drain = FALSE)
}
}
