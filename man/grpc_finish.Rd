% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_finish}
\alias{grpc_finish}
\title{End a server stream}
\usage{
grpc_finish(request, status = 0L, message = "", metadata = NULL)
}
\arguments{
\item{request}{A \code{"grpc_request"} event from
\code{\link{grpc_poll}}.}

\item{status}{Integer status code or name from
\code{\link{grpc_status_codes}}.}

\item{message}{Optional error detail string for non-\code{OK} status.}

\item{metadata}{Optional named character vector sent as trailing
metadata.}
}
\description{
Sends the terminal status for a streaming call after any messages
queued with \code{\link{grpc_send}} have drained. For unary replies
use \code{\link{grpc_reply}}, which sends a payload and the status in
one step. Returns (invisibly) \code{TRUE} if accepted, \code{FALSE}
if the call is no longer answerable.
}
\examples{
\dontrun{
for (m in msgs) grpc_send(ev, m)
grpc_finish(ev)
}
}
