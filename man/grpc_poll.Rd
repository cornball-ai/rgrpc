% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_poll}
\alias{grpc_poll}
\title{Receive completed events}
\usage{
grpc_poll(x, max_events = 64L, timeout_ms = 0L)
}
\arguments{
\item{x}{A \code{"grpc_client"} or \code{"grpc_server"} object.}

\item{max_events}{Maximum events to return in this batch.}

\item{timeout_ms}{How long to wait if no event is ready: \code{0}
returns immediately, a positive value waits up to that many
milliseconds, \code{-1} waits indefinitely.}
}
\value{
A list of events (possibly empty).
}
\description{
Drains up to \code{max_events} pending events from a client or server.
}
\details{
For a \code{"grpc_client"}, each event is a completed call: a list
with \code{id}, \code{status} (integer, see
\code{\link{grpc_status_codes}}), \code{status_name}, \code{message},
\code{response} (raw vector, \code{NULL} unless the status is
\code{OK}), \code{initial_metadata}, and \code{trailing_metadata}.

For a \code{"grpc_server"}, each event is either an incoming request
(class \code{"grpc_request"}: \code{type = "request"}, \code{id},
\code{method}, \code{request} raw vector, \code{metadata},
\code{deadline_ms}, answered via \code{\link{grpc_reply}}) or a
cancellation notice (\code{type = "cancelled"}, \code{id}) for a
request the peer abandoned before it was answered.

}
\examples{
\dontrun{
events <- grpc_poll(client, max_events = 64L, timeout_ms = 100L)
for (ev in events) if (ev$status_name == "OK") handle(ev$response)
}
}
