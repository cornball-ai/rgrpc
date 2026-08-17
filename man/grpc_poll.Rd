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

An empty result means the wait expired with nothing queued at that
moment; it says nothing about outstanding work, which ends only when
its terminal event is delivered (\code{"unary"} for a call,
\code{"stream_status"} for a stream). Loop until that event rather
than treating an empty batch as the end of a call;
\code{\link{grpc_pending}} reports what is still in flight.

One queue serves the whole client or server, so a batch can mix
events from every call in flight, in completion order. Dispatch on
\code{id}: a caller that consumes every \code{"stream_msg"} it sees
as belonging to the stream it happens to be reading will splice in
messages from another one. This bites hardest after a stream is
abandoned unread, since its queued messages keep arriving --
\code{\link{grpc_cancel}} bounds how many more are produced but
cannot recall events already queued.

}
\examples{
\dontrun{
events <- grpc_poll(client, max_events = 64L, timeout_ms = 100L)
for (ev in events) if (ev$status_name == "OK") handle(ev$response)
}
}
