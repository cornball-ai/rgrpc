% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_poll}
\alias{grpc_poll}
\title{Receive completed calls}
\usage{
grpc_poll(client, max_events = 64L, timeout_ms = 0L)
}
\arguments{
\item{client}{A \code{"grpc_client"} object.}

\item{max_events}{Maximum completions to return in this batch.}

\item{timeout_ms}{How long to wait if no completion is ready:
\code{0} returns immediately, a positive value waits up to that many
milliseconds, \code{-1} waits indefinitely.}
}
\value{
A list of completion events (possibly empty).
}
\description{
Drains up to \code{max_events} completed calls from the client. Each
completion is a list with elements \code{id}, \code{status} (integer,
see \code{\link{grpc_status_codes}}), \code{status_name},
\code{message}, \code{response} (raw vector, or \code{NULL} unless the
status is \code{OK}), \code{initial_metadata}, and
\code{trailing_metadata}.
}
\examples{
\dontrun{
events <- grpc_poll(client, max_events = 64L, timeout_ms = 100L)
for (ev in events) if (ev$status_name == "OK") handle(ev$response)
}
}
