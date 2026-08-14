% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_reply}
\alias{grpc_reply}
\title{Answer an incoming request}
\usage{
grpc_reply(
  request,
  response = NULL,
  status = 0L,
  message = "",
  metadata = NULL
)
}
\arguments{
\item{request}{A \code{"grpc_request"} event from
\code{\link{grpc_poll}}.}

\item{response}{Raw vector with the serialized response message, or an
\code{RProtoBuf} \code{Message} to serialize. Required when
\code{status} is \code{OK}; ignored otherwise.}

\item{status}{Integer status code or name from
\code{\link{grpc_status_codes}}, e.g. \code{"NOT_FOUND"}.}

\item{message}{Optional error detail string for non-\code{OK} status.}

\item{metadata}{Optional named character vector sent as trailing
metadata.}
}
\description{
Completes a request received from \code{\link{grpc_poll}} on a
server, either with a response payload (status \code{OK}) or with an
error status. Returns (invisibly) \code{TRUE} if the reply was
accepted, or \code{FALSE} if the call is no longer answerable (the
peer cancelled, timed out, or the request was already answered).
}
\examples{
\dontrun{
grpc_reply(ev, serialize(resp, NULL))
grpc_reply(ev, status = "NOT_FOUND", message = "no such sandbox")
}
}
