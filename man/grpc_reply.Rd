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
\value{
Invisibly, a logical scalar: \code{TRUE} if the reply was
  accepted for sending, \code{FALSE} if the call is no longer
  answerable.
}
\description{
Completes a request received from \code{\link{grpc_poll}} on a
server, either with a response payload (status \code{OK}) or with an
error status. Returns (invisibly) \code{TRUE} if the reply was
accepted, or \code{FALSE} if the call is no longer answerable (the
peer cancelled, timed out, or the request was already answered).
}
\examples{
srv <- grpc_server("127.0.0.1:0")
cl <- grpc_client(sprintf("127.0.0.1:\%d", grpc_server_port(srv)))

## a payload reply (status OK), with trailing metadata
call <- grpc_call(cl, "/demo.Echo/Say", charToRaw("hi"), deadline_ms = 5000)
evs <- grpc_poll(srv, timeout_ms = 5000L)
req <- Filter(function(e) e$type == "request", evs)[[1]]
(grpc_reply(req, req$request, metadata = c("x-served-by" = "example")))
(grpc_reply(req, req$request))          # FALSE: already answered
repeat {
  evs <- grpc_await(call, timeout_ms = 1000L)
  if (length(evs)) break
}
evs[[1]]$status_name
evs[[1]]$trailing_metadata

## an error status needs no payload
call <- grpc_call(cl, "/demo.Echo/Say", raw(0), deadline_ms = 5000)
evs <- grpc_poll(srv, timeout_ms = 5000L)
req <- Filter(function(e) e$type == "request", evs)[[1]]
grpc_reply(req, status = "NOT_FOUND", message = "no such thing")
repeat {
  evs <- grpc_await(call, timeout_ms = 1000L)
  if (length(evs)) break
}
evs[[1]]$status_name
evs[[1]]$message
is.null(evs[[1]]$response)

grpc_close(cl)
grpc_close(srv)
}
