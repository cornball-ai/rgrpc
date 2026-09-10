% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_call}
\alias{grpc_call}
\title{Start a unary call}
\usage{
grpc_call(
  client,
  method,
  request,
  deadline_ms = NULL,
  metadata = NULL,
  wait_for_ready = FALSE
)
}
\arguments{
\item{client}{A \code{"grpc_client"} object.}

\item{method}{Full method path, e.g.
\code{"/runtime.v1.RuntimeService/Version"}, or a
\code{"grpc_method"} object for a typed call.}

\item{request}{Raw vector with the serialized request message, or an
\code{RProtoBuf} \code{Message} to serialize.}

\item{deadline_ms}{Optional deadline in milliseconds; on expiry the
call completes with status \code{DEADLINE_EXCEEDED}.}

\item{metadata}{Optional named character vector of request metadata.}

\item{wait_for_ready}{If \code{TRUE}, queue the call until the channel
connects instead of failing fast with \code{UNAVAILABLE}.}
}
\value{
An object of class \code{"grpc_call"}: a list with the
  \code{client}, the call \code{id} that its completion event will
  carry, and the \code{method} path. Wait for the completion with
  \code{\link{grpc_await}} or \code{\link{grpc_poll}}.
}
\description{
Starts an asynchronous unary RPC. The request is opaque bytes (a
serialized protocol buffer, e.g. from \code{RProtoBuf}'s
\code{serialize()}). The call completes via \code{\link{grpc_poll}}.
}
\details{
Typed calls: when \code{method} is a \code{"grpc_method"} (from
\code{\link{grpc_method}}) and \code{request} is an \code{RProtoBuf}
\code{Message}, the request type is validated against the method's
\code{input_type} before sending, and the completion delivered by
\code{\link{grpc_poll}} carries the decoded response as
\code{response_message}. Streaming methods are refused here; open
them with \code{\link{grpc_stream}}.

}
\examples{
srv <- grpc_server("127.0.0.1:0")
cl <- grpc_client(sprintf("127.0.0.1:\%d", grpc_server_port(srv)))

## the next request event; the server has one queue for every call, so
## other events are stepped over, and 5 s of silence is an error
next_request <- function(srv) {
  repeat {
    evs <- grpc_poll(srv, timeout_ms = 5000L)
    if (!length(evs)) stop("no request within 5 s")
    for (ev in evs) if (ev$type == "request") return(ev)
  }
}

call <- grpc_call(cl, "/demo.Echo/Say", charToRaw("ping"), deadline_ms = 5000)
class(call)

## echo it back from the server half of the same process
req <- next_request(srv)
grpc_reply(req, req$request)

repeat {
  evs <- grpc_await(call, timeout_ms = 1000L)
  if (length(evs)) break
}
evs[[1]]$status_name
rawToChar(evs[[1]]$response)

## a call nobody answers ends at its deadline
call <- grpc_call(cl, "/demo.Echo/Say", raw(0), deadline_ms = 200)
repeat {
  evs <- grpc_await(call, timeout_ms = 1000L)
  if (length(evs)) break
}
evs[[1]]$status_name

grpc_close(cl)
grpc_close(srv)
}
