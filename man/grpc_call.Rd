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
An object of class \code{"grpc_call"} holding the call id.
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
\dontrun{
call <- grpc_call(client, "/runtime.v1.RuntimeService/Version",
                  serialize(req, NULL), deadline_ms = 500)
}
}
