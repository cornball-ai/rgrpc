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
\code{"/runtime.v1.RuntimeService/Version"}.}

\item{request}{Raw vector with the serialized request message.}

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
\examples{
\dontrun{
call <- grpc_call(client, "/runtime.v1.RuntimeService/Version",
                  serialize(req, NULL), deadline_ms = 500)
}
}
