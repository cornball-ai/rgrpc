% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_decode}
\alias{grpc_decode}
\title{Decode protocol buffer bytes to a message}
\usage{
grpc_decode(bytes, type)
}
\arguments{
\item{bytes}{Raw vector, e.g. the \code{request} field of a server
event or the \code{response} field of a client completion.}

\item{type}{Fully qualified message type name, e.g. an
\code{input_type} from a \code{"grpc_method"}.}
}
\value{
An \code{RProtoBuf} \code{Message} of type \code{type}, with
  the fields decoded from \code{bytes}.
}
\description{
Thin wrapper over \code{RProtoBuf::read()} for decoding a request or
response payload against a type from the runtime descriptor pool.
}
\examples{
if (requireNamespace("RProtoBuf", quietly = TRUE)) {
  RProtoBuf::readProtoFiles2("health.proto",
      protoPath = system.file("proto", "health", package = "rgrpc"))
  ## bytes as they arrive in a request or response event
  msg <- RProtoBuf::P("grpc.health.v1.HealthCheckRequest")$new(service = "demo")
  bytes <- RProtoBuf::serialize(msg, NULL)
  decoded <- grpc_decode(bytes, "grpc.health.v1.HealthCheckRequest")
  print(decoded$service)
}
}
