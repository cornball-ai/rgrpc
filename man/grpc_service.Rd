% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_service}
\alias{grpc_service}
\title{Resolve a gRPC service from the RProtoBuf descriptor pool}
\usage{
grpc_service(anchor, service = NULL)
}
\arguments{
\item{anchor}{A message type from the \code{.proto} file that defines
the service: a type name like \code{"runtime.v1.VersionRequest"}, an
\code{RProtoBuf} \code{Descriptor}, or a \code{Message}.}

\item{service}{Service to select, by short or fully qualified name.
May be omitted when the file defines exactly one service.}
}
\value{
An object of class \code{"grpc_service"}: \code{name},
  \code{package}, and a named list \code{methods} of
  \code{"grpc_method"} objects.
}
\description{
Looks up the service definitions in the \code{.proto} file that
defines \code{anchor}, a message type from that file (schemas are
loaded at runtime with \code{RProtoBuf::readProtoFiles2()}; there is
no generated code). Returns the service with its methods resolved:
full method paths, input and output message types, and streaming
flags.
}
\examples{
\dontrun{
RProtoBuf::readProtoFiles2("api.proto", protoPath = proto_dir)
svc <- grpc_service("runtime.v1.VersionRequest", "RuntimeService")
names(svc$methods)
}
}
