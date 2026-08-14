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
An \code{RProtoBuf} \code{Message}.
}
\description{
Thin wrapper over \code{RProtoBuf::read()} for decoding a request or
response payload against a type from the runtime descriptor pool.
}
\examples{
\dontrun{req <- grpc_decode(ev$request, m$input_type)}
}
