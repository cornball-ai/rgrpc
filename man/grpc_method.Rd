% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_method}
\alias{grpc_method}
\title{Look up a method in a resolved service}
\usage{
grpc_method(service, name)
}
\arguments{
\item{service}{A \code{"grpc_service"} from \code{\link{grpc_service}}.}

\item{name}{Method name, e.g. \code{"Version"}.}
}
\value{
A \code{"grpc_method"} object: \code{name}, \code{path},
  \code{input_type}, \code{output_type}, \code{client_streaming},
  \code{server_streaming}. Pass it as the \code{method} argument of
  \code{\link{grpc_call}} for typed calls.
}
\description{
Look up a method in a resolved service
}
\examples{
\dontrun{m <- grpc_method(svc, "Version")}
}
