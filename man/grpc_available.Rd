% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_available}
\alias{grpc_available}
\title{Was the package built with gRPC?}
\usage{
grpc_available()
}
\value{
\code{TRUE} if the package links gRPC C++, \code{FALSE} if it
  was installed as a stub.
}
\description{
The package installs even where gRPC C++ cannot be linked (no gRPC
library, or one built for a different C++ standard library). Such an
installation is a stub: every other native call errors. Reinstall
from source with the gRPC C++ development files available to get a
working build.
}
\examples{
grpc_available()
}
