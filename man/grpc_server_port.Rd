% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_server_port}
\alias{grpc_server_port}
\title{Bound TCP port of a server}
\usage{
grpc_server_port(server)
}
\arguments{
\item{server}{A \code{"grpc_server"} object.}
}
\value{
Integer port number.
}
\description{
The port actually bound, useful with an ephemeral \code{"host:0"}
bind address. Meaningless for unix-domain sockets.
}
\examples{
\dontrun{grpc_client(sprintf("127.0.0.1:\%d", grpc_server_port(srv)))}
}
