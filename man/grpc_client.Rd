% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_client}
\alias{grpc_client}
\title{Create an asynchronous gRPC client}
\usage{
grpc_client(target, credentials = NULL)
}
\arguments{
\item{target}{Server address, e.g. \code{"localhost:50051"} or
\code{"unix:///run/containerd/containerd.sock"}.}

\item{credentials}{\code{NULL} for a plaintext channel, or a
\code{\link{grpc_tls}} object.}
}
\value{
An object of class \code{"grpc_client"}.
}
\description{
Opens a channel to \code{target} and starts the client's completion
machinery: a background thread that drains the gRPC completion queue
and signals an eventfd. The background thread never calls the R API;
completions are received on the R main thread via \code{\link{grpc_poll}}.
}
\examples{
\dontrun{
client <- grpc_client("localhost:50051")
call <- grpc_call(client, "/demo.Echo/Say", serialize(msg, NULL),
                  deadline_ms = 1000)
events <- grpc_poll(client, timeout_ms = 1000L)
grpc_close(client)
}
}
