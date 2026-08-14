% tinyrox says don't edit this manually, but it can't stop you!
\name{grpc_tls}
\alias{grpc_tls}
\title{TLS credentials}
\usage{
grpc_tls(
  ca_file = NULL,
  cert_file = NULL,
  key_file = NULL,
  require_client_cert = FALSE,
  target_name_override = NULL
)
}
\arguments{
\item{ca_file}{Path to a PEM CA bundle (trust anchor).}

\item{cert_file}{Path to a PEM certificate chain (own identity).}

\item{key_file}{Path to the PEM private key for \code{cert_file}.}

\item{require_client_cert}{Server side: require and verify a client
certificate (mTLS). Needs \code{ca_file}.}

\item{target_name_override}{Client side: hostname to verify the
server certificate against instead of the dialed target. For
testing with certificates whose name does not match the address;
do not use in production.}
}
\value{
An object of class \code{"grpc_tls"}.
}
\description{
Builds a credentials object for \code{\link{grpc_client}} or
\code{\link{grpc_server}}. PEM files are read at construction time.
}
\details{
For a client: \code{ca_file} pins the CA that must have signed the
server certificate; \code{cert_file}/\code{key_file} present a client
identity (mTLS). For a server: \code{cert_file}/\code{key_file} are
its identity; with \code{require_client_cert = TRUE} the server
demands a client certificate signed by \code{ca_file}, and the
verified identity appears on request events as \code{peer_identity}.

}
\examples{
\dontrun{
creds <- grpc_tls(ca_file = "ca.pem",
                  cert_file = "client.pem", key_file = "client.key")
cl <- grpc_client("node1:41900", credentials = creds)
}
}
