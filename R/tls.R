#' TLS credentials
#'
#' Builds a credentials object for \code{\link{grpc_client}} or
#' \code{\link{grpc_server}}. PEM files are read at construction time.
#'
#' For a client: \code{ca_file} pins the CA that must have signed the
#' server certificate; \code{cert_file}/\code{key_file} present a client
#' identity (mTLS). For a server: \code{cert_file}/\code{key_file} are
#' its identity; with \code{require_client_cert = TRUE} the server
#' demands a client certificate signed by \code{ca_file}, and the
#' verified identity appears on request events as \code{peer_identity}.
#'
#' @param ca_file Path to a PEM CA bundle (trust anchor).
#' @param cert_file Path to a PEM certificate chain (own identity).
#' @param key_file Path to the PEM private key for \code{cert_file}.
#' @param require_client_cert Server side: require and verify a client
#'   certificate (mTLS). Needs \code{ca_file}.
#' @param target_name_override Client side: hostname to verify the
#'   server certificate against instead of the dialed target. For
#'   testing with certificates whose name does not match the address;
#'   do not use in production.
#' @return An object of class \code{"grpc_tls"}.
#' @examples
#' \dontrun{
#' creds <- grpc_tls(ca_file = "ca.pem",
#'                   cert_file = "client.pem", key_file = "client.key")
#' cl <- grpc_client("node1:41900", credentials = creds)
#' }
#' @export
grpc_tls <- function(ca_file = NULL, cert_file = NULL, key_file = NULL,
                     require_client_cert = FALSE,
                     target_name_override = NULL) {
    read_pem <- function(path) {
        if (is.null(path)) {
            return(NULL)
        }
        stopifnot(is.character(path), length(path) == 1L, file.exists(path))
        paste(readLines(path), collapse = "\n")
    }
    if (isTRUE(require_client_cert) && is.null(ca_file)) {
        stop("require_client_cert needs ca_file ",
             "(the CA that signs client certificates)")
    }
    if (!is.null(target_name_override)) {
        stopifnot(is.character(target_name_override),
                  length(target_name_override) == 1L)
    }
    structure(list(ca = read_pem(ca_file), cert = read_pem(cert_file),
                   key = read_pem(key_file),
                   require_client_cert = isTRUE(require_client_cert),
                   target_name_override = target_name_override),
              class = "grpc_tls")
}

#' Channel connectivity state
#'
#' Observes (without provoking a connection attempt) the client
#' channel's connectivity state.
#'
#' For deeper transport diagnostics, gRPC's built-in tracing applies to
#' this package unchanged: set the \code{GRPC_TRACE} and
#' \code{GRPC_VERBOSITY} environment variables before the package loads,
#' e.g. \code{GRPC_TRACE=http,connectivity_state GRPC_VERBOSITY=debug}.
#'
#' @param client A \code{"grpc_client"} object.
#' @return One of \code{"IDLE"}, \code{"CONNECTING"}, \code{"READY"},
#'   \code{"TRANSIENT_FAILURE"}, \code{"SHUTDOWN"}.
#' @examples
#' \dontrun{grpc_state(cl)}
#' @export
grpc_state <- function(client) {
    stopifnot(inherits(client, "grpc_client"))
    states <- c("IDLE", "CONNECTING", "READY", "TRANSIENT_FAILURE",
                "SHUTDOWN")
    states[.Call(grpc_r_client_state, client$ptr) + 1L]
}
