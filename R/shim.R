#' Version of the linked gRPC C++ library
#'
#' Returns the version string of the system gRPC C++ library this package
#' was built against.
#'
#' @return A character string, e.g. \code{"1.51.1"}.
#' @examples
#' grpc_version()
#' @export
grpc_version <- function() {
    .Call(grpc_r_version)
}

## Internal spike surface: object lifetime only, no RPC yet. These become
## the real channel/server API in later increments.

.channel_create <- function(target) {
    stopifnot(is.character(target), length(target) == 1L)
    .Call(grpc_r_channel_create, target)
}

.channel_destroy <- function(channel) {
    invisible(.Call(grpc_r_channel_destroy, channel))
}

.cq_create <- function() {
    .Call(grpc_r_cq_create)
}

.cq_destroy <- function(cq) {
    invisible(.Call(grpc_r_cq_destroy, cq))
}

.server_create <- function(address = "127.0.0.1:0") {
    stopifnot(is.character(address), length(address) == 1L)
    .Call(grpc_r_server_create, address)
}

.server_port <- function(server) {
    .Call(grpc_r_server_port, server)
}

.server_destroy <- function(server) {
    invisible(.Call(grpc_r_server_destroy, server))
}
