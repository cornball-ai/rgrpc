#' Version of the linked gRPC C++ library
#'
#' Returns the version string of the system gRPC C++ library this package
#' was built against.
#'
#' @return A character string, e.g. \code{"1.51.1"}.
#' @examples
#' if (grpc_available()) grpc_version()
#' @export
grpc_version <- function() {
    .Call(grpc_r_version)
}

#' Was the package built with gRPC?
#'
#' The package installs even where gRPC C++ cannot be linked (no gRPC
#' library, or one built for a different C++ standard library). Such an
#' installation is a stub: every other native call errors. Reinstall
#' from source with the gRPC C++ development files available to get a
#' working build.
#'
#' @return \code{TRUE} if the package links gRPC C++, \code{FALSE} if it
#'   was installed as a stub.
#' @examples
#' grpc_available()
#' @export
grpc_available <- function() {
    .Call(grpc_r_available)
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
