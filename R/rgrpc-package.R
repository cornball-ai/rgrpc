#' @useDynLib rgrpc, .registration = TRUE
#' @importFrom methods as
NULL

.onAttach <- function(libname, pkgname) {
    if (!grpc_available()) {
        packageStartupMessage(
            "rgrpc was installed without gRPC C++; its functions will ",
            "error. See ?grpc_available.")
    }
}
