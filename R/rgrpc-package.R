#' @useDynLib rgrpc, .registration = TRUE
#' @importFrom methods as
NULL

## R_init takes one gRPC initialization for the life of the DLL, but R
## does not unload package DLLs at exit; shut gRPC down then too, so its
## threads are joined rather than left running as R exits. The shutdown
## is idempotent, and skipped if the DLL is already gone.
.onLoad <- function(libname, pkgname) {
    reg.finalizer(environment(.onLoad), function(e) {
        if (!is.null(getLoadedDLLs()[["rgrpc"]])) {
            .Call(grpc_r_library_shutdown)
        }
    }, onexit = TRUE)
}

.onAttach <- function(libname, pkgname) {
    if (!grpc_available()) {
        packageStartupMessage(
            "rgrpc was installed without gRPC C++; its functions will ",
            "error. See ?grpc_available.")
    }
}
