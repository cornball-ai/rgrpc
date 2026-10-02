// Stub build, compiled instead of client.cpp, server.cpp, and shim.cpp
// when configure cannot link a program against gRPC C++ (no gRPC on the
// machine, or one built for a different C++ standard library). Every
// routine except grpc_r_available errors, so the package installs and
// grpc_available() reports FALSE.

#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>

#include "routines.h"

extern "C" SEXP grpc_r_available(void) {
    return Rf_ScalarLogical(FALSE);
}

extern "C" void grpc_r_library_init(void) {}

extern "C" SEXP grpc_r_library_shutdown(void) {
    return R_NilValue;
}

static SEXP no_grpc(void) {
    Rf_error("rgrpc was installed without gRPC C++; "
             "reinstall from source with gRPC C++ available "
             "(see grpc_available())");
    return R_NilValue;
}

#define RGRPC_STUB(name, n) \
    extern "C" SEXP name(RGRPC_ARGS_##n) { return no_grpc(); }
RGRPC_GRPC_ROUTINES(RGRPC_STUB)
