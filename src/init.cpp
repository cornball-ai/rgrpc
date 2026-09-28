// Routine registration. Includes no gRPC headers, so the stub build
// (stub.cpp, used when configure cannot link gRPC C++) shares it.

#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>

#include "routines.h"

#define RGRPC_PROTOTYPE(name, n) extern "C" SEXP name(RGRPC_ARGS_##n);
RGRPC_ROUTINES(RGRPC_PROTOTYPE)

#define RGRPC_CALLDEF(name, n) {#name, (DL_FUNC) &name, n},
static const R_CallMethodDef call_methods[] = {
    RGRPC_ROUTINES(RGRPC_CALLDEF)
    {NULL, NULL, 0}
};

// shim.cpp holds one gRPC initialization for the life of the DLL, and
// its shutdown (a routine, also called from an onexit finalizer);
// stub.cpp defines both as no-ops.
extern "C" void grpc_r_library_init(void);

extern "C" void R_init_rgrpc(DllInfo *dll) {
    R_registerRoutines(dll, NULL, call_methods, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
    grpc_r_library_init();
}

extern "C" void R_unload_rgrpc(DllInfo *dll) {
    (void) dll;
    grpc_r_library_shutdown();
}
