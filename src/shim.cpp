// Increment-1 spike: prove the system-linked gRPC C++ library loads,
// creates and destroys channels / completion queues / servers safely, and
// unloads cleanly from R. No RPC lifecycle yet.

#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include <grpcpp/generic/async_generic_service.h>

#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>

// ---- availability and version ----

// stub.cpp returns FALSE; routine registration lives in init.cpp.
extern "C" SEXP grpc_r_available(void) {
    return Rf_ScalarLogical(TRUE);
}

// ---- library lifetime ----

// One gRPC initialization for the life of the DLL, taken in R_init.
// Without it every client and server's GrpcLibrary reference brings
// gRPC up and, when the last one closes, back down: executor threads
// started and joined per object, and each start runs abseil's debug
// deadlock bookkeeping, whose frame-pointer stack walk reads through
// whatever R-called frame is on the stack (a valgrind report on
// CRAN's no-frame-pointer builds).
//
// R does not unload package DLLs at exit, so the matching shutdown also
// runs from an onexit finalizer (.onLoad); otherwise the executor
// threads outlive R's exit and valgrind reports their TLS as lost.
static bool library_up = false;

extern "C" void grpc_r_library_init(void) {
    grpc_init();
    library_up = true;
}

extern "C" SEXP grpc_r_library_shutdown(void) {
    if (library_up) {
        library_up = false;
        grpc_shutdown_blocking();
    }
    return R_NilValue;
}

extern "C" SEXP grpc_r_version(void) {
    return Rf_mkString(grpc::Version().c_str());
}

// ---- channel ----

static void channel_finalizer(SEXP xp) {
    auto *p = static_cast<std::shared_ptr<grpc::Channel> *>(R_ExternalPtrAddr(xp));
    if (p != nullptr) {
        delete p;
        R_ClearExternalPtr(xp);
    }
}

extern "C" SEXP grpc_r_channel_create(SEXP target) {
    const char *tgt = Rf_translateCharUTF8(STRING_ELT(target, 0));
    auto *p = new std::shared_ptr<grpc::Channel>(
        grpc::CreateChannel(tgt, grpc::InsecureChannelCredentials()));
    SEXP xp = PROTECT(R_MakeExternalPtr(p, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(xp, channel_finalizer, TRUE);
    UNPROTECT(1);
    return xp;
}

extern "C" SEXP grpc_r_channel_destroy(SEXP xp) {
    channel_finalizer(xp);
    return R_NilValue;
}

// ---- completion queue ----

static void cq_finalizer(SEXP xp) {
    auto *cq = static_cast<grpc::CompletionQueue *>(R_ExternalPtrAddr(xp));
    if (cq != nullptr) {
        cq->Shutdown();
        void *tag = nullptr;
        bool ok = false;
        while (cq->Next(&tag, &ok)) {
        }
        delete cq;
        R_ClearExternalPtr(xp);
    }
}

extern "C" SEXP grpc_r_cq_create(void) {
    auto *cq = new grpc::CompletionQueue();
    SEXP xp = PROTECT(R_MakeExternalPtr(cq, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(xp, cq_finalizer, TRUE);
    UNPROTECT(1);
    return xp;
}

extern "C" SEXP grpc_r_cq_destroy(SEXP xp) {
    cq_finalizer(xp);
    return R_NilValue;
}

// ---- server ----

// AsyncGenericService must outlive the Server; the ServerCompletionQueue
// must be shut down and drained after Server::Shutdown and before either
// is destroyed.
struct spike_server {
    grpc::AsyncGenericService generic;
    std::unique_ptr<grpc::ServerCompletionQueue> cq;
    std::unique_ptr<grpc::Server> server;
    int port = 0;
};

static void server_finalizer(SEXP xp) {
    auto *s = static_cast<spike_server *>(R_ExternalPtrAddr(xp));
    if (s == nullptr) return;
    if (s->server) s->server->Shutdown();
    if (s->cq) {
        s->cq->Shutdown();
        void *tag = nullptr;
        bool ok = false;
        while (s->cq->Next(&tag, &ok)) {
        }
    }
    s->server.reset();
    s->cq.reset();
    delete s;
    R_ClearExternalPtr(xp);
}

extern "C" SEXP grpc_r_server_create(SEXP address) {
    const char *addr = Rf_translateCharUTF8(STRING_ELT(address, 0));
    auto *s = new spike_server();
    grpc::ServerBuilder builder;
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials(), &s->port);
    builder.RegisterAsyncGenericService(&s->generic);
    s->cq = builder.AddCompletionQueue();
    s->server = builder.BuildAndStart();
    if (!s->server) {
        delete s;
        Rf_error("gRPC server failed to start on '%s'", addr);
    }
    SEXP xp = PROTECT(R_MakeExternalPtr(s, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(xp, server_finalizer, TRUE);
    UNPROTECT(1);
    return xp;
}

extern "C" SEXP grpc_r_server_port(SEXP xp) {
    auto *s = static_cast<spike_server *>(R_ExternalPtrAddr(xp));
    if (s == nullptr) Rf_error("server already destroyed");
    return Rf_ScalarInteger(s->port);
}

extern "C" SEXP grpc_r_server_destroy(SEXP xp) {
    server_finalizer(xp);
    return R_NilValue;
}
