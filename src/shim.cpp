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

// ---- version ----

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

// ---- registration ----

// client.cpp
extern "C" SEXP grpc_r_client_create(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP,
                                     SEXP, SEXP);
extern "C" SEXP grpc_r_client_state(SEXP);
extern "C" SEXP grpc_r_client_close(SEXP);
extern "C" SEXP grpc_r_client_fd(SEXP);
extern "C" SEXP grpc_r_client_pending(SEXP);
extern "C" SEXP grpc_r_client_poll(SEXP, SEXP, SEXP);
extern "C" SEXP grpc_r_call_start(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
extern "C" SEXP grpc_r_call_cancel(SEXP, SEXP);
extern "C" SEXP grpc_r_stream_start(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
extern "C" SEXP grpc_r_stream_send(SEXP, SEXP, SEXP);
extern "C" SEXP grpc_r_stream_writes_done(SEXP, SEXP);

// server.cpp
extern "C" SEXP grpc_r_server2_create(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP,
                                      SEXP, SEXP, SEXP, SEXP, SEXP);
extern "C" SEXP grpc_r_server2_close(SEXP);
extern "C" SEXP grpc_r_server2_fd(SEXP);
extern "C" SEXP grpc_r_server2_port(SEXP);
extern "C" SEXP grpc_r_server2_pending(SEXP);
extern "C" SEXP grpc_r_server2_reply(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
extern "C" SEXP grpc_r_server2_poll(SEXP, SEXP, SEXP);
extern "C" SEXP grpc_r_server2_read(SEXP, SEXP);
extern "C" SEXP grpc_r_server2_cancel(SEXP, SEXP);
extern "C" SEXP grpc_r_server2_send(SEXP, SEXP, SEXP);
extern "C" SEXP grpc_r_server2_finish(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);

static const R_CallMethodDef call_methods[] = {
    {"grpc_r_version",         (DL_FUNC) &grpc_r_version,         0},
    {"grpc_r_channel_create",  (DL_FUNC) &grpc_r_channel_create,  1},
    {"grpc_r_channel_destroy", (DL_FUNC) &grpc_r_channel_destroy, 1},
    {"grpc_r_cq_create",       (DL_FUNC) &grpc_r_cq_create,       0},
    {"grpc_r_cq_destroy",      (DL_FUNC) &grpc_r_cq_destroy,      1},
    {"grpc_r_server_create",   (DL_FUNC) &grpc_r_server_create,   1},
    {"grpc_r_server_port",     (DL_FUNC) &grpc_r_server_port,     1},
    {"grpc_r_server_destroy",  (DL_FUNC) &grpc_r_server_destroy,  1},
    {"grpc_r_client_create",   (DL_FUNC) &grpc_r_client_create,   8},
    {"grpc_r_client_state",    (DL_FUNC) &grpc_r_client_state,    1},
    {"grpc_r_client_close",    (DL_FUNC) &grpc_r_client_close,    1},
    {"grpc_r_client_fd",       (DL_FUNC) &grpc_r_client_fd,       1},
    {"grpc_r_client_pending",  (DL_FUNC) &grpc_r_client_pending,  1},
    {"grpc_r_client_poll",     (DL_FUNC) &grpc_r_client_poll,     3},
    {"grpc_r_call_start",      (DL_FUNC) &grpc_r_call_start,      6},
    {"grpc_r_call_cancel",     (DL_FUNC) &grpc_r_call_cancel,     2},
    {"grpc_r_server2_create",  (DL_FUNC) &grpc_r_server2_create,  11},
    {"grpc_r_server2_close",   (DL_FUNC) &grpc_r_server2_close,   1},
    {"grpc_r_server2_fd",      (DL_FUNC) &grpc_r_server2_fd,      1},
    {"grpc_r_server2_port",    (DL_FUNC) &grpc_r_server2_port,    1},
    {"grpc_r_server2_pending", (DL_FUNC) &grpc_r_server2_pending, 1},
    {"grpc_r_server2_reply",   (DL_FUNC) &grpc_r_server2_reply,   6},
    {"grpc_r_server2_poll",    (DL_FUNC) &grpc_r_server2_poll,    3},
    {"grpc_r_stream_start",    (DL_FUNC) &grpc_r_stream_start,    7},
    {"grpc_r_stream_send",     (DL_FUNC) &grpc_r_stream_send,     3},
    {"grpc_r_stream_writes_done", (DL_FUNC) &grpc_r_stream_writes_done, 2},
    {"grpc_r_server2_read",    (DL_FUNC) &grpc_r_server2_read,    2},
    {"grpc_r_server2_cancel",  (DL_FUNC) &grpc_r_server2_cancel,  2},
    {"grpc_r_server2_send",    (DL_FUNC) &grpc_r_server2_send,    3},
    {"grpc_r_server2_finish",  (DL_FUNC) &grpc_r_server2_finish,  6},
    {NULL, NULL, 0}
};

extern "C" void R_init_grpc(DllInfo *dll) {
    R_registerRoutines(dll, NULL, call_methods, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
}
