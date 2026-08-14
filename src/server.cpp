// Generic asynchronous server (increment 3).
//
// Same threading contract as the client: one background thread drains
// the server completion queue and never calls the R API. Request events
// are fully copied into plain C++ structs on the completion thread, so
// event delivery to R is independent of call-state lifetime. All call
// state transitions happen under the server mutex; RPC ops are posted
// while holding it so a concurrently completing call cannot be freed
// between the state check and the op post.
//
// Backpressure: at most accept_window RequestCall slots are outstanding,
// and no new slot is posted while active calls + outstanding slots would
// exceed max_active. Excess incoming calls queue in gRPC/kernel until a
// reply or call teardown frees capacity.

#include <chrono>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <grpcpp/grpcpp.h>
#include <grpcpp/generic/async_generic_service.h>

#include "common.h"

namespace {

struct sv_call;

enum class svop { accept, read, done, finish };

struct sv_tag {
    sv_call *call;
    svop kind;
};

struct sv_call {
    uint64_t id = 0;  // assigned at accept completion; 0 = accept slot
    grpc::GenericServerContext ctx;
    grpc::GenericServerAsyncReaderWriter stream{&ctx};
    grpc::ByteBuffer request;
    int pending = 0;  // outstanding CQ tags for this call
    bool delivered = false;
    bool replied = false;
    bool dead = false;
    sv_tag t_accept{this, svop::accept};
    sv_tag t_read{this, svop::read};
    sv_tag t_done{this, svop::done};
    sv_tag t_finish{this, svop::finish};
};

struct sv_event {
    int type = 0;  // 0 = request, 1 = cancelled
    uint64_t id = 0;
    std::string method;
    std::string request;
    std::vector<std::pair<std::string, std::string>> metadata;
    double deadline_ms = -1;  // -1 = none
};

struct rserver {
    grpc::AsyncGenericService generic;
    std::unique_ptr<grpc::ServerCompletionQueue> cq;
    std::unique_ptr<grpc::Server> server;
    std::thread completer;
    int event_fd = -1;
    int port = 0;
    int accept_window = 8;
    int max_active = 256;

    std::mutex mu;
    std::map<uint64_t, sv_call *> active;
    std::deque<sv_event> ready;
    int accepts_outstanding = 0;
    uint64_t next_id = 1;
    bool closed = false;
    bool shutting = false;

    void signal() {
        uint64_t one = 1;
        ssize_t n = write(event_fd, &one, sizeof one);
        (void) n;
    }

    void post_accepts_locked() {
        while (!shutting && accepts_outstanding < accept_window &&
               (int) active.size() + accepts_outstanding < max_active) {
            auto *c = new sv_call();
            c->pending = 1;  // the accept tag; done becomes live only on accept ok
            c->ctx.AsyncNotifyWhenDone(&c->t_done);
            generic.RequestCall(&c->ctx, &c->stream, cq.get(), cq.get(),
                                &c->t_accept);
            ++accepts_outstanding;
        }
    }

    void maybe_free_locked(sv_call *c) {
        if (c->pending == 0) {
            if (c->id != 0) active.erase(c->id);
            delete c;
        }
    }

    // Background completion thread: no R API calls allowed here.
    void run() {
        void *tag = nullptr;
        bool ok = false;
        while (cq->Next(&tag, &ok)) {
            auto *t = static_cast<sv_tag *>(tag);
            sv_call *c = t->call;
            std::lock_guard<std::mutex> lock(mu);
            --c->pending;
            switch (t->kind) {
            case svop::accept:
                --accepts_outstanding;
                if (ok) {
                    c->id = next_id++;
                    active[c->id] = c;
                    // AsyncNotifyWhenDone fires only for started RPCs, so
                    // its tag counts as pending from here on.
                    c->pending += 2;
                    c->stream.Read(&c->request, &c->t_read);
                    post_accepts_locked();
                } else {
                    maybe_free_locked(c);  // shutdown: slot never matched
                }
                break;
            case svop::read:
                if (ok && !c->dead) {
                    c->delivered = true;
                    sv_event ev;
                    ev.type = 0;
                    ev.id = c->id;
                    ev.method = c->ctx.method();
                    grpc_byte_buffer_to_string(c->request, &ev.request);
                    for (const auto &kv : c->ctx.client_metadata())
                        ev.metadata.emplace_back(
                            std::string(kv.first.data(), kv.first.size()),
                            std::string(kv.second.data(), kv.second.size()));
                    auto dl = c->ctx.deadline();
                    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  dl - std::chrono::system_clock::now())
                                  .count();
                    // gRPC encodes "no deadline" as a far-future time point.
                    ev.deadline_ms = (ms > 0 && ms < 31536000000LL) ? (double) ms : -1;
                    ready.push_back(std::move(ev));
                    signal();
                } else {
                    c->dead = true;
                }
                maybe_free_locked(c);
                break;
            case svop::done:
                if (c->delivered && !c->replied) {
                    c->dead = true;
                    sv_event ev;
                    ev.type = 1;
                    ev.id = c->id;
                    ready.push_back(std::move(ev));
                    signal();
                }
                maybe_free_locked(c);
                post_accepts_locked();
                break;
            case svop::finish:
                maybe_free_locked(c);
                post_accepts_locked();
                break;
            }
        }
    }
};

void rserver_shutdown(rserver *s) {
    if (s->closed) return;
    s->closed = true;
    {
        std::lock_guard<std::mutex> lock(s->mu);
        s->shutting = true;
    }
    // Hard deadline: cancel in-flight calls instead of waiting for replies.
    s->server->Shutdown(std::chrono::system_clock::now());
    s->cq->Shutdown();
    if (s->completer.joinable()) s->completer.join();
    for (auto &kv : s->active) delete kv.second;
    s->active.clear();
    if (s->event_fd >= 0) {
        close(s->event_fd);
        s->event_fd = -1;
    }
}

void rserver_finalizer(SEXP xp) {
    auto *s = static_cast<rserver *>(R_ExternalPtrAddr(xp));
    if (s == nullptr) return;
    rserver_shutdown(s);
    delete s;
    R_ClearExternalPtr(xp);
}

rserver *get_server(SEXP xp) {
    auto *s = static_cast<rserver *>(R_ExternalPtrAddr(xp));
    if (s == nullptr || s->closed) Rf_error("grpc server is closed");
    return s;
}

}  // namespace

// args: address (chr), accept_window (int), max_active (int)
extern "C" SEXP grpc_r_server2_create(SEXP address, SEXP accept_window,
                                      SEXP max_active) {
    const char *addr = Rf_translateCharUTF8(STRING_ELT(address, 0));
    int fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (fd < 0) Rf_error("eventfd creation failed");
    auto *s = new rserver();
    s->event_fd = fd;
    s->accept_window = Rf_asInteger(accept_window);
    s->max_active = Rf_asInteger(max_active);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials(), &s->port);
    builder.RegisterAsyncGenericService(&s->generic);
    s->cq = builder.AddCompletionQueue();
    s->server = builder.BuildAndStart();
    if (!s->server) {
        close(fd);
        delete s;
        Rf_error("gRPC server failed to start on '%s'", addr);
    }
    {
        std::lock_guard<std::mutex> lock(s->mu);
        s->post_accepts_locked();
    }
    s->completer = std::thread([s]() { s->run(); });
    SEXP xp = PROTECT(R_MakeExternalPtr(s, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(xp, rserver_finalizer, TRUE);
    UNPROTECT(1);
    return xp;
}

extern "C" SEXP grpc_r_server2_close(SEXP xp) {
    rserver_finalizer(xp);
    return R_NilValue;
}

extern "C" SEXP grpc_r_server2_fd(SEXP xp) {
    return Rf_ScalarInteger(get_server(xp)->event_fd);
}

extern "C" SEXP grpc_r_server2_port(SEXP xp) {
    return Rf_ScalarInteger(get_server(xp)->port);
}

extern "C" SEXP grpc_r_server2_pending(SEXP xp) {
    rserver *s = get_server(xp);
    std::lock_guard<std::mutex> lock(s->mu);
    return Rf_ScalarInteger((int) s->active.size());
}

// args: server, id (dbl), response (raw or NULL), status (int),
//       message (chr), metadata (named chr or NULL)
extern "C" SEXP grpc_r_server2_reply(SEXP xp, SEXP id, SEXP response,
                                     SEXP status, SEXP message, SEXP metadata) {
    rserver *s = get_server(xp);
    const uint64_t want = (uint64_t) Rf_asReal(id);
    const int code = Rf_asInteger(status);
    // Validate before taking the lock: Rf_error must not longjmp with the
    // mutex held.
    if (code == 0 && TYPEOF(response) != RAWSXP)
        Rf_error("response must be a raw vector when status is OK");
    if (code != 0 && TYPEOF(message) != STRSXP)
        Rf_error("message must be a character string");

    std::lock_guard<std::mutex> lock(s->mu);
    auto it = s->active.find(want);
    if (it == s->active.end()) return Rf_ScalarLogical(FALSE);
    sv_call *c = it->second;
    if (!c->delivered || c->replied || c->dead) return Rf_ScalarLogical(FALSE);
    c->replied = true;

    if (metadata != R_NilValue) {
        SEXP names = Rf_getAttrib(metadata, R_NamesSymbol);
        for (R_xlen_t i = 0; i < Rf_xlength(metadata); ++i) {
            c->ctx.AddTrailingMetadata(
                Rf_translateCharUTF8(STRING_ELT(names, i)),
                Rf_translateCharUTF8(STRING_ELT(metadata, i)));
        }
    }

    ++c->pending;
    if (code == 0) {
        grpc::Slice slice(RAW(response), (size_t) Rf_xlength(response));
        grpc::ByteBuffer resp(&slice, 1);
        c->stream.WriteAndFinish(resp, grpc::WriteOptions(), grpc::Status::OK,
                                 &c->t_finish);
    } else {
        const char *msg = Rf_translateCharUTF8(STRING_ELT(message, 0));
        c->stream.Finish(grpc::Status(static_cast<grpc::StatusCode>(code), msg),
                         &c->t_finish);
    }
    return Rf_ScalarLogical(TRUE);
}

// args: server, max_events (int), timeout_ms (int)
extern "C" SEXP grpc_r_server2_poll(SEXP xp, SEXP max_events, SEXP timeout_ms) {
    rserver *s = get_server(xp);
    const int maxn = Rf_asInteger(max_events);
    const int timeout = Rf_asInteger(timeout_ms);

    {
        std::unique_lock<std::mutex> lock(s->mu);
        const bool empty = s->ready.empty();
        lock.unlock();
        if (empty && timeout != 0) {
            struct pollfd pfd = {s->event_fd, POLLIN, 0};
            poll(&pfd, 1, timeout);
        }
    }
    uint64_t drained;
    while (read(s->event_fd, &drained, sizeof drained) > 0) {
    }

    std::vector<sv_event> batch;
    {
        std::lock_guard<std::mutex> lock(s->mu);
        while (!s->ready.empty() && (int) batch.size() < maxn) {
            batch.push_back(std::move(s->ready.front()));
            s->ready.pop_front();
        }
    }

    const R_xlen_t n = (R_xlen_t) batch.size();
    SEXP out = PROTECT(Rf_allocVector(VECSXP, n));
    const char *fields[] = {"type", "id", "method", "request", "metadata",
                            "deadline_ms"};
    SEXP field_names = PROTECT(Rf_allocVector(STRSXP, 6));
    for (int j = 0; j < 6; ++j) SET_STRING_ELT(field_names, j, Rf_mkChar(fields[j]));

    for (R_xlen_t i = 0; i < n; ++i) {
        const sv_event &ev = batch[i];
        SEXP e = PROTECT(Rf_allocVector(VECSXP, 6));
        Rf_setAttrib(e, R_NamesSymbol, field_names);
        SET_VECTOR_ELT(e, 0, Rf_mkString(ev.type == 0 ? "request" : "cancelled"));
        SET_VECTOR_ELT(e, 1, Rf_ScalarReal((double) ev.id));
        if (ev.type == 0) {
            SET_VECTOR_ELT(e, 2, Rf_mkString(ev.method.c_str()));
            SEXP req = Rf_allocVector(RAWSXP, (R_xlen_t) ev.request.size());
            SET_VECTOR_ELT(e, 3, req);
            std::memcpy(RAW(req), ev.request.data(), ev.request.size());
            SET_VECTOR_ELT(e, 4, grpc_pairs_to_r(ev.metadata));
            SET_VECTOR_ELT(e, 5, ev.deadline_ms < 0 ? Rf_ScalarReal(NA_REAL)
                                                    : Rf_ScalarReal(ev.deadline_ms));
        } else {
            SET_VECTOR_ELT(e, 2, R_NilValue);
            SET_VECTOR_ELT(e, 3, R_NilValue);
            SET_VECTOR_ELT(e, 4, R_NilValue);
            SET_VECTOR_ELT(e, 5, R_NilValue);
        }
        SET_VECTOR_ELT(out, i, e);
        UNPROTECT(1);
    }
    UNPROTECT(2);
    return out;
}
