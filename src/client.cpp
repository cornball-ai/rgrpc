// Generic asynchronous unary client (increment 2).
//
// Threading contract: one background thread per client drains the
// completion queue. It never calls the R API; it moves completed call
// states onto a mutex-guarded ready deque and signals an eventfd. R
// receives completions in batches via grpc_r_client_poll on the main
// thread. The eventfd is exposed to R so an event loop (e.g.
// later::later_fd) can wake on completions instead of spinning.

#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <grpcpp/grpcpp.h>
#include <grpcpp/generic/generic_stub.h>
#include <grpcpp/support/byte_buffer.h>
#include <grpcpp/support/slice.h>

#include "common.h"

namespace {

struct call_state {
    uint64_t id = 0;
    grpc::ClientContext context;
    grpc::ByteBuffer response;
    grpc::Status status;
    std::unique_ptr<grpc::GenericClientAsyncResponseReader> reader;
};

struct client {
    std::shared_ptr<grpc::Channel> channel;
    std::unique_ptr<grpc::GenericStub> stub;
    grpc::CompletionQueue cq;
    std::thread completer;
    int event_fd = -1;

    std::mutex mu;
    std::map<uint64_t, call_state *> in_flight;  // owns until polled
    std::deque<call_state *> ready;              // completed, not yet polled
    uint64_t next_id = 1;
    bool closed = false;

    // Background completion thread: no R API calls allowed here.
    void run() {
        void *tag = nullptr;
        bool ok = false;
        while (cq.Next(&tag, &ok)) {
            auto *cs = static_cast<call_state *>(tag);
            {
                std::lock_guard<std::mutex> lock(mu);
                ready.push_back(cs);
            }
            uint64_t one = 1;
            ssize_t n = write(event_fd, &one, sizeof one);
            (void) n;
        }
    }
};

void client_shutdown(client *c) {
    if (c->closed) return;
    c->closed = true;
    {
        std::lock_guard<std::mutex> lock(c->mu);
        for (auto &kv : c->in_flight) kv.second->context.TryCancel();
    }
    c->cq.Shutdown();
    if (c->completer.joinable()) c->completer.join();
    for (auto &kv : c->in_flight) delete kv.second;
    c->in_flight.clear();
    c->ready.clear();  // ready states were also in in_flight
    if (c->event_fd >= 0) {
        close(c->event_fd);
        c->event_fd = -1;
    }
}

void client_finalizer(SEXP xp) {
    auto *c = static_cast<client *>(R_ExternalPtrAddr(xp));
    if (c == nullptr) return;
    client_shutdown(c);
    delete c;
    R_ClearExternalPtr(xp);
}

client *get_client(SEXP xp) {
    auto *c = static_cast<client *>(R_ExternalPtrAddr(xp));
    if (c == nullptr || c->closed) Rf_error("grpc client is closed");
    return c;
}

}  // namespace

extern "C" SEXP grpc_r_client_create(SEXP target) {
    const char *tgt = Rf_translateCharUTF8(STRING_ELT(target, 0));
    int fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (fd < 0) Rf_error("eventfd creation failed");
    auto *c = new client();
    c->event_fd = fd;
    c->channel = grpc::CreateChannel(tgt, grpc::InsecureChannelCredentials());
    c->stub.reset(new grpc::GenericStub(c->channel));
    c->completer = std::thread([c]() { c->run(); });
    SEXP xp = PROTECT(R_MakeExternalPtr(c, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(xp, client_finalizer, TRUE);
    UNPROTECT(1);
    return xp;
}

extern "C" SEXP grpc_r_client_close(SEXP xp) {
    client_finalizer(xp);
    return R_NilValue;
}

extern "C" SEXP grpc_r_client_fd(SEXP xp) {
    return Rf_ScalarInteger(get_client(xp)->event_fd);
}

// args: client, method (chr), request (raw), deadline_ms (dbl or NULL),
//       metadata (named chr or NULL), wait_for_ready (lgl)
extern "C" SEXP grpc_r_call_start(SEXP xp, SEXP method, SEXP request,
                                  SEXP deadline_ms, SEXP metadata,
                                  SEXP wait_for_ready) {
    client *c = get_client(xp);
    const char *m = Rf_translateCharUTF8(STRING_ELT(method, 0));

    auto *cs = new call_state();
    if (deadline_ms != R_NilValue) {
        double ms = Rf_asReal(deadline_ms);
        cs->context.set_deadline(std::chrono::system_clock::now() +
                                 std::chrono::milliseconds((int64_t) ms));
    }
    if (metadata != R_NilValue) {
        SEXP names = Rf_getAttrib(metadata, R_NamesSymbol);
        for (R_xlen_t i = 0; i < Rf_xlength(metadata); ++i) {
            cs->context.AddMetadata(
                Rf_translateCharUTF8(STRING_ELT(names, i)),
                Rf_translateCharUTF8(STRING_ELT(metadata, i)));
        }
    }
    cs->context.set_wait_for_ready(Rf_asLogical(wait_for_ready) == TRUE);

    grpc::Slice slice(RAW(request), (size_t) Rf_xlength(request));
    grpc::ByteBuffer req(&slice, 1);

    {
        std::lock_guard<std::mutex> lock(c->mu);
        cs->id = c->next_id++;
        c->in_flight[cs->id] = cs;
    }
    cs->reader = c->stub->PrepareUnaryCall(&cs->context, m, req, &c->cq);
    cs->reader->StartCall();
    cs->reader->Finish(&cs->response, &cs->status, cs);

    return Rf_ScalarReal((double) cs->id);
}

extern "C" SEXP grpc_r_call_cancel(SEXP xp, SEXP id) {
    client *c = get_client(xp);
    const uint64_t want = (uint64_t) Rf_asReal(id);
    std::lock_guard<std::mutex> lock(c->mu);
    auto it = c->in_flight.find(want);
    if (it != c->in_flight.end()) it->second->context.TryCancel();
    return R_NilValue;
}

// args: client, max_events (int), timeout_ms (int)
extern "C" SEXP grpc_r_client_poll(SEXP xp, SEXP max_events, SEXP timeout_ms) {
    client *c = get_client(xp);
    const int maxn = Rf_asInteger(max_events);
    const int timeout = Rf_asInteger(timeout_ms);

    // Optionally wait for the completion thread's wakeup signal. The
    // eventfd is drained here, not in the completion thread, so a signal
    // can never be lost between check and wait.
    {
        std::unique_lock<std::mutex> lock(c->mu);
        if (c->ready.empty() && timeout != 0) {
            lock.unlock();
            struct pollfd pfd = {c->event_fd, POLLIN, 0};
            poll(&pfd, 1, timeout);
        } else {
            lock.unlock();
        }
    }
    uint64_t drained;
    while (read(c->event_fd, &drained, sizeof drained) > 0) {
    }

    // Detach up to maxn completed calls, then build R objects with no
    // lock held (allocation can longjmp on error).
    std::vector<call_state *> batch;
    {
        std::lock_guard<std::mutex> lock(c->mu);
        while (!c->ready.empty() && (int) batch.size() < maxn) {
            call_state *cs = c->ready.front();
            c->ready.pop_front();
            c->in_flight.erase(cs->id);
            batch.push_back(cs);
        }
    }

    const R_xlen_t n = (R_xlen_t) batch.size();
    SEXP out = PROTECT(Rf_allocVector(VECSXP, n));
    const char *fields[] = {"id",      "status",           "message",
                            "response", "initial_metadata", "trailing_metadata"};
    SEXP field_names = PROTECT(Rf_allocVector(STRSXP, 6));
    for (int j = 0; j < 6; ++j) SET_STRING_ELT(field_names, j, Rf_mkChar(fields[j]));

    for (R_xlen_t i = 0; i < n; ++i) {
        call_state *cs = batch[i];
        SEXP ev = PROTECT(Rf_allocVector(VECSXP, 6));
        Rf_setAttrib(ev, R_NamesSymbol, field_names);
        SET_VECTOR_ELT(ev, 0, Rf_ScalarReal((double) cs->id));
        SET_VECTOR_ELT(ev, 1, Rf_ScalarInteger((int) cs->status.error_code()));
        SET_VECTOR_ELT(ev, 2, Rf_mkString(cs->status.error_message().c_str()));
        SET_VECTOR_ELT(ev, 3, cs->status.ok() ? grpc_byte_buffer_to_raw(cs->response)
                                              : R_NilValue);
        SET_VECTOR_ELT(ev, 4, grpc_metadata_to_r(cs->context.GetServerInitialMetadata()));
        SET_VECTOR_ELT(ev, 5, grpc_metadata_to_r(cs->context.GetServerTrailingMetadata()));
        SET_VECTOR_ELT(out, i, ev);
        UNPROTECT(1);
        delete cs;
        batch[i] = nullptr;
    }
    UNPROTECT(2);
    return out;
}

extern "C" SEXP grpc_r_client_pending(SEXP xp) {
    client *c = get_client(xp);
    std::lock_guard<std::mutex> lock(c->mu);
    return Rf_ScalarInteger((int) c->in_flight.size());
}
