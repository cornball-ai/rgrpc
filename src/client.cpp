// Generic asynchronous client: unary calls (increment 2) and streams
// (increment 6).
//
// Threading contract: one background thread per client drains the
// completion queue. It never calls the R API; completions are copied
// into plain C++ event structs under the mutex and an eventfd is
// signaled. R receives events in batches via grpc_r_client_poll on the
// main thread. Stream ops are posted while holding the mutex so a
// concurrently completing stream cannot be freed between a state check
// and the op post.
//
// Stream discipline (one outstanding op per direction, per gRPC rules):
// writes go through a bounded queue drained by write completions; reads
// are reposted continuously but pause when `unread` delivered-to-R
// backlog reaches read_cap, resuming as grpc_r_client_poll drains —
// bounded buffering with real HTTP/2 backpressure. Finish is posted
// only when no read/write op is in flight.

#include <chrono>
#include <cstdint>
#include <cstring>
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
#include <grpcpp/generic/generic_stub.h>
#include <grpcpp/support/byte_buffer.h>
#include <grpcpp/support/slice.h>

#include "common.h"

namespace {

typedef std::vector<std::pair<std::string, std::string>> md_pairs;

enum tag_kind {
    T_UNARY = 0,
    T_START,
    T_READ,
    T_WRITE,
    T_WDONE,
    T_FINISH
};

struct ctag {
    void *obj;
    int kind;
};

enum ev_kind {
    EV_UNARY = 0,
    EV_SMSG,
    EV_WRITABLE,
    EV_STATUS
};

struct cl_event {
    int kind = EV_UNARY;
    uint64_t id = 0;
    int status = 0;
    std::string message;
    bool has_bytes = false;
    std::string bytes;
    md_pairs initial_md;
    md_pairs trailing_md;
};

void copy_md(const std::multimap<grpc::string_ref, grpc::string_ref> &src,
             md_pairs *dst) {
    for (const auto &kv : src)
        dst->emplace_back(std::string(kv.first.data(), kv.first.size()),
                          std::string(kv.second.data(), kv.second.size()));
}

struct call_state {
    uint64_t id = 0;
    grpc::ClientContext context;
    grpc::ByteBuffer response;
    grpc::Status status;
    std::unique_ptr<grpc::GenericClientAsyncResponseReader> reader;
    ctag tag{this, T_UNARY};
};

struct stream_state {
    uint64_t id = 0;
    grpc::ClientContext context;
    std::unique_ptr<grpc::GenericClientAsyncReaderWriter> rw;
    grpc::ByteBuffer read_buf;
    grpc::Status status;

    int pending = 0;  // outstanding CQ tags
    bool started = false;
    bool read_inflight = false;
    bool write_inflight = false;
    bool wdone_inflight = false;
    bool wdone_requested = false;
    bool wdone_sent = false;
    bool reads_closed = false;
    bool broken = false;
    bool want_finish = false;
    bool finish_posted = false;
    bool status_emitted = false;

    int unread = 0;  // EV_SMSG events delivered to the ready deque, not yet polled
    int read_cap = 16;
    size_t write_cap = 16;
    std::deque<grpc::ByteBuffer> write_queue;

    ctag t_start{this, T_START};
    ctag t_read{this, T_READ};
    ctag t_write{this, T_WRITE};
    ctag t_wdone{this, T_WDONE};
    ctag t_finish{this, T_FINISH};
};

struct client {
    std::shared_ptr<grpc::Channel> channel;
    std::unique_ptr<grpc::GenericStub> stub;
    grpc::CompletionQueue cq;
    std::thread completer;
    int event_fd = -1;

    std::mutex mu;
    std::map<uint64_t, call_state *> calls;      // unary, until completed
    std::map<uint64_t, stream_state *> streams;  // until freed
    std::deque<cl_event> ready;
    uint64_t next_id = 1;
    bool closed = false;
    // Set (under mu) before cq.Shutdown(): suppresses every op post from
    // the completion thread. Starting an op on a shut-down CQ aborts
    // (grpc_cq_begin_op assertion); already-started ops drain fine.
    bool shutting = false;

    void signal() {
        uint64_t one = 1;
        ssize_t n = write(event_fd, &one, sizeof one);
        (void) n;
    }

    // ---- stream helpers; all run under mu ----

    void s_post_read_locked(stream_state *s) {
        if (shutting || s->read_inflight || s->reads_closed || s->broken ||
            s->finish_posted || !s->started)
            return;
        s->read_inflight = true;
        ++s->pending;
        s->rw->Read(&s->read_buf, &s->t_read);
    }

    void s_pump_writes_locked(stream_state *s) {
        if (shutting || !s->started || s->broken || s->finish_posted) return;
        if (!s->write_inflight && !s->write_queue.empty()) {
            s->write_inflight = true;
            ++s->pending;
            s->rw->Write(s->write_queue.front(), &s->t_write);
            return;
        }
        if (!s->write_inflight && s->write_queue.empty() &&
            s->wdone_requested && !s->wdone_sent && !s->wdone_inflight) {
            s->wdone_inflight = true;
            ++s->pending;
            s->rw->WritesDone(&s->t_wdone);
        }
    }

    void s_try_finish_locked(stream_state *s) {
        if (shutting || !s->want_finish || s->finish_posted) return;
        if (s->read_inflight || s->write_inflight || s->wdone_inflight) return;
        if (!s->started) return;
        s->finish_posted = true;
        ++s->pending;
        s->rw->Finish(&s->status, &s->t_finish);
    }

    void s_maybe_free_locked(stream_state *s) {
        if (s->pending == 0 && s->status_emitted) {
            streams.erase(s->id);
            delete s;
        }
    }

    // ---- completion thread; no R API calls ----

    void run() {
        void *raw = nullptr;
        bool ok = false;
        while (cq.Next(&raw, &ok)) {
            auto *t = static_cast<ctag *>(raw);
            std::lock_guard<std::mutex> lock(mu);
            if (t->kind == T_UNARY) {
                auto *cs = static_cast<call_state *>(t->obj);
                cl_event ev;
                ev.kind = EV_UNARY;
                ev.id = cs->id;
                ev.status = (int) cs->status.error_code();
                ev.message = cs->status.error_message();
                if (cs->status.ok()) {
                    ev.has_bytes =
                        grpc_byte_buffer_to_string(cs->response, &ev.bytes);
                }
                copy_md(cs->context.GetServerInitialMetadata(), &ev.initial_md);
                copy_md(cs->context.GetServerTrailingMetadata(), &ev.trailing_md);
                ready.push_back(std::move(ev));
                calls.erase(cs->id);
                delete cs;
                signal();
                continue;
            }

            auto *s = static_cast<stream_state *>(t->obj);
            --s->pending;
            switch (t->kind) {
            case T_START:
                if (ok) {
                    s->started = true;
                    s_post_read_locked(s);
                    s_pump_writes_locked(s);
                } else {
                    // The call never started; Finish still yields status.
                    s->broken = true;
                    s->started = true;  // Finish is now legal
                    s->want_finish = true;
                }
                s_try_finish_locked(s);
                break;
            case T_READ:
                s->read_inflight = false;
                if (ok && !s->broken) {
                    cl_event ev;
                    ev.kind = EV_SMSG;
                    ev.id = s->id;
                    ev.has_bytes =
                        grpc_byte_buffer_to_string(s->read_buf, &ev.bytes);
                    ready.push_back(std::move(ev));
                    ++s->unread;
                    signal();
                    if (s->unread < s->read_cap) s_post_read_locked(s);
                } else {
                    s->reads_closed = true;
                    s->want_finish = true;
                }
                s_try_finish_locked(s);
                break;
            case T_WRITE:
                s->write_inflight = false;
                if (ok && !s->broken) {
                    s->write_queue.pop_front();
                    if (s->write_queue.empty()) {
                        cl_event ev;
                        ev.kind = EV_WRITABLE;
                        ev.id = s->id;
                        ready.push_back(std::move(ev));
                        signal();
                    }
                    s_pump_writes_locked(s);
                } else {
                    s->broken = true;
                    s->want_finish = true;
                }
                s_try_finish_locked(s);
                break;
            case T_WDONE:
                s->wdone_inflight = false;
                s->wdone_sent = true;
                if (!ok) {
                    s->broken = true;
                    s->want_finish = true;
                }
                s_try_finish_locked(s);
                break;
            case T_FINISH: {
                cl_event ev;
                ev.kind = EV_STATUS;
                ev.id = s->id;
                ev.status = (int) s->status.error_code();
                ev.message = s->status.error_message();
                copy_md(s->context.GetServerInitialMetadata(), &ev.initial_md);
                copy_md(s->context.GetServerTrailingMetadata(), &ev.trailing_md);
                ready.push_back(std::move(ev));
                s->status_emitted = true;
                signal();
                break;
            }
            default:
                break;
            }
            s_maybe_free_locked(s);
        }
    }
};

void client_shutdown(client *c) {
    if (c->closed) return;
    c->closed = true;
    {
        std::lock_guard<std::mutex> lock(c->mu);
        c->shutting = true;
        for (auto &kv : c->calls) kv.second->context.TryCancel();
        for (auto &kv : c->streams) kv.second->context.TryCancel();
    }
    c->cq.Shutdown();
    if (c->completer.joinable()) c->completer.join();
    for (auto &kv : c->calls) delete kv.second;
    c->calls.clear();
    for (auto &kv : c->streams) delete kv.second;
    c->streams.clear();
    c->ready.clear();
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

void apply_context_opts(grpc::ClientContext *ctx, SEXP deadline_ms,
                        SEXP metadata, SEXP wait_for_ready) {
    if (deadline_ms != R_NilValue) {
        double ms = Rf_asReal(deadline_ms);
        ctx->set_deadline(std::chrono::system_clock::now() +
                          std::chrono::milliseconds((int64_t) ms));
    }
    if (metadata != R_NilValue) {
        SEXP names = Rf_getAttrib(metadata, R_NamesSymbol);
        for (R_xlen_t i = 0; i < Rf_xlength(metadata); ++i) {
            ctx->AddMetadata(Rf_translateCharUTF8(STRING_ELT(names, i)),
                             Rf_translateCharUTF8(STRING_ELT(metadata, i)));
        }
    }
    ctx->set_wait_for_ready(Rf_asLogical(wait_for_ready) == TRUE);
}

SEXP md_pairs_or_null(const md_pairs &md) {
    return grpc_pairs_to_r(md);
}

std::string chr_or_empty(SEXP x) {
    if (x == R_NilValue) return std::string();
    return std::string(Rf_translateCharUTF8(STRING_ELT(x, 0)));
}

}  // namespace

// args: target, tls (lgl), ca/cert/key PEM strings (chr or NULL),
//       target_name_override (chr or NULL)
extern "C" SEXP grpc_r_client_create(SEXP target, SEXP tls, SEXP ca,
                                     SEXP cert, SEXP key, SEXP override_,
                                     SEXP keepalive_ms,
                                     SEXP keepalive_timeout_ms) {
    const char *tgt = Rf_translateCharUTF8(STRING_ELT(target, 0));
    int fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (fd < 0) Rf_error("eventfd creation failed");
    auto *c = new client();
    c->event_fd = fd;
    std::shared_ptr<grpc::ChannelCredentials> creds;
    if (Rf_asLogical(tls) == TRUE) {
        grpc::SslCredentialsOptions opts;
        opts.pem_root_certs = chr_or_empty(ca);
        opts.pem_private_key = chr_or_empty(key);
        opts.pem_cert_chain = chr_or_empty(cert);
        creds = grpc::SslCredentials(opts);
    } else {
        creds = grpc::InsecureChannelCredentials();
    }
    grpc::ChannelArguments args;
    std::string ov = chr_or_empty(override_);
    if (!ov.empty()) args.SetSslTargetNameOverride(ov);
    if (keepalive_ms != R_NilValue) {
        args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, Rf_asInteger(keepalive_ms));
        // Without these two, HTTP/2 ping policing silences keepalive on
        // a quiet connection: pings stop after 2 without payload data,
        // and none are sent at all with no active call.
        args.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);
        args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
    }
    if (keepalive_timeout_ms != R_NilValue) {
        args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS,
                    Rf_asInteger(keepalive_timeout_ms));
    }
    c->channel = grpc::CreateCustomChannel(tgt, creds, args);
    c->stub.reset(new grpc::GenericStub(c->channel));
    c->completer = std::thread([c]() { c->run(); });
    SEXP xp = PROTECT(R_MakeExternalPtr(c, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(xp, client_finalizer, TRUE);
    UNPROTECT(1);
    return xp;
}

extern "C" SEXP grpc_r_client_state(SEXP xp) {
    client *c = get_client(xp);
    // try_to_connect = false: observe, don't poke
    return Rf_ScalarInteger((int) c->channel->GetState(false));
}

extern "C" SEXP grpc_r_client_close(SEXP xp) {
    client_finalizer(xp);
    return R_NilValue;
}

extern "C" SEXP grpc_r_client_fd(SEXP xp) {
    return Rf_ScalarInteger(get_client(xp)->event_fd);
}

extern "C" SEXP grpc_r_client_pending(SEXP xp) {
    client *c = get_client(xp);
    std::lock_guard<std::mutex> lock(c->mu);
    return Rf_ScalarInteger((int) (c->calls.size() + c->streams.size()));
}

// args: client, method, request (raw), deadline_ms, metadata, wait_for_ready
extern "C" SEXP grpc_r_call_start(SEXP xp, SEXP method, SEXP request,
                                  SEXP deadline_ms, SEXP metadata,
                                  SEXP wait_for_ready) {
    client *c = get_client(xp);
    const char *m = Rf_translateCharUTF8(STRING_ELT(method, 0));

    auto *cs = new call_state();
    apply_context_opts(&cs->context, deadline_ms, metadata, wait_for_ready);

    grpc::Slice slice(RAW(request), (size_t) Rf_xlength(request));
    grpc::ByteBuffer req(&slice, 1);

    {
        std::lock_guard<std::mutex> lock(c->mu);
        cs->id = c->next_id++;
        c->calls[cs->id] = cs;
        cs->reader = c->stub->PrepareUnaryCall(&cs->context, m, req, &c->cq);
        cs->reader->StartCall();
        cs->reader->Finish(&cs->response, &cs->status, &cs->tag);
    }
    return Rf_ScalarReal((double) cs->id);
}

extern "C" SEXP grpc_r_call_cancel(SEXP xp, SEXP id) {
    client *c = get_client(xp);
    const uint64_t want = (uint64_t) Rf_asReal(id);
    std::lock_guard<std::mutex> lock(c->mu);
    auto it = c->calls.find(want);
    if (it != c->calls.end()) it->second->context.TryCancel();
    auto st = c->streams.find(want);
    if (st != c->streams.end()) st->second->context.TryCancel();
    return R_NilValue;
}

// args: client, method, deadline_ms, metadata, wait_for_ready,
//       read_cap (int), write_cap (int)
extern "C" SEXP grpc_r_stream_start(SEXP xp, SEXP method, SEXP deadline_ms,
                                    SEXP metadata, SEXP wait_for_ready,
                                    SEXP read_cap, SEXP write_cap) {
    client *c = get_client(xp);
    const char *m = Rf_translateCharUTF8(STRING_ELT(method, 0));

    auto *s = new stream_state();
    apply_context_opts(&s->context, deadline_ms, metadata, wait_for_ready);
    s->read_cap = Rf_asInteger(read_cap);
    s->write_cap = (size_t) Rf_asInteger(write_cap);

    {
        std::lock_guard<std::mutex> lock(c->mu);
        s->id = c->next_id++;
        c->streams[s->id] = s;
        s->rw = c->stub->PrepareCall(&s->context, m, &c->cq);
        ++s->pending;
        s->rw->StartCall(&s->t_start);
    }
    return Rf_ScalarReal((double) s->id);
}

// args: client, id, bytes (raw). Returns TRUE if queued, FALSE if the
// write queue is full or the stream cannot accept writes.
extern "C" SEXP grpc_r_stream_send(SEXP xp, SEXP id, SEXP bytes) {
    client *c = get_client(xp);
    const uint64_t want = (uint64_t) Rf_asReal(id);
    std::lock_guard<std::mutex> lock(c->mu);
    auto it = c->streams.find(want);
    if (it == c->streams.end()) return Rf_ScalarLogical(FALSE);
    stream_state *s = it->second;
    if (s->broken || s->finish_posted || s->wdone_requested)
        return Rf_ScalarLogical(FALSE);
    if (s->write_queue.size() >= s->write_cap) return Rf_ScalarLogical(FALSE);
    grpc::Slice slice(RAW(bytes), (size_t) Rf_xlength(bytes));
    s->write_queue.emplace_back(&slice, 1);
    c->s_pump_writes_locked(s);
    return Rf_ScalarLogical(TRUE);
}

extern "C" SEXP grpc_r_stream_writes_done(SEXP xp, SEXP id) {
    client *c = get_client(xp);
    const uint64_t want = (uint64_t) Rf_asReal(id);
    std::lock_guard<std::mutex> lock(c->mu);
    auto it = c->streams.find(want);
    if (it == c->streams.end()) return Rf_ScalarLogical(FALSE);
    stream_state *s = it->second;
    if (s->wdone_requested) return Rf_ScalarLogical(FALSE);
    s->wdone_requested = true;
    c->s_pump_writes_locked(s);
    return Rf_ScalarLogical(TRUE);
}

// args: client, max_events (int), timeout_ms (int)
extern "C" SEXP grpc_r_client_poll(SEXP xp, SEXP max_events, SEXP timeout_ms) {
    client *c = get_client(xp);
    const int maxn = Rf_asInteger(max_events);
    const int timeout = Rf_asInteger(timeout_ms);

    {
        std::unique_lock<std::mutex> lock(c->mu);
        const bool empty = c->ready.empty();
        lock.unlock();
        if (empty && timeout != 0) {
            struct pollfd pfd = {c->event_fd, POLLIN, 0};
            poll(&pfd, 1, timeout);
        }
    }
    uint64_t drained;
    while (read(c->event_fd, &drained, sizeof drained) > 0) {
    }

    std::vector<cl_event> batch;
    {
        std::lock_guard<std::mutex> lock(c->mu);
        while (!c->ready.empty() && (int) batch.size() < maxn) {
            cl_event ev = std::move(c->ready.front());
            c->ready.pop_front();
            if (ev.kind == EV_SMSG) {
                auto it = c->streams.find(ev.id);
                if (it != c->streams.end()) {
                    stream_state *s = it->second;
                    --s->unread;
                    if (s->unread < s->read_cap) c->s_post_read_locked(s);
                }
            }
            batch.push_back(std::move(ev));
        }
    }

    static const char *kind_names[] = {"unary", "stream_msg",
                                       "stream_writable", "stream_status"};
    const R_xlen_t n = (R_xlen_t) batch.size();
    SEXP out = PROTECT(Rf_allocVector(VECSXP, n));
    const char *fields[] = {"kind",     "id",       "status",
                            "message",  "response", "initial_metadata",
                            "trailing_metadata"};
    SEXP field_names = PROTECT(Rf_allocVector(STRSXP, 7));
    for (int j = 0; j < 7; ++j) SET_STRING_ELT(field_names, j, Rf_mkChar(fields[j]));

    for (R_xlen_t i = 0; i < n; ++i) {
        const cl_event &ev = batch[i];
        SEXP e = PROTECT(Rf_allocVector(VECSXP, 7));
        Rf_setAttrib(e, R_NamesSymbol, field_names);
        SET_VECTOR_ELT(e, 0, Rf_mkString(kind_names[ev.kind]));
        SET_VECTOR_ELT(e, 1, Rf_ScalarReal((double) ev.id));
        if (ev.kind == EV_UNARY || ev.kind == EV_STATUS) {
            SET_VECTOR_ELT(e, 2, Rf_ScalarInteger(ev.status));
            SET_VECTOR_ELT(e, 3, Rf_mkString(ev.message.c_str()));
            SET_VECTOR_ELT(e, 5, md_pairs_or_null(ev.initial_md));
            SET_VECTOR_ELT(e, 6, md_pairs_or_null(ev.trailing_md));
        } else {
            SET_VECTOR_ELT(e, 2, R_NilValue);
            SET_VECTOR_ELT(e, 3, R_NilValue);
            SET_VECTOR_ELT(e, 5, R_NilValue);
            SET_VECTOR_ELT(e, 6, R_NilValue);
        }
        if (ev.has_bytes) {
            SEXP raw = Rf_allocVector(RAWSXP, (R_xlen_t) ev.bytes.size());
            SET_VECTOR_ELT(e, 4, raw);
            std::memcpy(RAW(raw), ev.bytes.data(), ev.bytes.size());
        } else {
            SET_VECTOR_ELT(e, 4, R_NilValue);
        }
        SET_VECTOR_ELT(out, i, e);
        UNPROTECT(1);
    }
    UNPROTECT(2);
    return out;
}
