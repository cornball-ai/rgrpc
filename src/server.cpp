// Generic asynchronous server: unary request/reply (increment 3) and
// streaming (increment 6).
//
// Same threading contract as the client: one background thread drains
// the server completion queue and never calls the R API. Events are
// fully copied into plain C++ structs on the completion thread, so
// event delivery to R is independent of call-state lifetime. All call
// state transitions happen under the server mutex; RPC ops are posted
// while holding it so a concurrently completing call cannot be freed
// between the state check and the op post.
//
// Streaming model: the first inbound message becomes the "request"
// event, as in the unary case. Further inbound messages are pulled
// explicitly — R posts one read at a time via grpc_r_server2_read and
// receives "stream_msg" events, or "client_done" when the peer
// half-closes. Outbound messages go through a bounded write queue
// (grpc_r_server2_send) drained by write completions, with a
// "stream_writable" event when the queue empties; the terminal status
// (reply or finish) is deferred until queued writes drain.
//
// Backpressure: at most accept_window RequestCall slots are outstanding,
// and no new slot is posted while active calls + outstanding slots would
// exceed max_active.

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

#include <grpcpp/grpcpp.h>
#include <grpcpp/generic/async_generic_service.h>

#include "common.h"
#include "wake.h"

namespace {

struct sv_call;

enum class svop { accept, read, write, done, finish };

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
    bool replied = false;  // terminal op requested (reply or finish)
    bool dead = false;
    bool read_inflight = false;
    bool write_inflight = false;
    bool finish_posted = false;

    std::deque<grpc::ByteBuffer> write_queue;
    size_t write_cap = 16;
    bool has_final_write = false;
    grpc::ByteBuffer final_write;
    grpc::Status final_status;

    sv_tag t_accept{this, svop::accept};
    sv_tag t_read{this, svop::read};
    sv_tag t_write{this, svop::write};
    sv_tag t_done{this, svop::done};
    sv_tag t_finish{this, svop::finish};
};

// type: 0 request, 1 cancelled, 2 stream_msg, 3 client_done, 4 stream_writable
struct sv_event {
    int type = 0;
    uint64_t id = 0;
    std::string method;
    std::string request;
    std::vector<std::pair<std::string, std::string>> metadata;
    double deadline_ms = -1;
    std::string peer;
    std::vector<std::string> identity;
};

struct rserver {
    grpc::AsyncGenericService generic;
    std::unique_ptr<grpc::ServerCompletionQueue> cq;
    std::unique_ptr<grpc::Server> server;
    std::thread completer;
    wake_handle wake;
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

    void signal() { wake_signal(wake); }

    void push_event_locked(sv_event ev) {
        ready.push_back(std::move(ev));
        signal();
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

    // Drain the write queue one op at a time; when it is empty and a
    // terminal was requested, post it (WriteAndFinish or Finish).
    void pump_locked(sv_call *c) {
        if (shutting || c->dead || c->finish_posted) return;
        if (c->write_inflight) return;
        if (!c->write_queue.empty()) {
            c->write_inflight = true;
            ++c->pending;
            c->stream.Write(c->write_queue.front(), &c->t_write);
            return;
        }
        if (c->replied) {
            c->finish_posted = true;
            ++c->pending;
            if (c->has_final_write) {
                c->stream.WriteAndFinish(c->final_write, grpc::WriteOptions(),
                                         c->final_status, &c->t_finish);
            } else {
                c->stream.Finish(c->final_status, &c->t_finish);
            }
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
                    ++c->pending;
                    if (shutting) {
                        // No ops on a shutting CQ; the done tag still
                        // fires (the RPC is cancelled) and frees the slot.
                        c->dead = true;
                    } else {
                        ++c->pending;
                        c->read_inflight = true;
                        c->stream.Read(&c->request, &c->t_read);
                    }
                    post_accepts_locked();
                } else {
                    maybe_free_locked(c);  // shutdown: slot never matched
                }
                break;
            case svop::read:
                c->read_inflight = false;
                if (ok && !c->dead) {
                    sv_event ev;
                    ev.id = c->id;
                    grpc_byte_buffer_to_string(c->request, &ev.request);
                    if (!c->delivered) {
                        c->delivered = true;
                        ev.type = 0;
                        ev.method = c->ctx.method();
                        for (const auto &kv : c->ctx.client_metadata())
                            ev.metadata.emplace_back(
                                std::string(kv.first.data(), kv.first.size()),
                                std::string(kv.second.data(), kv.second.size()));
                        auto dl = c->ctx.deadline();
                        auto ms =
                            std::chrono::duration_cast<std::chrono::milliseconds>(
                                dl - std::chrono::system_clock::now())
                                .count();
                        ev.deadline_ms =
                            (ms > 0 && ms < 31536000000LL) ? (double) ms : -1;
                        ev.peer = c->ctx.peer();
                        auto auth = c->ctx.auth_context();
                        if (auth) {
                            for (const auto &idv : auth->GetPeerIdentity())
                                ev.identity.emplace_back(idv.data(),
                                                         idv.size());
                        }
                    } else {
                        ev.type = 2;  // stream_msg
                    }
                    push_event_locked(std::move(ev));
                } else if (c->delivered && !c->dead) {
                    sv_event ev;
                    ev.type = 3;  // client_done (peer half-closed)
                    ev.id = c->id;
                    push_event_locked(std::move(ev));
                } else {
                    c->dead = true;
                }
                maybe_free_locked(c);
                break;
            case svop::write:
                c->write_inflight = false;
                if (ok && !c->dead) {
                    c->write_queue.pop_front();
                    if (c->write_queue.empty() && !c->replied) {
                        sv_event ev;
                        ev.type = 4;  // stream_writable
                        ev.id = c->id;
                        push_event_locked(std::move(ev));
                    }
                    pump_locked(c);
                } else {
                    c->dead = true;
                }
                maybe_free_locked(c);
                break;
            case svop::done:
                if (c->delivered && !c->replied) {
                    c->dead = true;
                    sv_event ev;
                    ev.type = 1;  // cancelled
                    ev.id = c->id;
                    push_event_locked(std::move(ev));
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
    wake_close(&s->wake);
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

// args: address (chr), accept_window (int), max_active (int), tls (lgl),
//       ca/cert/key PEM strings (chr or NULL), require_client (lgl)
extern "C" SEXP grpc_r_server2_create(SEXP address, SEXP accept_window,
                                      SEXP max_active, SEXP tls, SEXP ca,
                                      SEXP cert, SEXP key,
                                      SEXP require_client, SEXP keepalive_ms,
                                      SEXP keepalive_timeout_ms,
                                      SEXP min_ping_interval_ms) {
    const char *addr = Rf_translateCharUTF8(STRING_ELT(address, 0));
    auto *s = new rserver();
    if (!wake_open(&s->wake)) {
        delete s;
        Rf_error("wake handle creation failed");
    }
    s->accept_window = Rf_asInteger(accept_window);
    s->max_active = Rf_asInteger(max_active);
    std::shared_ptr<grpc::ServerCredentials> creds;
    if (Rf_asLogical(tls) == TRUE) {
        grpc::SslServerCredentialsOptions opts(
            Rf_asLogical(require_client) == TRUE
                ? GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY
                : GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE);
        auto pem = [](SEXP x) {
            return x == R_NilValue
                       ? std::string()
                       : std::string(Rf_translateCharUTF8(STRING_ELT(x, 0)));
        };
        opts.pem_root_certs = pem(ca);
        grpc::SslServerCredentialsOptions::PemKeyCertPair pair;
        pair.private_key = pem(key);
        pair.cert_chain = pem(cert);
        opts.pem_key_cert_pairs.push_back(pair);
        creds = grpc::SslServerCredentials(opts);
    } else {
        creds = grpc::InsecureServerCredentials();
    }
    grpc::ServerBuilder builder;
    if (keepalive_ms != R_NilValue) {
        builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIME_MS,
                                   Rf_asInteger(keepalive_ms));
        // Same ping-policing overrides as the client: keepalive on a
        // quiet connection needs pings without data and without calls.
        builder.AddChannelArgument(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);
        builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS,
                                   1);
    }
    if (keepalive_timeout_ms != R_NilValue) {
        builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIMEOUT_MS,
                                   Rf_asInteger(keepalive_timeout_ms));
    }
    if (min_ping_interval_ms != R_NilValue) {
        // Tolerance for the peer's pings. The gRPC default is 5 minutes
        // with 2 ping strikes allowed, so a client pinging every few
        // seconds without payload data is killed with a too_many_pings
        // GOAWAY unless this is lowered to at most its ping interval.
        builder.AddChannelArgument(
            GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS,
            Rf_asInteger(min_ping_interval_ms));
    }
    builder.AddListeningPort(addr, creds, &s->port);
    builder.RegisterAsyncGenericService(&s->generic);
    s->cq = builder.AddCompletionQueue();
    s->server = builder.BuildAndStart();
    if (!s->server) {
        wake_close(&s->wake);
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
    return Rf_ScalarInteger(wake_fd(get_server(xp)->wake));
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

    if (code == 0) {
        grpc::Slice slice(RAW(response), (size_t) Rf_xlength(response));
        c->final_write = grpc::ByteBuffer(&slice, 1);
        c->has_final_write = true;
        c->final_status = grpc::Status::OK;
    } else {
        const char *msg = Rf_translateCharUTF8(STRING_ELT(message, 0));
        c->has_final_write = false;
        c->final_status =
            grpc::Status(static_cast<grpc::StatusCode>(code), msg);
    }
    s->pump_locked(c);
    return Rf_ScalarLogical(TRUE);
}

// args: server, id. Post one read; a "stream_msg" or "client_done" event
// follows. FALSE if a read is already in flight or the call is over.
extern "C" SEXP grpc_r_server2_read(SEXP xp, SEXP id) {
    rserver *s = get_server(xp);
    const uint64_t want = (uint64_t) Rf_asReal(id);
    std::lock_guard<std::mutex> lock(s->mu);
    auto it = s->active.find(want);
    if (it == s->active.end()) return Rf_ScalarLogical(FALSE);
    sv_call *c = it->second;
    if (!c->delivered || c->dead || c->read_inflight || c->finish_posted)
        return Rf_ScalarLogical(FALSE);
    c->read_inflight = true;
    ++c->pending;
    c->stream.Read(&c->request, &c->t_read);
    return Rf_ScalarLogical(TRUE);
}

// args: server, id. Hard-kill one call (TryCancel). The peer sees
// CANCELLED, not the application status — this is the escalation when
// even an abortive finish cannot get its status past a stalled peer's
// flow-control window. TRUE if the call was live.
extern "C" SEXP grpc_r_server2_cancel(SEXP xp, SEXP id) {
    rserver *s = get_server(xp);
    const uint64_t want = (uint64_t) Rf_asReal(id);
    std::lock_guard<std::mutex> lock(s->mu);
    auto it = s->active.find(want);
    if (it == s->active.end()) return Rf_ScalarLogical(FALSE);
    sv_call *c = it->second;
    if (c->dead) return Rf_ScalarLogical(FALSE);
    c->ctx.TryCancel();
    return Rf_ScalarLogical(TRUE);
}

// args: server, id, bytes (raw). TRUE if queued; FALSE if the queue is
// full or the call cannot accept writes.
extern "C" SEXP grpc_r_server2_send(SEXP xp, SEXP id, SEXP bytes) {
    rserver *s = get_server(xp);
    const uint64_t want = (uint64_t) Rf_asReal(id);
    std::lock_guard<std::mutex> lock(s->mu);
    auto it = s->active.find(want);
    if (it == s->active.end()) return Rf_ScalarLogical(FALSE);
    sv_call *c = it->second;
    if (!c->delivered || c->dead || c->replied) return Rf_ScalarLogical(FALSE);
    if (c->write_queue.size() >= c->write_cap) return Rf_ScalarLogical(FALSE);
    grpc::Slice slice(RAW(bytes), (size_t) Rf_xlength(bytes));
    c->write_queue.emplace_back(&slice, 1);
    s->pump_locked(c);
    return Rf_ScalarLogical(TRUE);
}

// args: server, id, status (int), message (chr), metadata, drain (lgl).
// Ends the stream: with drain, after queued writes; without, queued
// writes are discarded so the terminal status goes out first. TRUE if
// accepted.
extern "C" SEXP grpc_r_server2_finish(SEXP xp, SEXP id, SEXP status,
                                      SEXP message, SEXP metadata,
                                      SEXP drain) {
    rserver *s = get_server(xp);
    const uint64_t want = (uint64_t) Rf_asReal(id);
    const int code = Rf_asInteger(status);
    if (code != 0 && TYPEOF(message) != STRSXP)
        Rf_error("message must be a character string");

    std::lock_guard<std::mutex> lock(s->mu);
    auto it = s->active.find(want);
    if (it == s->active.end()) return Rf_ScalarLogical(FALSE);
    sv_call *c = it->second;
    if (!c->delivered || c->replied || c->dead) return Rf_ScalarLogical(FALSE);
    c->replied = true;
    if (Rf_asLogical(drain) != TRUE) {
        // Abortive close (fencing): discard queued writes so the
        // terminal status is not delayed behind them. A write already
        // posted to the CQ cannot be recalled, so the front element
        // stays until its completion tag; the added delay is bounded by
        // that one message (or by the peer's flow-control window if it
        // has stopped reading — escalate with cancel for a hard bound).
        if (c->write_inflight) {
            c->write_queue.erase(c->write_queue.begin() + 1,
                                 c->write_queue.end());
        } else {
            c->write_queue.clear();
        }
    }
    if (metadata != R_NilValue) {
        SEXP names = Rf_getAttrib(metadata, R_NamesSymbol);
        for (R_xlen_t i = 0; i < Rf_xlength(metadata); ++i) {
            c->ctx.AddTrailingMetadata(
                Rf_translateCharUTF8(STRING_ELT(names, i)),
                Rf_translateCharUTF8(STRING_ELT(metadata, i)));
        }
    }
    c->has_final_write = false;
    if (code == 0) {
        c->final_status = grpc::Status::OK;
    } else {
        const char *msg = Rf_translateCharUTF8(STRING_ELT(message, 0));
        c->final_status =
            grpc::Status(static_cast<grpc::StatusCode>(code), msg);
    }
    s->pump_locked(c);
    return Rf_ScalarLogical(TRUE);
}

// args: server, max_events (int), timeout_ms (int), only_id (real or NULL)
//
// The only_id filter mirrors grpc_r_client_poll: events for other calls
// are stepped over and left in `ready` in arrival order, and the wait is
// satisfied only by a matching event. See that function for why the
// wait cannot lean on the descriptor staying readable.
extern "C" SEXP grpc_r_server2_poll(SEXP xp, SEXP max_events, SEXP timeout_ms,
                                    SEXP only_id) {
    rserver *s = get_server(xp);
    const int maxn = Rf_asInteger(max_events);
    const int timeout = Rf_asInteger(timeout_ms);
    const bool filtered = only_id != R_NilValue;
    const uint64_t want = filtered ? (uint64_t) Rf_asReal(only_id) : 0;

    std::vector<sv_event> batch;
    {
        std::unique_lock<std::mutex> lock(s->mu);

        auto matches = [&](const sv_event &ev) {
            return !filtered || ev.id == want;
        };
        auto have_match = [&]() {
            for (const auto &ev : s->ready)
                if (matches(ev)) return true;
            return false;
        };

        if (timeout != 0) {
            const auto deadline =
                std::chrono::steady_clock::now() +
                std::chrono::milliseconds(timeout < 0 ? 0 : timeout);
            while (!have_match()) {
                wake_drain(s->wake);
                int wait_ms = -1;
                if (timeout > 0) {
                    const auto left = std::chrono::duration_cast<
                        std::chrono::milliseconds>(
                            deadline - std::chrono::steady_clock::now())
                                          .count();
                    if (left <= 0) break;
                    wait_ms = (int) left;
                }
                lock.unlock();
                const int pr = wake_poll(s->wake, wait_ms);
                lock.lock();
                if (pr <= 0) break;
            }
        }

        // Drain and re-arm under the same lock that guards `ready`; see
        // grpc_r_client_poll for why the drain cannot sit outside it.
        wake_drain(s->wake);
        auto it = s->ready.begin();
        while (it != s->ready.end() && (int) batch.size() < maxn) {
            if (!matches(*it)) {
                ++it;
                continue;
            }
            batch.push_back(std::move(*it));
            it = s->ready.erase(it);
        }
        if (!s->ready.empty()) s->signal();
    }

    static const char *type_names[] = {"request", "cancelled", "stream_msg",
                                       "client_done", "stream_writable"};
    const R_xlen_t n = (R_xlen_t) batch.size();
    SEXP out = PROTECT(Rf_allocVector(VECSXP, n));
    const char *fields[] = {"type",        "id",   "method",
                            "request",     "metadata", "deadline_ms",
                            "peer",        "peer_identity"};
    SEXP field_names = PROTECT(Rf_allocVector(STRSXP, 8));
    for (int j = 0; j < 8; ++j) SET_STRING_ELT(field_names, j, Rf_mkChar(fields[j]));

    for (R_xlen_t i = 0; i < n; ++i) {
        const sv_event &ev = batch[i];
        SEXP e = PROTECT(Rf_allocVector(VECSXP, 8));
        Rf_setAttrib(e, R_NamesSymbol, field_names);
        SET_VECTOR_ELT(e, 0, Rf_mkString(type_names[ev.type]));
        SET_VECTOR_ELT(e, 1, Rf_ScalarReal((double) ev.id));
        for (int j = 2; j < 8; ++j) SET_VECTOR_ELT(e, j, R_NilValue);
        if (ev.type == 0 || ev.type == 2) {
            if (ev.type == 0) {
                SET_VECTOR_ELT(e, 2, Rf_mkString(ev.method.c_str()));
                SET_VECTOR_ELT(e, 4, grpc_pairs_to_r(ev.metadata));
                SET_VECTOR_ELT(e, 5, ev.deadline_ms < 0
                                         ? Rf_ScalarReal(NA_REAL)
                                         : Rf_ScalarReal(ev.deadline_ms));
                SET_VECTOR_ELT(e, 6, Rf_mkString(ev.peer.c_str()));
                SEXP ids = Rf_allocVector(STRSXP, (R_xlen_t) ev.identity.size());
                SET_VECTOR_ELT(e, 7, ids);
                for (R_xlen_t k = 0; k < (R_xlen_t) ev.identity.size(); ++k)
                    SET_STRING_ELT(ids, k, Rf_mkChar(ev.identity[k].c_str()));
            }
            SEXP req = Rf_allocVector(RAWSXP, (R_xlen_t) ev.request.size());
            SET_VECTOR_ELT(e, 3, req);
            std::memcpy(RAW(req), ev.request.data(), ev.request.size());
        }
        SET_VECTOR_ELT(out, i, e);
        UNPROTECT(1);
    }
    UNPROTECT(2);
    return out;
}
