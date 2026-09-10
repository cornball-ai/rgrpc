#' gRPC status codes
#'
#' Named integer vector mapping gRPC status names to their wire codes.
#'
#' @export
grpc_status_codes <- c(OK = 0L, CANCELLED = 1L, UNKNOWN = 2L,
                       INVALID_ARGUMENT = 3L, DEADLINE_EXCEEDED = 4L,
                       NOT_FOUND = 5L, ALREADY_EXISTS = 6L,
                       PERMISSION_DENIED = 7L, RESOURCE_EXHAUSTED = 8L,
                       FAILED_PRECONDITION = 9L, ABORTED = 10L,
                       OUT_OF_RANGE = 11L, UNIMPLEMENTED = 12L,
                       INTERNAL = 13L, UNAVAILABLE = 14L, DATA_LOSS = 15L,
                       UNAUTHENTICATED = 16L)

#' Create an asynchronous gRPC client
#'
#' Opens a channel to \code{target} and starts the client's completion
#' machinery: a background thread that drains the gRPC completion queue
#' and signals a wake descriptor. The background thread never calls the
#' R API;
#' completions are received on the R main thread via \code{\link{grpc_poll}}.
#'
#' Keepalive: with \code{keepalive_ms} set, the client pings the peer
#' every \code{keepalive_ms} of transport inactivity and declares the
#' connection dead \code{keepalive_timeout_ms} after an unanswered ping
#' (gRPC's default timeout is 20000). Pings are enabled without payload
#' data and without active calls, so a quiet connection is genuinely
#' watched. The server must tolerate the cadence: see
#' \code{min_ping_interval_ms} in \code{\link{grpc_server}} — gRPC's
#' server default kills clients that ping more often than every 5
#' minutes.
#'
#' @section Forking:
#' A client must not be used across a \code{fork()}. The background
#' completion thread does not survive forking, so in the child there is
#' nothing left to drain the completion queue: calls are posted and
#' never complete. Worse, using an inherited client in the child also
#' breaks it in the \emph{parent} — the parent's subsequent calls fail
#' with \code{DEADLINE_EXCEEDED}. Setting
#' \code{GRPC_ENABLE_FORK_SUPPORT=1} does not change any of this.
#'
#' This matters because \code{parallel::mclapply} and much of the mirai
#' ecosystem fork. Measured behaviour (\code{tools/fork-probe.sh}):
#'
#' \itemize{
#'   \item Child uses the parent's client: the call never completes. A
#'     wait with a timeout returns empty; a wait without one does not
#'     return. Set \code{deadline_ms} and you get
#'     \code{DEADLINE_EXCEEDED} instead of a hang.
#'   \item Parent afterwards: broken, \code{DEADLINE_EXCEEDED}.
#'   \item Forking alone, with the child never touching the client:
#'     harmless, the parent keeps working.
#'   \item Child opens its \emph{own} client after the fork: works
#'     normally.
#' }
#'
#' So the rule is to open the client after forking, once per process,
#' and never to let a forked worker inherit one. Fork safety is a
#' non-goal; this is documented behaviour, not a defect to be fixed.
#'
#' @param target Server address, e.g. \code{"localhost:50051"} or
#'   \code{"unix:///run/containerd/containerd.sock"}.
#' @param credentials \code{NULL} for a plaintext channel, or a
#'   \code{\link{grpc_tls}} object.
#' @param keepalive_ms Interval of transport inactivity after which an
#'   HTTP/2 keepalive ping is sent. \code{NULL} (default) disables
#'   keepalive.
#' @param keepalive_timeout_ms Time to wait for a ping answer before
#'   the connection is declared dead.
#' @return An object of class \code{"grpc_client"}: a list holding the
#'   native channel handle (\code{ptr}) and the \code{target}. Pass it to
#'   \code{\link{grpc_call}}, \code{\link{grpc_stream}},
#'   \code{\link{grpc_poll}}, and \code{\link{grpc_close}}.
#' @examples
#' srv <- grpc_server("127.0.0.1:0")
#' cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))
#' grpc_state(cl)                          # "IDLE": nothing has connected yet
#'
#' ## the next request event; the server has one queue for every call, so
#' ## other events are stepped over, and 5 s of silence is an error
#' next_request <- function(srv) {
#'   repeat {
#'     evs <- grpc_poll(srv, timeout_ms = 5000L)
#'     if (!length(evs)) stop("no request within 5 s")
#'     for (ev in evs) if (ev$type == "request") return(ev)
#'   }
#' }
#'
#' call <- grpc_call(cl, "/demo.Echo/Say", as.raw(1:4), deadline_ms = 5000)
#'
#' ## answer it from the server half of the same process
#' req <- next_request(srv)
#' grpc_reply(req, req$request)
#'
#' ## completions arrive through grpc_poll(), or per call through grpc_await()
#' repeat {
#'   evs <- grpc_await(call, timeout_ms = 1000L)
#'   if (length(evs)) break
#' }
#' evs[[1]]$status_name
#' evs[[1]]$response
#'
#' grpc_close(cl)
#' grpc_close(srv)
#' @export
grpc_client <- function(target, credentials = NULL, keepalive_ms = NULL,
                        keepalive_timeout_ms = NULL) {
    stopifnot(is.character(target), length(target) == 1L)
    tls <- inherits(credentials, "grpc_tls")
    if (!is.null(credentials) && !tls) {
        stop("credentials must be a grpc_tls object")
    }
    ms <- function(x) {
        if (is.null(x)) {
            return(NULL)
        }
        stopifnot(is.numeric(x), length(x) == 1L, is.finite(x),
                  x == trunc(x), x >= 1, x <= .Machine$integer.max)
        as.integer(x)
    }
    xp <- .Call(grpc_r_client_create, target, tls, if (tls) credentials$ca,
        if (tls) credentials$cert, if (tls) credentials$key,
        if (tls) credentials$target_name_override, ms(keepalive_ms),
                ms(keepalive_timeout_ms))
    structure(list(ptr = xp, target = target,
                   calls = new.env(parent = emptyenv())),
              class = "grpc_client")
}

#' Start a unary call
#'
#' Starts an asynchronous unary RPC. The request is opaque bytes (a
#' serialized protocol buffer, e.g. from \code{RProtoBuf}'s
#' \code{serialize()}). The call completes via \code{\link{grpc_poll}}.
#'
#' Typed calls: when \code{method} is a \code{"grpc_method"} (from
#' \code{\link{grpc_method}}) and \code{request} is an \code{RProtoBuf}
#' \code{Message}, the request type is validated against the method's
#' \code{input_type} before sending, and the completion delivered by
#' \code{\link{grpc_poll}} carries the decoded response as
#' \code{response_message}. Streaming methods are refused here; open
#' them with \code{\link{grpc_stream}}.
#'
#' @param client A \code{"grpc_client"} object.
#' @param method Full method path, e.g.
#'   \code{"/runtime.v1.RuntimeService/Version"}, or a
#'   \code{"grpc_method"} object for a typed call.
#' @param request Raw vector with the serialized request message, or an
#'   \code{RProtoBuf} \code{Message} to serialize.
#' @param deadline_ms Optional deadline in milliseconds; on expiry the
#'   call completes with status \code{DEADLINE_EXCEEDED}.
#' @param metadata Optional named character vector of request metadata.
#' @param wait_for_ready If \code{TRUE}, queue the call until the channel
#'   connects instead of failing fast with \code{UNAVAILABLE}.
#' @return An object of class \code{"grpc_call"}: a list with the
#'   \code{client}, the call \code{id} that its completion event will
#'   carry, and the \code{method} path. Wait for the completion with
#'   \code{\link{grpc_await}} or \code{\link{grpc_poll}}.
#' @examples
#' srv <- grpc_server("127.0.0.1:0")
#' cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))
#'
#' ## the next request event; the server has one queue for every call, so
#' ## other events are stepped over, and 5 s of silence is an error
#' next_request <- function(srv) {
#'   repeat {
#'     evs <- grpc_poll(srv, timeout_ms = 5000L)
#'     if (!length(evs)) stop("no request within 5 s")
#'     for (ev in evs) if (ev$type == "request") return(ev)
#'   }
#' }
#'
#' call <- grpc_call(cl, "/demo.Echo/Say", charToRaw("ping"), deadline_ms = 5000)
#' class(call)
#'
#' ## echo it back from the server half of the same process
#' req <- next_request(srv)
#' grpc_reply(req, req$request)
#'
#' repeat {
#'   evs <- grpc_await(call, timeout_ms = 1000L)
#'   if (length(evs)) break
#' }
#' evs[[1]]$status_name
#' rawToChar(evs[[1]]$response)
#'
#' ## a call nobody answers ends at its deadline
#' call <- grpc_call(cl, "/demo.Echo/Say", raw(0), deadline_ms = 200)
#' repeat {
#'   evs <- grpc_await(call, timeout_ms = 1000L)
#'   if (length(evs)) break
#' }
#' evs[[1]]$status_name
#'
#' grpc_close(cl)
#' grpc_close(srv)
#' @export
grpc_call <- function(client, method, request, deadline_ms = NULL,
                      metadata = NULL, wait_for_ready = FALSE) {
    stopifnot(inherits(client, "grpc_client"))
    output_type <- NULL
    if (inherits(method, "grpc_method")) {
        if (method$client_streaming || method$server_streaming) {
            stop("method '", method$path, "' is streaming; use grpc_stream()")
        }
        if (inherits(request, "Message")) {
            got <- RProtoBuf::name(RProtoBuf::descriptor(request), TRUE)
            if (!identical(got, method$input_type)) {
                stop("request is '", got, "' but '", method$path,
                     "' expects '", method$input_type, "'")
            }
        }
        output_type <- method$output_type
        method <- method$path
    }
    if (inherits(request, "Message")) {
        request <- RProtoBuf::serialize(request, NULL)
    }
    stopifnot(is.character(method), length(method) == 1L, is.raw(request))
    if (!is.null(deadline_ms)) {
        stopifnot(is.numeric(deadline_ms), length(deadline_ms) == 1L,
                  deadline_ms > 0)
        deadline_ms <- as.numeric(deadline_ms)
    }
    if (!is.null(metadata)) {
        stopifnot(is.character(metadata), !is.null(names(metadata)),
                  all(nzchar(names(metadata))))
    }
    id <- .Call(grpc_r_call_start, client$ptr, method, request, deadline_ms,
                metadata, isTRUE(wait_for_ready))
    if (!is.null(output_type)) {
        assign(as.character(id), list(output = output_type),
               envir = client$calls)
    }
    structure(list(client = client, id = id, method = method),
              class = "grpc_call")
}

#' Open a streaming call
#'
#' Opens a client-, server-, or bidirectionally-streaming RPC. Messages
#' are sent with \code{\link{grpc_send}}, the request direction is
#' half-closed with \code{\link{grpc_writes_done}}, and everything
#' inbound arrives through \code{\link{grpc_poll}} on the client:
#' \code{"stream_msg"} events per message (with \code{response} bytes,
#' plus \code{response_message} decoded on a typed stream),
#' \code{"stream_writable"} when the send queue drains, and a final
#' \code{"stream_status"} with status and trailing metadata.
#'
#' Inbound flow control is automatic and bounded: at most
#' \code{read_buffer} undelivered messages are held; beyond that the
#' stream stops reading until \code{\link{grpc_poll}} drains, and HTTP/2
#' backpressure propagates to the peer.
#'
#' A stream runs until its \code{"stream_status"}, not until the caller
#' loses interest: dropping the returned object stops nothing, and the
#' stream's queued messages keep surfacing in \code{\link{grpc_poll}}
#' alongside later calls on the same client. Read every stream to its
#' status, or \code{\link{grpc_cancel}} the ones you are done with, and
#' dispatch events on \code{id} either way.
#'
#' @param client A \code{"grpc_client"} object.
#' @param method Full method path, or a \code{"grpc_method"} object for
#'   a typed stream (any streaming shape).
#' @param deadline_ms Optional deadline in milliseconds for the whole
#'   stream.
#' @param metadata Optional named character vector of request metadata.
#' @param wait_for_ready If \code{TRUE}, wait for the channel to connect
#'   instead of failing fast.
#' @param read_buffer Bound on undelivered inbound messages.
#' @param write_buffer Bound on queued outbound messages.
#' @return An object of class \code{"grpc_stream"}: a list with the
#'   \code{client}, the stream \code{id} that its events will carry, and
#'   the \code{method} path. Send on it with \code{\link{grpc_send}},
#'   half-close with \code{\link{grpc_writes_done}}, and receive with
#'   \code{\link{grpc_await}} or \code{\link{grpc_poll}}.
#' @examples
#' srv <- grpc_server("127.0.0.1:0")
#' cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))
#'
#' ## the next request event; the server has one queue for every call, so
#' ## other events are stepped over, and 5 s of silence is an error
#' next_request <- function(srv) {
#'   repeat {
#'     evs <- grpc_poll(srv, timeout_ms = 5000L)
#'     if (!length(evs)) stop("no request within 5 s")
#'     for (ev in evs) if (ev$type == "request") return(ev)
#'   }
#' }
#'
#' ## client: three messages out, then half-close the request direction
#' s <- grpc_stream(cl, "/demo.Echo/Chat", deadline_ms = 5000)
#' for (i in 1:3) grpc_send(s, as.raw(i))
#' grpc_writes_done(s)
#'
#' ## server: the first message rides on the request event; read the rest,
#' ## then echo each one back doubled and end the stream
#' req <- next_request(srv)
#' got <- list(req$request)
#' repeat {
#'   grpc_read(req)
#'   evs <- grpc_await(req, timeout_ms = 1000L)
#'   for (ev in evs) if (ev$type == "stream_msg") got <- c(got, list(ev$request))
#'   if (length(Filter(function(e) e$type %in% c("client_done", "cancelled"), evs))) break
#' }
#' for (m in got) grpc_send(req, as.raw(as.integer(m) * 2L))
#' grpc_finish(req)
#'
#' ## client: responses arrive as "stream_msg" events up to the "stream_status"
#' out <- list()
#' repeat {
#'   evs <- grpc_await(s, timeout_ms = 1000L)
#'   for (ev in evs) if (ev$kind == "stream_msg") out <- c(out, list(ev$response))
#'   st <- Filter(function(e) e$kind == "stream_status", evs)
#'   if (length(st)) break
#' }
#' unlist(out)
#' st[[1]]$status_name
#'
#' grpc_close(cl)
#' grpc_close(srv)
#' @export
grpc_stream <- function(client, method, deadline_ms = NULL, metadata = NULL,
                        wait_for_ready = FALSE, read_buffer = 16L,
                        write_buffer = 16L) {
    stopifnot(inherits(client, "grpc_client"))
    types <- NULL
    if (inherits(method, "grpc_method")) {
        types <- list(input = method$input_type, output = method$output_type)
        method <- method$path
    }
    stopifnot(is.character(method), length(method) == 1L)
    if (!is.null(deadline_ms)) {
        stopifnot(is.numeric(deadline_ms), length(deadline_ms) == 1L,
                  deadline_ms > 0)
        deadline_ms <- as.numeric(deadline_ms)
    }
    if (!is.null(metadata)) {
        stopifnot(is.character(metadata), !is.null(names(metadata)),
                  all(nzchar(names(metadata))))
    }
    id <- .Call(grpc_r_stream_start, client$ptr, method, deadline_ms,
                metadata, isTRUE(wait_for_ready), as.integer(read_buffer),
                as.integer(write_buffer))
    if (!is.null(types)) {
        assign(as.character(id), types, envir = client$calls)
    }
    structure(list(client = client, id = id, method = method),
              class = "grpc_stream")
}

#' @export
grpc_send.grpc_stream <- function(x, msg, ...) {
    if (inherits(msg, "Message")) {
        types <- get0(as.character(x$id), envir = x$client$calls,
                      inherits = FALSE)
        if (!is.null(types) && !is.null(types$input)) {
            got <- RProtoBuf::name(RProtoBuf::descriptor(msg), TRUE)
            if (!identical(got, types$input)) {
                stop("message is '", got, "' but '", x$method,
                     "' expects '", types$input, "'")
            }
        }
        msg <- RProtoBuf::serialize(msg, NULL)
    }
    stopifnot(is.raw(msg))
    invisible(.Call(grpc_r_stream_send, x$client$ptr, x$id, msg))
}

#' Half-close a client stream
#'
#' Signals that no further messages will be sent. Queued messages are
#' flushed first. Returns (invisibly) \code{FALSE} if already
#' half-closed.
#'
#' @param stream A \code{"grpc_stream"} object.
#' @return Invisibly, a logical scalar: \code{TRUE} if the half-close was
#'   posted, \code{FALSE} if the stream was already half-closed or is
#'   over.
#' @examples
#' srv <- grpc_server("127.0.0.1:0")
#' cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))
#'
#' ## the next request event; the server has one queue for every call, so
#' ## other events are stepped over, and 5 s of silence is an error
#' next_request <- function(srv) {
#'   repeat {
#'     evs <- grpc_poll(srv, timeout_ms = 5000L)
#'     if (!length(evs)) stop("no request within 5 s")
#'     for (ev in evs) if (ev$type == "request") return(ev)
#'   }
#' }
#'
#' s <- grpc_stream(cl, "/demo.Echo/Collect", deadline_ms = 5000)
#' grpc_send(s, as.raw(1))
#' (grpc_writes_done(s))                   # TRUE: request direction closed
#' (grpc_writes_done(s))                   # FALSE: already half-closed
#' (grpc_send(s, as.raw(2)))               # FALSE: no writes after the half-close
#'
#' ## the server reads past the first message and sees "client_done"
#' req <- next_request(srv)
#' grpc_read(req)
#' repeat {
#'   evs <- grpc_await(req, timeout_ms = 1000L)
#'   done <- Filter(function(e) e$type %in% c("client_done", "cancelled"), evs)
#'   if (length(done)) break
#' }
#' done[[1]]$type
#' grpc_reply(req, raw(0))
#'
#' grpc_close(cl)
#' grpc_close(srv)
#' @export
grpc_writes_done <- function(stream) {
    stopifnot(inherits(stream, "grpc_stream"))
    invisible(.Call(grpc_r_stream_writes_done, stream$client$ptr, stream$id))
}

#' @export
grpc_cancel.grpc_call <- function(x) {
    invisible(.Call(grpc_r_call_cancel, x$client$ptr, x$id))
}

#' @export
grpc_cancel.grpc_stream <- function(x) {
    invisible(.Call(grpc_r_call_cancel, x$client$ptr, x$id))
}

## Add status_name and, on a typed call, response_message; forget a
## call's types once its terminal event has been handed over. Shared by
## grpc_poll() and grpc_await() so both deliver identical events.
decorate_events <- function(client, events) {
    lapply(events, function(ev) {
        key <- as.character(ev$id)
        types <- get0(key, envir = client$calls, inherits = FALSE)
        terminal <- ev$kind %in% c("unary", "stream_status")
        if (terminal) {
            ev$status_name <- names(grpc_status_codes)[match(ev$status,
                    grpc_status_codes)]
        }
        if (!is.null(types) && !is.null(types$output) &&
            !is.null(ev$response) &&
            (identical(ev$kind, "stream_msg") ||
                (identical(ev$kind, "unary") &&
                    identical(ev$status_name, "OK")))) {
            ev$response_message <- grpc_decode(ev$response, types$output)
        }
        if (terminal && !is.null(types)) {
            rm(list = key, envir = client$calls)
        }
        ev
    })
}

#' @export
grpc_poll.grpc_client <- function(x, max_events = 64L, timeout_ms = 0L) {
    decorate_events(x, .Call(grpc_r_client_poll, x$ptr,
                             as.integer(max_events), as.integer(timeout_ms),
                             NULL))
}

## Shared argument checking for every grpc_await method.
check_await_args <- function(timeout_ms, max_events) {
    stopifnot(is.numeric(timeout_ms), length(timeout_ms) == 1L,
              is.finite(timeout_ms), timeout_ms >= -1,
              timeout_ms <= .Machine$integer.max)
    stopifnot(is.numeric(max_events), length(max_events) == 1L, max_events >= 1)
}

await_client <- function(x, timeout_ms, max_events) {
    check_await_args(timeout_ms, max_events)
    decorate_events(x$client,
                    .Call(grpc_r_client_poll, x$client$ptr, as.integer(max_events),
                          as.integer(timeout_ms), as.numeric(x$id)))
}

#' @export
grpc_await.grpc_call <- function(x, timeout_ms, max_events = 64L) {
    await_client(x, timeout_ms, max_events)
}

#' @export
grpc_await.grpc_stream <- function(x, timeout_ms, max_events = 64L) {
    await_client(x, timeout_ms, max_events)
}

#' @export
grpc_fd.grpc_client <- function(x) {
    .Call(grpc_r_client_fd, x$ptr)
}

#' @export
grpc_pending.grpc_client <- function(x) {
    .Call(grpc_r_client_pending, x$ptr)
}

#' @export
grpc_close.grpc_client <- function(x) {
    invisible(.Call(grpc_r_client_close, x$ptr))
}
