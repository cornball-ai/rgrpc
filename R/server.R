#' Create an asynchronous generic gRPC server
#'
#' Binds \code{address}, registers the generic asynchronous service (any
#' method name is accepted; there are no generated stubs), and starts the
#' server's completion machinery: a background thread that drains the
#' completion queue and signals an eventfd. Incoming requests are
#' received on the R main thread via \code{\link{grpc_poll}} and answered
#' with \code{\link{grpc_reply}}.
#'
#' Backpressure: at most \code{accept_window} accept slots are kept
#' outstanding, and no new slot is posted while active calls plus
#' outstanding slots would exceed \code{max_active}. Excess incoming
#' calls queue in the transport until capacity frees up.
#'
#' Request events carry the transport \code{peer} address and, on a TLS
#' listener with \code{require_client_cert}, the verified
#' \code{peer_identity} (the client certificate's identity values).
#'
#' Keepalive: \code{keepalive_ms}/\code{keepalive_timeout_ms} make the
#' server ping quiet clients, mirroring \code{\link{grpc_client}}.
#' \code{min_ping_interval_ms} is the tolerance for \emph{client} pings:
#' gRPC's server default allows one unsolicited ping per 5 minutes and
#' kills faster clients with a \code{too_many_pings} GOAWAY, so a
#' deployment where clients keep 10-second heartbeats must lower this to
#' at most the client ping interval.
#'
#' @param address Bind address, e.g. \code{"127.0.0.1:0"} for an
#'   ephemeral TCP port (see \code{\link{grpc_server_port}}) or
#'   \code{"unix:/path/to.sock"} for a unix-domain socket.
#' @param credentials \code{NULL} for a plaintext listener, or a
#'   \code{\link{grpc_tls}} object (needs \code{cert_file} and
#'   \code{key_file}; \code{require_client_cert = TRUE} for mTLS).
#' @param accept_window Outstanding accept slots (burst capacity).
#' @param max_active Bound on concurrently active calls.
#' @param keepalive_ms Interval of transport inactivity after which the
#'   server pings a client. \code{NULL} (default) disables keepalive.
#' @param keepalive_timeout_ms Time to wait for a ping answer before the
#'   connection is declared dead.
#' @param min_ping_interval_ms Minimum interval between client pings the
#'   server tolerates without counting a ping strike.
#' @return An object of class \code{"grpc_server"}.
#' @examples
#' \dontrun{
#' srv <- grpc_server("127.0.0.1:0")
#' events <- grpc_poll(srv, timeout_ms = 1000L)
#' for (ev in events) {
#'   if (ev$type == "request") grpc_reply(ev, handle(ev$request))
#' }
#' grpc_close(srv)
#' }
#' @export
grpc_server <- function(address = "127.0.0.1:0", credentials = NULL,
                        accept_window = 8L, max_active = 256L,
                        keepalive_ms = NULL, keepalive_timeout_ms = NULL,
                        min_ping_interval_ms = NULL) {
    stopifnot(is.character(address), length(address) == 1L,
              is.numeric(accept_window), accept_window >= 1,
              is.numeric(max_active), max_active >= 1)
    tls <- inherits(credentials, "grpc_tls")
    if (!is.null(credentials) && !tls) {
        stop("credentials must be a grpc_tls object")
    }
    if (tls && (is.null(credentials$cert) || is.null(credentials$key))) {
        stop("a TLS server needs cert_file and key_file")
    }
    ms <- function(x) {
        if (is.null(x)) {
            return(NULL)
        }
        stopifnot(is.numeric(x), length(x) == 1L, is.finite(x),
                  x == trunc(x), x >= 1, x <= .Machine$integer.max)
        as.integer(x)
    }
    xp <- .Call(grpc_r_server2_create, address, as.integer(accept_window),
                as.integer(max_active), tls,
        if (tls) credentials$ca, if (tls) credentials$cert,
        if (tls) credentials$key,
                tls && credentials$require_client_cert,
                ms(keepalive_ms), ms(keepalive_timeout_ms),
                ms(min_ping_interval_ms))
    structure(list(ptr = xp, address = address), class = "grpc_server")
}

#' Bound TCP port of a server
#'
#' The port actually bound, useful with an ephemeral \code{"host:0"}
#' bind address. Meaningless for unix-domain sockets.
#'
#' @param server A \code{"grpc_server"} object.
#' @return Integer port number.
#' @examples
#' \dontrun{grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))}
#' @export
grpc_server_port <- function(server) {
    stopifnot(inherits(server, "grpc_server"))
    .Call(grpc_r_server2_port, server$ptr)
}

#' Answer an incoming request
#'
#' Completes a request received from \code{\link{grpc_poll}} on a
#' server, either with a response payload (status \code{OK}) or with an
#' error status. Returns (invisibly) \code{TRUE} if the reply was
#' accepted, or \code{FALSE} if the call is no longer answerable (the
#' peer cancelled, timed out, or the request was already answered).
#'
#' @param request A \code{"grpc_request"} event from
#'   \code{\link{grpc_poll}}.
#' @param response Raw vector with the serialized response message, or an
#'   \code{RProtoBuf} \code{Message} to serialize. Required when
#'   \code{status} is \code{OK}; ignored otherwise.
#' @param status Integer status code or name from
#'   \code{\link{grpc_status_codes}}, e.g. \code{"NOT_FOUND"}.
#' @param message Optional error detail string for non-\code{OK} status.
#' @param metadata Optional named character vector sent as trailing
#'   metadata.
#' @examples
#' \dontrun{
#' grpc_reply(ev, serialize(resp, NULL))
#' grpc_reply(ev, status = "NOT_FOUND", message = "no such sandbox")
#' }
#' @export
grpc_reply <- function(request, response = NULL, status = 0L, message = "",
                       metadata = NULL) {
    stopifnot(inherits(request, "grpc_request"))
    if (inherits(response, "Message")) {
        response <- RProtoBuf::serialize(response, NULL)
    }
    if (is.character(status)) {
        status <- grpc_status_codes[[match.arg(status,
                    names(grpc_status_codes))]]
    }
    status <- as.integer(status)
    if (status == 0L) {
        stopifnot(is.raw(response))
    } else {
        stopifnot(is.character(message), length(message) == 1L)
    }
    if (!is.null(metadata)) {
        stopifnot(is.character(metadata), !is.null(names(metadata)),
                  all(nzchar(names(metadata))))
    }
    invisible(.Call(grpc_r_server2_reply, request$server$ptr, request$id,
                    response, status, message, metadata))
}

#' Pull the next inbound message on a server stream
#'
#' Posts one read on a client- or bidirectionally-streaming call. The
#' result arrives through \code{\link{grpc_poll}} as a
#' \code{"stream_msg"} event (with \code{request} bytes), or
#' \code{"client_done"} when the peer has half-closed. One read at a
#' time: returns (invisibly) \code{FALSE} if a read is already in flight
#' or the call is over.
#'
#' @param request A \code{"grpc_request"} event from
#'   \code{\link{grpc_poll}}.
#' @examples
#' \dontrun{grpc_read(ev)}
#' @export
grpc_read <- function(request) {
    stopifnot(inherits(request, "grpc_request"))
    invisible(.Call(grpc_r_server2_read, request$server$ptr, request$id))
}

#' @export
grpc_send.grpc_request <- function(x, msg, ...) {
    if (inherits(msg, "Message")) {
        msg <- RProtoBuf::serialize(msg, NULL)
    }
    stopifnot(is.raw(msg))
    invisible(.Call(grpc_r_server2_send, x$server$ptr, x$id, msg))
}

#' End a server stream
#'
#' Sends the terminal status for a streaming call after any messages
#' queued with \code{\link{grpc_send}} have drained. For unary replies
#' use \code{\link{grpc_reply}}, which sends a payload and the status in
#' one step. Returns (invisibly) \code{TRUE} if accepted, \code{FALSE}
#' if the call is no longer answerable.
#'
#' With \code{drain = FALSE} the close is abortive: queued messages are
#' discarded and the terminal status goes out first. This is for
#' fencing — e.g. \code{ABORTED} on session replacement — where
#' delivering queued-but-stale messages to the peer would be wrong and
#' waiting behind them (potentially forever, if the peer has stopped
#' reading) delays the fence. One already-posted message cannot be
#' recalled, so the status can still wait for that single in-flight
#' write; if the peer's flow-control window is exhausted even that may
#' not complete, and \code{\link{grpc_cancel}} on the request is the
#' hard escalation (the peer then sees \code{CANCELLED} rather than
#' this status).
#'
#' @param request A \code{"grpc_request"} event from
#'   \code{\link{grpc_poll}}.
#' @param status Integer status code or name from
#'   \code{\link{grpc_status_codes}}.
#' @param message Optional error detail string for non-\code{OK} status.
#' @param metadata Optional named character vector sent as trailing
#'   metadata.
#' @param drain If \code{TRUE} (default), queued messages are delivered
#'   before the status; if \code{FALSE}, they are discarded and the
#'   status is prioritized.
#' @examples
#' \dontrun{
#' for (m in msgs) grpc_send(ev, m)
#' grpc_finish(ev)
#' grpc_finish(ev, status = "ABORTED", message = "session replaced",
#'             drain = FALSE)
#' }
#' @export
grpc_finish <- function(request, status = 0L, message = "", metadata = NULL,
                        drain = TRUE) {
    stopifnot(inherits(request, "grpc_request"), is.logical(drain),
              length(drain) == 1L, !is.na(drain))
    if (is.character(status)) {
        status <- grpc_status_codes[[match.arg(status,
                    names(grpc_status_codes))]]
    }
    status <- as.integer(status)
    if (status != 0L) {
        stopifnot(is.character(message), length(message) == 1L)
    }
    if (!is.null(metadata)) {
        stopifnot(is.character(metadata), !is.null(names(metadata)),
                  all(nzchar(names(metadata))))
    }
    invisible(.Call(grpc_r_server2_finish, request$server$ptr, request$id,
                    status, message, metadata, drain))
}

#' @export
grpc_cancel.grpc_request <- function(x) {
    invisible(.Call(grpc_r_server2_cancel, x$server$ptr, x$id))
}

#' @export
grpc_poll.grpc_server <- function(x, max_events = 64L, timeout_ms = 0L) {
    events <- .Call(grpc_r_server2_poll, x$ptr, as.integer(max_events),
                    as.integer(timeout_ms))
    lapply(events, function(ev) {
        if (ev$type == "request") {
            ev$server <- x
            class(ev) <- "grpc_request"
        }
        ev
    })
}

#' @export
grpc_fd.grpc_server <- function(x) {
    .Call(grpc_r_server2_fd, x$ptr)
}

#' @export
grpc_pending.grpc_server <- function(x) {
    .Call(grpc_r_server2_pending, x$ptr)
}

#' @export
grpc_close.grpc_server <- function(x) {
    invisible(.Call(grpc_r_server2_close, x$ptr))
}
