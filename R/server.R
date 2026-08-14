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
#' @param address Bind address, e.g. \code{"127.0.0.1:0"} for an
#'   ephemeral TCP port (see \code{\link{grpc_server_port}}) or
#'   \code{"unix:/path/to.sock"} for a unix-domain socket.
#' @param accept_window Outstanding accept slots (burst capacity).
#' @param max_active Bound on concurrently active calls.
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
grpc_server <- function(address = "127.0.0.1:0", accept_window = 8L,
                        max_active = 256L) {
    stopifnot(is.character(address), length(address) == 1L,
              is.numeric(accept_window), accept_window >= 1,
              is.numeric(max_active), max_active >= 1)
    xp <- .Call(grpc_r_server2_create, address, as.integer(accept_window),
                as.integer(max_active))
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
#' @param response Raw vector with the serialized response message.
#'   Required when \code{status} is \code{OK}; ignored otherwise.
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
