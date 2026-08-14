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
#' and signals an eventfd. The background thread never calls the R API;
#' completions are received on the R main thread via \code{\link{grpc_poll}}.
#'
#' @param target Server address, e.g. \code{"localhost:50051"} or
#'   \code{"unix:///run/containerd/containerd.sock"}.
#' @return An object of class \code{"grpc_client"}.
#' @examples
#' \dontrun{
#' client <- grpc_client("localhost:50051")
#' call <- grpc_call(client, "/demo.Echo/Say", serialize(msg, NULL),
#'                   deadline_ms = 1000)
#' events <- grpc_poll(client, timeout_ms = 1000L)
#' grpc_close(client)
#' }
#' @export
grpc_client <- function(target) {
    stopifnot(is.character(target), length(target) == 1L)
    xp <- .Call(grpc_r_client_create, target)
    structure(list(ptr = xp, target = target), class = "grpc_client")
}

#' Start a unary call
#'
#' Starts an asynchronous unary RPC. The request is opaque bytes (a
#' serialized protocol buffer, e.g. from \code{RProtoBuf}'s
#' \code{serialize()}). The call completes via \code{\link{grpc_poll}}.
#'
#' @param client A \code{"grpc_client"} object.
#' @param method Full method path, e.g.
#'   \code{"/runtime.v1.RuntimeService/Version"}.
#' @param request Raw vector with the serialized request message.
#' @param deadline_ms Optional deadline in milliseconds; on expiry the
#'   call completes with status \code{DEADLINE_EXCEEDED}.
#' @param metadata Optional named character vector of request metadata.
#' @param wait_for_ready If \code{TRUE}, queue the call until the channel
#'   connects instead of failing fast with \code{UNAVAILABLE}.
#' @return An object of class \code{"grpc_call"} holding the call id.
#' @examples
#' \dontrun{
#' call <- grpc_call(client, "/runtime.v1.RuntimeService/Version",
#'                   serialize(req, NULL), deadline_ms = 500)
#' }
#' @export
grpc_call <- function(client, method, request, deadline_ms = NULL,
                      metadata = NULL, wait_for_ready = FALSE) {
    stopifnot(inherits(client, "grpc_client"), is.character(method),
              length(method) == 1L, is.raw(request))
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
    structure(list(client = client, id = id, method = method),
              class = "grpc_call")
}

#' Receive completed calls
#'
#' Drains up to \code{max_events} completed calls from the client. Each
#' completion is a list with elements \code{id}, \code{status} (integer,
#' see \code{\link{grpc_status_codes}}), \code{status_name},
#' \code{message}, \code{response} (raw vector, or \code{NULL} unless the
#' status is \code{OK}), \code{initial_metadata}, and
#' \code{trailing_metadata}.
#'
#' @param client A \code{"grpc_client"} object.
#' @param max_events Maximum completions to return in this batch.
#' @param timeout_ms How long to wait if no completion is ready:
#'   \code{0} returns immediately, a positive value waits up to that many
#'   milliseconds, \code{-1} waits indefinitely.
#' @return A list of completion events (possibly empty).
#' @examples
#' \dontrun{
#' events <- grpc_poll(client, max_events = 64L, timeout_ms = 100L)
#' for (ev in events) if (ev$status_name == "OK") handle(ev$response)
#' }
#' @export
grpc_poll <- function(client, max_events = 64L, timeout_ms = 0L) {
    stopifnot(inherits(client, "grpc_client"))
    events <- .Call(grpc_r_client_poll, client$ptr, as.integer(max_events),
                    as.integer(timeout_ms))
    lapply(events, function(ev) {
        ev$status_name <- names(grpc_status_codes)[match(ev$status,
                grpc_status_codes)]
        ev
    })
}

#' Cancel an in-flight call
#'
#' Requests cancellation. The call still completes through
#' \code{\link{grpc_poll}}, normally with status \code{CANCELLED}.
#' Cancelling a call that already completed is a no-op.
#'
#' @param call A \code{"grpc_call"} object.
#' @examples
#' \dontrun{grpc_cancel(call)}
#' @export
grpc_cancel <- function(call) {
    stopifnot(inherits(call, "grpc_call"))
    invisible(.Call(grpc_r_call_cancel, call$client$ptr, call$id))
}

#' Completion wakeup file descriptor
#'
#' Returns the client's eventfd. It becomes readable whenever a
#' completion is queued, so an event loop can wake on it instead of
#' polling, e.g. \code{later::later_fd(function(...) grpc_poll(client),
#' readfds = grpc_fd(client))}. Do not read from this descriptor;
#' \code{\link{grpc_poll}} drains it.
#'
#' @param client A \code{"grpc_client"} object.
#' @return Integer file descriptor.
#' @examples
#' \dontrun{
#' later::later_fd(function(ready) grpc_poll(client),
#'                 readfds = grpc_fd(client))
#' }
#' @export
grpc_fd <- function(client) {
    stopifnot(inherits(client, "grpc_client"))
    .Call(grpc_r_client_fd, client$ptr)
}

#' Number of in-flight calls
#'
#' @param client A \code{"grpc_client"} object.
#' @return Integer count of calls started but not yet delivered by
#'   \code{\link{grpc_poll}}.
#' @examples
#' \dontrun{while (grpc_pending(client) > 0) grpc_poll(client, timeout_ms = 100L)}
#' @export
grpc_pending <- function(client) {
    stopifnot(inherits(client, "grpc_client"))
    .Call(grpc_r_client_pending, client$ptr)
}

#' Close a client
#'
#' Cancels in-flight calls, shuts down the completion queue, joins the
#' completion thread, and releases the channel. Undelivered completions
#' are discarded. Closing twice is a no-op; the finalizer performs the
#' same shutdown if a client is garbage collected unclosed.
#'
#' @param client A \code{"grpc_client"} object.
#' @examples
#' \dontrun{grpc_close(client)}
#' @export
grpc_close <- function(client) {
    stopifnot(inherits(client, "grpc_client"))
    invisible(.Call(grpc_r_client_close, client$ptr))
}
