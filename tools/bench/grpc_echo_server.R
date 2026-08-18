## gRPC echo server for tools/bench/bench.sh. Echoes unary requests and
## streaming messages back unchanged.
##
## This is the R-on-both-ends peer. It exists because comparing our R
## client against a Go server with nanonext's R client against an R
## server would not be a transport comparison -- it would mostly measure
## which side had an interpreter in the loop. With this server the gRPC
## and nanonext rows have the same shape, and the Go row stays as the
## separate question it actually is: what our client costs against a peer
## that is not the bottleneck.
##
## argv: addr secs
library(grpc)
addr <- argv[[1L]]
secs <- if (length(argv) >= 2L) as.numeric(argv[[2L]]) else 120

srv <- grpc_server(addr, accept_window = 16L, max_active = 256L)
cat("ready\n"); flush(stdout())

## Two things about server-side streaming that are easy to get wrong,
## both of which cost a stalled benchmark before they were right:
##
## 1. Server events carry `type`, not `kind`. `kind` is the client-side
##    field. Branching on `ev$kind` here matches nothing and the loop
##    silently does no work, which reads as a slow transport rather than
##    as a bug.
## 2. Only the initial "request" event is decorated with the server
##    handle and the grpc_request class. Later "stream_msg" events carry
##    the payload but are not themselves answerable, so the original
##    request object has to be kept and used for grpc_send()/grpc_read()
##    while the message bytes come from the new event.
## 3. grpc_send() returning FALSE means the message was NOT queued. An
##    echo loop that ignores the return value drops messages whenever the
##    write queue is full, and at 64KB payloads that is often: the client
##    then waits forever for replies that were never sent, and the run
##    fails as DEADLINE_EXCEEDED with nothing in the log to say why. Hold
##    the refused payload and retry it before reading anything more.
calls <- new.env(parent = emptyenv())
pending <- new.env(parent = emptyenv())

## Try to echo one payload. On success post the next read; on refusal
## keep the payload so the main loop can retry. Reading again before the
## echo is placed would lose it.
echo_or_hold <- function(key, req, payload) {
    if (isTRUE(grpc_send(req, payload))) {
        if (exists(key, envir = pending, inherits = FALSE)) {
            rm(list = key, envir = pending)
        }
        grpc_read(req)
    } else {
        assign(key, payload, envir = pending)
    }
}

## Dispatch is on the method name, because a unary call and a streaming
## one are indistinguishable at the moment the request arrives: both are
## a call with one message pending. The benchmark client names them
## /Unary and /Stream for exactly this reason.
deadline <- as.numeric(Sys.time()) + secs
half_closed <- new.env(parent = emptyenv())
while (as.numeric(Sys.time()) < deadline) {
    ## Retry anything the write queue refused last time round, before
    ## taking on more work.
    for (key in ls(pending)) {
        req <- get0(key, envir = calls, inherits = FALSE)
        if (is.null(req)) {
            rm(list = key, envir = pending)
            next
        }
        echo_or_hold(key, req, get(key, envir = pending))
    }
    ## A client_done that arrives while an echo is still held cannot
    ## finish the call yet; the held payload still has to go out.
    for (key in ls(half_closed)) {
        if (!exists(key, envir = pending, inherits = FALSE)) {
            req <- get0(key, envir = calls, inherits = FALSE)
            if (!is.null(req)) grpc_finish(req)
            rm(list = key, envir = half_closed)
            if (exists(key, envir = calls, inherits = FALSE)) {
                rm(list = key, envir = calls)
            }
        }
    }
    for (ev in grpc_poll(srv, timeout_ms = 100L, max_events = 512L)) {
        key <- as.character(ev$id)
        if (identical(ev$type, "request")) {
            if (endsWith(ev$method, "/Stream")) {
                assign(key, ev, envir = calls)
                echo_or_hold(key, ev, ev$request)
            } else {
                grpc_reply(ev, ev$request)
            }
        } else if (identical(ev$type, "stream_msg")) {
            req <- get0(key, envir = calls, inherits = FALSE)
            if (!is.null(req)) echo_or_hold(key, req, ev$request)
        } else if (identical(ev$type, "client_done")) {
            assign(key, TRUE, envir = half_closed)
        } else if (identical(ev$type, "cancelled")) {
            for (e in list(calls, pending, half_closed)) {
                if (exists(key, envir = e, inherits = FALSE)) {
                    rm(list = key, envir = e)
                }
            }
        }
    }
}
grpc_close(srv)
