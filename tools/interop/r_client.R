## R interop peer, client side. Driven by tools/interop/interop.sh.
## Calls a foreign interop.v1.Interop server and asserts on the answers.
##
## Prints one R_IOP line per check and exits nonzero if any failed: a
## driver that only looked at exit codes could not tell "connected and
## every field matched" from "connected and answered nothing", and a
## check that never ran must not read as a pass.
##
## argv: target proto_dir expect_responder
library(grpc)
target <- argv[[1L]]
pdir   <- argv[[2L]]
who    <- argv[[3L]]
RProtoBuf::readProtoFiles2("interop.proto", protoPath = normalizePath(pdir))

failures <- character(0)
check <- function(name, got, want) {
    ok <- isTRUE(all.equal(got, want))
    if (!ok) failures <<- c(failures, name)
    cat(sprintf("R_IOP %s %s got=%s want=%s\n", if (ok) "ok" else "FAIL",
                name, paste(format(got), collapse = ","),
                paste(format(want), collapse = ",")))
    flush(stdout())
}

cl <- grpc_client(target)
svc <- grpc_service("interop.v1.EchoRequest", "Interop")

## A proto3 map is a repeated entry message on the wire, and RProtoBuf
## exposes it that way: build the entry and pass it like any repeated
## field. This is the shape a C++ or Python peer will send back, so the
## test covers map encoding rather than dodging it.
req <- RProtoBuf::P("interop.v1.EchoRequest")$new(
    text = "hello",
    items = list(RProtoBuf::P("interop.v1.Item")$new(name = "a", count = 1),
                 RProtoBuf::P("interop.v1.Item")$new(name = "b", count = 2)),
    labels = list(RProtoBuf::P("interop.v1.EchoRequest.LabelsEntry")$new(
        key = "k", value = "v")),
    number = 3L)

## ---- unary ----
call <- grpc_call(cl, grpc_method(svc, "Echo"), req, deadline_ms = 5000)
evs <- grpc_await(call, timeout_ms = 5000L)
if (!length(evs)) {
    check("unary.answered", "TIMEOUT", "answered")
} else {
    ev <- evs[[1L]]
    check("unary.status", ev$status_name, "OK")
    if (identical(ev$status_name, "OK")) {
        m <- ev$response_message
        check("unary.text", m$text, "hello")
        check("unary.responder", m$responder, who)
        check("unary.items", vapply(m$items, function(i) i$name, ""),
              c("a", "b"))
        check("unary.counts", vapply(m$items, function(i) as.integer(i$count), 0L),
              c(1L, 2L))
    }
}

## ---- server streaming ----
s <- grpc_stream(cl, grpc_method(svc, "EchoStream"), deadline_ms = 5000)
grpc_send(s, req)
grpc_writes_done(s)
texts <- character(0)
status <- NA_character_
t0 <- Sys.time()
while (as.numeric(Sys.time() - t0, units = "secs") < 10) {
    evs <- grpc_await(s, timeout_ms = 500L)
    if (!length(evs)) next
    done <- FALSE
    for (ev in evs) {
        if (identical(ev$kind, "stream_msg")) {
            texts <- c(texts, ev$response_message$text)
        } else if (identical(ev$kind, "stream_status")) {
            status <- ev$status_name
            done <- TRUE
        }
    }
    if (done) break
}
check("stream.status", status, "OK")
check("stream.texts", texts, c("hello-0", "hello-1", "hello-2"))

## ---- non-OK status ----
call <- grpc_call(cl, grpc_method(svc, "Fail"), req, deadline_ms = 5000)
evs <- grpc_await(call, timeout_ms = 5000L)
if (!length(evs)) {
    check("fail.answered", "TIMEOUT", "answered")
} else {
    check("fail.status", evs[[1L]]$status_name, "FAILED_PRECONDITION")
    ## The event field is `message`, not `status_message`.
    check("fail.message", evs[[1L]]$message, "interop failure")
}

grpc_close(cl)
if (length(failures)) {
    cat(sprintf("R_IOP FAILED: %s\n", paste(failures, collapse = ", ")))
    quit(status = 1L)
}
cat("R_IOP all ok\n")
