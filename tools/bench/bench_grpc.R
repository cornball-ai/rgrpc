## gRPC benchmark client. Driven by tools/bench/bench.sh.
##
## Two shapes, because they answer different questions:
##
##   unary   sequential round trips, one outstanding at a time. This is
##           latency: p50/p99 of a request that must complete before the
##           next begins, which is what a control-plane call costs.
##   stream  one bidi stream, all messages sent then all replies drained.
##           This is throughput, and it is the number that matters for an
##           event feed.
##
## Latency and throughput are deliberately not measured in the same run.
## A pipelined loop reports a latency figure that is really a queueing
## delay, and a sequential loop reports a throughput figure that is
## really 1/latency; both are misleading and neither is what the caller
## asked for.
##
## argv: target mode size n label
library(rgrpc)
target <- argv[[1L]]
mode   <- argv[[2L]]
size   <- as.integer(argv[[3L]])
n      <- as.integer(argv[[4L]])
label  <- argv[[5L]]

payload <- as.raw(rep(65L, size))
## Distinct method names per shape. A server cannot tell a unary call
## from a streaming one at the moment the request arrives -- both are
## just a call with a message pending -- so the method name has to carry
## it. Sharing one name made the R echo server answer unary calls as
## though they were streams, and every call failed.
method <- if (identical(mode, "stream")) {
    "/bench.v1.Bench/Stream"
} else {
    "/bench.v1.Bench/Unary"
}
cl <- grpc_client(target)

## proc.time() is not usable here: on Linux its elapsed field comes from
## times() and is quantised to 1ms, so a 50us round trip reports p50=0
## and p99=1 -- a latency histogram made entirely of rounding. Sys.time()
## is gettimeofday and resolves microseconds.
now <- function() as.numeric(Sys.time())

report <- function(kind, lat_ms, elapsed_s, count) {
    if (!is.null(lat_ms) && length(lat_ms)) {
        q <- stats::quantile(lat_ms, c(0.5, 0.9, 0.99), names = FALSE)
        cat(sprintf(
            "BENCH %s %s size=%d n=%d p50_ms=%.3f p90_ms=%.3f p99_ms=%.3f mean_ms=%.3f\n",
            label, kind, size, count, q[1], q[2], q[3], mean(lat_ms)))
    }
    cat(sprintf("BENCH %s %s size=%d n=%d elapsed_s=%.3f msgs_per_s=%.0f mb_per_s=%.1f\n",
                label, kind, size, count, elapsed_s, count / elapsed_s,
                count * size / elapsed_s / 1048576))
    flush(stdout())
}

if (identical(mode, "unary")) {
    ## Warm the channel first: the first call pays connection setup, and
    ## folding that into p99 would report a one-off as a tail latency.
    warm <- grpc_call(cl, method, payload, deadline_ms = 5000)
    if (!length(grpc_await(warm, timeout_ms = 5000L))) {
        cat("BENCH ABORT warmup did not complete\n"); quit(status = 1L)
    }
    lat <- numeric(n)
    bad <- 0L
    t0 <- now()
    for (i in seq_len(n)) {
        s <- now()
        call <- grpc_call(cl, method, payload, deadline_ms = 5000)
        evs <- grpc_await(call, timeout_ms = 5000L)
        lat[i] <- (now() - s) * 1000
        if (!length(evs) || !identical(evs[[1L]]$status_name, "OK")) {
            bad <- bad + 1L
        }
    }
    el <- now() - t0
    if (bad > 0L) {
        cat(sprintf("BENCH ABORT %d/%d calls did not return OK\n", bad, n))
        quit(status = 1L)
    }
    report("unary", lat, el, n)
} else if (identical(mode, "stream")) {
    s <- grpc_stream(cl, method, deadline_ms = 60000, read_buffer = 256L)
    got <- 0L
    sent <- 0L
    t0 <- now()
    ## Send and drain in the same loop. Sending all n first would just
    ## measure how much the transport buffers before refusing.
    while (got < n) {
        while (sent < n && grpc_send(s, payload)) sent <- sent + 1L
        for (ev in grpc_await(s, timeout_ms = 1000L, max_events = 512L)) {
            if (identical(ev$kind, "stream_msg")) got <- got + 1L
            else if (identical(ev$kind, "stream_status")) {
                cat(sprintf("BENCH ABORT stream ended early: %s\n",
                            ev$status_name))
                quit(status = 1L)
            }
        }
        if (now() - t0 > 120) {
            cat(sprintf("BENCH ABORT stream stalled at %d/%d\n", got, n))
            quit(status = 1L)
        }
    }
    el <- now() - t0
    grpc_writes_done(s)
    report("stream", NULL, el, got)
} else {
    cat(sprintf("BENCH ABORT unknown mode %s\n", mode)); quit(status = 2L)
}
grpc_close(cl)
