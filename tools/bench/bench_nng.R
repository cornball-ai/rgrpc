## nanonext benchmark client. Driven by tools/bench/bench.sh.
##
## Mirrors tools/bench/bench_grpc.R exactly: same payload sizes, same two
## shapes, same statistics, same microsecond timer. The point is a
## like-for-like number for the vientito transport decision, so anything
## that differs between the two scripts other than the transport is a
## bug in the comparison.
##
##   unary   req/rep, one outstanding at a time -> latency
##   stream  pair socket, send and drain -> throughput
##
## argv: url mode size n label
library(nanonext)
url   <- argv[[1L]]
mode  <- argv[[2L]]
size  <- as.integer(argv[[3L]])
n     <- as.integer(argv[[4L]])
label <- argv[[5L]]

payload <- as.raw(rep(65L, size))
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
    s <- socket(protocol = "req", dial = url)
    ## Warm up, for the same reason the gRPC script does: first-message
    ## setup is a one-off and does not belong in a tail statistic.
    send(s, payload, mode = "raw", block = TRUE)
    if (inherits(recv(s, mode = "raw", block = 5000L), "errorValue")) {
        cat("BENCH ABORT nng warmup failed\n"); quit(status = 1L)
    }
    lat <- numeric(n)
    bad <- 0L
    t0 <- now()
    for (i in seq_len(n)) {
        st <- now()
        send(s, payload, mode = "raw", block = TRUE)
        r <- recv(s, mode = "raw", block = 5000L)
        lat[i] <- (now() - st) * 1000
        if (inherits(r, "errorValue")) bad <- bad + 1L
    }
    el <- now() - t0
    if (bad > 0L) {
        cat(sprintf("BENCH ABORT %d/%d nng round trips failed\n", bad, n))
        quit(status = 1L)
    }
    report("unary", lat, el, n)
} else if (identical(mode, "stream")) {
    s <- socket(protocol = "pair", dial = url)
    got <- 0L
    sent <- 0L
    t0 <- now()
    ## Send-and-drain interleaved, matching the gRPC stream loop rather
    ## than sending everything first.
    while (got < n) {
        ## Non-blocking send, the direct analogue of grpc_send() returning
        ## FALSE when its queue is full: push until the transport refuses,
        ## then go drain. A blocking send here charges the benchmark its
        ## own timeout whenever the peer is briefly behind -- at
        ## block = 1000 that was about one message per second, which is a
        ## measurement of the constant rather than of nanonext.
        while (sent < n) {
            if (inherits(send(s, payload, mode = "raw", block = FALSE),
                         "errorValue")) break
            sent <- sent + 1L
        }
        ## Drain non-blocking. A blocking recv here would charge the
        ## benchmark its own timeout once per burst: with block = 200 and
        ## a 64-message burst, 20k messages spent 62 of 65 seconds
        ## waiting on an empty socket and reported 308 msgs/s for a
        ## transport doing far better than that. The number measured the
        ## constant, not nanonext.
        progressed <- FALSE
        repeat {
            r <- recv(s, mode = "raw", block = FALSE)
            if (inherits(r, "errorValue")) break
            got <- got + 1L
            progressed <- TRUE
            if (got >= n) break
        }
        ## Only wait when there was genuinely nothing to take.
        if (!progressed && got < n) {
            r <- recv(s, mode = "raw", block = 200L)
            if (!inherits(r, "errorValue")) got <- got + 1L
        }
        if (now() - t0 > 120) {
            cat(sprintf("BENCH ABORT nng stream stalled at %d/%d\n", got, n))
            quit(status = 1L)
        }
    }
    el <- now() - t0
    report("stream", NULL, el, got)
} else {
    cat(sprintf("BENCH ABORT unknown mode %s\n", mode)); quit(status = 2L)
}
close(s)
