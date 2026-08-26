## Flow-control probe, server side. Driven by tools/fc-probe.sh; see that
## script's header for what the numbers mean and how to read them.
##
## Two modes, answering two different questions:
##
##   ceiling  How much does a subscriber that never reads absorb before
##            the server can make no further progress? Sends until
##            grpc_send() has been refused continuously for `settle_s`,
##            retrying throughout -- a single refusal is not the answer,
##            because the first one is usually the local write queue.
##
##   duration How long does a refused grpc_send() stay refused? Records
##            every refusal episode from the first FALSE to the next
##            TRUE. This is the measurement that separates the two
##            reasons a send can be refused, which look identical at the
##            call site and mean completely different things.
##
## argv: sock mode size n settle_s delay_us
library(rgrpc)
sock     <- argv[[1L]]
mode     <- argv[[2L]]
size     <- as.integer(argv[[3L]])
n        <- as.integer(argv[[4L]])
settle_s <- if (length(argv) >= 5L) as.numeric(argv[[5L]]) else 2
delay_us <- if (length(argv) >= 6L) as.numeric(argv[[6L]]) else 0

srv <- grpc_server(sprintf("unix:%s", sock), max_active = 16L)
cat("ready\n"); flush(stdout())

sub <- NULL
t0 <- Sys.time()
while (is.null(sub) && as.numeric(Sys.time() - t0, units = "secs") < 30) {
    for (ev in grpc_poll(srv, timeout_ms = 100L)) {
        if (identical(ev$type, "request")) sub <- ev
    }
}
if (is.null(sub)) { cat("ABORT: no subscriber\n"); quit(status = 1L) }

payload <- as.raw(rep(65L, size))

if (identical(mode, "ceiling")) {
    accepted <- 0L
    refusals <- 0L
    first_refusal_at <- NA_integer_
    stalled <- FALSE
    t_send0 <- proc.time()[["elapsed"]]
    for (i in seq_len(n)) {
        if (grpc_send(sub, payload)) {
            accepted <- accepted + 1L
            if (delay_us > 0) Sys.sleep(delay_us / 1e6)
        } else {
            refusals <- refusals + 1L
            if (is.na(first_refusal_at)) first_refusal_at <- accepted
            ## The queue drains on the CQ thread, so retry rather than
            ## stop: this is what distinguishes "momentarily full" from
            ## "the peer has stopped granting credit". Only settle_s of
            ## no progress at all counts as the ceiling.
            t1 <- Sys.time()
            drained <- FALSE
            while (as.numeric(Sys.time() - t1, units = "secs") < settle_s) {
                invisible(grpc_poll(srv, timeout_ms = 20L))
                if (grpc_send(sub, payload)) {
                    accepted <- accepted + 1L
                    drained <- TRUE
                    break
                }
            }
            if (!drained) { stalled <- TRUE; break }
        }
        invisible(grpc_poll(srv, timeout_ms = 0L))
    }
    t_send <- proc.time()[["elapsed"]] - t_send0
    cat(sprintf("FC size=%d delay_us=%.0f settle_s=%.1f first_refusal_at=%s stalled=%s\n",
                size, delay_us, settle_s, first_refusal_at, stalled))
    cat(sprintf("FC accepted=%d bytes=%d mb=%.2f refusals=%d elapsed_s=%.2f\n",
                accepted, accepted * size, accepted * size / 1048576,
                refusals, t_send))
} else if (identical(mode, "duration")) {
    ## Episodes that never end are censored and reported separately.
    ## Averaging them in would beg the entire question.
    durs <- numeric(0)
    accepted <- 0L
    censored_ms <- NA_real_
    for (i in seq_len(n)) {
        if (grpc_send(sub, payload)) { accepted <- accepted + 1L; next }
        t_ref <- proc.time()[["elapsed"]]
        ok <- FALSE
        while (proc.time()[["elapsed"]] - t_ref < settle_s) {
            invisible(grpc_poll(srv, timeout_ms = 0L))
            if (grpc_send(sub, payload)) { ok <- TRUE; break }
        }
        d <- (proc.time()[["elapsed"]] - t_ref) * 1000
        if (ok) { durs <- c(durs, d); accepted <- accepted + 1L }
        else { censored_ms <- d; break }
    }
    cat(sprintf("DUR size=%d accepted=%d episodes=%d censored_ms=%.0f\n",
                size, accepted, length(durs), censored_ms))
    if (length(durs)) {
        cat(sprintf("DUR transient_ms min=%.3f median=%.3f p95=%.3f max=%.3f\n",
                    min(durs), median(durs), quantile(durs, 0.95), max(durs)))
        cat(sprintf("DUR over_10ms=%d over_100ms=%d over_500ms=%d of=%d\n",
                    sum(durs > 10), sum(durs > 100), sum(durs > 500),
                    length(durs)))
    } else {
        cat("DUR transient_ms none\n")
    }
} else {
    cat(sprintf("ABORT: unknown mode %s\n", mode)); quit(status = 1L)
}
flush(stdout())

grpc_finish(sub, status = "ABORTED", message = "done", drain = FALSE)
t1 <- Sys.time()
while (as.numeric(Sys.time() - t1, units = "secs") < 1) {
    invisible(grpc_poll(srv, timeout_ms = 50L))
}
cat("DONE\n"); flush(stdout())
grpc_close(srv)
