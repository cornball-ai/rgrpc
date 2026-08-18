## Fan-out soak, server side. Holds N subscription streams open and
## pushes events to all of them, measuring how fast the single R loop
## fans out and what happens when one subscriber stops draining.
## Driven by tools/soak-fanout.sh; see that script's header for what the
## numbers mean.
##
## argv: sock nsub nevents size policy accept_window kfence
library(grpc)
sock   <- argv[[1L]]
nsub   <- as.integer(argv[[2L]])
nev    <- as.integer(argv[[3L]])
size   <- as.integer(argv[[4L]])
policy <- argv[[5L]]               # keep | fence | fenceK
aw     <- as.integer(argv[[6L]])
kfence <- if (length(argv) >= 7L) as.integer(argv[[7L]]) else 20L

srv <- grpc_server(sprintf("unix:%s", sock), accept_window = aw,
                   max_active = 1024L)
cat("ready\n"); flush(stdout())

subs <- list()
t_accept0 <- proc.time()[["elapsed"]]
t0 <- Sys.time()
while (length(subs) < nsub && as.numeric(Sys.time() - t0, units = "secs") < 120) {
    for (ev in grpc_poll(srv, timeout_ms = 100L)) {
        if (identical(ev$type, "request")) subs[[length(subs) + 1L]] <- ev
    }
}
t_accept <- proc.time()[["elapsed"]] - t_accept0
cat(sprintf("accepted %d subscribers in %.0f ms (accept_window=%d)\n",
            length(subs), t_accept * 1000, aw))
flush(stdout())
if (length(subs) < nsub) {
    cat("ABORT: not all subscribers arrived\n"); quit(status = 1L)
}

## Subscribers self-identify in their first message: 1 = drains, 2 = the
## deliberately slow one. Refusals have to be attributed, or a transient
## stall on a healthy subscriber is indistinguishable from a genuinely
## stuck one -- which is the distinction a fencing policy rests on.
is_slow <- vapply(subs, function(r) as.integer(r$request[[1L]]) == 2L, TRUE)

payload <- as.raw(rep(65L, size))
alive <- rep(TRUE, length(subs))
sent <- 0L; refused <- 0L; fenced <- 0L
refusals_by_sub <- integer(length(subs))
## Consecutive refusals per subscriber. A single FALSE means "queue full
## right now", which under load is true of healthy subscribers too; only
## a subscriber that never recovers is actually stuck.
consec <- integer(length(subs))
round_ms <- numeric(nev)

t_fan0 <- proc.time()[["elapsed"]]
for (e in seq_len(nev)) {
    r0 <- proc.time()[["elapsed"]]
    for (i in seq_along(subs)) {
        if (!alive[i]) next
        if (grpc_send(subs[[i]], payload)) {
            sent <- sent + 1L
            consec[i] <- 0L
        } else {
            refused <- refused + 1L
            refusals_by_sub[i] <- refusals_by_sub[i] + 1L
            consec[i] <- consec[i] + 1L
            drop <- identical(policy, "fence") ||
                (identical(policy, "fenceK") && consec[i] >= kfence)
            if (drop) {
                ## Backpressure as a correctness-preserving move: drop the
                ## subscriber, it reconnects from its cursor.
                grpc_finish(subs[[i]], status = "ABORTED",
                            message = "backpressure", drain = FALSE)
                alive[i] <- FALSE
                fenced <- fenced + 1L
            }
        }
    }
    ## Keep the event loop turning so write completions are processed.
    invisible(grpc_poll(srv, timeout_ms = 0L))
    round_ms[e] <- (proc.time()[["elapsed"]] - r0) * 1000
}
t_fan <- proc.time()[["elapsed"]] - t_fan0

cat(sprintf("RESULT policy=%s subs=%d events=%d size=%d kfence=%d\n",
            policy, length(subs), nev, size, kfence))
cat(sprintf("RESULT sent=%d refused=%d fenced=%d alive=%d\n",
            sent, refused, fenced, sum(alive)))
cat(sprintf("RESULT fanout_s=%.3f sends_per_s=%.0f events_per_s=%.1f\n",
            t_fan, sent / t_fan, nev / t_fan))
cat(sprintf("RESULT round_ms_median=%.2f round_ms_p95=%.2f round_ms_max=%.2f\n",
            median(round_ms), quantile(round_ms, 0.95), max(round_ms)))
cat(sprintf("RESULT refused_slow=%d refused_fast=%d fast_subs_refusing=%d\n",
            sum(refusals_by_sub[is_slow]), sum(refusals_by_sub[!is_slow]),
            sum(refusals_by_sub[!is_slow] > 0L)))
cat(sprintf("RESULT fenced_slow=%d fenced_fast=%d\n",
            sum(is_slow & !alive), sum(!is_slow & !alive)))
flush(stdout())

t1 <- Sys.time()
while (as.numeric(Sys.time() - t1, units = "secs") < 5) {
    invisible(grpc_poll(srv, timeout_ms = 100L))
}
for (i in seq_along(subs)) if (alive[i]) grpc_finish(subs[[i]])
t1 <- Sys.time()
while (as.numeric(Sys.time() - t1, units = "secs") < 3) {
    invisible(grpc_poll(srv, timeout_ms = 100L))
}
cat("DONE\n"); flush(stdout())
grpc_close(srv)
