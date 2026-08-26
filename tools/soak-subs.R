## Fan-out soak, subscriber side. nfast streams that drain continuously,
## plus optionally one that never drains -- the slow consumer whose queue
## fills and makes the server's grpc_send() refuse.
## Driven by tools/soak-fanout.sh.
##
## argv: sock nfast nclients slow|noslow
library(rgrpc)
sock     <- argv[[1L]]
nfast    <- as.integer(argv[[2L]])
nclients <- as.integer(argv[[3L]])
slow     <- identical(argv[[4L]], "slow")
tgt <- sprintf("unix://%s", sock)

clients <- lapply(seq_len(nclients), function(i) grpc_client(tgt))
streams <- vector("list", nfast)
owner <- integer(nfast)
for (i in seq_len(nfast)) {
    ci <- ((i - 1L) %% nclients) + 1L
    s <- grpc_stream(clients[[ci]], "/room.v1.Room/Subscribe",
                     read_buffer = 64L)
    grpc_send(s, as.raw(1L))
    grpc_writes_done(s)
    streams[[i]] <- s
    owner[i] <- ci
}

## The slow consumer gets its own client so that never polling it starves
## only itself rather than its neighbours.
slow_cl <- NULL
if (slow) {
    slow_cl <- grpc_client(tgt)
    ss <- grpc_stream(slow_cl, "/room.v1.Room/Subscribe", read_buffer = 4L)
    grpc_send(ss, as.raw(2L))
    grpc_writes_done(ss)
}
cat("subscribed\n"); flush(stdout())

## Stream ids are per-client and restart at 1 for each, so the key has to
## carry the client too -- keying on the id alone silently double-counts
## one stream and zeroes another, which reads as a plausible total.
counts <- integer(nfast)
names(counts) <- vapply(seq_len(nfast),
                        function(i) sprintf("%d:%s", owner[i],
                                            as.character(streams[[i]]$id)), "")
status_seen <- 0L
t0 <- Sys.time()
while (as.numeric(Sys.time() - t0, units = "secs") < 45) {
    quiet <- TRUE
    for (ci in seq_len(nclients)) {
        evs <- grpc_poll(clients[[ci]], max_events = 256L, timeout_ms = 5L)
        if (length(evs)) quiet <- FALSE
        for (ev in evs) {
            key <- sprintf("%d:%s", ci, as.character(ev$id))
            if (identical(ev$kind, "stream_msg")) {
                idx <- match(key, names(counts))
                if (!is.na(idx)) counts[idx] <- counts[idx] + 1L
            } else if (identical(ev$kind, "stream_status")) {
                status_seen <- status_seen + 1L
            }
        }
    }
    if (quiet && status_seen >= nfast) break
}

cat(sprintf("SUBS fast=%d clients=%d slow=%s\n", nfast, nclients, slow))
cat(sprintf("SUBS received_total=%d min=%d median=%.0f max=%d\n",
            sum(counts), min(counts), median(counts), max(counts)))
cat(sprintf("SUBS spread_max_minus_min=%d statuses=%d\n",
            max(counts) - min(counts), status_seen))
flush(stdout())
for (cl in clients) grpc_close(cl)
if (!is.null(slow_cl)) grpc_close(slow_cl)
