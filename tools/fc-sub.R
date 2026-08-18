## Flow-control probe, subscriber side. Driven by tools/fc-probe.sh.
##
##   stuck  Opens the stream and never reads, so every byte the server
##          pushes has to sit in a buffer between the two processes.
##   drain  Polls continuously, so the flow-control window never closes
##          and any refusal the server sees can only be its own write
##          pipeline, not the peer.
##
## The pair is the control: the same refusal at the same call site means
## opposite things in the two modes, which is the point being measured.
##
## argv: sock read_buffer hold_s mode
library(grpc)
sock <- argv[[1L]]
rbuf <- as.integer(argv[[2L]])
hold <- as.numeric(argv[[3L]])
mode <- if (length(argv) >= 4L) argv[[4L]] else "stuck"

## Report which build is loaded, so a run against a patched diagnostic
## build cannot be confused with one against the installed package. A
## probe that reports "no effect" is worthless if it silently loaded a
## different library than the one being tested.
cat(sprintf("WHICH so=%s mode=%s read_buffer=%d\n",
            getLoadedDLLs()[["grpc"]][["path"]], mode, rbuf))
flush(stdout())

cl <- grpc_client(sprintf("unix://%s", sock))
s <- grpc_stream(cl, "/room.v1.Room/Subscribe", read_buffer = rbuf)
grpc_send(s, as.raw(1L))
grpc_writes_done(s)
cat("subscribed\n"); flush(stdout())

if (identical(mode, "drain")) {
    got <- 0L
    t0 <- Sys.time()
    while (as.numeric(Sys.time() - t0, units = "secs") < hold) {
        for (ev in grpc_poll(cl, max_events = 512L, timeout_ms = 20L)) {
            if (identical(ev$kind, "stream_msg")) got <- got + 1L
        }
    }
    cat(sprintf("SUB drained=%d\n", got))
} else {
    ## Deliberately no grpc_poll(): not draining is the whole point.
    ## Polling here would replenish the window and there would be
    ## nothing to measure.
    Sys.sleep(hold)
}
cat("SUB done\n"); flush(stdout())
