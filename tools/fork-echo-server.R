## Minimal echo server for tools/fork-probe.sh and tools/interop.sh.
## Answers every unary request with the bytes it received. Runs until
## the socket's parent script kills it or `secs` elapses.
##
## argv: sock secs
library(rgrpc)
sock <- argv[[1L]]
secs <- if (length(argv) >= 2L) as.numeric(argv[[2L]]) else 120

srv <- grpc_server(sprintf("unix:%s", sock))
cat("ready\n"); flush(stdout())

t0 <- Sys.time()
while (as.numeric(Sys.time() - t0, units = "secs") < secs) {
    for (ev in grpc_poll(srv, timeout_ms = 200L)) {
        if (identical(ev$type, "request")) grpc_reply(ev, ev$request)
    }
}
grpc_close(srv)
