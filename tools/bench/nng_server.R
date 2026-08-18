## nanonext echo server for tools/bench/bench.sh. The incumbent
## transport, measured under the same message shapes as the gRPC path so
## the vientito comparison is like-for-like.
##
## rep  request/reply, the shape vientito's control plane uses
## pair full-duplex, the closest nanonext analogue to a bidi stream
##
## argv: url mode secs
library(nanonext)
url  <- argv[[1L]]
mode <- argv[[2L]]
secs <- if (length(argv) >= 3L) as.numeric(argv[[3L]]) else 120

s <- socket(protocol = if (identical(mode, "rep")) "rep" else "pair",
            listen = url)
cat("ready\n"); flush(stdout())

deadline <- as.numeric(Sys.time()) + secs
repeat {
    if (as.numeric(Sys.time()) > deadline) break
    ## block = 500 so the loop can notice the deadline; an unbounded
    ## recv would leave the process alive after the driver moved on.
    msg <- recv(s, mode = "raw", block = 500L)
    if (inherits(msg, "errorValue")) next
    send(s, msg, mode = "raw", block = TRUE)
}
close(s)
