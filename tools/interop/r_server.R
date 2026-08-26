## R interop peer, server side. Driven by tools/interop/interop.sh.
## Serves interop.v1.Interop so the C++ and Python clients have something
## of ours to talk to.
##
## argv: addr proto_dir secs
library(rgrpc)
addr  <- argv[[1L]]
pdir  <- argv[[2L]]
secs  <- if (length(argv) >= 3L) as.numeric(argv[[3L]]) else 60
RProtoBuf::readProtoFiles2("interop.proto", protoPath = normalizePath(pdir))

srv <- grpc_server(addr)
cat("ready\n"); flush(stdout())

reply_for <- function(dec) {
    ## Echo the fields back. Building the reply out of the decoded
    ## request is the point: if a peer's encoding of maps, repeated
    ## fields or oneofs does not survive RProtoBuf, it shows up here.
    ##
    ## Map entries cannot be moved between messages as-is. A proto3 map
    ## generates a nested entry type per field, so EchoRequest.LabelsEntry
    ## and EchoReply.LabelsEntry are distinct types despite being
    ## identical on the wire; RProtoBuf enforces that. Rebuild them.
    labels <- lapply(dec$labels, function(e) {
        RProtoBuf::P("interop.v1.EchoReply.LabelsEntry")$new(
            key = e$key, value = e$value)
    })
    items <- lapply(dec$items, function(i) {
        RProtoBuf::P("interop.v1.Item")$new(name = i$name, count = i$count)
    })
    RProtoBuf::P("interop.v1.EchoReply")$new(
        text = dec$text, items = items, labels = labels, responder = "r")
}

t0 <- Sys.time()
while (as.numeric(Sys.time() - t0, units = "secs") < secs) {
    for (ev in grpc_poll(srv, timeout_ms = 200L)) {
        if (!identical(ev$type, "request")) next
        dec <- grpc_decode(ev$request, "interop.v1.EchoRequest")
        if (endsWith(ev$method, "/Echo")) {
            grpc_reply(ev, reply_for(dec))
        } else if (endsWith(ev$method, "/EchoStream")) {
            n <- as.integer(dec$number)
            for (i in seq_len(n)) {
                grpc_send(ev, RProtoBuf::P("interop.v1.EchoReply")$new(
                    text = sprintf("%s-%d", dec$text, i - 1L),
                    responder = "r"))
            }
            grpc_finish(ev)
        } else if (endsWith(ev$method, "/Fail")) {
            grpc_reply(ev, status = "FAILED_PRECONDITION",
                       message = "interop failure")
        } else {
            grpc_reply(ev, status = "UNIMPLEMENTED", message = ev$method)
        }
    }
}
grpc_close(srv)
