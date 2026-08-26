## Spike lifetime tests: create/destroy cycles must not crash, leak, or
## leave the process wedged. No RPC traffic; channel creation is lazy and
## attempts no connection.

expect_true(is.character(grpc_version()))
expect_true(nchar(grpc_version()) > 0L)

## channel create/destroy cycles
for (i in 1:50) {
  ch <- rgrpc:::.channel_create("dns:///127.0.0.1:1")
  expect_inherits(ch, "externalptr")
  rgrpc:::.channel_destroy(ch)
}

## double destroy is a no-op
ch <- rgrpc:::.channel_create("dns:///127.0.0.1:1")
rgrpc:::.channel_destroy(ch)
expect_silent(rgrpc:::.channel_destroy(ch))

## completion queues shut down and drain cleanly
for (i in 1:50) {
  cq <- rgrpc:::.cq_create()
  rgrpc:::.cq_destroy(cq)
}

## finalizers handle objects never explicitly destroyed
ch <- rgrpc:::.channel_create("dns:///127.0.0.1:1")
cq <- rgrpc:::.cq_create()
rm(ch, cq)
invisible(gc())

if (at_home()) {
  ## ephemeral-port localhost bind; skipped during R CMD check
  for (i in 1:10) {
    srv <- rgrpc:::.server_create("127.0.0.1:0")
    expect_true(rgrpc:::.server_port(srv) > 0L)
    rgrpc:::.server_destroy(srv)
  }
  srv <- rgrpc:::.server_create("127.0.0.1:0")
  rgrpc:::.server_destroy(srv)
  expect_silent(rgrpc:::.server_destroy(srv))
}
