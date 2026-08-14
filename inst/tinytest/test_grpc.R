## Spike lifetime tests: create/destroy cycles must not crash, leak, or
## leave the process wedged. No RPC traffic; channel creation is lazy and
## attempts no connection.

expect_true(is.character(grpc_version()))
expect_true(nchar(grpc_version()) > 0L)

## channel create/destroy cycles
for (i in 1:50) {
  ch <- grpc:::.channel_create("dns:///127.0.0.1:1")
  expect_inherits(ch, "externalptr")
  grpc:::.channel_destroy(ch)
}

## double destroy is a no-op
ch <- grpc:::.channel_create("dns:///127.0.0.1:1")
grpc:::.channel_destroy(ch)
expect_silent(grpc:::.channel_destroy(ch))

## completion queues shut down and drain cleanly
for (i in 1:50) {
  cq <- grpc:::.cq_create()
  grpc:::.cq_destroy(cq)
}

## finalizers handle objects never explicitly destroyed
ch <- grpc:::.channel_create("dns:///127.0.0.1:1")
cq <- grpc:::.cq_create()
rm(ch, cq)
invisible(gc())

if (at_home()) {
  ## ephemeral-port localhost bind; skipped during R CMD check
  for (i in 1:10) {
    srv <- grpc:::.server_create("127.0.0.1:0")
    expect_true(grpc:::.server_port(srv) > 0L)
    grpc:::.server_destroy(srv)
  }
  srv <- grpc:::.server_create("127.0.0.1:0")
  grpc:::.server_destroy(srv)
  expect_silent(grpc:::.server_destroy(srv))
}
