## Unary client lifecycle (increment 2). No responding peer exists until
## the server increment, so these tests exercise the error paths that
## complete without one: UNAVAILABLE, DEADLINE_EXCEEDED, CANCELLED.

cl <- grpc_client("127.0.0.1:1")
expect_inherits(cl, "grpc_client")
expect_true(grpc_fd(cl) > 0L)
expect_equal(grpc_pending(cl), 0L)
grpc_close(cl)
expect_error(grpc_fd(cl), "closed")
expect_silent(grpc_close(cl))

if (at_home()) {
  drain <- function(client, n = 1L, budget_ms = 5000L) {
    evs <- list()
    t0 <- Sys.time()
    while (length(evs) < n &&
           as.numeric(Sys.time() - t0, units = "secs") * 1000 < budget_ms) {
      evs <- c(evs, grpc_poll(client, timeout_ms = 200L))
    }
    evs
  }

  ## fast failure against a closed port
  cl <- grpc_client("127.0.0.1:1")
  call <- grpc_call(cl, "/demo.Echo/Say", as.raw(c(0x0a, 0x02, 0x68, 0x69)))
  expect_inherits(call, "grpc_call")
  evs <- drain(cl)
  expect_equal(length(evs), 1L)
  ev <- evs[[1]]
  expect_equal(ev$id, call$id)
  expect_equal(ev$status_name, "UNAVAILABLE")
  expect_null(ev$response)
  expect_equal(grpc_pending(cl), 0L)
  grpc_close(cl)

  ## deadline against a live generic server that never responds
  srv <- grpc:::.server_create("127.0.0.1:0")
  cl <- grpc_client(sprintf("127.0.0.1:%d", grpc:::.server_port(srv)))
  call <- grpc_call(cl, "/demo.Echo/Say", raw(0), deadline_ms = 200)
  evs <- drain(cl)
  expect_equal(evs[[1]]$status_name, "DEADLINE_EXCEEDED")

  ## explicit cancellation
  call2 <- grpc_call(cl, "/demo.Echo/Say", raw(0))
  expect_equal(grpc_pending(cl), 1L)
  grpc_cancel(call2)
  evs <- drain(cl)
  expect_equal(evs[[1]]$status_name, "CANCELLED")
  grpc_close(cl)
  grpc:::.server_destroy(srv)

  ## metadata on the request does not disturb completion
  cl <- grpc_client("127.0.0.1:1")
  call <- grpc_call(cl, "/demo.Echo/Say", raw(0),
                    metadata = c("x-trace-id" = "abc123"))
  evs <- drain(cl)
  expect_equal(evs[[1]]$status_name, "UNAVAILABLE")
  grpc_close(cl)

  ## stress: 100 concurrent calls, batched polls, GC interleaved
  cl <- grpc_client("127.0.0.1:1")
  ids <- vapply(1:100, function(i) grpc_call(cl, "/x/Y", raw(0))$id,
                numeric(1))
  expect_equal(length(unique(ids)), 100L)
  got <- 0L
  t0 <- Sys.time()
  while (got < 100L && as.numeric(Sys.time() - t0, units = "secs") < 10) {
    got <- got + length(grpc_poll(cl, max_events = 7L, timeout_ms = 100L))
    invisible(gc())
  }
  expect_equal(got, 100L)
  expect_equal(grpc_pending(cl), 0L)

  ## close with a queued call still in flight
  call <- grpc_call(cl, "/x/Y", raw(0), wait_for_ready = TRUE)
  expect_silent(grpc_close(cl))

  ## finalizer path: client garbage collected while a call is in flight
  cl <- grpc_client("127.0.0.1:1")
  call <- grpc_call(cl, "/x/Y", raw(0), wait_for_ready = TRUE)
  rm(cl, call)
  invisible(gc())
}
