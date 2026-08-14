## Generic server (increment 3): construction, then in-process
## client/server round trips over TCP and unix-domain sockets.

srv <- grpc_server()
expect_inherits(srv, "grpc_server")
expect_true(grpc_server_port(srv) > 0L)
expect_true(grpc_fd(srv) > 0L)
expect_equal(grpc_pending(srv), 0L)
grpc_close(srv)
expect_error(grpc_fd(srv), "closed")
expect_silent(grpc_close(srv))

if (at_home()) {
  await <- function(x, n = 1L, budget_ms = 5000L) {
    evs <- list()
    t0 <- Sys.time()
    while (length(evs) < n &&
           as.numeric(Sys.time() - t0, units = "secs") * 1000 < budget_ms) {
      evs <- c(evs, grpc_poll(x, timeout_ms = 200L))
    }
    evs
  }

  ## echo round trip over TCP, with metadata both ways
  srv <- grpc_server()
  cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))
  payload <- as.raw(sample(0:255, 300, replace = TRUE))
  call <- grpc_call(cl, "/demo.Echo/Say", payload, deadline_ms = 5000,
                    metadata = c("x-req-id" = "42"))
  sevs <- await(srv)
  expect_equal(length(sevs), 1L)
  sev <- sevs[[1]]
  expect_inherits(sev, "grpc_request")
  expect_equal(sev$type, "request")
  expect_equal(sev$method, "/demo.Echo/Say")
  expect_identical(sev$request, payload)
  expect_equal(unname(sev$metadata[["x-req-id"]]), "42")
  expect_true(is.finite(sev$deadline_ms) && sev$deadline_ms > 0)
  expect_true(grpc_reply(sev, sev$request, metadata = c("x-served" = "yes")))
  cevs <- await(cl)
  expect_equal(length(cevs), 1L)
  cev <- cevs[[1]]
  expect_equal(cev$id, call$id)
  expect_equal(cev$status_name, "OK")
  expect_identical(cev$response, payload)
  expect_equal(unname(cev$trailing_metadata[["x-served"]]), "yes")

  ## replying twice to the same request is refused
  expect_false(grpc_reply(sev, payload))

  ## error status propagates with message, no response payload
  call2 <- grpc_call(cl, "/demo.Echo/Say", raw(0), deadline_ms = 5000)
  sev2 <- await(srv)[[1]]
  expect_true(grpc_reply(sev2, status = "NOT_FOUND", message = "no such thing"))
  cev2 <- await(cl)[[1]]
  expect_equal(cev2$status_name, "NOT_FOUND")
  expect_equal(cev2$message, "no such thing")
  expect_null(cev2$response)

  ## client cancel surfaces as a cancelled event; late reply is refused
  call3 <- grpc_call(cl, "/demo.Echo/Say", raw(0))
  sev3 <- await(srv)[[1]]
  expect_equal(sev3$type, "request")
  grpc_cancel(call3)
  cev3 <- await(cl)[[1]]
  expect_equal(cev3$status_name, "CANCELLED")
  xev <- await(srv)[[1]]
  expect_equal(xev$type, "cancelled")
  expect_equal(xev$id, sev3$id)
  expect_false(grpc_reply(sev3, raw(0)))

  ## client deadline fires while the server sits on the request
  call4 <- grpc_call(cl, "/demo.Echo/Say", raw(0), deadline_ms = 200)
  sev4 <- await(srv)[[1]]
  cev4 <- await(cl)[[1]]
  expect_equal(cev4$status_name, "DEADLINE_EXCEEDED")
  xev4 <- await(srv)[[1]]
  expect_equal(xev4$type, "cancelled")

  ## pipelined burst: 20 echoes served out of one poll loop
  ids <- vapply(1:20, function(i) {
    grpc_call(cl, "/demo.Echo/N", as.raw(i), deadline_ms = 5000)$id
  }, numeric(1))
  served <- 0L
  t0 <- Sys.time()
  while (served < 20L && as.numeric(Sys.time() - t0, units = "secs") < 10) {
    for (ev in grpc_poll(srv, timeout_ms = 100L)) {
      if (ev$type == "request") {
        grpc_reply(ev, ev$request)
        served <- served + 1L
      }
    }
  }
  expect_equal(served, 20L)
  cevs <- await(cl, n = 20L, budget_ms = 10000L)
  expect_equal(length(cevs), 20L)
  expect_true(all(vapply(cevs, function(e) e$status_name, "") == "OK"))
  ## each echoed byte matches the byte sent for that call id
  for (e in cevs) {
    expect_identical(e$response, as.raw(which(ids == e$id)))
  }

  grpc_close(cl)
  grpc_close(srv)

  ## unix-domain socket round trip
  sock <- file.path(tempdir(), "grpc-test.sock")
  srvu <- grpc_server(sprintf("unix:%s", sock))
  clu <- grpc_client(sprintf("unix:%s", sock))
  callu <- grpc_call(clu, "/demo.Echo/Say", as.raw(7:9), deadline_ms = 5000)
  sevu <- await(srvu)[[1]]
  expect_identical(sevu$request, as.raw(7:9))
  grpc_reply(sevu, sevu$request)
  cevu <- await(clu)[[1]]
  expect_equal(cevu$status_name, "OK")
  expect_identical(cevu$response, as.raw(7:9))
  grpc_close(clu)
  grpc_close(srvu)
  unlink(sock)

  ## server close with a request delivered but unanswered
  srv <- grpc_server()
  cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))
  call <- grpc_call(cl, "/demo.Echo/Say", raw(0), deadline_ms = 5000)
  sev <- await(srv)[[1]]
  expect_silent(grpc_close(srv))
  cev <- await(cl)[[1]]
  expect_true(cev$status_name %in% c("UNAVAILABLE", "CANCELLED"))
  grpc_close(cl)

  ## finalizer path: server garbage collected with an active call
  srv <- grpc_server()
  cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))
  call <- grpc_call(cl, "/demo.Echo/Say", raw(0))
  invisible(await(srv))
  rm(srv)
  invisible(gc())
  grpc_close(cl)
}
