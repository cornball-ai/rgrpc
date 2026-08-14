## Forced-error cleanup (increment 8): every operation on a closed
## handle errors cleanly, finalizers run with work in flight, and
## create/destroy churn leaves nothing behind. This file is the main
## workload for the sanitizer passes (ASan/TSan/valgrind).

## ---- operations on a closed client error, close stays idempotent ----
cl <- grpc_client("127.0.0.1:1")
grpc_close(cl)
expect_silent(grpc_close(cl))
expect_error(grpc_call(cl, "/x/Y", raw(0)), "closed")
expect_error(grpc_poll(cl), "closed")
expect_error(grpc_pending(cl), "closed")
expect_error(grpc_fd(cl), "closed")
expect_error(grpc_state(cl), "closed")

if (at_home()) {
  await <- function(x, budget_ms = 5000L) {
    t0 <- Sys.time()
    repeat {
      evs <- grpc_poll(x, timeout_ms = 200L)
      if (length(evs)) return(evs)
      if (as.numeric(Sys.time() - t0, units = "secs") * 1000 > budget_ms) {
        return(list())
      }
    }
  }

  ## ---- operations on a closed server error ----
  srv <- grpc_server()
  cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))
  call <- grpc_call(cl, "/demo.Echo/Say", raw(0), deadline_ms = 5000)
  req <- await(srv)[[1]]
  grpc_close(srv)
  expect_silent(grpc_close(srv))
  expect_error(grpc_reply(req, raw(0)), "closed")
  expect_error(grpc_read(req), "closed")
  expect_error(grpc_send(req, raw(0)), "closed")
  expect_error(grpc_finish(req), "closed")
  expect_error(grpc_poll(srv), "closed")
  expect_error(grpc_server_port(srv), "closed")
  grpc_close(cl)

  ## ---- stream operations after the client is closed ----
  srv <- grpc_server()
  cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))
  s <- grpc_stream(cl, "/demo.Echo/Chat", deadline_ms = 10000)
  grpc_send(s, raw(1))
  grpc_close(cl)
  expect_error(grpc_send(s, raw(1)), "closed")
  expect_error(grpc_writes_done(s), "closed")
  expect_error(grpc_cancel(s), "closed")
  grpc_close(srv)

  ## ---- finalizer with an open stream and queued writes ----
  srv <- grpc_server()
  cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))
  s <- grpc_stream(cl, "/demo.Echo/Chat", deadline_ms = 10000)
  for (i in 1:8) grpc_send(s, as.raw(i))
  rm(cl, s)
  invisible(gc())
  grpc_close(srv)

  ## ---- finalizer on the server with a live stream ----
  srv <- grpc_server()
  cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))
  s <- grpc_stream(cl, "/demo.Echo/Chat", deadline_ms = 10000)
  grpc_send(s, raw(1))
  invisible(await(srv))
  rm(srv)
  invisible(gc())
  grpc_close(cl)

  ## ---- server close with queued writes still pumping ----
  ## Reaches the shutting guard in the server's write pump: write-ok
  ## completions queued at the instant of cq shutdown drain afterwards
  ## and must not post follow-on ops. The window is a race; 32 streams
  ## with large payloads keep enough writes in flight that removing the
  ## guard aborts the process (verified by mutation, 5/5).
  payload <- as.raw(rep(1L, 131072))
  for (i in 1:20) {
    srv <- grpc_server()
    cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))
    streams <- lapply(1:32, function(j) {
      s <- grpc_stream(cl, "/demo.Echo/Watch", deadline_ms = 10000)
      grpc_send(s, raw(1))
      s
    })
    reqs <- list()
    t0 <- Sys.time()
    while (length(reqs) < 32 &&
           as.numeric(Sys.time() - t0, units = "secs") < 5) {
      for (ev in grpc_poll(srv, timeout_ms = 200L)) {
        if (ev$type == "request") reqs[[length(reqs) + 1L]] <- ev
      }
    }
    for (req in reqs) for (j in 1:8) grpc_send(req, payload)
    grpc_close(srv)
    grpc_close(cl)
  }

  ## ---- accepts matching concurrently with shutdown ----
  ## Reaches the shutting branch of the accept handler: a call matched
  ## just as the server closes must not post its first read.
  for (i in 1:5) {
    srv <- grpc_server()
    cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))
    for (j in 1:5) {
      grpc_call(cl, "/x/Y", raw(0), deadline_ms = 5000,
                wait_for_ready = TRUE)
    }
    grpc_close(srv)
    grpc_close(cl)
  }
  expect_true(TRUE)

  ## ---- create/destroy churn with work in flight ----
  for (i in 1:20) {
    srv <- grpc_server()
    cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))
    call <- grpc_call(cl, "/x/Y", raw(0), deadline_ms = 5000)
    if (i %% 2L == 0L) {
      req <- await(srv)
      if (length(req)) grpc_reply(req[[1]], raw(0))
    }
    grpc_close(cl)
    grpc_close(srv)
  }
  invisible(gc())
  expect_true(TRUE)
}
