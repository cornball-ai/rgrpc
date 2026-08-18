## Abortive close (fencing, vientito follow-up): grpc_finish(drain =
## FALSE) discards queued writes and prioritizes the terminal status;
## grpc_cancel() on a request is the hard escalation past a peer that
## has stopped reading. The scenario is session replacement: stale
## assignments queued for an evicted node must not be delivered, and
## must not delay the ABORTED fence.

if (at_home()) {
  await_req <- function(srv, budget_ms = 5000L) {
    t0 <- Sys.time()
    repeat {
      for (ev in grpc_poll(srv, timeout_ms = 200L)) {
        if (ev$type == "request") return(ev)
      }
      if (as.numeric(Sys.time() - t0, units = "secs") * 1000 > budget_ms) {
        return(NULL)
      }
    }
  }
  ## The queue only backs up once payloads exceed what flow control
  ## will absorb for a client that is not reading; gRPC's BDP probing
  ## grows windows well past 64KB on loopback, so the messages must be
  ## large — 12 x 1MB reliably leaves several queued server-side.
  payload <- as.raw(rep(1L, 1048576))
  fence_setup <- function() {
    srv <- grpc_server()
    cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))
    s <- grpc_stream(cl, "/demo.Echo/Watch", deadline_ms = 30000,
                     read_buffer = 1L)
    grpc_send(s, raw(1))
    req <- await_req(srv)
    sent <- vapply(1:12, function(j) grpc_send(req, payload), logical(1))
    list(srv = srv, cl = cl, req = req, sent = sent)
  }
  drain_client <- function(cl, budget_s = 10) {
    got <- 0L
    st <- NULL
    t0 <- Sys.time()
    while (is.null(st) &&
           as.numeric(Sys.time() - t0, units = "secs") < budget_s) {
      for (ev in grpc_poll(cl, timeout_ms = 200L)) {
        if (ev$kind == "stream_msg") got <- got + 1L
        if (ev$kind == "stream_status") st <- ev
      }
    }
    list(got = got, st = st)
  }

  ## ---- abortive: queued stale writes must not delay the fence ----
  f <- fence_setup()
  expect_true(all(f$sent))
  expect_true(grpc_finish(f$req, status = "ABORTED",
                          message = "session replaced", drain = FALSE))
  r <- drain_client(f$cl)
  expect_false(is.null(r$st))
  expect_equal(r$st$status_name, "ABORTED")
  expect_equal(r$st$message, "session replaced")
  ## the queue was discarded: only in-flight/buffered messages arrive
  expect_true(r$got >= 1L)
  expect_true(r$got < 12L)
  grpc_close(f$cl)
  grpc_close(f$srv)

  ## ---- graceful contrast: drain = TRUE delivers everything first ----
  f <- fence_setup()
  expect_true(all(f$sent))
  expect_true(grpc_finish(f$req))
  r <- drain_client(f$cl)
  expect_false(is.null(r$st))
  expect_equal(r$st$status_name, "OK")
  expect_equal(r$got, 12L)
  grpc_close(f$cl)
  grpc_close(f$srv)

  ## ---- hard escalation: peer never reads, cancel cuts through ----
  f <- fence_setup()
  expect_true(all(f$sent))
  expect_true(grpc_finish(f$req, status = "ABORTED", message = "fence",
                          drain = FALSE))
  ## the client is not polling; normally the in-flight write is stalled
  ## by flow control and even the abortive status cannot get out, so
  ## cancel finds a live call (TRUE). Under heavy slowdown (valgrind)
  ## the write can complete and the finish post first, in which case
  ## cancel reports FALSE. FALSE means only that the call is no longer
  ## live server-side — no further stale writes are possible — NOT that
  ## the ABORTED status reached the peer (a dead call reports FALSE
  ## too). In this controlled scenario the client is alive, so on the
  ## FALSE path the completed finish does deliver ABORTED.
  Sys.sleep(0.3)
  cancelled <- grpc_cancel(f$req)
  r <- drain_client(f$cl)
  expect_false(is.null(r$st))
  if (isTRUE(cancelled)) {
    expect_true(r$st$status_name %in% c("CANCELLED", "ABORTED"))
  } else {
    expect_equal(r$st$status_name, "ABORTED")
  }
  grpc_close(f$cl)
  grpc_close(f$srv)
}
