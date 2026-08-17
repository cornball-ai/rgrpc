## Poll readiness invariant: an empty grpc_poll() must have waited out its
## timeout. The eventfd carries one signal per event, so draining it outside
## the lock that guards the ready deque leaves every signal posted after the
## read still counted, even though the batch below takes those events. The
## counter then holds a stale credit that the next poll spends as an instant,
## empty wake-up, and a caller reading "no events" as "the call never
## started" abandons a live stream (issue #12).

if (at_home()) {
  ## Poll, and record any empty result that cost far less than its timeout.
  spurious <- 0L
  poll_checked <- function(x, timeout_ms) {
    t0 <- proc.time()[["elapsed"]]
    evs <- grpc_poll(x, timeout_ms = timeout_ms)
    if (!length(evs) &&
        (proc.time()[["elapsed"]] - t0) * 1000 < timeout_ms / 2) {
      spurious <<- spurious + 1L
    }
    evs
  }

  ## Big enough that the completion thread is still pushing reads while the
  ## main thread crosses from the eventfd drain to the ready deque.
  payload <- as.raw(rep(65L, 32768))
  srv <- grpc_server()
  cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))

  for (round in 1:60) {
    s <- grpc_stream(cl, "/demo.Big/List", deadline_ms = 15000)
    expect_true(grpc_send(s, as.raw(1)))
    expect_true(grpc_writes_done(s))

    ## Budgets below are anti-hang guards, not pacing: generous enough
    ## that a valgrind or sanitizer run does not trip them.
    req <- NULL
    t0 <- Sys.time()
    while (is.null(req) && as.numeric(Sys.time() - t0, units = "secs") < 30) {
      for (ev in grpc_poll(srv, timeout_ms = 100L)) {
        if (identical(ev$type, "request")) req <- ev
      }
    }
    expect_false(is.null(req))

    ## Server writes and client reads interleave: send what the server's
    ## queue accepts, draining both sides, until 16 messages are away.
    sent <- 0L
    t0 <- Sys.time()
    while (sent < 16L && as.numeric(Sys.time() - t0, units = "secs") < 60) {
      if (grpc_send(req, payload)) {
        sent <- sent + 1L
      } else {
        grpc_poll(srv, timeout_ms = 20L)
      }
      poll_checked(cl, 20L)
    }
    expect_equal(sent, 16L)
    expect_true(grpc_finish(req))

    ## Drain to the terminal status, never treating an empty poll as the end.
    seen <- FALSE
    t0 <- Sys.time()
    while (!seen && as.numeric(Sys.time() - t0, units = "secs") < 60) {
      grpc_poll(srv, timeout_ms = 0L)
      for (ev in poll_checked(cl, 200L)) {
        if (identical(ev$kind, "stream_status")) seen <- TRUE
      }
    }
    expect_true(seen)
  }

  ## Both sides quiet: an idle poll blocks for its whole timeout.
  while (length(grpc_poll(srv, timeout_ms = 50L))) NULL
  while (length(grpc_poll(cl, timeout_ms = 50L))) NULL
  expect_equal(length(poll_checked(cl, 400L)), 0L)
  expect_equal(length(poll_checked(srv, 400L)), 0L)

  expect_equal(spurious, 0L)

  grpc_close(cl)
  grpc_close(srv)
}
