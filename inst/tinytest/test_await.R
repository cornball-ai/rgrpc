## grpc_await(): per-call scoping over the shared queue. The events of
## other calls must be stepped over rather than consumed, dropped, or
## reordered, and a call with nothing pending must genuinely wait even
## while another call's events sit queued -- that is where a filtered
## wait can degenerate into a spin on a permanently readable descriptor.

if (at_home()) {
  nmsg <- 12L
  size <- 256L

  srv <- grpc_server()
  cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))

  ## Answer the call that opened with `marker` using nmsg messages
  ## carrying that marker and a sequence number, so both the origin and
  ## the order of any message are readable from its content. Requests
  ## for other calls are passed over: the quiet call below must stay
  ## unanswered, and picking the first request that happens to be queued
  ## would answer it instead once the machine is slow enough (valgrind
  ## caught exactly that).
  answer <- function(marker) {
    req <- NULL
    t0 <- Sys.time()
    while (is.null(req) && as.numeric(Sys.time() - t0, units = "secs") < 60) {
      for (ev in grpc_poll(srv, timeout_ms = 100L)) {
        if (identical(ev$type, "request") &&
            identical(ev$request[[1L]], marker)) {
          req <- ev
        }
      }
    }
    expect_false(is.null(req))
    for (i in seq_len(nmsg)) {
      msg <- as.raw(c(as.integer(marker), i, rep(0L, size - 2L)))
      expect_true(grpc_send(req, msg))
    }
    expect_true(grpc_finish(req))
  }

  mark_a <- as.raw(11L)
  mark_b <- as.raw(22L)

  ## ---- stream A, left with a backlog of unread messages ----
  a <- grpc_stream(cl, "/demo.Mark/List", deadline_ms = 20000)
  expect_true(grpc_send(a, mark_a))
  expect_true(grpc_writes_done(a))
  answer(mark_a)

  seen <- 0L
  t0 <- Sys.time()
  while (seen < 1L && as.numeric(Sys.time() - t0, units = "secs") < 30) {
    for (ev in grpc_poll(cl, max_events = 1L, timeout_ms = 100L)) {
      if (identical(ev$kind, "stream_msg")) seen <- seen + 1L
    }
  }
  expect_equal(seen, 1L)

  ## ---- a call with nothing pending still waits ----
  ## The descriptor is readable because A has events queued. A filtered
  ## wait that trusted it would return instantly, over and over.
  quiet <- grpc_call(cl, "/demo.Quiet/Never", as.raw(1L), deadline_ms = 60000)
  t0 <- proc.time()[["elapsed"]]
  none <- grpc_await(quiet, timeout_ms = 500L)
  waited <- (proc.time()[["elapsed"]] - t0) * 1000
  expect_equal(length(none), 0L)
  expect_true(waited > 400)

  ## ---- awaiting B returns B and only B ----
  b <- grpc_stream(cl, "/demo.Mark/List", deadline_ms = 20000)
  expect_true(grpc_send(b, mark_b))
  expect_true(grpc_writes_done(b))
  answer(mark_b)

  b_msgs <- 0L
  b_seq <- integer(0)
  foreign <- 0L
  b_done <- FALSE
  t0 <- Sys.time()
  while (!b_done && as.numeric(Sys.time() - t0, units = "secs") < 30) {
    for (ev in grpc_await(b, timeout_ms = 200L)) {
      if (!identical(ev$id, b$id)) foreign <- foreign + 1L
      if (identical(ev$kind, "stream_msg")) {
        b_msgs <- b_msgs + 1L
        expect_equal(ev$response[[1L]], mark_b)
        b_seq <- c(b_seq, as.integer(ev$response[[2L]]))
      }
      if (identical(ev$kind, "stream_status")) b_done <- TRUE
    }
  }
  expect_true(b_done)
  expect_equal(foreign, 0L)
  expect_equal(b_msgs, nmsg)
  expect_equal(b_seq, seq_len(nmsg))          # order preserved

  ## ---- A's events survived the filtered reads, in order ----
  ## They must also still be signalled: this poll returns at once rather
  ## than waiting out its timeout.
  a_msgs <- 0L
  a_seq <- integer(0)
  a_done <- FALSE
  t0 <- proc.time()[["elapsed"]]
  first_wait <- NA_real_
  t1 <- Sys.time()
  while (!a_done && as.numeric(Sys.time() - t1, units = "secs") < 30) {
    evs <- grpc_poll(cl, timeout_ms = 5000L)
    if (is.na(first_wait)) first_wait <- (proc.time()[["elapsed"]] - t0) * 1000
    for (ev in evs) {
      if (identical(ev$id, a$id)) {
        if (identical(ev$kind, "stream_msg")) {
          a_msgs <- a_msgs + 1L
          expect_equal(ev$response[[1L]], mark_a)
          a_seq <- c(a_seq, as.integer(ev$response[[2L]]))
        }
        if (identical(ev$kind, "stream_status")) a_done <- TRUE
      }
    }
  }
  expect_true(a_done)
  expect_true(first_wait < 1000)              # still signalled, not stalled
  expect_equal(a_msgs, nmsg - 1L)             # one was consumed before B
  expect_equal(a_seq, seq_len(nmsg)[-1L])     # and order survived

  ## ---- an expired await leaves the call untouched ----
  ## This is what keeps a required timeout from being the empty-batch
  ## trap in new clothing: expiry costs a loop, not the call. Match the
  ## reply on method, since the quiet call above is still unanswered.
  again <- grpc_call(cl, "/demo.Echo/Say", as.raw(1:4), deadline_ms = 30000)
  expect_equal(length(grpc_await(again, timeout_ms = 1L)), 0L)

  replied <- FALSE
  t0 <- Sys.time()
  while (!replied && as.numeric(Sys.time() - t0, units = "secs") < 60) {
    for (ev in grpc_poll(srv, timeout_ms = 100L)) {
      if (identical(ev$type, "request") &&
          identical(ev$method, "/demo.Echo/Say")) {
        expect_true(grpc_reply(ev, ev$request))
        replied <- TRUE
      }
    }
  }
  expect_true(replied)

  resumed <- grpc_await(again, timeout_ms = 30000L)
  expect_equal(length(resumed), 1L)
  expect_equal(resumed[[1]]$status_name, "OK")
  expect_equal(resumed[[1]]$response, as.raw(1:4))

  ## ---- argument checking ----
  expect_error(grpc_await(cl, timeout_ms = 0L))        # not a call
  expect_error(grpc_await(b))                          # timeout required
  expect_error(grpc_await(b, timeout_ms = -2L))

  grpc_close(cl)
  grpc_close(srv)
}
