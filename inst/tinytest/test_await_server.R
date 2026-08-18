## grpc_await() on a server request: the same per-call scoping the
## client got. A handler draining one client-streaming call must not see
## another call's messages, must not lose them, and must not treat
## another call's queued event as a reason to stop waiting.

if (at_home()) {
  srv <- grpc_server()
  cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))

  nmsg <- 8L

  ## Two concurrent client-streaming calls, each message tagged with its
  ## call's marker byte and a sequence number.
  open_stream <- function(marker) {
    s <- grpc_stream(cl, "/demo.Mark/Chat", deadline_ms = 30000)
    for (i in seq_len(nmsg)) {
      expect_true(grpc_send(s, as.raw(c(as.integer(marker), i))))
    }
    expect_true(grpc_writes_done(s))
    s
  }

  ## Collect both requests before draining either, so the server's queue
  ## genuinely holds two calls at once.
  a <- open_stream(1L)
  b <- open_stream(2L)

  reqs <- list()
  t0 <- Sys.time()
  while (length(reqs) < 2L &&
         as.numeric(Sys.time() - t0, units = "secs") < 60) {
    for (ev in grpc_poll(srv, timeout_ms = 100L)) {
      if (identical(ev$type, "request")) reqs[[length(reqs) + 1L]] <- ev
    }
  }
  expect_equal(length(reqs), 2L)

  markers <- vapply(reqs, function(r) as.integer(r$request[[1L]]), 1L)
  expect_equal(sort(markers), c(1L, 2L))
  req_a <- reqs[[which(markers == 1L)]]
  req_b <- reqs[[which(markers == 2L)]]

  ## Sequence numbers seen for one call, starting with the message that
  ## arrived on the request event itself.
  seq_of <- function(req) as.integer(req$request[[2L]])
  msg_seq <- function(evs, marker) {
    out <- integer(0)
    for (ev in evs) {
      if (identical(ev$type, "stream_msg")) {
        expect_equal(as.integer(ev$request[[1L]]), marker)
        out <- c(out, as.integer(ev$request[[2L]]))
      }
    }
    out
  }
  saw_done <- function(evs) {
    any(vapply(evs, function(e) identical(e$type, "client_done"), TRUE))
  }

  ## Queue a message on B before A is drained, so stepping over another
  ## call's events is exercised rather than assumed. One read at a time,
  ## so this queues exactly B's second message.
  expect_true(grpc_read(req_b))

  ## ---- drain A only ----
  seq_a <- seq_of(req_a)
  foreign <- 0L
  done_a <- FALSE
  t0 <- Sys.time()
  while (!done_a && as.numeric(Sys.time() - t0, units = "secs") < 60) {
    grpc_read(req_a)
    evs <- grpc_await(req_a, timeout_ms = 1000L)
    for (ev in evs) if (!identical(ev$id, req_a$id)) foreign <- foreign + 1L
    seq_a <- c(seq_a, msg_seq(evs, 1L))
    if (saw_done(evs)) done_a <- TRUE
  }
  expect_true(done_a)
  expect_equal(foreign, 0L)
  expect_equal(seq_a, seq_len(nmsg))     # all of A's, in order, only A's
  expect_true(grpc_finish(req_a))

  ## ---- a foreign call's queued event neither satisfies nor spins ----
  ## A is finished and has nothing outstanding; B has a message sitting
  ## in the queue. The await must block for its whole timeout, and B's
  ## message must still be there afterwards -- a zero-timeout await
  ## returning it proves it was queued rather than arriving late.
  verified <- FALSE
  stash <- list()
  for (attempt in 1:10) {
    t0 <- proc.time()[["elapsed"]]
    idle <- grpc_await(req_a, timeout_ms = 400L)
    waited <- (proc.time()[["elapsed"]] - t0) * 1000
    queued <- grpc_await(req_b, timeout_ms = 0L)
    stash <- c(stash, queued)
    if (!length(idle) && length(queued)) {
      expect_true(waited > 300)
      verified <- TRUE
      break
    }
    grpc_read(req_b)
  }
  expect_true(verified)

  ## ---- B's events survived the filtered reads, in order ----
  seq_b <- c(seq_of(req_b), msg_seq(stash, 2L))
  done_b <- saw_done(stash)
  t0 <- Sys.time()
  while (!done_b && as.numeric(Sys.time() - t0, units = "secs") < 60) {
    grpc_read(req_b)
    evs <- grpc_await(req_b, timeout_ms = 1000L)
    seq_b <- c(seq_b, msg_seq(evs, 2L))
    if (saw_done(evs)) done_b <- TRUE
  }
  expect_true(done_b)
  expect_equal(seq_b, seq_len(nmsg))
  expect_true(grpc_finish(req_b))

  ## Both streams close out cleanly on the client side.
  for (s in list(a, b)) {
    closed <- FALSE
    t0 <- Sys.time()
    while (!closed && as.numeric(Sys.time() - t0, units = "secs") < 60) {
      for (ev in grpc_await(s, timeout_ms = 1000L)) {
        if (identical(ev$kind, "stream_status")) closed <- TRUE
      }
    }
    expect_true(closed)
  }

  ## ---- argument checking ----
  expect_error(grpc_await(srv, timeout_ms = 0L))   # no method for a server
  expect_error(grpc_await(req_a))                  # timeout required

  grpc_close(cl)
  grpc_close(srv)
}
