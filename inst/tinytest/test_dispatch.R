## Event attribution on a shared client. grpc_poll() drains one queue for
## the whole client, so events from a stream the caller has stopped reading
## keep arriving alongside a later stream's. Each event carries the id of
## the stream it belongs to, and this checks that the id and the payload
## never disagree -- a caller that dispatches on `id` is safe, one that
## accumulates every "stream_msg" it sees is not (issue #12 follow-up).

if (at_home()) {
  size <- 4096L
  nmsg <- 12L

  srv <- grpc_server()
  cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))

  ## Answer the next accepted call with nmsg copies of its own marker byte,
  ## so any message's origin is readable from its content.
  answer <- function() {
    req <- NULL
    t0 <- Sys.time()
    while (is.null(req) && as.numeric(Sys.time() - t0, units = "secs") < 30) {
      for (ev in grpc_poll(srv, timeout_ms = 100L)) {
        if (identical(ev$type, "request")) req <- ev
      }
    }
    expect_false(is.null(req))
    payload <- rep(req$request[[1L]], size)
    for (i in seq_len(nmsg)) expect_true(grpc_send(req, payload))
    expect_true(grpc_finish(req))
  }

  mark_a <- as.raw(11L)
  mark_b <- as.raw(22L)

  ## ---- stream A: read two messages, then stop reading it ----
  a <- grpc_stream(cl, "/demo.Mark/List", deadline_ms = 15000)
  expect_true(grpc_send(a, mark_a))
  expect_true(grpc_writes_done(a))
  answer()

  ## One event per poll, so A keeps a backlog of unread messages rather
  ## than being drained whole by a single batch.
  seen_a <- 0L
  t0 <- Sys.time()
  while (seen_a < 2L && as.numeric(Sys.time() - t0, units = "secs") < 30) {
    for (ev in grpc_poll(cl, max_events = 1L, timeout_ms = 100L)) {
      if (identical(ev$kind, "stream_msg")) seen_a <- seen_a + 1L
    }
  }
  expect_equal(seen_a, 2L)

  ## ---- stream B on the same client, while A still has messages queued ----
  b <- grpc_stream(cl, "/demo.Mark/List", deadline_ms = 15000)
  expect_true(grpc_send(b, mark_b))
  expect_true(grpc_writes_done(b))
  answer()

  mismatched <- 0L   # id and payload disagree
  leaked <- 0L       # A's messages arriving while B is being read
  wrong_size <- 0L
  b_done <- FALSE
  t0 <- Sys.time()
  while (!b_done && as.numeric(Sys.time() - t0, units = "secs") < 60) {
    for (ev in grpc_poll(cl, timeout_ms = 100L)) {
      if (identical(ev$kind, "stream_msg")) {
        if (length(ev$response) != size) wrong_size <- wrong_size + 1L
        want <- if (identical(ev$id, b$id)) mark_b else mark_a
        if (!identical(unique(ev$response), want)) {
          mismatched <- mismatched + 1L
        }
        if (identical(ev$id, a$id)) leaked <- leaked + 1L
      }
      if (identical(ev$kind, "stream_status") && identical(ev$id, b$id)) {
        b_done <- TRUE
      }
    }
  }

  expect_true(b_done)
  ## The scenario has to actually happen, or the check above is vacuous.
  expect_true(leaked > 0L)
  expect_equal(mismatched, 0L)
  expect_equal(wrong_size, 0L)

  ## ---- unary: a batch holds several completions, in completion order ----
  ## Taking events[[1]] as "my" answer is the same bad assumption as
  ## accumulating every stream_msg into one stream.
  ncall <- 8L
  want <- list()
  for (i in seq_len(ncall)) {
    payload <- as.raw(rep(100L + i, 64L))
    call <- grpc_call(cl, "/demo.Mark/Say", payload, deadline_ms = 15000)
    want[[as.character(call$id)]] <- payload
  }

  answered <- 0L
  t0 <- Sys.time()
  while (answered < ncall &&
         as.numeric(Sys.time() - t0, units = "secs") < 30) {
    for (ev in grpc_poll(srv, timeout_ms = 100L)) {
      if (identical(ev$type, "request")) {
        expect_true(grpc_reply(ev, ev$request))   # echo
        answered <- answered + 1L
      }
    }
  }
  expect_equal(answered, ncall)

  got <- 0L
  wrong_answer <- 0L
  batched <- FALSE
  t0 <- Sys.time()
  while (got < ncall && as.numeric(Sys.time() - t0, units = "secs") < 30) {
    evs <- grpc_poll(cl, timeout_ms = 100L)
    if (length(evs) > 1L) batched <- TRUE
    for (ev in evs) {
      if (identical(ev$kind, "unary")) {
        got <- got + 1L
        if (!identical(ev$response, want[[as.character(ev$id)]])) {
          wrong_answer <- wrong_answer + 1L
        }
      }
    }
  }
  expect_equal(got, ncall)
  ## Non-vacuous: at least one batch really did carry several completions,
  ## so events[[1]] was not the only thing in it.
  expect_true(batched)
  expect_equal(wrong_answer, 0L)

  grpc_close(cl)
  grpc_close(srv)
}
