## Streaming (increment 6): client-, server-, and bidirectional streams
## over an in-process loopback, plus flow-control and cancellation.

if (at_home()) {
  ## Event inbox: polls accumulate; take() consumes matches and KEEPS
  ## everything else, so an event arriving early is never lost.
  inbox <- function(x) {
    e <- new.env(parent = emptyenv())
    e$q <- list()
    e$x <- x
    e
  }
  take <- function(ib, want, field = "kind", n = 1L, budget_ms = 5000L) {
    got <- list()
    t0 <- Sys.time()
    repeat {
      keep <- list()
      for (ev in ib$q) {
        if (length(got) < n && ev[[field]] %in% want) {
          got[[length(got) + 1L]] <- ev
        } else {
          keep[[length(keep) + 1L]] <- ev
        }
      }
      ib$q <- keep
      if (length(got) >= n) return(got)
      if (as.numeric(Sys.time() - t0, units = "secs") * 1000 >= budget_ms) {
        return(got)
      }
      ib$q <- c(ib$q, grpc_poll(ib$x, timeout_ms = 200L))
    }
  }

  srv <- grpc_server()
  cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))
  icl <- inbox(cl)
  isrv <- inbox(srv)

  ## ---- bidirectional echo: 5 out, 5 back, clean close both ways ----
  s <- grpc_stream(cl, "/demo.Echo/Chat", deadline_ms = 10000)
  expect_inherits(s, "grpc_stream")
  for (i in 1:5) expect_true(grpc_send(s, as.raw(i)))
  expect_true(grpc_writes_done(s))
  expect_false(grpc_writes_done(s))          # second half-close refused
  expect_false(grpc_send(s, as.raw(9)))      # send after half-close refused

  req <- take(isrv, "request", "type")[[1]]  # first message
  expect_equal(req$request, as.raw(1))
  got <- as.integer(req$request)
  repeat {
    grpc_read(req)
    ev <- take(isrv, c("stream_msg", "client_done"), "type")[[1]]
    if (ev$type == "client_done") break
    got <- c(got, as.integer(ev$request))
  }
  expect_equal(got, 1:5)
  for (i in got) expect_true(grpc_send(req, as.raw(i * 2L)))
  expect_true(grpc_finish(req, metadata = c("x-count" = "5")))

  msgs <- take(icl, "stream_msg", n = 5L)
  expect_equal(length(msgs), 5L)
  expect_equal(sort(vapply(msgs, function(m) as.integer(m$response), 1L)),
               c(2L, 4L, 6L, 8L, 10L))
  st <- take(icl, "stream_status")[[1]]
  expect_equal(st$status_name, "OK")
  expect_equal(unname(st$trailing_metadata[["x-count"]]), "5")

  ## ---- server streaming: one request, many responses ----
  s2 <- grpc_stream(cl, "/demo.Echo/Watch", deadline_ms = 10000)
  expect_true(grpc_send(s2, as.raw(7)))
  grpc_writes_done(s2)
  req2 <- take(isrv, "request", "type")[[1]]
  expect_equal(req2$request, as.raw(7))
  for (i in 1:10) expect_true(grpc_send(req2, as.raw(i)))
  expect_true(grpc_finish(req2))
  msgs2 <- take(icl, "stream_msg", n = 10L)
  expect_equal(length(msgs2), 10L)
  expect_equal(take(icl, "stream_status")[[1]]$status_name, "OK")

  ## ---- client streaming: many requests, one unary-style reply ----
  s3 <- grpc_stream(cl, "/demo.Echo/Collect", deadline_ms = 10000)
  for (i in 1:8) expect_true(grpc_send(s3, as.raw(i)))
  grpc_writes_done(s3)
  req3 <- take(isrv, "request", "type")[[1]]
  seen <- 1L
  repeat {
    grpc_read(req3)
    ev <- take(isrv, c("stream_msg", "client_done"), "type")[[1]]
    if (ev$type == "client_done") break
    seen <- seen + 1L
  }
  expect_equal(seen, 8L)
  expect_true(grpc_reply(req3, as.raw(seen)))   # unary-style final reply
  msgs3 <- take(icl, "stream_msg")
  expect_equal(msgs3[[1]]$response, as.raw(8))
  expect_equal(take(icl, "stream_status")[[1]]$status_name, "OK")

  ## ---- error status ends a stream ----
  s4 <- grpc_stream(cl, "/demo.Echo/Watch", deadline_ms = 10000)
  grpc_send(s4, raw(1))
  grpc_writes_done(s4)
  req4 <- take(isrv, "request", "type")[[1]]
  expect_true(grpc_finish(req4, status = "NOT_FOUND", message = "nope"))
  st4 <- take(icl, "stream_status")[[1]]
  expect_equal(st4$status_name, "NOT_FOUND")
  expect_equal(st4$message, "nope")

  ## ---- client cancellation surfaces on both ends ----
  s5 <- grpc_stream(cl, "/demo.Echo/Chat", deadline_ms = 10000)
  grpc_send(s5, raw(1))
  req5 <- take(isrv, "request", "type")[[1]]
  grpc_cancel(s5)
  st5 <- take(icl, "stream_status")[[1]]
  expect_equal(st5$status_name, "CANCELLED")
  cancels <- take(isrv, "cancelled", "type")
  expect_equal(cancels[[1]]$id, req5$id)
  expect_false(grpc_send(req5, raw(1)))
  expect_false(grpc_finish(req5))

  ## ---- flow control: small buffers, everything still arrives ----
  s6 <- grpc_stream(cl, "/demo.Echo/Chat", deadline_ms = 20000,
                    read_buffer = 2L, write_buffer = 2L)
  target <- 40L
  sent <- 0L
  req6 <- NULL
  echoed <- 0L
  received <- 0L
  t0 <- Sys.time()
  while ((sent < target || echoed < target || received < target) &&
         as.numeric(Sys.time() - t0, units = "secs") < 20) {
    if (sent < target && isTRUE(grpc_send(s6, as.raw(sent %% 256L)))) {
      sent <- sent + 1L
    }
    for (ev in grpc_poll(srv, timeout_ms = 20L)) {
      if (ev$type %in% c("request", "stream_msg")) {
        if (ev$type == "request") req6 <- ev
        grpc_send(req6, ev$request)
        echoed <- echoed + 1L
        grpc_read(req6)
      }
    }
    for (ev in grpc_poll(cl, timeout_ms = 20L)) {
      if (ev$kind == "stream_msg") received <- received + 1L
    }
  }
  expect_equal(sent, target)
  expect_equal(echoed, target)
  expect_equal(received, target)
  grpc_writes_done(s6)
  expect_true(grpc_finish(req6))
  expect_equal(take(icl, "stream_status", budget_ms = 10000)[[1]]$status_name,
               "OK")

  ## ---- typed stream: validation and decode ----
  if (requireNamespace("RProtoBuf", quietly = TRUE)) {
    RProtoBuf::readProtoFiles2("demo.proto",
                               protoPath = normalizePath("protos"))
    svc <- grpc_service("demo.Ping")
    m <- grpc_method(svc, "Watch")
    s7 <- grpc_stream(cl, m, deadline_ms = 10000)
    ping <- RProtoBuf::P("demo.Ping")$new(msg = "sub")
    expect_true(grpc_send(s7, ping))
    pong <- RProtoBuf::P("demo.Pong")$new(msg = "x")
    expect_error(grpc_send(s7, pong), "expects 'demo.Ping'")
    grpc_writes_done(s7)
    req7 <- take(isrv, "request", "type")[[1]]
    expect_equal(grpc_decode(req7$request, "demo.Ping")$msg, "sub")
    for (i in 1:3) {
      grpc_send(req7, RProtoBuf::P("demo.Pong")$new(msg = "ev", n = i))
    }
    expect_true(grpc_finish(req7))
    msgs7 <- take(icl, "stream_msg", n = 3L)
    expect_equal(vapply(msgs7, function(x) x$response_message$n, 1),
                 c(1, 2, 3))
    expect_equal(take(icl, "stream_status")[[1]]$status_name, "OK")
  }

  grpc_close(cl)
  grpc_close(srv)

  ## ---- dirty shutdown with a live stream ----
  srv <- grpc_server()
  cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))
  icl <- inbox(cl)
  isrv <- inbox(srv)
  s8 <- grpc_stream(cl, "/demo.Echo/Chat", deadline_ms = 10000)
  grpc_send(s8, raw(1))
  invisible(take(isrv, "request", "type"))
  expect_silent(grpc_close(srv))
  st8 <- take(icl, "stream_status", budget_ms = 10000)[[1]]
  expect_true(st8$status_name %in% c("UNAVAILABLE", "CANCELLED"))
  expect_silent(grpc_close(cl))
}
