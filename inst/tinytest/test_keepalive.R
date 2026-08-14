## Keepalive configuration (vientote follow-up): channel args on both
## sides. What the behavioral tests prove: client-originated pings
## reach the wire (a fast-pinging client is killed by a default
## server's too_many_pings policing and survives at a tolerant one —
## which also proves the server-side min_ping_interval_ms argument),
## both with an active stream and on a call-less connection
## (PERMIT_WITHOUT_CALLS). What they cannot prove: server-originated
## dead-peer *detection*, which needs a network partition — TCP on
## loopback answers pings at the kernel level regardless of the app.
## The server keepalive_ms/keepalive_timeout_ms arguments are plumbed
## identically to the client's.

## ---- validation: whole, finite, positive milliseconds only ----
expect_error(grpc_client("127.0.0.1:1", keepalive_ms = 0.5))
expect_error(grpc_client("127.0.0.1:1", keepalive_ms = 1.9))
expect_error(grpc_client("127.0.0.1:1", keepalive_ms = Inf))
expect_error(grpc_client("127.0.0.1:1", keepalive_ms = 0))
expect_error(grpc_client("127.0.0.1:1", keepalive_timeout_ms = -1))
expect_error(grpc_client("127.0.0.1:1", keepalive_ms = NA_real_))
expect_error(grpc_server(keepalive_ms = 0.5))
expect_error(grpc_server(min_ping_interval_ms = Inf))
## a whole-valued double is fine
cl <- grpc_client("127.0.0.1:1", keepalive_ms = 10000,
                  keepalive_timeout_ms = 5000)
expect_equal(grpc_state(cl), "IDLE")
grpc_close(cl)

if (at_home()) {
  take_status <- function(cl, budget_ms) {
    t0 <- Sys.time()
    repeat {
      for (ev in grpc_poll(cl, timeout_ms = 200L)) {
        if (ev$kind == "stream_status") return(ev)
      }
      if (as.numeric(Sys.time() - t0, units = "secs") * 1000 > budget_ms) {
        return(NULL)
      }
    }
  }

  ## ---- fast client pings vs default server policing: GOAWAY ----
  srv <- grpc_server()
  cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)),
                    keepalive_ms = 200, keepalive_timeout_ms = 1000)
  s <- grpc_stream(cl, "/demo.Echo/Chat", deadline_ms = 30000)
  grpc_send(s, raw(1))
  st <- take_status(cl, budget_ms = 8000L)
  expect_false(is.null(st))
  expect_true(st$status_name != "OK")
  grpc_close(cl)
  grpc_close(srv)

  ## ---- same cadence at a tolerant server: the stream survives ----
  srv <- grpc_server(keepalive_ms = 200, keepalive_timeout_ms = 1000,
                     min_ping_interval_ms = 100)
  cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)),
                    keepalive_ms = 200, keepalive_timeout_ms = 1000)
  s <- grpc_stream(cl, "/demo.Echo/Chat", deadline_ms = 30000)
  grpc_send(s, raw(1))
  st <- take_status(cl, budget_ms = 1500L)
  expect_null(st)
  expect_equal(grpc_state(cl), "READY")
  grpc_close(cl)
  grpc_close(srv)

  ## ---- pings continue with no active call (PERMIT_WITHOUT_CALLS) ----
  ## One unary round trip establishes the connection, then it idles.
  ## The client's pings must keep flowing on the call-less connection,
  ## which the default-policed server answers with a GOAWAY: the
  ## channel leaves READY. Without PERMIT_WITHOUT_CALLS there are no
  ## pings, no GOAWAY, and the channel stays READY (client idle
  ## timeout is 30 minutes).
  srv <- grpc_server()
  cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)),
                    keepalive_ms = 200, keepalive_timeout_ms = 1000)
  call <- grpc_call(cl, "/demo.Echo/Say", raw(0), deadline_ms = 5000)
  t0 <- Sys.time()
  req <- NULL
  while (is.null(req) &&
         as.numeric(Sys.time() - t0, units = "secs") < 5) {
    for (ev in grpc_poll(srv, timeout_ms = 200L)) {
      if (ev$type == "request") req <- ev
    }
  }
  grpc_reply(req, raw(0))
  done <- FALSE
  t0 <- Sys.time()
  while (!done && as.numeric(Sys.time() - t0, units = "secs") < 5) {
    for (ev in grpc_poll(cl, timeout_ms = 200L)) {
      if (ev$kind == "unary") done <- TRUE
    }
  }
  expect_true(done)
  expect_equal(grpc_state(cl), "READY")
  t0 <- Sys.time()
  while (grpc_state(cl) == "READY" &&
         as.numeric(Sys.time() - t0, units = "secs") < 8) {
    Sys.sleep(0.1)
  }
  expect_true(grpc_state(cl) != "READY")
  grpc_close(cl)
  grpc_close(srv)
}
