## Keepalive configuration (vientote follow-up): channel args on both
## sides. The behavioral proof runs both directions: a client pinging
## fast at a server with gRPC's default ping policing is killed with a
## too_many_pings GOAWAY, and the same cadence at a tolerant server
## survives. That asymmetry proves the arguments actually reach the
## transport on each side.

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
}
