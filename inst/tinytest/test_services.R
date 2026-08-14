## Health checking and reflection (increment 7): both are ordinary proto
## services, served through the generic path with no special machinery.

if (at_home() && requireNamespace("RProtoBuf", quietly = TRUE)) {
  RProtoBuf::readProtoFiles2("health.proto",
                             protoPath = normalizePath(file.path("..", "proto",
                                                                 "health")))
  RProtoBuf::readProtoFiles2("reflection.proto",
                             protoPath = normalizePath(file.path("..", "proto",
                                                                 "reflection")))

  await_cl <- function(cl, kinds, budget_ms = 5000L) {
    t0 <- Sys.time()
    repeat {
      for (ev in grpc_poll(cl, timeout_ms = 200L)) {
        if (ev$kind %in% kinds) return(ev)
      }
      if (as.numeric(Sys.time() - t0, units = "secs") * 1000 > budget_ms) {
        return(NULL)
      }
    }
  }
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

  srv <- grpc_server()
  cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))

  ## ---- grpc.health.v1.Health/Check ----
  hsvc <- grpc_service("grpc.health.v1.HealthCheckRequest", "Health")
  expect_equal(hsvc$name, "grpc.health.v1.Health")
  check <- grpc_method(hsvc, "Check")
  call <- grpc_call(cl, check,
                    RProtoBuf::P("grpc.health.v1.HealthCheckRequest")$new(),
                    deadline_ms = 5000)
  req <- await_req(srv)
  expect_equal(req$method, "/grpc.health.v1.Health/Check")
  ## SERVING = 1
  resp <- RProtoBuf::P("grpc.health.v1.HealthCheckResponse")$new(status = 1L)
  grpc_reply(req, resp)
  ev <- await_cl(cl, "unary")
  expect_equal(ev$status_name, "OK")
  expect_equal(as.integer(ev$response_message$status), 1L)

  ## ---- grpc.reflection.v1.ServerReflection/ServerReflectionInfo ----
  rsvc <- grpc_service("grpc.reflection.v1.ServerReflectionRequest",
                       "ServerReflection")
  info <- grpc_method(rsvc, "ServerReflectionInfo")
  expect_true(info$client_streaming && info$server_streaming)
  s <- grpc_stream(cl, info, deadline_ms = 5000)
  rreq <- RProtoBuf::P("grpc.reflection.v1.ServerReflectionRequest")$new(
      list_services = "")
  expect_true(grpc_send(s, rreq))
  sreq <- await_req(srv)
  decoded <- grpc_decode(sreq$request,
                         "grpc.reflection.v1.ServerReflectionRequest")
  expect_equal(decoded$list_services, "")
  lsr <- RProtoBuf::P("grpc.reflection.v1.ListServiceResponse")$new(
      service = list(
          RProtoBuf::P("grpc.reflection.v1.ServiceResponse")$new(
              name = "demo.Echo"),
          RProtoBuf::P("grpc.reflection.v1.ServiceResponse")$new(
              name = "grpc.health.v1.Health")))
  rresp <- RProtoBuf::P("grpc.reflection.v1.ServerReflectionResponse")$new(
      list_services_response = lsr)
  grpc_send(sreq, rresp)
  msg <- await_cl(cl, "stream_msg")
  names_back <- vapply(msg$response_message$list_services_response$service,
                       function(s) s$name, "")
  expect_true("demo.Echo" %in% names_back)
  grpc_writes_done(s)
  ## client half-close ends the session; server finishes cleanly
  grpc_read(sreq)
  t0 <- Sys.time()
  done <- FALSE
  while (!done && as.numeric(Sys.time() - t0, units = "secs") < 5) {
    for (ev in grpc_poll(srv, timeout_ms = 200L)) {
      if (ev$type == "client_done") done <- TRUE
    }
  }
  expect_true(done)
  grpc_finish(sreq)
  st <- await_cl(cl, "stream_status")
  expect_equal(st$status_name, "OK")

  grpc_close(cl)
  grpc_close(srv)
}
