## RProtoBuf integration (increment 4): descriptor resolution, typed
## calls, and typed replies. RProtoBuf is a Suggests, so everything here
## is guarded.

if (requireNamespace("RProtoBuf", quietly = TRUE)) {
  RProtoBuf::readProtoFiles2("demo.proto",
                             protoPath = normalizePath("protos"))

  ## service and method resolution via the descriptor pool
  svc <- grpc_service("demo.Ping")
  expect_inherits(svc, "grpc_service")
  expect_equal(svc$name, "demo.Echo")
  expect_equal(svc$package, "demo")
  expect_equal(names(svc$methods), c("Say", "Watch", "Collect", "Chat"))
  m <- grpc_method(svc, "Say")
  expect_inherits(m, "grpc_method")
  expect_equal(m$path, "/demo.Echo/Say")
  expect_equal(m$input_type, "demo.Ping")
  expect_equal(m$output_type, "demo.Pong")
  expect_false(m$client_streaming)
  expect_false(m$server_streaming)
  expect_true(grpc_method(svc, "Watch")$server_streaming)
  expect_error(grpc_method(svc, "Nope"), "not in service")
  expect_error(grpc_service("demo.Ping", "Nada"), "not found")

  ## anchor forms: Descriptor and Message resolve like the type name
  expect_equal(grpc_service(RProtoBuf::P("demo.Ping"))$name, "demo.Echo")
  expect_equal(grpc_service(RProtoBuf::P("demo.Ping")$new())$name,
               "demo.Echo")

  ## decode helper
  ping <- RProtoBuf::P("demo.Ping")$new(msg = "hola")
  bytes <- RProtoBuf::serialize(ping, NULL)
  expect_equal(grpc_decode(bytes, "demo.Ping")$msg, "hola")

  if (at_home()) {
    await <- function(x, n = 1L, budget_ms = 5000L) {
      evs <- list()
      t0 <- Sys.time()
      while (length(evs) < n &&
             as.numeric(Sys.time() - t0, units = "secs") * 1000 < budget_ms) {
        evs <- c(evs, grpc_poll(x, timeout_ms = 200L))
      }
      evs
    }

    srv <- grpc_server()
    cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))

    ## typed round trip: Message request in, decoded Message response out
    call <- grpc_call(cl, m, ping, deadline_ms = 5000)
    sev <- await(srv)[[1]]
    expect_equal(sev$method, "/demo.Echo/Say")
    dreq <- grpc_decode(sev$request, m$input_type)
    expect_equal(dreq$msg, "hola")
    resp <- RProtoBuf::P("demo.Pong")$new(msg = dreq$msg, n = 7L)
    expect_true(grpc_reply(sev, resp))
    cev <- await(cl)[[1]]
    expect_equal(cev$status_name, "OK")
    expect_inherits(cev$response_message, "Message")
    expect_equal(cev$response_message$msg, "hola")
    expect_equal(cev$response_message$n, 7)
    expect_equal(length(ls(cl$calls)), 0L)

    ## wrong request type is refused before anything is sent
    pong <- RProtoBuf::P("demo.Pong")$new()
    expect_error(grpc_call(cl, m, pong), "expects 'demo.Ping'")

    ## streaming methods are refused for now
    expect_error(grpc_call(cl, grpc_method(svc, "Watch"), ping), "streaming")

    ## raw request with a typed method still decodes the response
    call2 <- grpc_call(cl, m, bytes, deadline_ms = 5000)
    sev2 <- await(srv)[[1]]
    expect_true(grpc_reply(sev2, RProtoBuf::P("demo.Pong")$new(msg = "raw",
                                                               n = 1L)))
    cev2 <- await(cl)[[1]]
    expect_equal(cev2$response_message$msg, "raw")

    ## a failed typed call carries no decoded message and cleans its slot
    call3 <- grpc_call(cl, m, ping, deadline_ms = 150)
    sev3 <- await(srv)[[1]]  # deliver but never reply
    cev3 <- await(cl)[[1]]
    expect_equal(cev3$status_name, "DEADLINE_EXCEEDED")
    expect_null(cev3$response_message)
    expect_equal(length(ls(cl$calls)), 0L)

    grpc_close(cl)
    grpc_close(srv)
  }
}
