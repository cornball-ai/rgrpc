## Real-world proof (increment 5): CRI client against a live containerd.
##
## Needs RProtoBuf, plus a reachable containerd socket with the CRI
## plugin enabled. Default socket: /run/containerd/containerd.sock;
## override with the GRPC_R_CRI_SOCKET environment variable. Everything
## is skipped silently when the environment doesn't provide that.

cri_socket <- Sys.getenv("GRPC_R_CRI_SOCKET", "/run/containerd/containerd.sock")
cri_ready <- at_home() &&
  requireNamespace("RProtoBuf", quietly = TRUE) &&
  file.exists(cri_socket) &&
  file.access(cri_socket, mode = 6L) == 0L

## The vendored proto root works from both layouts: source checkout
## (inst/tinytest -> inst/proto) and installed package (tinytest -> proto).
proto_root <- normalizePath(file.path("..", "proto", "cri"), mustWork = FALSE)

if (requireNamespace("RProtoBuf", quietly = TRUE) && dir.exists(proto_root)) {
  ## The CRI schema itself loads and resolves regardless of any socket.
  RProtoBuf::readProtoFiles2("api.proto", protoPath = proto_root)
  rt <- grpc_service("runtime.v1.VersionRequest", "RuntimeService")
  expect_equal(rt$name, "runtime.v1.RuntimeService")
  expect_true(length(rt$methods) >= 30L)
  expect_equal(grpc_method(rt, "Version")$output_type,
               "runtime.v1.VersionResponse")
  expect_true(grpc_method(rt, "GetContainerEvents")$server_streaming)
  img <- grpc_service("runtime.v1.VersionRequest", "ImageService")
  expect_equal(img$name, "runtime.v1.ImageService")
}

if (cri_ready && exists("rt")) {
  await <- function(x, n = 1L, budget_ms = 5000L) {
    evs <- list()
    t0 <- Sys.time()
    while (length(evs) < n &&
           as.numeric(Sys.time() - t0, units = "secs") * 1000 < budget_ms) {
      evs <- c(evs, grpc_poll(x, timeout_ms = 200L))
    }
    evs
  }

  cl <- grpc_client(sprintf("unix://%s", cri_socket))

  ## Version: the canonical CRI hello
  vreq <- RProtoBuf::P("runtime.v1.VersionRequest")$new(version = "v1")
  call <- grpc_call(cl, grpc_method(rt, "Version"), vreq, deadline_ms = 5000)
  ev <- await(cl)[[1]]
  expect_equal(ev$status_name, "OK")
  expect_inherits(ev$response_message, "Message")
  expect_equal(ev$response_message$runtime_name, "containerd")
  expect_true(nzchar(ev$response_message$runtime_version))

  ## ListPodSandbox: the increment-5 milestone
  lreq <- RProtoBuf::P("runtime.v1.ListPodSandboxRequest")$new()
  call2 <- grpc_call(cl, grpc_method(rt, "ListPodSandbox"), lreq,
                     deadline_ms = 5000)
  ev2 <- await(cl)[[1]]
  expect_equal(ev2$status_name, "OK")
  expect_inherits(ev2$response_message, "Message")
  ## a machine that isn't running Kubernetes has zero sandboxes; the RPC
  ## succeeding and decoding is the proof
  expect_true(length(ev2$response_message$items) >= 0L)

  ## ListImages via the second service on the same channel
  ireq <- RProtoBuf::P("runtime.v1.ListImagesRequest")$new()
  call3 <- grpc_call(cl, grpc_method(img, "ListImages"), ireq,
                     deadline_ms = 5000)
  ev3 <- await(cl)[[1]]
  expect_equal(ev3$status_name, "OK")

  ## GetContainerEvents: subscribe to the live event stream, hold it
  ## open, cancel it. No container churn happens on this host during the
  ## test, so the stream staying open (no terminal status within 1s) is
  ## the assertion; message flow is covered by the loopback stream tests.
  s <- grpc_stream(cl, grpc_method(rt, "GetContainerEvents"))
  expect_true(grpc_send(s, RProtoBuf::P("runtime.v1.GetEventsRequest")$new()))
  grpc_writes_done(s)
  quiet <- grpc_poll(cl, timeout_ms = 1000L)
  expect_false(any(vapply(quiet, function(e) e$kind == "stream_status",
                          TRUE)))
  grpc_cancel(s)
  st <- NULL
  t0 <- Sys.time()
  while (is.null(st) && as.numeric(Sys.time() - t0, units = "secs") < 5) {
    for (e in grpc_poll(cl, timeout_ms = 200L)) {
      if (e$kind == "stream_status") st <- e
    }
  }
  expect_equal(st$status_name, "CANCELLED")

  grpc_close(cl)
}
