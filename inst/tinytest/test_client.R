## Unary client lifecycle (increment 2). No responding peer exists until
## the server increment, so these tests exercise the error paths that
## complete without one: UNAVAILABLE, DEADLINE_EXCEEDED, CANCELLED.

exit_if_not(grpc_available())

cl <- grpc_client("127.0.0.1:1")
expect_inherits(cl, "grpc_client")
expect_true(grpc_fd(cl) > 0L)
expect_equal(grpc_pending(cl), 0L)
grpc_close(cl)
expect_error(grpc_fd(cl), "closed")
expect_silent(grpc_close(cl))

if (at_home()) {
  drain <- function(client, n = 1L, budget_ms = 5000L) {
    evs <- list()
    t0 <- Sys.time()
    while (length(evs) < n &&
           as.numeric(Sys.time() - t0, units = "secs") * 1000 < budget_ms) {
      evs <- c(evs, grpc_poll(client, timeout_ms = 200L))
    }
    evs
  }

  ## fast failure against a closed port
  cl <- grpc_client("127.0.0.1:1")
  call <- grpc_call(cl, "/demo.Echo/Say", as.raw(c(0x0a, 0x02, 0x68, 0x69)))
  expect_inherits(call, "grpc_call")
  evs <- drain(cl)
  expect_equal(length(evs), 1L)
  ev <- evs[[1]]
  expect_equal(ev$id, call$id)
  expect_equal(ev$status_name, "UNAVAILABLE")
  expect_null(ev$response)
  expect_equal(grpc_pending(cl), 0L)
  grpc_close(cl)

  ## deadline against a live generic server that never responds
  srv <- rgrpc:::.server_create("127.0.0.1:0")
  cl <- grpc_client(sprintf("127.0.0.1:%d", rgrpc:::.server_port(srv)))
  call <- grpc_call(cl, "/demo.Echo/Say", raw(0), deadline_ms = 200)
  evs <- drain(cl)
  expect_equal(evs[[1]]$status_name, "DEADLINE_EXCEEDED")

  ## explicit cancellation
  call2 <- grpc_call(cl, "/demo.Echo/Say", raw(0))
  expect_equal(grpc_pending(cl), 1L)
  grpc_cancel(call2)
  evs <- drain(cl)
  expect_equal(evs[[1]]$status_name, "CANCELLED")
  grpc_close(cl)
  rgrpc:::.server_destroy(srv)

  ## metadata on the request does not disturb completion
  cl <- grpc_client("127.0.0.1:1")
  call <- grpc_call(cl, "/demo.Echo/Say", raw(0),
                    metadata = c("x-trace-id" = "abc123"))
  evs <- drain(cl)
  expect_equal(evs[[1]]$status_name, "UNAVAILABLE")
  grpc_close(cl)

  ## stress: 100 concurrent calls, batched polls, GC interleaved
  cl <- grpc_client("127.0.0.1:1")
  ids <- vapply(1:100, function(i) grpc_call(cl, "/x/Y", raw(0))$id,
                numeric(1))
  expect_equal(length(unique(ids)), 100L)
  got <- 0L
  t0 <- Sys.time()
  while (got < 100L && as.numeric(Sys.time() - t0, units = "secs") < 10) {
    got <- got + length(grpc_poll(cl, max_events = 7L, timeout_ms = 100L))
    invisible(gc())
  }
  expect_equal(got, 100L)
  expect_equal(grpc_pending(cl), 0L)

  ## Calls must not grow the process. The call table used to be a binding
  ## per id (assign/get0/rm on as.character(id)), and each of those
  ## installs an R symbol, which R never frees; the event lookup did it
  ## for every call, typed or not. A leaked id is three nodes (the
  ## symbol, its name, a symbol-table cell) that gc() cannot take back,
  ## so 2000 calls leaked about 6000 nodes; the table now holds one named
  ## list and the count stays within noise.
  ##
  ## MEASURED IN A FRESH PROCESS. Call ids start at 1 for every client,
  ## so a second measurement in the same session finds its symbols
  ## already interned by the first and passes on a build that leaks. The
  ## child inherits this session's library paths. The method is typed by
  ## hand in the shape grpc_service() builds; the untyped leg uses a bare
  ## path.
  measure <- c(
    'suppressMessages(library(rgrpc))',
    'cl <- grpc_client("127.0.0.1:1")',
    'typed <- structure(list(name = "Y", path = "/x/Y", input_type = "x.Req",',
    '                        output_type = "x.Reply", client_streaming = FALSE,',
    '                        server_streaming = FALSE), class = "grpc_method")',
    'settle <- function(method, k) {',
    '  for (i in seq_len(k)) {',
    '    call <- grpc_call(cl, method, raw(0))',
    '    got <- 0L',
    '    t0 <- Sys.time()',
    '    while (got < 1L && as.numeric(Sys.time() - t0, units = "secs") < 10) {',
    '      got <- got + length(grpc_poll(cl, timeout_ms = 200L))',
    '    }',
    '  }',
    '}',
    'growth <- function(method) {',
    '  settle(method, 200L)',
    '  invisible(gc()); invisible(gc())',
    '  base <- gc()[1L, 1L]',
    '  settle(method, 2000L)',
    '  invisible(gc()); invisible(gc())',
    '  gc()[1L, 1L] - base',
    '}',
    'cat(growth(typed), growth("/x/Y"), length(cl$calls$types), "\n")')
  script <- tempfile("rgrpc-symbols-", fileext = ".R")
  writeLines(measure, script)
  out <- system2(file.path(R.home("bin"), "Rscript"),
                 c("--vanilla", shQuote(script)), stdout = TRUE, stderr = FALSE,
                 env = paste0("R_LIBS=", paste(.libPaths(), collapse = ":")))
  unlink(script)
  got <- suppressWarnings(as.integer(strsplit(trimws(out[length(out)]),
                                              "\\s+")[[1L]]))
  expect_equal(length(got), 3L, info = paste(out, collapse = "\n"))
  expect_true(isTRUE(got[1L] < 1000L),
              info = sprintf("Ncells grew by %d over 2000 typed calls", got[1L]))
  expect_true(isTRUE(got[2L] < 1000L),
              info = sprintf("Ncells grew by %d over 2000 untyped calls", got[2L]))
  expect_equal(got[3L], 0L)
  expect_equal(length(cl$calls$types), 0L)
  expect_equal(grpc_pending(cl), 0L)

  ## close with a queued call still in flight
  call <- grpc_call(cl, "/x/Y", raw(0), wait_for_ready = TRUE)
  expect_silent(grpc_close(cl))

  ## finalizer path: client garbage collected while a call is in flight
  cl <- grpc_client("127.0.0.1:1")
  call <- grpc_call(cl, "/x/Y", raw(0), wait_for_ready = TRUE)
  rm(cl, call)
  invisible(gc())
}
