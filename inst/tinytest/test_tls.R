## TLS and mTLS (increment 7). Certificates are generated on the fly
## with the openssl CLI; everything is skipped without it.

if (at_home() && nzchar(Sys.which("openssl"))) {
  certdir <- file.path(tempdir(), "grpc-tls")
  dir.create(certdir, showWarnings = FALSE)
  ossl <- function(...) {
    system2("openssl", c(...), stdout = FALSE, stderr = FALSE) == 0L
  }
  p <- function(f) file.path(certdir, f)
  ## CA, a server cert pinned to DNS:localhost only (deliberately no IP
  ## SAN, so the name-override path gets exercised), and a client cert.
  expect_true(ossl("req", "-x509", "-newkey", "rsa:2048", "-nodes",
                   "-keyout", p("ca.key"), "-out", p("ca.pem"),
                   "-days", "2", "-subj", "/CN=grpc-test-ca"))
  expect_true(ossl("req", "-newkey", "rsa:2048", "-nodes",
                   "-keyout", p("server.key"), "-out", p("server.csr"),
                   "-subj", "/CN=localhost",
                   "-addext", "subjectAltName=DNS:localhost"))
  expect_true(ossl("x509", "-req", "-in", p("server.csr"),
                   "-CA", p("ca.pem"), "-CAkey", p("ca.key"),
                   "-CAcreateserial", "-out", p("server.pem"),
                   "-days", "2", "-copy_extensions", "copy"))
  expect_true(ossl("req", "-newkey", "rsa:2048", "-nodes",
                   "-keyout", p("client.key"), "-out", p("client.csr"),
                   "-subj", "/CN=test-client"))
  expect_true(ossl("x509", "-req", "-in", p("client.csr"),
                   "-CA", p("ca.pem"), "-CAkey", p("ca.key"),
                   "-out", p("client.pem"), "-days", "2"))

  await_cl <- function(cl, budget_ms = 5000L) {
    t0 <- Sys.time()
    repeat {
      evs <- grpc_poll(cl, timeout_ms = 200L)
      if (length(evs)) return(evs[[1]])
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

  ## ---- TLS echo with a pinned CA and name override ----
  srv <- grpc_server("127.0.0.1:0",
                     credentials = grpc_tls(cert_file = p("server.pem"),
                                            key_file = p("server.key")))
  target <- sprintf("127.0.0.1:%d", grpc_server_port(srv))

  ## hostname mismatch: dialing the IP against a DNS-only cert refuses
  bad <- grpc_client(target, grpc_tls(ca_file = p("ca.pem")))
  call <- grpc_call(bad, "/demo.Echo/Say", raw(0), deadline_ms = 3000)
  ev <- await_cl(bad)
  expect_true(ev$status_name %in% c("UNAVAILABLE", "DEADLINE_EXCEEDED"))
  grpc_close(bad)

  ## with the override, the round trip works over TLS
  cl <- grpc_client(target,
                    grpc_tls(ca_file = p("ca.pem"),
                             target_name_override = "localhost"))
  expect_equal(grpc_state(cl), "IDLE")
  call <- grpc_call(cl, "/demo.Echo/Say", as.raw(1:4), deadline_ms = 5000)
  req <- await_req(srv)
  expect_identical(req$request, as.raw(1:4))
  expect_true(nzchar(req$peer))
  grpc_reply(req, req$request)
  ev <- await_cl(cl)
  expect_equal(ev$status_name, "OK")
  expect_identical(ev$response, as.raw(1:4))
  expect_equal(grpc_state(cl), "READY")
  grpc_close(cl)
  grpc_close(srv)

  ## ---- mTLS: server requires a verified client certificate ----
  srv <- grpc_server("127.0.0.1:0",
                     credentials = grpc_tls(ca_file = p("ca.pem"),
                                            cert_file = p("server.pem"),
                                            key_file = p("server.key"),
                                            require_client_cert = TRUE))
  target <- sprintf("127.0.0.1:%d", grpc_server_port(srv))

  ## no client certificate: refused at the handshake
  bad <- grpc_client(target,
                     grpc_tls(ca_file = p("ca.pem"),
                              target_name_override = "localhost"))
  call <- grpc_call(bad, "/demo.Echo/Say", raw(0), deadline_ms = 3000)
  ev <- await_cl(bad)
  expect_true(ev$status_name %in% c("UNAVAILABLE", "DEADLINE_EXCEEDED"))
  grpc_close(bad)

  ## with one: served, and the verified identity reaches the app
  cl <- grpc_client(target,
                    grpc_tls(ca_file = p("ca.pem"),
                             cert_file = p("client.pem"),
                             key_file = p("client.key"),
                             target_name_override = "localhost"))
  call <- grpc_call(cl, "/demo.Echo/Say", as.raw(9), deadline_ms = 5000)
  req <- await_req(srv)
  expect_true("test-client" %in% req$peer_identity)
  grpc_reply(req, req$request)
  ev <- await_cl(cl)
  expect_equal(ev$status_name, "OK")
  grpc_close(cl)
  grpc_close(srv)

  unlink(certdir, recursive = TRUE)
}
