# TLS/mTLS exercise for the TSan pass (tools/sanitize.sh). The regular
# TLS tests shell out to openssl, and subprocesses segfault under a
# preloaded libtsan, so this script takes pre-generated certificates
# via the CERTDIR environment variable instead.
lib <- Sys.getenv("GRPC_SANITIZE_LIB")
stopifnot(nzchar(lib), identical(find.package("grpc"), file.path(lib, "grpc")))
library(grpc)
d <- Sys.getenv("CERTDIR")
p <- function(f) file.path(d, f)
sc <- grpc_tls(ca_file = p("ca.pem"), cert_file = p("server.pem"),
               key_file = p("server.key"), require_client_cert = TRUE)
cc <- grpc_tls(ca_file = p("ca.pem"), cert_file = p("client.pem"),
               key_file = p("client.key"), target_name_override = "localhost")
srv <- grpc_server(credentials = sc)
cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)),
                  credentials = cc)
call <- grpc_call(cl, "/demo.Echo/Say", as.raw(1:4), deadline_ms = 10000,
                  wait_for_ready = TRUE)
req <- NULL
t0 <- Sys.time()
while (is.null(req) && as.numeric(Sys.time() - t0, units = "secs") < 15) {
  for (ev in grpc_poll(srv, timeout_ms = 200L)) {
    if (ev$type == "request") req <- ev
  }
}
stopifnot(!is.null(req), identical(req$peer_identity, "test-client"))
grpc_reply(req, as.raw(9))
done <- FALSE
t0 <- Sys.time()
while (!done && as.numeric(Sys.time() - t0, units = "secs") < 15) {
  for (ev in grpc_poll(cl, timeout_ms = 200L)) {
    if (ev$kind == "unary") done <- TRUE
  }
}
stopifnot(done)
# stream over TLS, then dirty client close (the CQ-shutdown crash path)
s <- grpc_stream(cl, "/demo.Echo/Chat", deadline_ms = 10000)
grpc_send(s, raw(1))
grpc_close(cl)
grpc_close(srv)
cat("TLS-TSAN-OK\n")
