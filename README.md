# grpc

An asynchronous gRPC client and server runtime for R.

Built on gRPC's generic asynchronous C++ API (`GenericStub`,
`AsyncGenericService`): requests and responses cross the native
boundary as method names plus opaque bytes, so there is no generated
stub code and no `protoc` step. Schemas are loaded at runtime, and
[RProtoBuf](https://github.com/eddelbuettel/rprotobuf) supplies and
consumes the message bytes. The two packages split the work cleanly:
RProtoBuf owns descriptors and messages, grpc owns transport.

The package links against the distribution's gRPC C++ library: one
`apt install`, no vendored copies, no Rust toolchain. That keeps the
notorious gRPC library dependence down to the same system packages the
rest of the OS already uses, at the cost of tying the binary to the
distro release it was built on (see `docker/` for the container
consequences).

## Design

- **Asynchronous throughout.** A background thread drains each
  completion queue and never touches the R API; completions are
  buffered natively and delivered in batches on the R main thread via
  `grpc_poll()`. An eventfd (exposed through `grpc_fd()`) integrates
  with `later::later_fd()`-style event loops, so polling is the
  fallback, not the primitive.
- **Backpressure end to end.** Bounded write queues on both sides,
  bounded inbound buffering with real HTTP/2 flow control to the peer,
  and a bounded accept window on the server.
- **Operational surface.** TLS and mTLS with verified peer identity on
  request events, channel-state observation, per-call deadlines,
  keepalive with the ping-policing overrides that make it actually
  work, graceful and abortive stream termination, and cancellation on
  both sides.
- **Standard ecosystem services** (`grpc.health.v1`,
  `grpc.reflection.v1`) are ordinary proto services here; their
  schemas ship in `inst/proto/`.

## Installation

On Ubuntu (noble) or Debian:

```sh
sudo apt install libgrpc++-dev libprotobuf-dev pkgconf
```

Then, from a checkout:

```sh
R CMD INSTALL .
```

RProtoBuf (`apt install r-cran-rprotobuf` with
[r2u](https://eddelbuettel.github.io/r2u/), or from CRAN) is a
suggested dependency: raw byte payloads work without it, typed calls
need it.

## A first round trip

Client and server in one process, raw bytes, no schema:

```r
library(grpc)

srv <- grpc_server("127.0.0.1:0")
cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))

call <- grpc_call(cl, "/demo.Echo/Say", as.raw(1:4), deadline_ms = 5000)

repeat {
  evs <- grpc_poll(srv, timeout_ms = 100L)
  if (length(evs)) {
    grpc_reply(evs[[1]], evs[[1]]$request)
    break
  }
}

repeat {
  evs <- grpc_poll(cl, timeout_ms = 100L)
  if (length(evs)) break
}
evs[[1]]$status_name   # "OK"
evs[[1]]$response      # the echoed bytes

grpc_close(cl)
grpc_close(srv)
```

The server accepts any method name; there are no registered handlers,
just events. What arrives is the method path, metadata, deadline, peer
address (and, under mTLS, the certificate-verified peer identity), and
the request bytes.

## Typed calls with RProtoBuf

Load a schema at runtime, resolve the service, and the byte handling
disappears:

```r
library(grpc)
library(RProtoBuf)

proto <- tempfile(fileext = ".proto")
writeLines(c(
  'syntax = "proto3";',
  'package demo;',
  'message Ping { string msg = 1; }',
  'service Echo { rpc Say (Ping) returns (Ping); }'
), proto)
readProtoFiles2(basename(proto), protoPath = dirname(proto))

svc <- grpc_service("demo.Ping", "Echo")
say <- grpc_method(svc, "Say")

srv <- grpc_server("127.0.0.1:0")
cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))
call <- grpc_call(cl, say, P("demo.Ping")$new(msg = "hello"),
                  deadline_ms = 5000)

repeat {
  evs <- grpc_poll(srv, timeout_ms = 100L)
  if (length(evs)) {
    req <- grpc_decode(evs[[1]]$request, say$input_type)
    grpc_reply(evs[[1]], P("demo.Ping")$new(msg = toupper(req$msg)))
    break
  }
}
repeat {
  evs <- grpc_poll(cl, timeout_ms = 100L)
  if (length(evs)) break
}
evs[[1]]$response_message$msg   # "HELLO"

grpc_close(cl)
grpc_close(srv)
```

Requests are validated against the method's input type before sending,
and completions carry the decoded response. Streaming methods work the
same way through `grpc_stream()` / `grpc_send()` / `grpc_writes_done()`,
with per-message `stream_msg` events and a terminal `stream_status`.

## A real peer: the Kubernetes CRI

The package talks to containerd over its unix socket using the CRI v1
schema vendored under `inst/proto/cri`:

```r
library(grpc)
library(RProtoBuf)
readProtoFiles2("api.proto",
                protoPath = system.file("proto", "cri", package = "grpc"))

cl <- grpc_client("unix:///run/containerd/containerd.sock")
rt <- grpc_service("runtime.v1.VersionRequest", "RuntimeService")
call <- grpc_call(cl, grpc_method(rt, "Version"),
                  P("runtime.v1.VersionRequest")$new(), deadline_ms = 2000)
```

`inst/examples/cri-list-pods.R` is the runnable version (runtime
version, pod sandboxes, images), and the test suite exercises the
server-streaming `GetContainerEvents` against a live containerd when
the socket is accessible.

## Status

Private and pre-release. The API is still allowed to move. PLAN.md
records the design decisions and their evidence; `tools/sanitize.sh`
is the valgrind/ASan/TSan gate.

## License

Apache License (>= 2), matching gRPC itself.
