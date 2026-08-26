# grpc

An asynchronous gRPC client and server runtime for R.

Built on gRPC's generic asynchronous C++ API (`GenericStub`,
`AsyncGenericService`): requests and responses cross the native
boundary as method names plus opaque bytes, so there is no generated
stub code and no `protoc` step. Schemas are loaded at runtime, and
[RProtoBuf](https://github.com/eddelbuettel/rprotobuf) supplies and
consumes the message bytes. The two packages split the work cleanly:
RProtoBuf owns descriptors and messages, grpc owns transport.

The package links against the system's gRPC C++ library: one
`apt install` on Linux, the copy Rtools already bundles on Windows,
Homebrew's on macOS. No vendored copies, no Rust toolchain. That keeps
the notorious gRPC library dependence down to the same packages the
rest of the system already uses, at the cost of tying the binary to
the toolchain release it was built on (see `docker/` for the container
consequences).

## Design

- **Asynchronous throughout.** A background thread drains each
  completion queue and never touches the R API; completions are
  buffered natively and delivered in batches on the R main thread via
  `grpc_poll()`. A wake descriptor (exposed through `grpc_fd()`)
  integrates with `later::later_fd()`-style event loops, so polling is
  the fallback, not the primitive.
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

On Windows, Rtools 4.3 or later already carries gRPC and protobuf;
there is nothing to install beyond Rtools itself. On macOS:

```sh
brew install grpc protobuf pkgconf
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

answered <- FALSE
while (!answered) {
  for (ev in grpc_poll(srv, timeout_ms = 100L)) {
    if (ev$type == "request") {
      grpc_reply(ev, ev$request)
      answered <- TRUE
    }
  }
}

repeat {
  evs <- grpc_await(call, timeout_ms = 1000L)
  if (length(evs)) break          # empty means "not yet", never "failed"
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

Note the two ways to receive. `grpc_poll()` drains one queue for the
whole client or server, so a batch can mix events from every call in
flight, in completion order: iterate it and dispatch on `id`.
`grpc_await()` scopes the wait to a single call and steps over
everything else, which is what you want in sequential code. Indexing a
poll batch as `evs[[1]]` works only while nothing else is in flight,
and stops working silently once something is.

Either way an empty result means the wait expired, never that the call
failed. `grpc_await()` leaves the call untouched on expiry, so awaiting
again resumes it; the call's own `deadline_ms` is what bounds the loop.
Check the length before indexing, or a slow peer turns a timeout into
`subscript out of bounds`.

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

answered <- FALSE
while (!answered) {
  for (ev in grpc_poll(srv, timeout_ms = 100L)) {
    if (ev$type == "request") {
      req <- grpc_decode(ev$request, say$input_type)
      grpc_reply(ev, P("demo.Ping")$new(msg = toupper(req$msg)))
      answered <- TRUE
    }
  }
}

repeat {
  evs <- grpc_await(call, timeout_ms = 1000L)
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

repeat {
  evs <- grpc_await(call, timeout_ms = 500L)
  if (length(evs)) break
}
evs[[1]]$response_message$runtime_name
```

`inst/examples/cri-list-pods.R` is the runnable version (runtime
version, pod sandboxes, images), and the test suite exercises the
server-streaming `GetContainerEvents` against a live containerd when
the socket is accessible.

## Status

Public and pre-release. The API is still allowed to move. PLAN.md
records the design decisions and their evidence; `tools/sanitize.sh`
is the valgrind/ASan/TSan gate.

## License

Apache License (>= 2), matching gRPC itself.
