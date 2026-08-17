# grpc 0.0.1.10

- Fixed spurious wake-ups from `grpc_poll()` (#12). The eventfd
  carries one signal per event, but the counter was drained outside
  the mutex guarding the ready deque, so every signal posted between
  the drain and the batch below stayed counted even though the batch
  took those events. The leftover count was then spent as instant,
  empty polls: a caller that reads "no events" as "this call never
  started" abandons a live stream, which showed up as sequential
  server-streaming calls on a shared client failing 1-7 times in 30.
  The drain and a re-arm now happen under that mutex, so the
  descriptor is readable exactly while events are queued. Affects
  clients and servers alike, and `grpc_fd()` integrations most of all,
  since a partial batch used to leave the descriptor unreadable with
  events still pending.
- Documented what an empty `grpc_poll()` result means: the wait
  expired, not that the call is over.

# grpc 0.0.1.9

- Fixed a use-after-free race found by the first genuinely
  instrumented ThreadSanitizer run: `grpc_r_call_start()` and
  `grpc_r_stream_start()` read the call id after releasing the mutex,
  and a fast completion can free the call state on the drain thread in
  that window. The id is now captured under the lock.
- `tools/sanitize.sh` is fail-closed about its own preconditions,
  after three stacked silent failures let earlier sanitizer passes run
  the plain build: libraries are injected via `R_LIBS` and asserted
  with `find.package()` (littler's `-L` silently does not prepend),
  sanitizer flags go through `CXX17FLAGS` and instrumentation is
  proven via `__asan`/`__tsan` symbols in the built `.so`, installs
  use `--preclean`, an exit trap removes flagged objects even on
  failure, non-data-race TSan warnings are fatal, and the report
  classifier self-tests against synthetic logs at every run.

# grpc 0.0.1.8

- Keepalive: `keepalive_ms`/`keepalive_timeout_ms` on `grpc_client()`
  and `grpc_server()`, plus `min_ping_interval_ms` (server tolerance
  for client pings; gRPC's default is 5 minutes and kills faster
  clients). Enabling keepalive also lifts gRPC's ping policing so
  pings flow on quiet connections.
- Abortive close: `grpc_finish(drain = FALSE)` discards queued stream
  writes and prioritizes the terminal status (fencing); `grpc_cancel()`
  now also works on server requests as the hard escalation past a peer
  that has stopped reading.

# grpc 0.0.1.7

- Fixed a completion-queue shutdown race that aborted the process when
  closing a client (or server) with stream operations in flight: the
  drain thread could post a follow-on op after `cq.Shutdown()`. A
  `shutting` flag now suppresses all drain-thread op posts during
  close, on both client and server.
- Forced-error cleanup tests: post-close operations error cleanly,
  finalizers with work in flight, create/destroy churn.
- Verified under valgrind (0 errors, 0 definite/indirect leaks;
  possibly-lost noise from gRPC/absl thread-locals and R
  remains), AddressSanitizer
  (clean), and ThreadSanitizer (all reports trace to the
  uninstrumented system libgrpc boundary; none in package code).
- Reference node image under `docker/`: two-stage build on
  `rocker/r2u:noble`, runtime carries only `libgrpc++1.51t64` and
  `r-cran-rprotobuf`; built and smoke-tested. The base image release
  must match the build host's — the binary is tied to the distro ABI.
- configure and `SystemRequirements` now name `libprotobuf-dev`
  alongside `libgrpc++-dev` (not pulled automatically under
  `--no-install-recommends`).

# grpc 0.0.1.6

- Operational surface: `grpc_tls()` builds PEM credentials for both
  sides (TLS server, CA-pinned client, mTLS with
  `require_client_cert`, `target_name_override` for testing); server
  request events carry the transport `peer` and, under mTLS, the
  verified `peer_identity`; `grpc_state()` observes channel
  connectivity; `GRPC_TRACE`/`GRPC_VERBOSITY` documented.
- Health checking (`grpc.health.v1`) and server reflection
  (`grpc.reflection.v1`) protos vendored and proven through the
  generic path — unary Check and bidi ServerReflectionInfo — with no
  special machinery.

# grpc 0.0.1.5

- Streaming: `grpc_stream()` opens client-, server-, and bidirectional
  streams (typed or raw); `grpc_send()` (bounded write queue with
  `stream_writable` backpressure events), `grpc_writes_done()`,
  automatic bounded inbound buffering with HTTP/2 backpressure, and a
  terminal `stream_status` event. Server side: `grpc_read()` pulls
  inbound stream messages, `grpc_send()` queues outbound ones,
  `grpc_finish()` ends the stream after writes drain. `grpc_cancel()`
  is now a generic over calls and streams; client events carry `kind`.
- Live CRI streaming smoke: subscribe, hold, and cancel
  `GetContainerEvents` against containerd.

# grpc 0.0.1.4

- Real-world proof: CRI client against live containerd (v2.2.3) over a
  unix-domain socket — `Version`, `ListPodSandbox`, `ListImages`, all
  through the typed layer. CRI v1 schema vendored as a self-contained
  import root under `inst/proto/cri`; live tests gate on socket access
  (`GRPC_R_CRI_SOCKET`); `inst/examples/cri-list-pods.R` for humans.
- PLAN.md: Viento experiment reframed as control-plane candidacy.

# grpc 0.0.1.3

- RProtoBuf integration: `grpc_service()` / `grpc_method()` resolve
  services and methods from the runtime descriptor pool (via the
  FileDescriptorProto, sidestepping rprotobuf#116); `grpc_call()`
  accepts a `grpc_method` plus a `Message`, validates the request type,
  and auto-decodes OK responses as `response_message`; `grpc_reply()`
  accepts a `Message`; `grpc_decode()` decodes payload bytes. RProtoBuf
  remains a Suggests.

# grpc 0.0.1.2

- Generic asynchronous server on `AsyncGenericService`: `grpc_server()`
  accepts any method dynamically; `grpc_reply()` answers with a payload
  or a typed status plus trailing metadata. Bounded accept window and
  `max_active` apply backpressure.
- `grpc_poll()`, `grpc_fd()`, `grpc_pending()`, and `grpc_close()` are
  now S3 generics over clients and servers. Peer cancellation and
  deadline expiry surface as `"cancelled"` events; late replies are
  refused rather than crashing.
- Full in-process round trips (TCP and unix-domain sockets) covered in
  tests.

# grpc 0.0.1.1

- Generic asynchronous unary client on `GenericStub`: `grpc_client()`,
  `grpc_call()`, `grpc_poll()`, `grpc_cancel()`, `grpc_pending()`,
  `grpc_close()`, with deadlines, metadata, and typed status codes.
- Event-loop wakeup via eventfd, exposed through `grpc_fd()` for
  `later::later_fd()`-style integration; polling is the fallback, not
  the primitive.

# grpc 0.0.1

- Initial skeleton: system-linked build against the distro gRPC C++
  library via `pkg-config`, spike shim proving safe create/destroy of
  channels, completion queues, and generic servers.
