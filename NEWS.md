# grpc 0.1.0

- First CRAN release: an asynchronous gRPC client and server runtime on
  the generic C++ API, with unary and streaming calls, TLS/mTLS,
  deadlines, keepalive, cancellation, and event-loop integration via
  `grpc_fd()`. Runs on Linux, Windows (Rtools >= 4.3), and macOS.
  The 0.0.1.x entries below record the development history.

# grpc 0.0.1.14

- The package builds on Windows and macOS. The completion wake-up was
  the one platform-bound piece of the runtime: eventfd is Linux-only,
  and `src/wake.h` replaces it with the same contract everywhere -- a
  self-pipe on Unix, a loopback socket pair on Windows, readable
  exactly while events are queued. Windows links the gRPC that
  Rtools >= 4.3 bundles, through a static `Makevars.win`, and
  `OS_type: unix` left DESCRIPTION. CI gains a macOS leg building
  against Homebrew's gRPC.

# grpc 0.0.1.13

- Depends dropped from R (>= 4.4.0) to R (>= 4.3.0), the version noble
  ships. (This heading was added retroactively; the bump rode with the
  change in #23 without a NEWS entry.)

# grpc 0.0.1.12

- `grpc_await()` now works on a server `"grpc_request"` too, closing the
  asymmetry 0.0.1.11 left behind: the docs said "one queue serves the
  whole client *or server*" while the remedy shipped for the client
  only, so a server driving concurrent streaming calls still hand-rolled
  the `id` dispatch that had already gone wrong five times downstream.
  A handler can drain one client-streaming call with
  `grpc_await(req, timeout_ms)` and see nothing belonging to any other.
- `grpc_await()` became an S3 generic to accommodate that third class.
  Call sites are unaffected: the signature is unchanged and the existing
  `"grpc_call"` and `"grpc_stream"` behaviour is identical.
- On a server request the awaited events are the ones that follow the
  request itself (`"stream_msg"`, `"client_done"`, `"stream_writable"`,
  `"cancelled"`), since the request arrives from `grpc_poll()` in the
  first place. A server call has no terminal event of its own -- it ends
  when the handler ends it -- so `"client_done"` is what a
  client-streaming handler loops to.

# grpc 0.0.1.11

- New `grpc_await(x, timeout_ms)`: waits for events belonging to one
  call or stream. Other calls' events are stepped over and left queued
  in arrival order, and the wait ends only when a matching event
  arrives. This is the demultiplexing every other gRPC binding does
  for you -- Python hands back a per-call future or response iterator
  and never exposes a completion queue -- and its absence is what made
  the attribution mistakes in 0.0.1.10 possible in the first place.
  Use it for sequential code; use `grpc_poll()` and dispatch on `id`
  when calls really are concurrent, since awaiting one call means not
  looking at the others.
- The filter lives in C over the existing ready deque rather than in
  an R-side buffer, so there is no second queue to keep in sync and a
  filtered read cannot lose or reorder another call's events. The wait
  is filter-aware: it cannot lean on the eventfd, because another
  call's queued events keep the descriptor readable and the wait would
  spin instead of blocking.
- An expired `grpc_await()` leaves the call exactly as it was: await it
  again to keep waiting. That resumability is what keeps a required
  `timeout_ms` from being the empty-batch trap in new clothing, so it
  is now stated rather than implied, along with which clock is which
  (`deadline_ms` bounds the RPC, `timeout_ms` bounds one wait).
  Examples check the batch before indexing it: on a slow peer
  `grpc_await(call, timeout_ms = 1000)[[1]]` raises `subscript out of
  bounds`, which reads like a caller bug rather than the timeout it is.
- The documentation stopped teaching the bug. The README's round-trip
  and typed-call examples, and `inst/examples/cri-list-pods.R`, all
  indexed a poll batch as `evs[[1]]` or hand-rolled a `drain()` helper
  that returned the first event of whatever arrived. Correct only
  while nothing else is in flight, which is exactly how a caller
  infers the wrong model.

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
- Documented event attribution: one queue serves the whole client, so
  a batch can mix events from every call in flight, in completion
  order, and callers must dispatch on `id`. Taking `events[[1]]` as
  the answer to a unary call and accumulating every `"stream_msg"`
  into one stream's payload are the same assumption -- one queue per
  call -- and it does not hold. Abandoning a stream unread does not
  stop it, and
  its queued messages go on surfacing alongside later calls;
  `grpc_cancel()` bounds how many more are produced but cannot recall
  events already queued. Verified against the runtime rather than
  assumed: over 40 rounds of a deliberately abandoned stream followed
  by a second stream on the same client, no event's `id` ever
  disagreed with its payload, while an accumulator that ignored `id`
  got the wrong byte count 36 times out of 40.

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
