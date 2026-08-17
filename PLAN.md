# R gRPC runtime: plan

Status: exploratory. This work is separate from Viento's active nanonext
implementation and does not authorize a Viento transport migration.

Working name was gRPCpp; the name is an open decision (see below).

## Objective

Build a first-class asynchronous gRPC runtime for R on the public gRPC C++
API, designed to complement RProtoBuf rather than replace it.

The package should make R a genuine gRPC client and server while preserving
R's main-thread API rule and exposing completion-driven operation suitable
for event loops such as Viento's.

The founding use case was southbound interop — containerd (CRI) and etcd
speak gRPC and nothing else — proven live in increment 5. The package's
first consumer is the vientote rebuild (see the split, below): a
gRPC-native orchestrator using this package for its control plane, its
event-driven CRI container execution, and eventually its R executors.
Beyond that: Triton, OTLP telemetry, gRPC-only cloud APIs, and the typed
streaming channel between glinty's Flutter frontend and R backends.

## Platform commitment

Ubuntu/apt first. This is built by and mostly for us, on r2u. System
linking against the distro gRPC is the only build path; static vendoring
and the Rust/tonic alternative are dropped (they existed to serve CRAN and
Windows binaries, neither of which is a goal).

- **One protobuf runtime by construction.** Verified on noble:
  `libgrpc++1.51t64` depends on `libprotobuf32t64 (>= 3.21.12)`, and r2u's
  `r-cran-rprotobuf` depends on that same `libprotobuf32t64`. RProtoBuf and
  gRPC share the distro libprotobuf, so the ODR/dual-descriptor-pool
  collision cannot occur on this path. r2u preserves the lockstep across
  distro upgrades: a new Ubuntu release moves gRPC, protobuf, and the
  RProtoBuf rebuild together.
- **Pin to the distro gRPC.** Building gRPC from source against a
  different protobuf reintroduces the collision. Noble ships 1.51.1;
  everything needed is present (generic async APIs, completion queues,
  deadlines, keepalive, TLS, unix-domain transport). What we give up is
  newer xDS and observability surface, none of which matters for
  containerd or etcd. The wire protocol is stable, so interop with modern
  Go/Python peers is unaffected; that stays in the verification matrix as
  the claim most worth proving.
- **configure uses `pkg-config --cflags --libs grpc++ protobuf`.**
  `grpc++.pc` ships in `libgrpc++-dev` (verified). Install
  `libgrpc++-dev`, not `libgrpc-dev` alone. Do not use CMake
  `find_package(gRPC)`; noble's cmake packaging is broken across split
  packages. `protobuf-compiler-grpc` is never needed: the generic-API
  design does no C++ stub codegen, so the one genuinely broken piece of
  noble's gRPC packaging is a piece this package never touches.
- **Distribution: drat plus apt-based Docker images.** CRAN is not a
  target. No r-universe.

## Ownership boundary

RProtoBuf owns:

- `.proto` loading and descriptor pools
- message, service, and method descriptors
- R objects to and from Protocol Buffer bytes
- enums, oneofs, maps, reflection, and unknown fields

This package owns:

- channels and servers
- HTTP/2 transport and TLS
- unary and streaming RPC lifecycle
- completion queues
- deadlines, cancellation, status, and metadata
- flow control, keepalive, health, and transport observability

Applications continue to own durable state, idempotency, authorization,
fencing, reconciliation, and exactly-once effects.

The boundary is negotiable upstream: Dirk maintains RProtoBuf, so gaps
found on our side (proto3 coverage, C-level payload access) have a fix
path there rather than a workaround here. Upstream asks should be shaped
as generic RProtoBuf features useful to any transport, not grpc-specific
hooks, and are never a prerequisite for an increment.

## Initial architecture

Use gRPC's generic asynchronous C++ APIs (`GenericStub`,
`AsyncGenericService`). Requests and responses cross the native boundary as
method names plus opaque byte buffers; RProtoBuf supplies and consumes the
bytes. This avoids generated C++ service stubs in the first version and
permits runtime-loaded schemas.

Native gRPC threads must never call the R API. They place completions onto a
native queue; R receives batches on its main thread.

Polling is a fallback surface, not the integration primitive. The
completion thread must be able to wake the R event loop (mechanism chosen
in increment 2); an event loop that spins on `grpc_poll(timeout_ms = 0L)`
is a design failure, not an integration. Linux-only makes this the easy
case: eventfd plus `later_fd()` is clean, and no Windows path exists to
break.

Provisional surface:

```r
channel <- grpc_channel(target, credentials = grpc_tls(...))
call <- grpc_start(channel, method, request, deadline = NULL,
                   metadata = list())
events <- grpc_poll(call, max_events = 64L, timeout_ms = 0L)
grpc_cancel(call)

server <- grpc_server(address, credentials = grpc_tls(...))
grpc_listen(server)
events <- grpc_poll(server, max_events = 64L, timeout_ms = 0L)
grpc_reply(events[[1]], response, status = grpc_status_ok())
```

The API is illustrative, not committed.

## Build increments

The platform commitment retired the two project-killing risks (symbol
collision, distribution feasibility) by construction. Increment 1 now
verifies them cheaply instead of deciding them.

1. **Apt spike** (about an afternoon)
   - `apt install libgrpc++-dev`; minimal package with a pkg-config
     configure; create and destroy a channel, completion queue, and server
     safely.
   - Linkage check, once:
     `ldd .../RProtoBuf/libs/RProtoBuf.so | grep -E 'protobuf|absl'`
     against the same for `libgrpc++.so`. Agreement closes the ODR
     question permanently.
   - `RProtoBuf::readProtoFiles()` on the real containerd CRI
     `runtime.proto`. This is the biggest untested claim in the design:
     a large proto3 file with maps, oneofs, and well-known-type imports,
     against RProtoBuf's historically partial proto3 coverage. Run it now,
     while any gap it surfaces is an upstream bug report with a maintainer
     who will take a patch, not a late blocker.

2. **Generic unary client and event-loop wakeup**
   - Start, poll, complete, cancel, and time out unary calls.
   - Carry raw Protocol Buffer payloads, status, and metadata.
   - Prove no native thread touches R.
   - **Pick the wakeup mechanism here**, because it constrains the C++
     threading design and retrofitting it after bidirectional streaming is
     unpleasant. Leading candidate: eventfd signaled from the completion
     thread, watched via `later_fd()`. Alternative: a nanonext condition
     variable signalable from C, so Viento's loop can wait on one
     primitive.

3. **Generic unary server**
   - Accept methods dynamically through `AsyncGenericService`.
   - Dispatch completions to R and return responses or typed status.
   - Bound pending calls and apply backpressure.

4. **RProtoBuf integration**
   - Resolve service and method descriptors dynamically.
   - Validate request and response message types.
   - Raw-vector exchange first, and it must work against CRAN RProtoBuf
     0.4.27 as shipped.
   - If profiling shows a material copy cost, the upgrade path is an
     upstream RProtoBuf PR exporting C-callable payload access
     (serialize-into-buffer, parse-from-slices via `R_RegisterCCallable`),
     adopted here behind a version floor bump
     (`Imports: RProtoBuf (>= x.y.z)`) once it reaches CRAN. Framed as a
     generic feature for any package that transports protobuf.

5. **Real-world proof: CRI client**
   - List pod sandboxes against `/run/containerd/containerd.sock`.
   - Exercises unix-domain transport, a large real-world `.proto`, and a
     real descriptor pool, and proves the prospective interop before any
     streaming work is committed. A synthetic greeter proves none of that.

6. **Streaming**
   - Client, server, and bidirectional streams.
   - Explicit read/write readiness, half-close, cancellation, and bounded
     buffering.
   - Real-world target: CRI `GetContainerEvents` (server stream from
     containerd), the call that makes vientote's container
     execution event-driven instead of polling.

7. **Operational surface**
   - TLS identity and trust configuration.
   - Health checking and reflection. Both are ordinary proto services and
     fall out of the generic path almost for free; the real work in this
     increment is TLS and diagnostics.
   - Structured diagnostics, channel state, and tracing hooks.

8. **Packaging and deployment**
   - drat builds against distro gRPC. (Deferred 2026-08-14: no drat repo
     yet, Troy's call.)
   - Node container images: `libgrpc++1.51t64` in the base image, and the
     image base must match the build host's, since the package binary is
     tied to the distro ABI. One apt line in the Dockerfile, but the
     base-image pairing is a stated constraint, not an accident.
   - Sanitizer, Valgrind, and forced-error cleanup coverage.
   - The guarantee is distro-release/ABI pairing, not a hermetic or
     bit-reproducible image; pin base digest and apt versions when a
     frozen image is needed.

## Spike results (increment 1, done 2026-08-14)

Environment: noble, `libgrpc++-dev` 1.51.1-4.1build5, r2u
`r-cran-rprotobuf` 0.4.27.

- **Linkage verified, ODR closed.** `RProtoBuf.so` links
  `libprotobuf.so.32`. gRPC's core libraries (`libgrpc.so.29`,
  `libgrpc++.so.1.51`) link no protobuf at all; the one component that
  does (`libgrpc++_reflection`) links the same `libprotobuf.so.32`. One
  protobuf runtime on the system, and .so.32 is the only major present.
- **Shim works.** Channel, completion queue, and generic-service server
  create/destroy with finalizers and shutdown-then-drain ordering: all
  tinytest results pass, including 10 ephemeral-port server bind cycles
  (note: the at-home tests need `run_test_dir()`; `test_package()`
  defaults to `at_home = FALSE`). `R CMD check`: 0 errors, 0 warnings,
  3 benign NOTEs.
- **CRI proto loads.** cri-api v0.33.2 `api.proto` (2,128 lines, proto3)
  loads via `readProtoFiles2(protoPath = ...)`. It imports gogoproto, so
  `gogo.proto` and `google/protobuf/descriptor.proto` must be staged
  under the import root; the increment-5 client should vendor these as
  test fixtures.
- **Combined session clean.** gRPC channel and server alive while the CRI
  descriptor pool loads; `VersionRequest` round-trips; teardown clean.
- **RProtoBuf gap found (not a blocker).** `fd$RuntimeService` returns a
  `ServiceDescriptor`, but `method_count()` fails: the C routine
  `ServiceDescriptor_method_count` is not in RProtoBuf's registration
  table in 0.4.27. Upstream fix candidate, framed generically (register
  the service/method introspection routines).
- **Workaround that unblocks everything:** `as(fileDescriptor(msg),
  "Message")` yields the `FileDescriptorProto`. From it, both CRI
  services enumerate (RuntimeService: 30 methods; ImageService), with
  `input_type`/`output_type` and streaming flags
  (`GetContainerEvents` shows `server_streaming = TRUE`). Method path,
  message types, and streaming arity — all the generic client needs —
  are available at R level against stock CRAN 0.4.27.

Dirk contact remains gated on an explicit greenlight from Troy.

## CRI proof results (increment 5, done 2026-08-14)

- Live against containerd v2.2.3 on this host (CRI plugin enabled,
  socket ACL'd for the user): `Version`, `ListPodSandbox`, and
  `ListImages` all answer over `unix:///run/containerd/containerd.sock`
  and decode through the typed layer (`response_message`). Zero
  sandboxes on a non-Kubernetes host is the expected result; the round
  trip is the proof.
- gRPC 1.51 (2022) client interoperating with containerd v2.2.3 (2026)
  confirms the wire-stability claim behind pinning to the distro
  library.
- The CRI v1 schema (cri-api v0.33.2 plus its gogoproto and
  descriptor.proto imports) is vendored as a self-contained import root
  at `inst/proto/cri` and resolves via the increment-4 layer:
  RuntimeService (30+ methods, `GetContainerEvents` server-streaming
  flag intact) and ImageService.
- Live tests gate on a readable socket (`GRPC_R_CRI_SOCKET`
  overridable) and skip cleanly elsewhere;
  `inst/examples/cri-list-pods.R` is the human-runnable version.
- Host note: Docker's stock containerd ships `disabled_plugins =
  ["cri"]`; enabling CRI plus a transient socket ACL was required
  (tmpfs, so re-grant after reboot; durable access would use
  containerd's `[grpc] gid` config instead).

## Hardening results (increment 8, done 2026-08-14)

- **Forced-error cleanup tests caught a real shutdown race.** Closing a
  client with a stream write in flight aborted the process
  deterministically (`grpc_cq_begin_op` assertion): `TryCancel` during
  close completes the in-flight op on the drain thread, whose handler
  then posted a follow-on op (Finish/next write/read) after the main
  thread had already shut the CQ down. Fixed with a `shutting` flag,
  set under the mutex before `cq.Shutdown()`, that suppresses every
  drain-thread op post; already-started ops drain legally through
  `Next` after CQ shutdown. The same latent race existed server-side
  (`pump_locked`, the accept handler's read post) and is closed the
  same way. `test_cleanup.R` pins the whole family: post-close
  operations error cleanly, finalizers with work in flight,
  create/destroy churn, and a server-side stress (32 streams with
  128KB writes pumping at close).
- **Mutation-verified guards.** Removing the server write-pump guard
  aborts the stress test 5/5 runs. The accept-branch guard could not
  be made to crash under a deliberately hostile workload (two repro
  shapes, 5 runs each): `Server::Shutdown` resolves unconsumed accepts
  before the CQ shutdown, so that guard is defense-in-depth, not a
  reachable crash. The client-side guard has the original
  deterministic reproducer (close with a stream write in flight).
- **Recipes are committed.** `tools/sanitize.sh` is the exact
  valgrind/ASan/TSan procedure (with `tools/tls-exercise.R` for TLS
  under TSan) and reproduces the results below from a clean checkout;
  reference environment and TSan triage criterion are documented in
  the script header.
- **Valgrind memcheck**: full suite passes, 0 memory access errors, 0
  definite/indirect leaks. (Possibly-lost records only, from gRPC/absl
  thread-locals and R itself — interior-pointer noise.)
- **ASan**: full suite passes clean.
- **TSan** (`setarch -R`; this kernel's ASLR breaks TSan's shadow
  mapping): full suite plus a dedicated TLS/mTLS exercise, since
  subprocesses segfault under a preloaded libtsan and the TLS tests
  shell out to openssl — certs pre-generated outside instead. Every
  report (~250-320 per run; composition varies) traces to the
  uninstrumented system libgrpc boundary: synchronization edges inside
  `libgrpc.so` are invisible to TSan, so batch handoffs to the
  completion thread look unsynchronized. The enforced criterion
  (`tools/sanitize.sh`, fatal on violation, both logs): no racing
  access may have a package source frame in its top two frames, except
  an access whose own #0 is the allocator — construction paired
  against first post-handoff use is the known false-positive shape
  (`new call_state` in `grpc_r_call_start` vs `FinishOp` inside
  libgrpc). Deallocation is not exempt. The analyzer was validated in
  both directions: passes the known-false-positive log, trips on
  synthetic memcpy-race and delete-race reports with package frames.
  Under that criterion the threading contract (all shared state under
  the mutex) has no TSan-visible violation.
- **Correction (2026-08-14, later): the ASan/TSan passes above were
  vacuous, and honest reruns found a real bug.** Three stacked silent
  failures meant the "instrumented" suites had been running the plain
  ambient build: littler's `-L` flag does not actually prepend the
  library path (so the scratch builds never loaded); the sanitizer
  flags were set via `CXXFLAGS`, which a `CXX_STD = CXX17` package
  never reads (`CXX17FLAGS` is the one that counts); and
  `R CMD INSTALL` reused stale objects from `src/`, so even corrected
  flags never reached the compiler. Each failure produces output
  identical to a clean pass. `tools/sanitize.sh` now proves its own
  preconditions: libraries injected via `R_LIBS` with a
  `find.package()` assertion baked into the suite, `__asan`/`__tsan`
  symbol checks on the built `.so`, and `--preclean` everywhere.
- **Honest instrumented results.** ASan: full suite clean. TSan
  (instrumented, ~1500-1600 reports/run): the criterion is
  performing-frame classification — a report is fatal iff BOTH racing
  accesses execute in package source, the only class TSan can prove
  through an uninstrumented libgrpc; one-sided construction/handoff
  pairs (~15/run) are counted, printed, and documented as
  boundary-unverifiable, with use-after-free covered by ASan and
  valgrind instead. This is a conservative, high-signal gate, not
  absolute proof: an ordering bug living entirely inside
  uninstrumented libgrpc, or one TSan's timing never observes, stays
  invisible. Non-data-race TSan warnings are fatal outright, and the
  classifier is exercised against synthetic reports (including an
  inlined-header performing frame above a package caller) at every
  gate run. The criterion promptly caught a real race:
  `grpc_r_call_start`/`grpc_r_stream_start` returned `cs->id` /
  `s->id` after releasing the mutex, and a fast completion can delete
  the state on the drain thread in that window (unary: any completed
  call; stream: instant failure path) — a read of freed memory. Fixed
  by capturing the id under the lock. Three consecutive full gate
  runs clean after the fix.
- **Reference node image** (`docker/`): two-stage build on
  `rocker/r2u:noble` — build stage compiles against `libgrpc++-dev`,
  runtime stage carries only `libgrpc++1.51t64` + `r-cran-rprotobuf`.
  Built and smoke-tested: unary round trip inside the container. The
  build stage exposed that `libgrpc++-dev` does not pull
  `libprotobuf-dev` under `--no-install-recommends`; the configure hint
  and `SystemRequirements` now name both packages.

## Vientote transport follow-up (done 2026-08-14)

Green-lit by Troy after increments 6-8 merged; with it, the vientote
control-plane gate (streaming + mTLS + the binding 10s/5s keepalive
contract) is complete. Three primitives, all driven by vientote's
session design:

- **Keepalive**: `keepalive_ms`/`keepalive_timeout_ms` on both
  constructors, plus `min_ping_interval_ms` on the server. Setting
  keepalive also sets the two ping-policing overrides without which it
  silently fails (`GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA = 0`,
  `GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS = 1`); the server tolerance
  matters because gRPC's default kills clients pinging more than once
  per 5 minutes with a too_many_pings GOAWAY. Behavioral tests prove
  client-originated pings (with an active stream and on a call-less
  connection) and the server's receive tolerance; server-originated
  dead-peer *detection* is not testable on loopback (TCP answers pings
  in the kernel regardless of the app) — those arguments are plumbed
  identically and taken on the gRPC core's word.
- **Abortive finish** (`grpc_finish(drain = FALSE)`): discards queued
  writes and prioritizes the terminal status — the fence notice
  (ABORTED on session replacement, FAILED_PRECONDITION on lease
  expiry) must not wait behind stale assignments, and the evicted node
  must not receive them. One already-posted write cannot be recalled;
  the test proves the discard under real flow-control backpressure
  (12 x 1MB at a non-reading client).
- **Server-side cancel** (`grpc_cancel` on a request): hard escalation
  when even the abortive status is stalled behind the peer's exhausted
  flow-control window; the peer sees CANCELLED. Its FALSE return means
  the call was already terminal — no further stale writes possible —
  not that the fence status was received; there is no delivery receipt
  in the protocol.

## Event delivery results (increment 9, done 2026-08-16)

Opened by vientito issue #12: sequential server-streaming calls on a
shared client intermittently never starting, 1-7 failures in 30 against
a live containerd. The reported diagnosis was wrong, and the way it was
wrong is the more useful finding.

- **The calls always started; `grpc_poll` was waking spuriously.** The
  eventfd carries one signal per event, but the counter was drained
  outside the mutex guarding the ready deque, so every signal posted
  between the drain and the batch take stayed counted even though the
  batch took those events. The leftover became a credit the next poll
  spent as an instant empty wake-up. Instrumented pre-fix run against
  an external peer: descriptor readable, nine stale signals in the
  counter, empty deque, and a 3000ms poll returning in **0ms**. Drain
  and re-arm now happen under that mutex, so the descriptor is readable
  exactly while events are queued. `grpc_r_server2_poll` had it too.
- **The tell was in the original measurements and nobody read it.**
  Three of the four reported observations (rate scaling with the
  previous stream's size, fresh-client fixing it, explicit drain fixing
  it) are equally consistent with either mechanism. Only timing the
  poll separates them, and an empty batch returning in 0ms against a
  3000ms timeout is not a timeout. Downstream confirmation: on the
  **pre-fix** build a loop that keeps polling instead of quitting on an
  empty batch fails 0/150, versus 21/150 for the loop that treats empty
  as terminal. Post-fix, 0/600 both ways.
- **Reproducing it needed a separate process.** The in-process loopback
  never reproduces, because R drives both sides sequentially and the
  events are always already queued when the poll runs. The regression
  test forces the interleave instead of waiting for it, and asserts the
  invariant an empty poll must satisfy: that it waited out its timeout.
  Catches the bug 6/6 pre-fix, cannot false-alarm post-fix.
- **A second, caller-side hazard surfaced underneath it.** One queue
  serves the whole client, so an abandoned stream's queued messages
  keep arriving alongside later calls. Payload-tagged probe over 40
  rounds of a deliberately abandoned stream: **0** events whose id
  disagreed with their payload, **0** wrong lengths, but **298**
  abandoned-stream messages delivered during the next stream, and an
  accumulator ignoring `id` got the wrong byte count **36/40** (0/40
  filtering on `id`). The runtime is correct; the contract was simply
  never written down. `grpc_cancel()` bounds the leak (298 to 57)
  rather than stopping it, since cancellation cannot recall queued
  events — so the docs say bounds, not stops.
- **Downstream found the same mistake in five places, three of them
  unary** (`events[[1L]]` taken as the answer to the call just
  started). Both shapes are one assumption — one queue per call — and
  neither holds: a batch mixes every call in flight, in completion
  order rather than start order.
- **The package's own documentation taught it.** The README's
  round-trip and typed-call examples and `inst/examples/cri-list-pods.R`
  all indexed a poll batch as `evs[[1]]`, the last through a
  hand-rolled `drain()` helper returning the first event of whatever
  arrived. Correct only while nothing else is in flight, which is
  exactly how a caller infers the wrong model. Recorded as a root
  cause, not as downstream carelessness.

## Per-call waiting: grpc_await (increment 10, done 2026-08-16)

The demultiplexing gap the above kept exposing. Checked against the
reference implementations rather than assumed: Python never exposes a
completion queue — `__call__()` returns the response, `.future()` a
per-call Call/Future, server streaming a response iterator — and Go,
Java and Node are the same shape. The shared queue belongs to the
C-core and C++ async API this package wraps, whose own docs describe
the callback replacement as "easier to use". We exposed the hardest API
in the family raw and left the demux to every caller.

- **The filter lives in C over the existing ready deque, not an R-side
  buffer.** The deque is already the buffer, so ordering is preserved
  for free and `grpc_fd()` stays correct because the increment-9 re-arm
  already signals whenever anything is left queued. An R stash would
  have rebuilt increment 9's descriptor hazard one layer up: await
  stashes events, returns, the event loop waits on the fd and sleeps
  with work pending.
- **The wait must be filter-aware.** It cannot lean on the eventfd,
  because another call's queued events keep the descriptor readable and
  `poll()` returns instantly forever. Draining before each wait makes
  it mean "a completion arrived since I last looked"; the readable-iff-
  queued invariant is restored before returning, and no other thread
  reads it in between. Verified by building the naive variant that
  waits on "anything queued": it fails exactly the spin assertion and
  passes the other 68.
- **`timeout_ms` is required, and expiry is resumable.** An expired
  await leaves the call exactly as it was — `timeout_ms = 1` returns
  empty in 0ms, awaiting the same call then returns its completion with
  status OK. That is what keeps a required timeout from being increment
  9's empty-batch trap in new clothing, so it is documented and pinned
  by a test rather than left implicit.
- **The fix for one documentation trap introduced another.** Replacing
  `evs[[1]]` with `grpc_await(call, timeout_ms = 5000)[[1]]` raises
  `subscript out of bounds` on expiry, which reads like a caller bug
  and gives no hint that waiting longer would have worked. Examples now
  check the batch before indexing, and both runnable README examples
  are executed with their documented outputs asserted so this cannot
  rot again. Distinguishing expiry at the type level was considered and
  declined: `NULL` for unary makes `ev$status_name` silently `NULL`,
  and erroring on timeout breaks the loop idiom streams need.
- **The sanitizer gate caught a test bug the native runs passed by
  luck.** `test_await.R` originally answered whichever request happened
  to be queued first, which only fails once the machine is slow enough
  for an unrelated call's request to arrive first; valgrind found it.
  It matches on a marker byte now.

## Per-call waiting, server side (increment 11, done 2026-08-17)

Closes the asymmetry increment 10 left: the documentation said "one
queue serves the whole client **or server**" while the remedy shipped
for the client only, so a server driving concurrent streaming calls
still hand-rolled the `id` dispatch that had already gone wrong five
times downstream.

- **`grpc_await()` is now an S3 generic** over `"grpc_call"`,
  `"grpc_stream"`, and `"grpc_request"`. Call sites are unaffected — the
  signature is unchanged — and `saber::blast_radius()` confirmed no
  caller depends on it being a plain function.
- **The server filter is the same shape as the client's**, over
  `rserver::ready`, with the same filter-aware wait. The server's take
  loop has no per-event bookkeeping, so it is strictly simpler.
- **The awaited scope differs, and that is inherent.** A `grpc_request`
  arrives *from* `grpc_poll()`, so there is nothing to await until one
  has been received, and a server call has no terminal event of its own:
  it ends when the handler ends it. `"client_done"` is what a
  client-streaming handler loops to, not a status.
- **Verified non-vacuous the same way as the client.** Built the naive
  server variant that waits on "anything queued": it fails exactly the
  spin assertion 3/3 and passes the other 47. Writing the test also
  surfaced that the obvious version is vacuous — the server only queues
  a message when `grpc_read()` posts one, so without a read deliberately
  posted on the second call there is nothing for the filter to step
  over, and the scoping assertion passes trivially.

## Verification

- Interoperate with official C++, Go, and Python gRPC implementations
  (this is the proof that pinning to distro 1.51.1 costs nothing on the
  wire).
- Test malformed frames, cancellation races, deadline races, peer loss,
  server shutdown, and completion after R object collection.
- Assert every external pointer has one owner and one terminal transition.
- **Fork safety.** R users fork constantly (`mclapply`, anything
  mirai-adjacent), and gRPC's fork support is fragile even with
  `GRPC_ENABLE_FORK_SUPPORT`. Test forking after a channel is open and
  document the observed failure mode. A documented failure beats a
  mystery hang; a guarantee is a non-goal.
- Benchmark unary and bidirectional streaming against Go and against
  nanonext for Viento-shaped messages.
- Record p50/p99 latency, saturation throughput, memory, CPU, connection
  count, and R event-loop delay.

## The split: vientito and viento (decided 2026-08-14)

The control-plane candidacy is resolved by splitting instead of
migrating. Every hedge the one-codebase path required — a
transport-neutral interface with a conformance suite for two adapters,
capability-flagged engine contracts, events-with-polling fallbacks, an
Imports/Suggests dance — was complexity spent making one codebase span
two architectures. The split deletes all of it. Those hedges are
rejected; do not reintroduce them.

**vientito** (today's viento, renamed when grpc is ready):

- Finishes phase 1 exactly as architected: nanonext control plane,
  strict-JSON wire, CLI engine adapters (podman/docker), mirai and
  systemd execution drivers.
- Feature-frozen after phase 1. It runs the fleet while the rebuild
  happens. It is allowed to rot; it gets updated only if someone turns
  out to find it useful.
- Its engine-adapter contract stays as-is; the fidelity question is
  moot for CLI adapters, which poll because that is what CLIs do.

**vientote** (the gRPC-native rebuild; working name, decided
2026-08-14 — the augmentative pairs with vientito and decouples the
rebuild's start from the rename; whether it claims the bare `viento`
name at 1.0 stays open and blocks nothing):

- gRPC control plane from line one: bidirectional node-control streams,
  mTLS peer identity, deadlines as a primitive, protobuf `.proto`
  contracts as the wire truth.
- CRI-native container execution: containerd consumed directly and
  event-driven (`GetContainerEvents`), with no
  lowest-common-denominator engine contract in the way.
- systemd native driver carried over; mirai carried over initially as
  the R-execution driver, with R-executors-as-grpc-services (this
  package's server side, supervised like any native service) as the
  later increment that retires it.
- **Crown jewels are copied, not shared.** WAL, fold, events, states,
  fencing, canonical identity move over wholesale and diverge freely.
  No shared core package; vientito's copies stand as-is. Semantics
  preserved: WAL-before-ack, logical operation ids, boot/session
  fencing, delivery confirmation, reconciliation.
- Design input before the control plane is drafted: mine the incumbents
  with the bonsaisitter + treesitter.go stack, protos first,
  implementations only for specific questions. The map: **swarmkit**
  (Apache-2.0) for the manager/worker gRPC wire contract — its
  dispatcher.proto session/heartbeat/assignment-stream design is the
  closest prior art, and Nomad offers no equivalent because its
  server/client RPC is net/rpc + msgpack, not gRPC; **Nomad 1.6.x**
  (the pre-BUSL MPL-2.0 snapshot, fully minable) for scheduler
  semantics — the evaluation/allocation/plan pipeline, client
  heartbeats and drain — plus its go-plugin gRPC boundaries (task
  driver, device plugins); **k8s** only for kubelet node-lifecycle
  patterns and api-machinery conventions, plus the already-vendored
  cri-api. Post-2023 Nomad is BUSL and off limits.
- Gets its own plan document in its own repo when the rebuild starts;
  this section is the charter, not the plan.

Sequencing:

1. vientito finishes phase 1 (g5 gpu_service objective) uninterrupted;
   its live momentum is not stalled for the rebuild.
2. grpc completes increments 6-8 (streaming, TLS/operational surface,
   packaging) — a control plane needs streams and mTLS before it can
   carry one.
3. The rename (current repo -> vientito, one deliberate pass while
   private: repo, unit names, config docs) happens when grpc is ready,
   freeing the name.
4. The vientote rebuild starts (its repo can exist any time; the
   working name does not wait on the rename): crown-jewel copy, then
   control plane, then CRI execution, then executors.

Rationale on record: gRPC is the industry-standard control-plane
substrate for this class of system; vientote is instantly
legible to that world and open to non-R agents via generated stubs.
Dogfooding the rebuild is how grpc earns maturity, and grpc is also the
planned typed/streaming channel between glinty's Flutter frontend and R
backends, so it is shared infrastructure, not a viento-only bet.
Pre-production is when this is cheap. Strategically, cornball.ai builds
alternatives to the Posit-adjacent stack (nanonext/mirai); the rebuild
moves the control path to first-party transport, and the executor
increment finishes the job. Latency was never a criterion: nanonext
would very likely win R-to-R, and that is not evidence against any of
this.

## Encoding decision: no protobuf retrofit inside Viento

Considered and declined. Viento's control plane already has the
properties a protobuf migration would be sold on: the wire and the WAL
are strict janssonr JSON with fail-closed validation on both ends, a
versioned envelope, and closed per-message field maps; the WAL is
length-framed with torn-tail recovery and sha256 chain hashes.

Protobuf's headline feature, silent field-number evolution across
versions, is the exact behavior Viento's versioning policy refuses on
purpose (exact-match protocol version, "fail closed, never 'probably
compatible'"). And there is no hot path to optimize: steady state is
roughly one small JSON message per node per 15 seconds. A migration
would trade working, validated, hash-pinned schemas for properties
Viento either has already or deliberately rejected. JSON is also the
better format for a log humans read after a crash; a protobuf blob
without its schema is field numbers, not a record.

Revisit only if a non-R agent actually materializes, and even then the
first question is a `.proto` contract for new surfaces, not re-encoding
existing ones.

This declines re-encoding the existing nanonext channel in place, and
the split (below) makes it permanent: vientito keeps JSON-on-nanonext
until retirement, and vientote is protobuf/gRPC by design —
new surfaces with `.proto` contracts, not a retrofit.

## Non-goals

- Replacing RProtoBuf
- Reimplementing Protocol Buffers
- Static vendoring of gRPC/abseil, or any Rust/tonic implementation
- CRAN submission; Windows and macOS support (revisit only on real
  outside demand)
- Retrofitting protobuf or gRPC onto vientito in place (the split
  supersedes migration; see encoding decision and the split section)
- Dual-transport machinery in either orchestrator: no transport-neutral
  adapter interface, no capability-flagged engine contracts
- Putting Viento's WAL or domain policy into the transport package
- Claiming exactly-once RPC delivery
- Guaranteeing fork safety after a channel is open (the failure mode is
  documented instead)
- Beating nanonext on R-to-R latency
- Starting the vientote rebuild before grpc has streaming and TLS
  (increments 6-7), or stalling vientito's phase 1 for it

## Open decisions

- Final package name and repository. With system-linked C++ committed,
  `gRPCpp` no longer risks naming an implementation that might change,
  but `grpc` is cleaner and unclaimed on CRAN (not that CRAN is the
  target). Decide before the first push.
- Generic dynamic API only versus optional generated R conveniences.
- Strict handling of unknown fields versus normal protobuf evolution.
- **A one-call-one-answer convenience.** Both the CRI example and the
  guarded unary idiom want "block until this call answers"; the example
  grew an `await1()` helper to say it. Against: a helper that hides the
  loop also hides the `deadline_ms` relationship that makes the loop
  terminate. Whatever is decided should probably have a server twin.
- **`grpc_poll`/`grpc_await` and EINTR.** An interrupted `poll()`
  returns an empty batch, so "empty means the wait expired" is
  narrowly untrue. Left alone deliberately: returning early is what
  lets R process the interrupt, and retrying would swallow a Ctrl-C for
  the whole timeout. It matters more now that the sentence is a
  documented contract.

Resolved: static versus system linking (system only; see platform
commitment), distribution channel (drat plus apt/Docker; no r-universe),
event-loop and promises/mirai integration (increment 2), Rust/tonic
(dropped), protobuf-on-nanonext (declined; see encoding decision),
license (Apache-2.0, matching gRPC; RProtoBuf is GPL (>= 2) per CRAN
0.4.27, accepted as plumbing-level exposure), the vientito/vientote split
(vientito finishes on nanonext and freezes; vientote is the gRPC-native
rebuild, bare `viento` name parked — see the split section).
