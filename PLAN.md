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

The prospective use case is southbound interop: containerd (CRI) and etcd
speak gRPC and nothing else. Viento speaks to neither today; its OCI driver
shells out to the podman/docker CLI on a polling cadence, and the only
containerd reference in the repo is a "maybe later" note in a draft. This
package buys the option of a containerd engine adapter driven by CRI events
(`GetContainerEvents`) instead of CLI polling. The plan proves that boundary
early precisely because it is prospective, not established.

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
- **Distribution: r-universe/drat plus apt-based Docker images.** CRAN is
  not a target.

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
     containerd), the call that would let a Viento containerd adapter be
     event-driven instead of polling.

7. **Operational surface**
   - TLS identity and trust configuration.
   - Health checking and reflection. Both are ordinary proto services and
     fall out of the generic path almost for free; the real work in this
     increment is TLS and diagnostics.
   - Structured diagnostics, channel state, and tracing hooks.

8. **Packaging and deployment**
   - r-universe/drat builds against distro gRPC.
   - Node container images: `libgrpc++1.51t64` in the base image, and the
     image base must match the build host's, since the package binary is
     tied to the distro ABI. One apt line in the Dockerfile, but the
     base-image pairing is a stated constraint, not an accident.
   - Sanitizer, Valgrind, and forced-error cleanup coverage.
   - Reproducibility comes from pinned distro package versions, not
     hermetic vendoring.

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

## Viento experiment: control-plane candidacy, after the runtime is sound

gRPC is a candidate for Viento's node-control plane, not only the
southbound boundary. Rationale, updated 2026-08-14:

- gRPC is the industry-standard control-plane substrate for this class
  of system (kubelet/CRI, etcd, CSI); speaking it makes Viento legible
  to that world and lets non-R agents join the fleet with generated
  stubs.
- Viento is pre-production and private: a control-plane swap is at its
  cheapest now, and dogfooding is how the grpc package earns maturity.
- Strategic: nanonext/mirai are Posit-adjacent; cornball.ai builds
  alternatives to that stack. Moving the control path to first-party
  transport reduces that reliance. (mirai remains the execution driver
  either way; retiring nanonext as a transitive dependency is a
  separate decision about driver mainlining, out of scope here.)

nanonext remains the working control plane until the experiment
concludes; it is the scaffold, not necessarily the destination.

Steps:

- Define a transport-neutral Viento operation interface.
- Implement a gRPC adapter without changing WAL/fold semantics.
- Prototype registration and one bidirectional node-control stream.
- Preserve WAL-before-ack, logical operation ids, boot/session fencing,
  delivery confirmation, and reconciliation.
- **Decision criteria, fixed now while it is cheap.** Southbound
  (containerd adapter): capability — CRI speaks nothing else; the
  benchmark only establishes that overhead is *acceptable*. Control
  plane: standardness, cross-language legibility, and dogfooding value,
  judged with both adapters working. Latency is a criterion for
  neither: nanonext will very likely win R-to-R for Viento-shaped
  messages, and that outcome is not evidence against migrating.

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

This declines re-encoding the existing nanonext channel in place. It
does not conflict with the control-plane candidacy above: if the Viento
experiment leads to a gRPC control plane, the `.proto` contracts arrive
with the new adapter as new surfaces, which is exactly the sanctioned
path.

## Non-goals

- Replacing RProtoBuf
- Reimplementing Protocol Buffers
- Static vendoring of gRPC/abseil, or any Rust/tonic implementation
- CRAN submission; Windows and macOS support (revisit only on real
  outside demand)
- Retrofitting protobuf onto Viento's internal nanonext control plane
  (see encoding decision above)
- Putting Viento's WAL or domain policy into the transport package
- Claiming exactly-once RPC delivery
- Guaranteeing fork safety after a channel is open (the failure mode is
  documented instead)
- Beating nanonext on R-to-R latency
- Pivoting Viento before the package and benchmark evidence exist

## Open decisions

- Final package name and repository. With system-linked C++ committed,
  `gRPCpp` no longer risks naming an implementation that might change,
  but `grpc` is cleaner and unclaimed on CRAN (not that CRAN is the
  target). Decide before the first push.
- Generic dynamic API only versus optional generated R conveniences.
- Strict handling of unknown fields versus normal protobuf evolution.

Resolved: static versus system linking (system only; see platform
commitment), distribution channel (r-universe/drat plus apt/Docker),
event-loop and promises/mirai integration (increment 2), Rust/tonic
(dropped), protobuf-on-nanonext (declined; see encoding decision),
license (Apache-2.0, matching gRPC; RProtoBuf is GPL (>= 2) per CRAN
0.4.27, accepted as plumbing-level exposure), nanonext's role (today's
working control plane and mirai's substrate; gRPC is an explicit
control-plane candidate pending the Viento experiment — see that
section).
