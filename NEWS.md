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
