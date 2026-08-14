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
