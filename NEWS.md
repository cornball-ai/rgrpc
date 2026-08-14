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
