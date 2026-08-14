# Vendored proto definitions

| Dir | Contents | Source | Version | License |
|---|---|---|---|---|
| `cri/` | Kubernetes CRI v1 (+ gogoproto, descriptor.proto imports) | see `cri/README.md` | v0.33.2 | Apache-2.0 / BSD-3 |
| `health/` | `grpc.health.v1` health checking | [grpc/grpc](https://github.com/grpc/grpc) `src/proto/grpc/health/v1` | v1.51.1 | Apache-2.0 |
| `reflection/` | `grpc.reflection.v1` server reflection | [grpc/grpc](https://github.com/grpc/grpc) `src/proto/grpc/reflection/v1` | v1.51.1 | Apache-2.0 |

All files are unmodified copies, loadable at runtime with
`RProtoBuf::readProtoFiles2(file, protoPath = dir)`.
