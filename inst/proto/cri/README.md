# Vendored CRI proto definitions

A complete, self-contained import root for loading the Kubernetes
Container Runtime Interface at runtime with
`RProtoBuf::readProtoFiles2("api.proto", protoPath = this_dir)`.

| File | Source | Version | License |
|---|---|---|---|
| `api.proto` | [kubernetes/cri-api](https://github.com/kubernetes/cri-api) `pkg/apis/runtime/v1/api.proto` | v0.33.2 | Apache-2.0 |
| `github.com/gogo/protobuf/gogoproto/gogo.proto` | [gogo/protobuf](https://github.com/gogo/protobuf) | v1.3.2 | BSD-3-Clause |
| `google/protobuf/descriptor.proto` | protobuf (Ubuntu `libprotobuf-dev`) | 3.21.12 | BSD-3-Clause |

`api.proto` imports gogoproto annotations, which import
`descriptor.proto`; all three must sit under one import root in these
relative locations. Files are unmodified copies.
