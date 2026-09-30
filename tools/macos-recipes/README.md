Candidate recipes for R-macos/recipes (https://github.com/R-macos/recipes),
so CRAN's macOS builders can link rgrpc against a static gRPC C++.

- `c-ares`, `re2`: new recipes gRPC needs.
- `grpc-1.51.1`, `grpc-1.48.4`: two candidate gRPC versions; the
  workflow copies one to `recipes/grpc`. Both build against the
  existing `absl` (20250127.1) and `protobuf` (3.20.2) recipes, with
  protobuf found through CMake's FindProtobuf module
  (`gRPC_PROTOBUF_PACKAGE_TYPE=MODULE`) because the protobuf recipe is
  an autotools build.

`.github/workflows/macos-recipes.yaml` builds them with the recipes'
own `build.sh` on arm64 and x86_64, then checks rgrpc against
`/opt/R/$arch`. Not part of the package; `tools/` is in .Rbuildignore.
