Candidate recipes for R-macos/recipes (https://github.com/R-macos/recipes),
so CRAN's macOS builders can link rgrpc against a static gRPC C++.

- `c-ares`, `re2`: new recipes gRPC needs.
- `grpc-1.51.1`, `grpc-1.48.4`: two candidate gRPC versions; the
  workflow copies one to `recipes/grpc`. Both build against the
  existing `absl` (20250127.1) and `protobuf` (3.20.2) recipes, with
  protobuf found through CMake's FindProtobuf module
  (`gRPC_PROTOBUF_PACKAGE_TYPE=MODULE`) because the protobuf recipe is
  an autotools build.

`grpc-1.48.4.patch` (installed as `recipes/grpc.patch`) backports the
fix for CVE-2023-32732, upstream commit 29d8beee0ac2 (PR #32309), as
Fedora ships it for 1.48.4
(https://src.fedoraproject.org/rpms/grpc, file
0001-http2-Dont-drop-connections-on-metadata-limit-exceed.patch).
1.48.4 remains affected by CVE-2023-33953, CVE-2023-4785,
CVE-2023-44487, CVE-2024-7246 and the gRPC advisory of 2026-08-28
(heap exhaustion, fixed in 1.82.2 and 1.83.1), none of which Fedora
backports.

`.github/workflows/macos-recipes.yaml` builds them with the recipes'
own `build.sh` on arm64 and x86_64, then checks rgrpc against
`/opt/R/$arch`. Not part of the package; `tools/` is in .Rbuildignore.
