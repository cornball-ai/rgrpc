## Test environments

- Ubuntu 24.04 (noble), R 4.6.1, gRPC 1.51.1 (local, `--as-cran`)
- Debian sid (rocker/r-base), R 4.6.1, gRPC 1.51.1 (container, `--as-cran`)
- Fedora 44, R 4.6.1, gRPC 1.48.4 (container, `--as-cran`)
- win-builder: R-release 4.6.1 and R-devel (2026-08-24 r90445), Rtools gRPC
- macOS (GitHub Actions macos-latest), Homebrew gRPC 1.83.0, full test suite
- r-universe: Linux x86_64 and aarch64, R 4.6.1 and R-devel 4.7.0

## R CMD check results

0 errors | 0 warnings | 1 note

- New submission.

## SystemRequirements

The package links the system gRPC C++ library, found via
`pkg-config grpc++ protobuf` (Debian/Ubuntu: libgrpc++-dev,
libprotobuf-dev; Fedora: grpc-devel, protobuf-devel; Windows: bundled
with Rtools 4.3 and later; macOS: Homebrew grpc). The environments
above cover gRPC 1.48 through 1.83.

## Downstream dependencies

None; this is a new package.
