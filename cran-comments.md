## Resubmission

Resubmission of 0.1.0, as 0.1.1, after CRAN review:

- DESCRIPTION links the 'gRPC' project <https://grpc.io/> and its C++
  API reference <https://grpc.github.io/grpc/cpp/> in angle brackets.
- Every exported function has a \value section giving the class and
  meaning of its result. The seven called for their side effects
  (grpc_cancel, grpc_close, grpc_finish, grpc_read, grpc_reply,
  grpc_send, grpc_writes_done) document the invisible logical or NULL
  they return.
- The \dontrun{} examples are unwrapped and run under R CMD check: each
  drives a client and a server in the same process over the loopback
  interface and finishes in under 0.3 s. The schema examples
  (grpc_service, grpc_method, grpc_decode) are guarded by
  requireNamespace("RProtoBuf"). One \dontrun{} remains, in grpc_tls():
  it needs certificate files, which an example cannot produce portably;
  that code path is exercised by the package's TLS tests.

## Test environments

- Ubuntu 24.04 (noble), R 4.6.1, gRPC 1.51.1 (local, `--as-cran`)
- win-builder: R-release 4.6.1 and R-devel (2026-09-08 r90509), examples
  and tests run, 1 NOTE (new submission)
- 0.1.0 was additionally checked on Debian sid, Fedora 44 (gRPC 1.48.4),
  macOS (Homebrew gRPC 1.83.0), and r-universe; 0.1.1 changes only
  documentation and DESCRIPTION.

## R CMD check results

0 errors | 0 warnings | 1 note

- New submission.

The local check also notes the Ubuntu toolchain's non-portable compiler
flags, which CRAN's own machines do not emit.

## SystemRequirements

The package links the system gRPC C++ library, found via
`pkg-config grpc++ protobuf` (Debian/Ubuntu: libgrpc++-dev,
libprotobuf-dev; Fedora: grpc-devel, protobuf-devel; Windows: bundled
with Rtools 4.3 and later; macOS: Homebrew grpc). The environments
above cover gRPC 1.48 through 1.83.

## Downstream dependencies

None; this is a new package.
