## Resubmission

Fixes the check failures CRAN reported for 0.1.1 (deadline 2026-10-19).

### Installation ERROR on r-release/r-oldrel macOS (4 flavors)

The macOS build machines have no gRPC C++ library (the recipes at
mac.r-project.org carry protobuf but not gRPC or abseil), so
`configure` stopped. It now installs a stub build instead: every
native function errors with a clear message, the new exported
`grpc_available()` returns FALSE, examples are wrapped in
`if (grpc_available())`, and the tests skip. Linux and Windows are
unchanged.

### Installation ERROR on r-devel-linux-x86_64-fedora-clang and clang-ASAN

These flavors compile with libc++, while the only gRPC available is
Fedora's, built for libstdc++. The shared object linked, then failed to
load (`undefined symbol: grpc::CreateChannel(std::__1::basic_string...)`).
`configure` now links a test executable with R's own C++ compiler and
flags. That link fails the same way on a libc++ toolchain, so the
package takes the stub path above. Reproduced and verified with clang
18 and `-stdlib=libc++` against Ubuntu's libstdc++ gRPC 1.51.1.

### valgrind additional issue

The valgrind reports contain no errors in this package's code:

- 2 "Conditional jump or move depends on uninitialised value(s)"
  contexts, repeated, inside abseil's `DebugOnlyDeadlockCheck` stack
  unwinder (`absl::GetStackTrace`), reached from `grpc_init()`. The
  unwinder walks frame pointers through this package's
  `grpc_r_server2_create` frame, which is why valgrind names it as the
  origin of the stack allocation. Fedora's abseil has deadlock detection
  enabled.
- "possibly lost" records only (0 bytes definitely or indirectly lost).
  All of them are in protobuf's `DescriptorPool` or `Message` cloning
  called from RProtoBuf (`readProtoFiles`, `getMessageField`), which the
  schema examples and tests use.

## Test environments

- Ubuntu 24.04, R 4.6.1, gRPC 1.51.1 (local, `--as-cran`): real build
- Ubuntu 24.04 in Docker, clang 18 with libc++, R 4.6.1: stub build
- GitHub Actions: Ubuntu (system gRPC), macOS (Homebrew gRPC), and
  Ubuntu without gRPC (stub build)
- win-builder: TODO

## R CMD check results

Real build (Ubuntu, `--as-cran`): 0 errors | 0 warnings | 1 note. The
note is the Ubuntu toolchain's `-mno-omit-leaf-frame-pointer`, which
CRAN's own machines do not emit.

Stub build (libc++, Docker): Status OK.

## Downstream dependencies

None on CRAN.
