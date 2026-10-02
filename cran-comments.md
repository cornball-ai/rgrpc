## Resubmission

Fixes the check failures CRAN reported for 0.1.1 (deadline 2026-10-19).

### Installation ERROR on r-release/r-oldrel macOS (4 flavors)

The macOS build machines have no gRPC C++ library (the recipes at
mac.r-project.org carry abseil and protobuf but not gRPC), so
`configure` stopped. It now installs a stub build instead: every
native function errors with a clear message, the new exported
`grpc_available()` returns FALSE, examples are wrapped in
`if (grpc_available())`, and the tests skip. Linux and Windows are
unchanged.

As Prof. Ripley asked, I have proposed recipes for gRPC 1.48.4, re2
and c-ares (https://github.com/R-macos/recipes/pull/93) and let Simon
Urbanek know. With them, rgrpc passes R CMD check on arm64 and x86_64
against `/opt/R/$arch`; until they are on the builders, the macOS
binary is the stub.

### SystemRequirements

Now names the Fedora (grpc-cpp, grpc-devel, protobuf-devel) and
Homebrew packages as well as the Debian ones.

### Installation ERROR on r-devel-linux-x86_64-fedora-clang and clang-ASAN

These flavors compile with libc++, while the only gRPC available is
Fedora's, built for libstdc++. The shared object linked, then failed to
load (`undefined symbol: grpc::CreateChannel(std::__1::basic_string...)`).
`configure` now links a test executable with R's own C++ compiler and
flags. That link fails the same way on a libc++ toolchain, so the
package takes the stub path above. Reproduced and verified with clang
18 and `-stdlib=libc++` against Ubuntu's libstdc++ gRPC 1.51.1.

### valgrind additional issue

Fixed. The 2 "Conditional jump or move depends on uninitialised
value(s)" contexts came from abseil's debug deadlock bookkeeping (on in
Fedora's abseil build), whose frame-pointer stack walk read the stack
through this package's frames, compiled without frame pointers. The
package brought gRPC up and down with every client and server, so every
start repeated the walk. It now initializes gRPC once, when the DLL
loads, and shuts it down at unload or R exit.

Reproduced on Fedora 44 (gRPC 1.48.4, abseil 20260107.1, valgrind
3.27.1) with R 4.6.1 built from source with the memtests config.site
flags and `--with-valgrind-instrumentation=2`: 0.1.1 gives 128 and 8
errors from those 2 contexts in examples and tests; this version gives
0 errors and 0 bytes definitely or possibly lost in both.

The remaining "possibly lost" records in the CRAN log are protobuf
`DescriptorPool` and `Message` allocations made by RProtoBuf
(`readProtoFiles`, `getMessageField`), which the schema examples and
tests call; RProtoBuf keeps its descriptor pool for the life of the
process.

## Test environments

- Ubuntu 24.04, R 4.6.1, gRPC 1.51.1 (local, `--as-cran`): real build
- Ubuntu 24.04 in Docker, clang 18 with libc++, R 4.6.1: stub build
- Fedora 44 in Docker, R 4.6.1 built with the memtests valgrind
  config.site, `--use-valgrind`: 0 valgrind errors
- GitHub Actions: Ubuntu (system gRPC), macOS (Homebrew gRPC), and
  Ubuntu without gRPC (stub build)
- GitHub Actions, macOS arm64 and x86_64, CRAN's R 4.6.1 against the
  proposed recipes in `/opt/R/$arch`: Status OK
- win-builder, R-devel (2026-09-30 r90605) and R 4.6.1, linking
  Rtools' gRPC: Status OK on both

## R CMD check results

Real build (Ubuntu, `--as-cran`): 0 errors | 0 warnings | 1 note. The
note is the Ubuntu toolchain's `-mno-omit-leaf-frame-pointer`, which
CRAN's own machines do not emit.

Stub build (libc++, Docker): Status OK.

## Downstream dependencies

None on CRAN.
