# gRPC recipes for R-macos/recipes

Candidate recipes for [R-macos/recipes](https://github.com/R-macos/recipes)
so CRAN's macOS builders can link the CRAN package rgrpc against a static
gRPC C++. Not part of the package; `tools/` is in `.Rbuildignore`.

## What's here

| File | Recipe |
|---|---|
| `c-ares` | c-ares 1.34.5 (new) |
| `re2` | re2 2022-06-01 (new; the last release that does not need abseil) |
| `grpc-1.48.4` | gRPC 1.48.4 (new), the proposed version; becomes `recipes/grpc` |
| `grpc-1.48.4.patch` | security fix for 1.48.4; becomes `recipes/grpc.patch` |
| `grpc-1.51.1` | gRPC 1.51.1, not proposed: it needs a patch to build (below) |

gRPC builds against the existing `absl` (20250127.1), `protobuf`
(3.20.2), `openssl` and `zlib-stub` recipes. Nothing already in the
recipes changes.

## What rgrpc links

grpc++, grpc, gpr, address_sorting, upb, re2, c-ares, abseil, openssl and
zlib, plus the CoreFoundation framework (abseil's time zone code needs
it). rgrpc does not link protobuf: messages cross into C++ as opaque
bytes, and RProtoBuf serializes them on the R side. gRPC's CMake build
only needs to find a protobuf, which it does through CMake's FindProtobuf
module (`gRPC_PROTOBUF_PACKAGE_TYPE=MODULE`) because the protobuf recipe
is an autotools build.

## How it was tested

`.github/workflows/macos-recipes.yaml`, on GitHub's `macos-14` (arm64)
and `macos-15-intel` runners:

1. `/usr/local` and Homebrew moved away, as in the recipes' `cook.yml`.
2. R-macos/recipes at commit 31e34aa, with these files added, built
   with its own `build.sh grpc`.
3. CRAN's R 4.6.1 installer (sonoma-arm64, big-sur-x86_64).
4. `R CMD check` of rgrpc against `/opt/R/$arch`: Status: OK on both.
   The resulting `rgrpc.so` links only libz, libc++, libSystem,
   CoreFoundation and libR.
5. rgrpc's full test suite, including client/server round trips,
   streaming and TLS: 806 results, all pass on both (RProtoBuf is not
   installed on the runners, so its tests are skipped).

Latest run:
https://github.com/cornball-ai/rgrpc/actions/runs/36775046660

## Why gRPC 1.48.4

It builds against the current absl recipe as released. gRPC 1.51.1
fails on `src/core/lib/iomgr/tcp_posix.cc`, which uses `absl::StrCat`
without including `absl/strings/str_cat.h`; newer abseil no longer
provides that header indirectly. 1.48.4 is also the version Fedora 44
ships, built there against abseil 20260107.

## Security

`grpc-1.48.4.patch` fixes CVE-2023-32732. It is upstream commit
29d8beee0ac2 (PR #32309), adapted to 1.48.4's older error API
(`GRPC_ERROR_CREATE_FROM_STATIC_STRING` and `GRPC_ERROR_INT_*` in place
of `GRPC_ERROR_CREATE` and `StatusIntProperty`); the fix logic is
unchanged. The adaptation is the one Fedora ships for 1.48.4
(https://src.fedoraproject.org/rpms/grpc, file
`0001-http2-Dont-drop-connections-on-metadata-limit-exceed.patch`).

1.48.4 remains affected by CVE-2023-33953, CVE-2023-4785,
CVE-2023-44487, CVE-2024-7246 and gRPC's advisory GHSA-hf3w-6hpw-qp67
(2026-08-28, heap exhaustion). Their upstream fixes do not apply to
1.48.4: they depend on code gRPC rewrote after 1.48 (the HPACK parser,
the HTTP/2 transport, EventEngine). Debian (security tracker, all
releases) and Fedora carry the same set unpatched.

Every one of them is fixed in gRPC 1.82.2 and 1.83.1, which need abseil
20250512.1 and protobuf 35. That means upgrading the absl and protobuf
recipes, which affects RProtoBuf and other packages that use them; it
can be coordinated with RProtoBuf's maintainer if preferred.

## The autobrew alternative

bigrquerystorage gets gRPC on CRAN's Macs by downloading the prebuilt
autobrew bundle `grpc-1.51.1-universal`
(https://github.com/autobrew/bundler/releases/tag/grpc-1.51.1). rgrpc
could do the same. That bundle is gRPC 1.51.1, which predates the
CVE-2023-32732 fix, so it carries all of the CVEs above.
