# Reference node image

A two-stage image for running R processes that use the rgrpc package:
the build stage compiles the package against `libgrpc++-dev`, the
runtime stage carries only `libgrpc++1.51t64` (the shared library) and
`r-cran-rprotobuf`.

## The base-image constraint

rgrpc links the distro's gRPC at its ABI; nothing is vendored. That
makes the pairing rule explicit:

**The image base must be the same distro release the package was built
on.** A package built on noble needs a noble userland (`libgrpc++1.51t64`,
`libprotobuf32t64`). Moving to a newer release means rebuilding the
package against that release's gRPC, not copying the installed library
over.

Both stages in the Dockerfile use `rocker/r2u:noble`, so the constraint
holds by construction.

What this guarantees is release/ABI pairing, not a bit-reproducible
image: `rocker/r2u:noble` is a mutable tag and apt package versions
track noble's archive. If you need a frozen image, pin the base by
digest (`rocker/r2u@sha256:...`) and pin apt versions
(`libgrpc++1.51t64=1.51.1-4.1build5`).

## Build

The tarball goes in under a fixed name — one artifact, explicitly
chosen, rather than a wildcard that would install every cached version
in glob order:

```sh
r -e 'tinypkgr::build()'   # writes to ~/.cache/R/tinypkgr/
cp ~/.cache/R/tinypkgr/rgrpc_<version>.tar.gz docker/rgrpc.tar.gz
docker build -t cornball/rgrpc-node docker/
```

## Smoke test

```sh
docker run --rm cornball/rgrpc-node Rscript -e '
  library(rgrpc)
  srv <- grpc_server()
  cl <- grpc_client(sprintf("127.0.0.1:%d", grpc_server_port(srv)))
  call <- grpc_call(cl, "/demo.Echo/Say", as.raw(1:4), deadline_ms = 5000)
  repeat {
    evs <- grpc_poll(srv, timeout_ms = 200L)
    if (length(evs)) { grpc_reply(evs[[1]], as.raw(9)); break }
  }
  repeat {
    evs <- grpc_poll(cl, timeout_ms = 200L)
    if (length(evs)) { stopifnot(evs[[1]]$status == 0L); break }
  }
  cat("round trip ok\n")'
```
