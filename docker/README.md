# Reference node image

A two-stage image for running R processes that use the grpc package:
the build stage compiles the package against `libgrpc++-dev`, the
runtime stage carries only `libgrpc++1.51t64` (the shared library) and
`r-cran-rprotobuf`.

## The base-image constraint

grpc links the distro's gRPC at its ABI; nothing is vendored. That
makes the pairing rule explicit:

**The image base must be the same distro release the package was built
on.** A package built on noble needs a noble userland (`libgrpc++1.51t64`,
`libprotobuf32t64`). Moving to a newer release means rebuilding the
package against that release's gRPC, not copying the installed library
over.

Both stages in the Dockerfile use `rocker/r2u:noble`, so the constraint
holds by construction. Reproducibility comes from pinned distro package
versions, not hermetic vendoring.

## Build

```sh
r -e 'tinypkgr::build()'   # writes to ~/.cache/R/tinypkgr/
cp ~/.cache/R/tinypkgr/grpc_*.tar.gz docker/
docker build -t cornball/grpc-node docker/
```

## Smoke test

```sh
docker run --rm cornball/grpc-node Rscript -e '
  library(grpc)
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
