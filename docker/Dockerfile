# Reference node image for R processes using the grpc package.
#
# The one constraint that matters: the base image release must match the
# release the package was built on. grpc links the distro's libgrpc++ at
# its ABI (noble: libgrpc++1.51t64), with no vendoring, so a binary
# built on noble loads only on a noble userland. Both stages below use
# the same base, which settles the pairing by construction.
#
# Build context: this directory, with the package tarball copied in.
#   r -e 'tinypkgr::build()'   # writes to ~/.cache/R/tinypkgr/
#   cp ~/.cache/R/tinypkgr/grpc_*.tar.gz .
#   docker build -t cornball/grpc-node .

FROM rocker/r2u:noble AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libgrpc++-dev libprotobuf-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
COPY grpc_*.tar.gz /tmp/
RUN mkdir -p /opt/rlib && R CMD INSTALL --library=/opt/rlib /tmp/grpc_*.tar.gz

FROM rocker/r2u:noble
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libgrpc++1.51t64 r-cran-rprotobuf \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /opt/rlib/grpc /usr/local/lib/R/site-library/grpc
