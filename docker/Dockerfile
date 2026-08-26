# Reference node image for R processes using the rgrpc package.
#
# The one constraint that matters: the base image release must match the
# release the package was built on. rgrpc links the distro's libgrpc++ at
# its ABI (noble: libgrpc++1.51t64), with no vendoring, so a binary
# built on noble loads only on a noble userland. Both stages below use
# the same base, which settles the pairing by construction.
#
# Build context: this directory, with exactly one tarball under a fixed
# name (a wildcard COPY would install every cached version in glob
# order; the fixed name makes the artifact explicit).
#   r -e 'tinypkgr::build()'   # writes to ~/.cache/R/tinypkgr/
#   cp ~/.cache/R/tinypkgr/rgrpc_<version>.tar.gz rgrpc.tar.gz
#   docker build -t cornball/rgrpc-node .

FROM rocker/r2u:noble AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libgrpc++-dev libprotobuf-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
COPY rgrpc.tar.gz /tmp/rgrpc.tar.gz
RUN mkdir -p /opt/rlib && R CMD INSTALL --library=/opt/rlib /tmp/rgrpc.tar.gz

FROM rocker/r2u:noble
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libgrpc++1.51t64 r-cran-rprotobuf \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /opt/rlib/rgrpc /usr/local/lib/R/site-library/rgrpc
