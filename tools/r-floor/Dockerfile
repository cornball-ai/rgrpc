# Does the package build and pass on Ubuntu noble's own R?
#
# DESCRIPTION declares a minimum R version. This checks it against the R
# the target platform actually ships, rather than against whatever the
# developer happens to be running. Noble ships R 4.3.x while r2u tracks
# current R, so both exist in the wild on the same distro -- and CI
# runners land on either, which is how a too-high floor shows up as an
# intermittent failure rather than an obvious one.
#
# Noble's R against noble's libgrpc++, which is the ABI pairing the
# platform commitment requires anyway.
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        r-base-dev libgrpc++-dev libprotobuf-dev pkgconf \
        r-cran-rprotobuf r-cran-tinytest ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY pkg /pkg
WORKDIR /pkg
