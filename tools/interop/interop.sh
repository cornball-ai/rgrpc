#!/bin/sh
# Cross-implementation interop gate: this package against the reference
# C++ and Python gRPC implementations, both directions. The proof behind
# PLAN.md's "interoperate with official implementations" verification
# item -- i.e. that pinning to distro gRPC 1.51.1 costs nothing on the
# wire. Run from the package root against an installed grpc.
#
#   tools/interop/interop.sh
#
# Four legs, because one direction proves only half of it:
#   R client   -> Python server
#   Python cl. -> R server
#   R client   -> C++ server
#   C++ client -> R server
#
# Go is covered separately and for real by inst/tinytest/test_cri.R,
# which talks to containerd; a purpose-built Go echo peer would prove
# less than the production server already does.
#
# interop.proto is deliberately not a string echo. It carries a nested
# message, a repeated field, a proto3 map and a oneof, because those are
# where implementations actually diverge -- and the map in particular is
# what RProtoBuf represents differently (repeated entry messages) from
# the dict a Python peer sees.
#
# The C++ peers use the generic API (GenericStub / AsyncGenericService)
# because libgrpc++-dev ships no grpc_cpp_plugin. That is a codegen
# difference, not a wire difference: a generated stub wraps this same
# core, and the payloads still come from protoc-generated message
# classes. Python does use ordinary generated stubs, so the
# generated-stub path is covered on that side.
#
# A skipped leg is reported as SKIP and fails the run. A missing
# toolchain must not read as interop success -- that is the failure mode
# this script exists to rule out.
set -u

SP="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SP/../.." && pwd)"

if [ -n "${WORK:-}" ]; then
    mkdir -p "$WORK"
else
    WORK="$(mktemp -d)"
    trap 'rm -rf "$WORK"' EXIT
fi
GEN="$WORK/pygen"; mkdir -p "$GEN"
BLD="$WORK/cpp"; mkdir -p "$BLD"
export INTEROP_GEN="$GEN"

fails=0
legs=0
note() { echo "== $*"; }
leg_result() {  # <name> <rc> <outfile> <marker>
    legs=$((legs + 1))
    if [ "$2" -eq 0 ] && grep -q "$4" "$3"; then
        echo "LEG ok   $1"
    else
        echo "LEG FAIL $1 (rc=$2)"
        sed 's/^/    /' "$3"
        fails=$((fails + 1))
    fi
}
skip() { echo "LEG SKIP $1 -- $2"; fails=$((fails + 1)); legs=$((legs + 1)); }

wait_sock() {  # <path>
    i=0
    while [ ! -S "$1" ] && [ $i -lt 200 ]; do i=$((i + 1)); sleep 0.1; done
    [ -S "$1" ]
}

## ---- toolchains -------------------------------------------------
have_py=no
if command -v uv > /dev/null 2>&1; then
    if uv run --quiet --with grpcio-tools python -m grpc_tools.protoc \
        -I"$SP" --python_out="$GEN" --grpc_python_out="$GEN" \
        "$SP/interop.proto" > "$WORK/pygen.log" 2>&1; then
        have_py=yes
    else
        note "python stub generation failed:"; sed 's/^/    /' "$WORK/pygen.log"
    fi
fi

have_cpp=no
if pkg-config --exists grpc++ protobuf 2>/dev/null && command -v protoc > /dev/null 2>&1; then
    if protoc -I"$SP" --cpp_out="$BLD" "$SP/interop.proto" > "$WORK/cpp.log" 2>&1 &&
       g++ -std=c++17 -O1 -I"$BLD" -I"$SP" -o "$BLD/cpp_client" \
           "$SP/cpp_client.cc" "$BLD/interop.pb.cc" \
           $(pkg-config --cflags --libs grpc++ protobuf) >> "$WORK/cpp.log" 2>&1 &&
       g++ -std=c++17 -O1 -I"$BLD" -I"$SP" -o "$BLD/cpp_server" \
           "$SP/cpp_server.cc" "$BLD/interop.pb.cc" \
           $(pkg-config --cflags --libs grpc++ protobuf) >> "$WORK/cpp.log" 2>&1; then
        have_cpp=yes
    else
        note "C++ peer build failed:"; sed 's/^/    /' "$WORK/cpp.log"
    fi
fi

## ---- leg 1: R client -> Python server ---------------------------
if [ "$have_py" = yes ]; then
    S="$WORK/py.sock"
    uv run --quiet --with grpcio --with protobuf python "$SP/py_server.py" \
        "unix://$S" > "$WORK/l1-srv.out" 2>&1 &
    P=$!
    if wait_sock "$S"; then
        r "$SP/r_client.R" "unix://$S" "$SP" python > "$WORK/l1.out" 2>&1
        leg_result "R client   -> Python server" $? "$WORK/l1.out" "R_IOP all ok"
    else
        skip "R client   -> Python server" "python server never bound"
        cat "$WORK/l1-srv.out"
    fi
    kill $P 2>/dev/null; wait $P 2>/dev/null || true
else
    skip "R client   -> Python server" "uv or grpcio-tools unavailable"
fi

## ---- leg 2: Python client -> R server ---------------------------
if [ "$have_py" = yes ]; then
    S="$WORK/r1.sock"
    r "$SP/r_server.R" "unix:$S" "$SP" 60 > "$WORK/l2-srv.out" 2>&1 &
    P=$!
    if wait_sock "$S"; then
        uv run --quiet --with grpcio --with protobuf python "$SP/py_client.py" \
            "unix://$S" > "$WORK/l2.out" 2>&1
        leg_result "Python cl. -> R server    " $? "$WORK/l2.out" "PY all ok"
    else
        skip "Python cl. -> R server    " "R server never bound"
        cat "$WORK/l2-srv.out"
    fi
    kill $P 2>/dev/null; wait $P 2>/dev/null || true
else
    skip "Python cl. -> R server    " "uv or grpcio-tools unavailable"
fi

## ---- leg 3: R client -> C++ server ------------------------------
if [ "$have_cpp" = yes ]; then
    S="$WORK/cpp.sock"
    "$BLD/cpp_server" "unix://$S" > "$WORK/l3-srv.out" 2>&1 &
    P=$!
    if wait_sock "$S"; then
        r "$SP/r_client.R" "unix://$S" "$SP" cpp > "$WORK/l3.out" 2>&1
        leg_result "R client   -> C++ server  " $? "$WORK/l3.out" "R_IOP all ok"
    else
        skip "R client   -> C++ server  " "C++ server never bound"
        cat "$WORK/l3-srv.out"
    fi
    kill $P 2>/dev/null; wait $P 2>/dev/null || true
else
    skip "R client   -> C++ server  " "grpc++/protobuf dev or protoc unavailable"
fi

## ---- leg 4: C++ client -> R server ------------------------------
if [ "$have_cpp" = yes ]; then
    S="$WORK/r2.sock"
    r "$SP/r_server.R" "unix:$S" "$SP" 60 > "$WORK/l4-srv.out" 2>&1 &
    P=$!
    if wait_sock "$S"; then
        "$BLD/cpp_client" "unix://$S" > "$WORK/l4.out" 2>&1
        leg_result "C++ client -> R server    " $? "$WORK/l4.out" "CPP all ok"
    else
        skip "C++ client -> R server    " "R server never bound"
        cat "$WORK/l4-srv.out"
    fi
    kill $P 2>/dev/null; wait $P 2>/dev/null || true
else
    skip "C++ client -> R server    " "grpc++/protobuf dev or protoc unavailable"
fi

echo "-- $legs legs, $fails failed"
[ "$fails" -eq 0 ] || exit 1
exit 0
