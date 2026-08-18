#!/bin/sh
# Transport benchmark: this package against nanonext (the incumbent) and
# against a Go gRPC server (the reference implementation). The recipe
# behind PLAN.md's "Benchmark results" section. Run from the package root
# against an installed grpc.
#
#   tools/bench/bench.sh [size ...]     default: 256 4096 65536
#
# Three rows, and the difference between them is the whole point:
#
#   r-to-r     our client and our server, R at both ends. This is the
#              row to compare against nanonext, because it is the shape
#              vientito actually runs.
#   nanonext   R client, R server, same payloads, same statistics, same
#              microsecond timer. The incumbent.
#   r-to-go    our client against a Go server that is not the
#              bottleneck. This is not comparable to the other two; it
#              isolates what our client costs when the peer is fast.
#
# Two shapes, measured separately on purpose:
#
#   unary   sequential round trips, one outstanding. This is latency.
#   stream  pipelined send-and-drain. This is throughput.
#
# Measuring both from one loop gives a latency number that is really
# queueing delay and a throughput number that is really 1/latency.
#
# Everything is a loopback unix socket, so these are lower bounds on
# latency and upper bounds on throughput; a real network moves both and
# moves them differently for the two transports.
#
# Beware the timing traps this script already fell into, both of which
# produce a plausible wrong number rather than an error:
#   - proc.time() is quantised to 1ms on Linux, which reports a 50us
#     round trip as p50=0 and p99=1. Both clients use Sys.time().
#   - a blocking receive in the drain loop charges the benchmark its own
#     timeout. At block = 200 per 64-message burst that read as
#     308 msgs/s for a transport doing 70,000. Both drain non-blocking.
set -u

SP="$(cd "$(dirname "$0")" && pwd)"
SIZES="${*:-256 4096 65536}"
UNARY_N="${UNARY_N:-3000}"
STREAM_N="${STREAM_N:-20000}"

if [ -n "${WORK:-}" ]; then
    mkdir -p "$WORK"
else
    WORK="$(mktemp -d)"
    trap 'rm -rf "$WORK"' EXIT
fi

wait_sock() {
    i=0
    while [ ! -S "$1" ] && [ $i -lt 200 ]; do i=$((i + 1)); sleep 0.1; done
    [ -S "$1" ]
}

have_go=no
GOBIN="$WORK/go_echo_server"
if command -v go > /dev/null 2>&1; then
    mkdir -p "$WORK/go" && cp "$SP/go_echo_server.go" "$WORK/go/"
    printf 'module bench\n\ngo 1.22\n' > "$WORK/go/go.mod"
    if (cd "$WORK/go" && go mod tidy && go build -o "$GOBIN" .) \
        > "$WORK/go.log" 2>&1; then
        have_go=yes
    else
        echo "NOTE: Go peer unavailable, skipping the r-to-go row:"
        sed 's/^/    /' "$WORK/go.log"
    fi
fi

have_nng=no
if r -e 'quit(status = if (requireNamespace("nanonext", quietly = TRUE)) 0 else 1)' \
    > /dev/null 2>&1; then
    have_nng=yes
else
    echo "NOTE: nanonext not installed, skipping the nanonext row"
fi

run_grpc() {  # <label> <server-cmd-tag> <addr> <mode> <size> <n>
    label=$1; addr=$3; mode=$4; size=$5; n=$6
    r "$SP/bench_grpc.R" "$addr" "$mode" "$size" "$n" "$label" 2>&1 |
        grep -E '^BENCH' || echo "BENCH $label $mode size=$size FAILED"
}

for size in $SIZES; do
    ## ---- r-to-r ------------------------------------------------
    S="$WORK/rr-$size.sock"; rm -f "$S"
    r "$SP/grpc_echo_server.R" "unix:$S" 300 > "$WORK/rr-$size.out" 2>&1 &
    P=$!
    if wait_sock "$S"; then
        run_grpc r-to-r x "unix://$S" unary  "$size" "$UNARY_N"
        run_grpc r-to-r x "unix://$S" stream "$size" "$STREAM_N"
    else
        echo "BENCH r-to-r size=$size FAILED (server never bound)"
        cat "$WORK/rr-$size.out"
    fi
    kill $P 2>/dev/null; wait $P 2>/dev/null || true

    ## ---- nanonext ----------------------------------------------
    if [ "$have_nng" = yes ]; then
        U="$WORK/nng-rep-$size.sock"; rm -f "$U"
        r "$SP/nng_server.R" "ipc://$U" rep 300 > "$WORK/nngr-$size.out" 2>&1 &
        P=$!
        if wait_sock "$U"; then
            r "$SP/bench_nng.R" "ipc://$U" unary "$size" "$UNARY_N" nanonext \
                2>&1 | grep -E '^BENCH' ||
                echo "BENCH nanonext unary size=$size FAILED"
        else
            echo "BENCH nanonext unary size=$size FAILED (server never bound)"
        fi
        kill $P 2>/dev/null; wait $P 2>/dev/null || true

        U="$WORK/nng-pair-$size.sock"; rm -f "$U"
        r "$SP/nng_server.R" "ipc://$U" pair 300 > "$WORK/nngp-$size.out" 2>&1 &
        P=$!
        if wait_sock "$U"; then
            r "$SP/bench_nng.R" "ipc://$U" stream "$size" "$STREAM_N" nanonext \
                2>&1 | grep -E '^BENCH' ||
                echo "BENCH nanonext stream size=$size FAILED"
        else
            echo "BENCH nanonext stream size=$size FAILED (server never bound)"
        fi
        kill $P 2>/dev/null; wait $P 2>/dev/null || true
    fi

    ## ---- r-to-go -----------------------------------------------
    if [ "$have_go" = yes ]; then
        S="$WORK/go-$size.sock"; rm -f "$S"
        "$GOBIN" "unix:$S" > "$WORK/go-$size.out" 2>&1 &
        P=$!
        if wait_sock "$S"; then
            run_grpc r-to-go x "unix://$S" unary  "$size" "$UNARY_N"
            run_grpc r-to-go x "unix://$S" stream "$size" "$STREAM_N"
        else
            echo "BENCH r-to-go size=$size FAILED (server never bound)"
            cat "$WORK/go-$size.out"
        fi
        kill $P 2>/dev/null; wait $P 2>/dev/null || true
    fi
    echo "--"
done
exit 0
