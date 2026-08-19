#!/bin/sh
# CPU and memory of the server during a streaming run. Companion to
# tools/bench/bench.sh, which measures how fast things go but not what it
# costs to go that fast.
#
#   tools/bench/resource-probe.sh [size] [n] [runs]
#
# Why this exists: bench.sh showed the R server reaching a third of the
# throughput a Go server does, and the conclusion drawn from it -- "the R
# server is the bottleneck" -- was an inference from a controlled swap
# rather than a measurement of saturation. A throughput number cannot
# tell you whether a server is out of CPU or merely waiting. This
# measures the resource directly.
#
# What it reports:
#   cores    CPU seconds burned / wall seconds elapsed, per thread. The R
#            main thread and gRPC's completion thread imply different
#            fixes, so they are reported separately: a busy R thread
#            means "do less per message in R", a busy completion thread
#            means the transport itself is the cost.
#   rss      VmRSS across `runs` consecutive streams against ONE server.
#            A single run cannot distinguish a leak from an allocator
#            high-water mark; only repetition can, and the shape of the
#            sequence is the answer -- climbing then flat is a plateau,
#            climbing linearly is a leak.
#
# VmHWM rather than a final VmRSS sample for the peak, because a spike
# that has already been freed would otherwise go unseen.
set -u

SP="$(cd "$(dirname "$0")" && pwd)"
SIZE="${1:-65536}"; N="${2:-20000}"; RUNS="${3:-6}"

if [ -n "${WORK:-}" ]; then
    mkdir -p "$WORK"
else
    WORK="$(mktemp -d)"
    trap 'rm -rf "$WORK"' EXIT
fi
TICK=$(getconf CLK_TCK)
S="$WORK/res.sock"

r "$SP/grpc_echo_server.R" "unix:$S" 600 > "$WORK/srv.out" 2>&1 &
RPID=$!
i=0
while [ ! -S "$S" ] && [ $i -lt 200 ]; do i=$((i + 1)); sleep 0.1; done
if [ ! -S "$S" ]; then
    echo "FATAL: server never bound"; cat "$WORK/srv.out"
    kill $RPID 2>/dev/null; exit 1
fi
# Sampling a pid that is not there returns nothing and reads as zero
# cost, so assert the precondition rather than trusting it.
if [ ! -r "/proc/$RPID/status" ]; then
    echo "FATAL: no /proc/$RPID"; kill $RPID 2>/dev/null; exit 1
fi

rss_kb() { awk '$1 == "VmRSS:" {print $2}' "/proc/$RPID/status"; }
hwm_kb() { awk '$1 == "VmHWM:" {print $2}' "/proc/$RPID/status"; }
threads() {
    for t in /proc/$RPID/task/*; do
        [ -r "$t/stat" ] || continue
        awk -v tid="$(basename "$t")" -v tick="$TICK" \
            '{print tid, ($14 + $15) / tick}' "$t/stat"
    done
}

echo "RSS run0 idle $(( $(rss_kb) / 1024 ))MB"
threads > "$WORK/t0"
w0=$(date +%s.%N)

run=1
while [ "$run" -le "$RUNS" ]; do
    r "$SP/bench_grpc.R" "unix://$S" stream "$SIZE" "$N" resource \
        > "$WORK/b$run.out" 2>&1
    rate=$(grep -o 'msgs_per_s=[0-9]*' "$WORK/b$run.out" | head -1)
    echo "RSS run$run $(( $(rss_kb) / 1024 ))MB $rate"
    if [ "$run" -eq 1 ]; then
        w1=$(date +%s.%N); threads > "$WORK/t1"
    fi
    run=$((run + 1))
done

echo "RSS peak $(( $(hwm_kb) / 1024 ))MB"

# Per-thread CPU over the first run only: later runs would average the
# gaps between them into the figure.
awk -v pid="$RPID" -v w0="$w0" -v w1="$w1" '
  NR == FNR { b[$1] = $2; next }
  { d = $2 - ($1 in b ? b[$1] : 0)
    if (d <= 0.001) next
    printf "CPU %-16s tid=%s cpu_s=%.3f cores=%.2f\n",
      ($1 == pid ? "R-main" : "grpc-thread"), $1, d, d / (w1 - w0) }
' "$WORK/t0" "$WORK/t1"

kill $RPID 2>/dev/null; wait $RPID 2>/dev/null || true
exit 0
