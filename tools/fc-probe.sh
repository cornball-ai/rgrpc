#!/bin/sh
# Flow-control probe: what a subscriber that stops reading actually
# absorbs, and how long a refused grpc_send() stays refused. The recipe
# behind the numbers recorded in PLAN.md ("Flow control and what a
# refused send means"). Run from the package root against an installed
# grpc.
#
#   tools/fc-probe.sh ceiling  <size> <n> [settle_s] [delay_us]
#   tools/fc-probe.sh duration <size> <n> <drain|stuck> [settle_s]
#
# WHAT THE TWO MODES ARE FOR
#
# grpc_send() returns FALSE for two unrelated reasons that are
# indistinguishable at the call site:
#
#   a) the server's own write queue is full. Only one write is in flight
#      per call (pump_locked in src/server.cpp), so a fast R loop fills
#      the queue and is refused while the peer is perfectly healthy.
#      Clears in single-digit milliseconds.
#   b) the peer stopped reading and its flow-control window is shut.
#      Does not clear.
#
# `ceiling` retries until nothing has moved for settle_s, so it measures
# past (a) to (b). `duration` records how long each refusal episode
# lasts, which is what separates them.
#
# Invocations that reproduce the recorded results:
#   tools/fc-probe.sh ceiling 16384 200000
#       ~7 MB absorbed; note first_refusal_at is around 16, not the
#       ceiling -- that is (a), and it is why a first-refusal measurement
#       reports a number several times too small
#   tools/fc-probe.sh ceiling 16384 200000 2 500
#       the same ceiling, but paced: first_refusal_at now equals the
#       ceiling because the send loop no longer outruns the queue
#   tools/fc-probe.sh duration 65536 4000 drain
#       a healthy subscriber: >100 refusal episodes, max under 5ms, none
#       over 10ms
#   tools/fc-probe.sh duration 65536 4000 stuck
#       the same transient episodes, then one that never clears
#
# HOW TO READ THE CEILING NUMBERS
#
# They are observations of this transport and configuration, not a
# property of gRPC. They depend on the settle_s cutoff (a peer draining
# more slowly than settle_s reads as stalled), on the loopback unix
# socket, on gRPC's BDP window tuning, and on message size. Treat them
# as "what this setup absorbed", and re-measure rather than porting the
# figure to another deployment.
#
# CHANNEL-ARG EXPERIMENTS ARE NOT REPRODUCIBLE FROM A CLEAN CHECKOUT
#
# PLAN.md also records what happens with HTTP/2 BDP probing disabled and
# the stream lookahead pinned. The package does not expose those channel
# args, so those runs used a throwaway diagnostic build. To repeat them,
# add to src/client.cpp before CreateCustomChannel:
#
#     if (const char *e = getenv("GRPC_R_BDP_PROBE"))
#         args.SetInt(GRPC_ARG_HTTP2_BDP_PROBE, atoi(e));
#     if (const char *e = getenv("GRPC_R_LOOKAHEAD"))
#         args.SetInt(GRPC_ARG_HTTP2_STREAM_LOOKAHEAD_BYTES, atoi(e));
#
# and, to vary the server write queue, make sv_call::write_cap in
# src/server.cpp read getenv("GRPC_R_WRITE_CAP"). Install to a scratch
# library and point R_LIBS at it -- not littler's -L, which silently
# fails to prepend and would measure the stock build while reporting the
# patched one. The WHICH line in the output names the .so actually
# loaded; check it.
set -u

SP="$(cd "$(dirname "$0")" && pwd)"
if [ $# -lt 3 ]; then sed -n '2,30p' "$0"; exit 2; fi
MODE="$1"; SIZE="$2"; N="$3"

case "$MODE" in
    ceiling)
        SETTLE="${4:-2}"; DELAY="${5:-0}"; SUBMODE="stuck" ;;
    duration)
        SUBMODE="${4:-drain}"; SETTLE="${5:-3}"; DELAY=0 ;;
    *)
        echo "unknown mode: $MODE (want ceiling or duration)"; exit 2 ;;
esac

## Only ever remove a directory this script created, matching
## tools/soak-fanout.sh: an inherited WORK is the caller's and must
## survive.
if [ -n "${WORK:-}" ]; then
    mkdir -p "$WORK"
else
    WORK="$(mktemp -d)"
    trap 'rm -rf "$WORK"' EXIT
fi
SOCK="$WORK/fc.sock"

r "$SP/fc-server.R" "$SOCK" "$MODE" "$SIZE" "$N" "$SETTLE" "$DELAY" \
    > "$WORK/srv.out" 2>&1 &
SRVPID=$!

i=0
while [ ! -S "$SOCK" ] && [ $i -lt 100 ]; do i=$((i+1)); sleep 0.1; done
if [ ! -S "$SOCK" ]; then
    echo "FATAL: server never bound"; cat "$WORK/srv.out"
    kill $SRVPID 2>/dev/null; exit 1
fi

r "$SP/fc-sub.R" "$SOCK" 64 120 "$SUBMODE" > "$WORK/sub.out" 2>&1 &
SUBPID=$!
wait $SRVPID 2>/dev/null
kill $SUBPID 2>/dev/null

echo "--- mode=$MODE size=$SIZE n=$N sub=$SUBMODE settle_s=$SETTLE delay_us=$DELAY"
grep -E "^WHICH" "$WORK/sub.out" || echo "WHICH unknown"
if ! grep -qE "^(FC|DUR)" "$WORK/srv.out"; then
    echo "FATAL: server produced no results"; cat "$WORK/srv.out"; exit 1
fi
grep -E "^(FC|DUR|ABORT)" "$WORK/srv.out"
