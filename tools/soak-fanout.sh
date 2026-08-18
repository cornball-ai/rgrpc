#!/bin/sh
# Fan-out soak for the server: N subscription streams held open, events
# pushed to all of them, optionally with one subscriber that never drains.
# The exact recipe behind the numbers recorded in PLAN.md ("Fan-out soak
# results"). Run from the package root against an installed grpc.
#
#   tools/soak-fanout.sh <nfast> <nclients> <slow|noslow> \
#                        <nevents> <size> <keep|fence|fenceK> \
#                        <accept_window> [kfence]
#
# What the output means:
#   sends_per_s          cost of the R fan-out loop; ~flat in subscriber
#                        count, so the room's event rate is sends_per_s/N
#   refused_slow/fast    which subscribers grpc_send() refused. The whole
#                        point: under load healthy subscribers refuse too,
#                        so a refusal is not a verdict that one is stuck
#   fenced_slow/fast     what a policy actually dropped. fenced_fast > 0
#                        means the policy killed healthy subscribers
#   spread_max_minus_min per-subscriber delivery skew; 0 means no
#                        subscriber was starved relative to its peers
#
# Reference results (2026-08-18, this machine, unix socket, 49 fast + 1
# non-draining subscriber) are in PLAN.md. The headline: `fence` on a
# single refusal is correct at 16KB and empties the room at 64KB;
# `fenceK` on sustained refusal is correct in both.
set -u

SP="$(cd "$(dirname "$0")" && pwd)"
if [ $# -lt 7 ]; then sed -n '2,30p' "$0"; exit 2; fi
NFAST="$1"; NCLI="$2"; SLOW="$3"; NEV="$4"; SIZE="$5"; POLICY="$6"; AW="$7"
KFENCE="${8:-20}"

WORK="${WORK:-$(mktemp -d)}"
trap 'rm -rf "$WORK"' EXIT

if [ "$SLOW" = "slow" ]; then NSUB=$((NFAST + 1)); else NSUB="$NFAST"; fi
SOCK="$WORK/room.sock"

r "$SP/soak-server.R" "$SOCK" "$NSUB" "$NEV" "$SIZE" "$POLICY" "$AW" "$KFENCE" \
    > "$WORK/srv.out" 2>&1 &
SRVPID=$!

i=0
while [ ! -S "$SOCK" ] && [ $i -lt 100 ]; do i=$((i+1)); sleep 0.1; done
if [ ! -S "$SOCK" ]; then
    echo "FATAL: server never bound"; cat "$WORK/srv.out"
    kill $SRVPID 2>/dev/null; exit 1
fi

r "$SP/soak-subs.R" "$SOCK" "$NFAST" "$NCLI" "$SLOW" > "$WORK/sub.out" 2>&1
wait $SRVPID 2>/dev/null

echo "--- fast=$NFAST clients=$NCLI $SLOW events=$NEV size=$SIZE policy=$POLICY aw=$AW k=$KFENCE"
if ! grep -qE "^RESULT" "$WORK/srv.out"; then
    echo "FATAL: server produced no results"; cat "$WORK/srv.out"; exit 1
fi
grep -E "^(accepted|RESULT|ABORT)" "$WORK/srv.out"
if ! grep -qE "^SUBS" "$WORK/sub.out"; then
    echo "FATAL: subscribers produced no results"; cat "$WORK/sub.out"; exit 1
fi
grep -E "^SUBS" "$WORK/sub.out"
