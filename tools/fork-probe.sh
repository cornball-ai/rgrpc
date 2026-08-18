#!/bin/sh
# Fork-safety probe. Establishes what happens when an R process with an
# open gRPC channel forks, so the failure mode can be documented rather
# than discovered by a user running mclapply(). Fork safety is a stated
# non-goal (see PLAN.md); a documented failure beats a mystery hang.
#
#   tools/fork-probe.sh [case ...]
#
# Cases (all four run by default):
#   reuse        parent's channel used inside the forked child
#   reuse_block  same, but waiting 30s rather than 3s, to tell "late"
#                from "never completes"
#   parent       does the parent's channel still work after forking?
#   fresh        child opens its own channel after the fork
#
# Each case runs twice, with GRPC_ENABLE_FORK_SUPPORT unset and set to 1,
# because that variable is the only thing upstream offers here and its
# effect needs to be on the record either way.
#
# Every child gets a hard deadline and a SIGKILL. The expected failure is
# a hang, not an error, so a probe without a deadline would inherit the
# hang and report nothing -- and a hung child holding the socket would
# make the next case look broken too.
set -u

SP="$(cd "$(dirname "$0")" && pwd)"
CASES="${*:-reuse reuse_block parent fresh}"

if [ -n "${WORK:-}" ]; then
    mkdir -p "$WORK"
else
    WORK="$(mktemp -d)"
    trap 'rm -rf "$WORK"' EXIT
fi

cases_run=0
for fs in unset 1; do
    for c in $CASES; do
        SOCK="$WORK/fork-$c-$fs.sock"
        r "$SP/fork-echo-server.R" "$SOCK" 60 > "$WORK/srv-$c-$fs.out" 2>&1 &
        SRVPID=$!
        i=0
        while [ ! -S "$SOCK" ] && [ $i -lt 100 ]; do i=$((i+1)); sleep 0.1; done
        if [ ! -S "$SOCK" ]; then
            echo "FATAL: server never bound"; cat "$WORK/srv-$c-$fs.out"
            kill $SRVPID 2>/dev/null; exit 1
        fi

        # Outer timeout as well as the in-R one: if the hang happens
        # somewhere the R-level deadline cannot reach, the probe still
        # terminates and says so.
        if [ "$fs" = "unset" ]; then
            timeout -s KILL 60 r "$SP/fork-probe.R" "$SOCK" "$c" 10 \
                > "$WORK/probe-$c-$fs.out" 2>&1
        else
            GRPC_ENABLE_FORK_SUPPORT=1 timeout -s KILL 60 \
                r "$SP/fork-probe.R" "$SOCK" "$c" 10 \
                > "$WORK/probe-$c-$fs.out" 2>&1
        fi
        rc=$?

        grep -E "^FORK" "$WORK/probe-$c-$fs.out"
        if [ $rc -ne 0 ]; then
            echo "FORK probe_exit=$rc (137 = killed at the outer deadline)"
        fi
        echo "--"
        kill $SRVPID 2>/dev/null
        # `wait` on a signalled child returns 143, and as the last
        # command in the loop that becomes the script's exit status --
        # a clean run would look like a failure to any caller. Discard
        # it deliberately rather than letting it leak out.
        wait $SRVPID 2>/dev/null || true
        cases_run=$((cases_run + 1))
    done
done

# Exit nonzero only if a case produced no verdict at all: the probe
# observes rather than judges, so "the child hung" is a result, not a
# failure, but "no result" is.
if [ "$cases_run" -eq 0 ]; then
    echo "FATAL: no cases ran"; exit 1
fi
echo "ran $cases_run case-runs"
exit 0
