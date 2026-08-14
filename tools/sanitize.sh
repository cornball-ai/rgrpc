#!/bin/sh
# Hardening gate for the grpc package: valgrind, ASan, TSan.
# The exact recipes behind the results recorded in PLAN.md
# ("Hardening results"). Run from the package root. Self-contained:
# the checkout is installed into scratch libraries under $WORK for
# every pass — the ambient installed grpc is never exercised — and
# any failure (failed expectation, memcheck error, sanitizer abort,
# missing completion marker, package frame in a TSan report) exits
# nonzero.
#
# Reference environment for the recorded results (2026-08-14):
#   Ubuntu noble, gcc 13.3.0, valgrind 3.22.0, libasan8/libtsan2
#   (gcc-13), libgrpc++ 1.51.1-4.1build5, kernel 7.0.0-28-generic.
#
# TSan notes:
# - setarch -R (disable ASLR) is required: this kernel's address-space
#   layout breaks TSan's shadow mapping ("unexpected memory mapping").
# - Subprocesses segfault with libtsan preloaded, so the TLS tests
#   (which shell out to openssl) skip themselves; gen_certs supplies
#   pre-generated certs to a dedicated TLS/mTLS exercise instead.
# - Expected outcome is NOT zero reports: the system libgrpc is not
#   TSan-instrumented, so its internal synchronization is invisible
#   and every CQ batch handoff looks like a race. The pass criterion,
#   enforced below on both TSan logs: no racing access may have a
#   package source frame in its top two frames — with one carve-out.
#   An access whose #0 frame is the allocator itself (operator new,
#   malloc, calloc, realloc) is the object's construction; TSan pairs
#   it against the completion thread's first use because the handoff
#   in between happens inside uninstrumented libgrpc. That exact
#   pattern (allocation in grpc_r_call_start vs FinishOp in libgrpc)
#   is the known false positive. Deallocation is NOT exempt: operator
#   delete racing an access is a genuine use-after-free candidate.

set -eu

WORK="${WORK:-$(mktemp -d)}"
echo "work dir: $WORK"

## Run the suite, fail hard on any failed expectation, and print a
## completion marker so callers whose exit status is unusable (TSan
## exits 66 whenever reports exist) can still be gated.
SUITE='res <- tinytest::run_test_dir("inst/tinytest"); print(res); stopifnot(tinytest::all_pass(res)); cat("SUITE-ALL-PASS\n")'

## Per-report TSan triage (criterion documented in the header). Walks
## each racing-access stack; a package source frame at #0/#1 is fatal
## unless the access's #0 is an allocation. Matches on the source path
## only — no assumptions about symbol names, so client::run(),
## operator(), and lambda frames all count.
check_frames() {
    awk '
    function flush() {
        if (insec && pkghit && !alloc0) {
            bad++
            print "FATAL racing access with package frame:" ctx
        }
        insec = 0; pkghit = 0; alloc0 = 0; ctx = ""
    }
    /([Ww]rite|[Rr]ead) of size [0-9]+ at 0x/ { flush(); insec = 1; next }
    /Location is|Mutex M|Thread T[0-9]+ [(]|SUMMARY: ThreadSanitizer/ {
        flush()
    }
    /WARNING: ThreadSanitizer/ { flush() }
    insec && /#0 (operator new|malloc|calloc|realloc|posix_memalign)/ {
        alloc0 = 1
    }
    insec && /#[01] .*src\/(client|server|shim|common)\.(cpp|h):[0-9]+/ {
        pkghit = 1; ctx = ctx "\n" $0
    }
    END { flush(); exit(bad > 0 ? 1 : 0) }
    ' "$1" || { echo "FATAL: package-frame race in $1"; exit 1; }
}

## ---- plain build of the checkout, for the valgrind pass ----
mkdir -p "$WORK/lib-plain"
R CMD INSTALL -l "$WORK/lib-plain" .

## ---- valgrind memcheck ----
## Gate on access errors and definite leaks. gRPC/absl thread-locals
## and R itself produce possibly-lost records (interior pointers);
## those are noise, not failures.
valgrind --leak-check=full --show-leak-kinds=definite \
    --errors-for-leak-kinds=definite --error-exitcode=99 \
    r -L "$WORK/lib-plain" -l grpc -e "$SUITE"

## ---- AddressSanitizer ----
printf 'CXXFLAGS = -g -O1 -fsanitize=address -fno-omit-frame-pointer\nCFLAGS = -g -O1 -fsanitize=address -fno-omit-frame-pointer\n' \
    > "$WORK/Makevars.asan"
mkdir -p "$WORK/lib-asan"
R_MAKEVARS_USER="$WORK/Makevars.asan" R CMD INSTALL -l "$WORK/lib-asan" .
## detect_leaks=0: valgrind above owns leak checking; R's exit-time
## reachable allocations drown LSan otherwise. ASan halts on its first
## error, so a nonzero exit here is fatal via set -e.
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.8 \
    ASAN_OPTIONS=detect_leaks=0 \
    r -L "$WORK/lib-asan" -l grpc -e "$SUITE"

## ---- ThreadSanitizer ----
printf 'CXXFLAGS = -g -O1 -fsanitize=thread -fno-omit-frame-pointer\nCFLAGS = -g -O1 -fsanitize=thread -fno-omit-frame-pointer\n' \
    > "$WORK/Makevars.tsan"
mkdir -p "$WORK/lib-tsan"
R_MAKEVARS_USER="$WORK/Makevars.tsan" R CMD INSTALL -l "$WORK/lib-tsan" .
setarch "$(uname -m)" -R env \
    LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libtsan.so.2 \
    TSAN_OPTIONS="halt_on_error=0 report_thread_leaks=0" \
    r -L "$WORK/lib-tsan" -l grpc -e "$SUITE" \
    > "$WORK/tsan.log" 2>&1 || true
grep -q "SUITE-ALL-PASS" "$WORK/tsan.log" || {
    echo "FATAL: TSan suite did not complete with all tests passing"
    tail -20 "$WORK/tsan.log"
    exit 1
}
echo "TSan reports: $(grep -c 'WARNING: ThreadSanitizer' "$WORK/tsan.log" || true)"
check_frames "$WORK/tsan.log"

## TLS/mTLS under TSan, with certs generated outside the TSan process.
CERTDIR="$WORK/certs"
mkdir -p "$CERTDIR"
(
    cd "$CERTDIR"
    openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.pem \
        -days 2 -subj "/CN=grpc-test-ca" 2>/dev/null
    openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr \
        -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost" 2>/dev/null
    openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key \
        -CAcreateserial -out server.pem -days 2 -copy_extensions copy 2>/dev/null
    openssl req -newkey rsa:2048 -nodes -keyout client.key -out client.csr \
        -subj "/CN=test-client" 2>/dev/null
    openssl x509 -req -in client.csr -CA ca.pem -CAkey ca.key \
        -out client.pem -days 2 2>/dev/null
)
setarch "$(uname -m)" -R env \
    LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libtsan.so.2 \
    TSAN_OPTIONS="halt_on_error=0 report_thread_leaks=0" \
    CERTDIR="$CERTDIR" \
    r -L "$WORK/lib-tsan" tools/tls-exercise.R \
    > "$WORK/tsan_tls.log" 2>&1 || true
grep -q "TLS-TSAN-OK" "$WORK/tsan_tls.log" || {
    echo "FATAL: TLS exercise did not complete"
    tail -20 "$WORK/tsan_tls.log"
    exit 1
}
echo "TSan TLS reports: $(grep -c 'WARNING: ThreadSanitizer' "$WORK/tsan_tls.log" || true)"
check_frames "$WORK/tsan_tls.log"

echo "all passes clean; logs in $WORK"
