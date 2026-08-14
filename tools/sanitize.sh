#!/bin/sh
# Hardening passes for the grpc package: valgrind, ASan, TSan.
# The exact recipes behind the results recorded in PLAN.md
# ("Hardening results"). Run from the package root. Nothing here
# touches the normal installed library: sanitizer builds go to
# scratch libraries under $WORK.
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
#   and every CQ batch handoff looks like a race. The pass criterion
#   is that no report has both racing accesses in package code
#   (src/*.cpp). Triage with:
#     grep -oE '#[01] [a-z_]+ [^ ]*src/[a-z]+\.cpp:[0-9]+' tsan.log

set -eu

WORK="${WORK:-$(mktemp -d)}"
echo "work dir: $WORK"

run_suite='print(tinytest::run_test_dir("inst/tinytest"))'

## ---- valgrind memcheck ----
## Gate on access errors and definite leaks. gRPC/absl thread-locals
## and R itself produce possibly-lost records (interior pointers);
## those are noise, not failures.
valgrind --leak-check=full --show-leak-kinds=definite \
    --errors-for-leak-kinds=definite --error-exitcode=99 \
    r -l grpc -e "$run_suite"

## ---- AddressSanitizer ----
printf 'CXXFLAGS = -g -O1 -fsanitize=address -fno-omit-frame-pointer\nCFLAGS = -g -O1 -fsanitize=address -fno-omit-frame-pointer\n' \
    > "$WORK/Makevars.asan"
mkdir -p "$WORK/lib-asan"
R_MAKEVARS_USER="$WORK/Makevars.asan" R CMD INSTALL -l "$WORK/lib-asan" .
## detect_leaks=0: valgrind above owns leak checking; R's exit-time
## reachable allocations drown LSan otherwise.
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.8 \
    ASAN_OPTIONS=detect_leaks=0 \
    r -L "$WORK/lib-asan" -l grpc -e "$run_suite"

## ---- ThreadSanitizer ----
printf 'CXXFLAGS = -g -O1 -fsanitize=thread -fno-omit-frame-pointer\nCFLAGS = -g -O1 -fsanitize=thread -fno-omit-frame-pointer\n' \
    > "$WORK/Makevars.tsan"
mkdir -p "$WORK/lib-tsan"
R_MAKEVARS_USER="$WORK/Makevars.tsan" R CMD INSTALL -l "$WORK/lib-tsan" .
setarch "$(uname -m)" -R env \
    LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libtsan.so.2 \
    TSAN_OPTIONS="halt_on_error=0 report_thread_leaks=0" \
    r -L "$WORK/lib-tsan" -l grpc -e "$run_suite" \
    > "$WORK/tsan.log" 2>&1 || true
echo "TSan reports: $(grep -c 'WARNING: ThreadSanitizer' "$WORK/tsan.log")"
echo "TSan reports with package frames at #0/#1 (pass = none):"
grep -oE '#[01] [a-z_]+ [^ ]*src/(client|server|shim)\.cpp:[0-9]+' \
    "$WORK/tsan.log" | sort | uniq -c || true

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
    echo "TLS exercise did not complete"; exit 1;
}
echo "TSan TLS reports: $(grep -c 'WARNING: ThreadSanitizer' "$WORK/tsan_tls.log")"
echo "all passes done; logs in $WORK"
