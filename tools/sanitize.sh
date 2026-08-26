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
#   and every op-post/completion handoff looks like a race. Since this
#   package's code (and the grpc++ header templates it instantiates)
#   IS instrumented, those boundary reports carry package frames too:
#   constructor stores paired against first post-handoff use,
#   WritesDone's flag store paired against FinalizeResult, and so on.
#   TSan cannot verify one-sided pairs through an uninstrumented
#   runtime in either direction.
#
#   The enforced criterion, on both TSan logs: classify each racing
#   access by its PERFORMING frame — the first frame not inside the
#   TSan interceptor runtime (libtsan) — and fail iff a report has
#   BOTH racing accesses performing in package source files. Both
#   sides of such a pair are fully instrumented, so a legitimate
#   common lock would be visible to TSan: a both-package race is a
#   provable violation of the mutex discipline. One-sided pairs are
#   counted and printed for the record but are not gate failures;
#   their one dangerous subcase, use-after-free, is covered by the
#   ASan and valgrind passes above, which must be clean.
#
#   This is a conservative, high-signal gate, not absolute proof: a
#   real ordering bug whose racing accesses all execute inside
#   uninstrumented libgrpc — or that TSan's timing never observes —
#   is invisible here. Any TSan warning that is not a data race
#   (lock-order inversion, signal-unsafe call, ...) is fatal outright,
#   since the classifier only adjudicates data-race access stacks.
#   The classifier itself is exercised against synthetic reports at
#   startup, so a regression in it fails the gate rather than
#   silently passing everything.

set -eu

## Never leave sanitizer-flagged objects in src/ for the next normal
## build to silently reuse — on failure as much as on success.
trap 'rm -f src/*.o src/rgrpc.so' EXIT

WORK="${WORK:-$(mktemp -d)}"
echo "work dir: $WORK"

## Run the suite, fail hard on any failed expectation, and print a
## completion marker so callers whose exit status is unusable (TSan
## exits 66 whenever reports exist) can still be gated. The first
## stopifnot proves the grpc under test is the scratch build named in
## GRPC_SANITIZE_LIB — littler's -L flag silently fails to prepend the
## library path (observed with littler on noble), which once made these
## passes run the ambient install; libraries are injected via R_LIBS
## and verified here so that cannot happen silently again.
SUITE='lib <- Sys.getenv("GRPC_SANITIZE_LIB"); stopifnot(nzchar(lib), identical(find.package("rgrpc"), file.path(lib, "rgrpc"))); res <- tinytest::run_test_dir("inst/tinytest"); print(res); stopifnot(tinytest::all_pass(res)); cat("SUITE-ALL-PASS\n")'

## Per-report TSan triage (criterion documented in the header). For
## each report, each racing access is classified by its performing
## frame — the first frame not inside libtsan (interceptors are not
## real frames). Fatal iff BOTH racing accesses in a report perform in
## package source. Matches on source path only — no assumptions about
## symbol names, so client::run(), operator(), and lambda frames all
## count.
check_frames() {
    awk '
    function endsec() {
        if (insec && perf_pkg) { npkg++; ctx = ctx "\n" perf_line }
        insec = 0
    }
    function endrep() {
        endsec()
        if (inrep && npkg >= 2) {
            bad++
            print "FATAL both-package race:" ctx
        }
        if (inrep && npkg == 1) oneside++
        inrep = 0; npkg = 0; ctx = ""
    }
    /WARNING: ThreadSanitizer/ { endrep(); inrep = 1 }
    inrep && /([Ww]rite|[Rr]ead) of size [0-9]+ at 0x/ {
        endsec(); insec = 1; perf_seen = 0; perf_pkg = 0; next
    }
    inrep && /Location is|Mutex M|Thread T[0-9]+ [(]|As if synchronized/ {
        endsec()
    }
    /SUMMARY: ThreadSanitizer/ { endrep() }
    insec && !perf_seen && /#[0-9]+ / {
        if ($0 ~ /libtsan\.so/) next  # interceptor, not a real frame
        perf_seen = 1
        if ($0 ~ /src\/(client|server|shim|common)\.(cpp|h):[0-9]+/) {
            perf_pkg = 1; perf_line = $0
        }
    }
    END {
        endrep()
        printf "one-sided boundary reports (not fatal): %d\n", oneside
        exit(bad > 0 ? 1 : 0)
    }
    ' "$1" || { echo "FATAL: both-package race in $1"; exit 1; }
}

## The classifier only adjudicates data-race access stacks; every other
## TSan warning shape (lock-order inversion, signal-unsafe call, ...)
## is fatal outright rather than silently unexamined.
check_warning_types() {
    unknown=$(grep "WARNING: ThreadSanitizer:" "$1" \
        | sed 's/.*WARNING: ThreadSanitizer: //; s/ (pid=[0-9]*)$//' \
        | sort -u | grep -v '^data race' || true)
    if [ -n "$unknown" ]; then
        echo "FATAL: unrecognized TSan warning types in $1:"
        printf '%s\n' "$unknown"
        exit 1
    fi
}

## Self-test: the classifier is itself gate-critical, so exercise it
## against synthetic reports before trusting it on real logs. Covers:
## a both-package race (must fail, including the libtsan-interceptor
## skip at #0), a one-sided package-vs-libgrpc pair (must pass, counted
## once), a package caller beneath an inlined C++/gRPC-header
## performing frame (boundary, must pass), and a non-data-race warning
## (must be rejected by check_warning_types).
selftest_classifier() {
    st="$WORK/selftest"
    mkdir -p "$st"
    cat > "$st/both.log" <<'EOF'
WARNING: ThreadSanitizer: data race (pid=1)
  Write of size 8 at 0x7b0400000000 by thread T7:
    #0 memcpy ../../libtsan/x.inc:115 (libtsan.so.2+0x8bd30)
    #1 client::run() /x/grpc/src/client.cpp:210 (grpc.so+0x1111)

  Previous read of size 8 at 0x7b0400000000 by main thread:
    #0 grpc_r_client_poll /x/grpc/src/client.cpp:520 (grpc.so+0x2222)

SUMMARY: ThreadSanitizer: data race src/client.cpp:210 in client::run()
EOF
    cat > "$st/oneside.log" <<'EOF'
WARNING: ThreadSanitizer: data race (pid=1)
  Read of size 8 at 0x7b0400000000 by thread T7:
    #0 grpc::internal::CallOpSet<int>::FinalizeResult(void**, bool*) <null> (grpc.so+0x1111)
    #1 grpc::CompletionQueue::AsyncNextInternal(void**, bool*, gpr_timespec) <null> (libgrpc++.so.1.51+0x60d99)

  Previous write of size 8 at 0x7b0400000000 by main thread:
    #0 grpc_r_stream_start /x/grpc/src/client.cpp:476 (grpc.so+0x2222)

SUMMARY: ThreadSanitizer: data race in grpc_r_stream_start
EOF
    cat > "$st/inline.log" <<'EOF'
WARNING: ThreadSanitizer: data race (pid=1)
  Write of size 1 at 0x7b0400000000 by main thread (mutexes: write M0):
    #0 grpc::ClientAsyncReaderWriter<grpc::ByteBuffer, grpc::ByteBuffer>::WritesDone(void*) /usr/include/grpcpp/impl/codegen/async_stream.h:123 (grpc.so+0x1111)
    #1 s_pump_writes_locked /x/grpc/src/client.cpp:173 (grpc.so+0x1111)

  Previous write of size 1 at 0x7b0400000000 by thread T7:
    #0 grpc::internal::CallOpSet<int>::FinalizeResult(void**, bool*) <null> (grpc.so+0x2222)

SUMMARY: ThreadSanitizer: data race in WritesDone
EOF
    cat > "$st/mutex.log" <<'EOF'
WARNING: ThreadSanitizer: lock-order-inversion (potential deadlock) (pid=1)
  Cycle in lock order graph: M0 => M1 => M0

SUMMARY: ThreadSanitizer: lock-order-inversion (potential deadlock)
EOF
    if (check_frames "$st/both.log") >/dev/null 2>&1; then
        echo "FATAL: classifier self-test: both-package race not caught"
        exit 1
    fi
    out=$( (check_frames "$st/oneside.log") 2>&1 ) || {
        echo "FATAL: classifier self-test: one-sided report treated as fatal"
        exit 1
    }
    case "$out" in *"(not fatal): 1"*) : ;; *)
        echo "FATAL: classifier self-test: one-sided report not counted"
        exit 1
    ;; esac
    (check_frames "$st/inline.log") >/dev/null 2>&1 || {
        echo "FATAL: classifier self-test: inlined-header performing frame"
        echo "misclassified as a package access"
        exit 1
    }
    if (check_warning_types "$st/mutex.log") >/dev/null 2>&1; then
        echo "FATAL: classifier self-test: non-data-race warning accepted"
        exit 1
    fi
    check_warning_types "$st/both.log"
    echo "classifier self-test passed"
}
selftest_classifier

## ---- plain build of the checkout, for the valgrind pass ----
mkdir -p "$WORK/lib-plain"
## --preclean everywhere: R CMD INSTALL builds in src/ and happily
## reuses stale objects from a previous (differently-flagged) build,
## in which case new flags never reach the compiler.
R CMD INSTALL --preclean -l "$WORK/lib-plain" .

## ---- valgrind memcheck ----
## Gate on access errors and definite leaks. gRPC/absl thread-locals
## and R itself produce possibly-lost records (interior pointers);
## those are noise, not failures.
R_LIBS="$WORK/lib-plain" GRPC_SANITIZE_LIB="$WORK/lib-plain" \
    valgrind --leak-check=full --show-leak-kinds=definite \
    --errors-for-leak-kinds=definite --error-exitcode=99 \
    r -l rgrpc -e "$SUITE"

## The package builds with CXX_STD = CXX17, so R reads CXX17FLAGS —
## setting CXXFLAGS alone is silently ignored. Belt and braces: set
## both, and PROVE instrumentation landed by checking the built .so
## for sanitizer runtime references before trusting any "clean" run.
sanitizer_flags() {
    printf 'CXX17FLAGS = -g -O1 -fsanitize=%s -fno-omit-frame-pointer\nCXXFLAGS = -g -O1 -fsanitize=%s -fno-omit-frame-pointer\nCFLAGS = -g -O1 -fsanitize=%s -fno-omit-frame-pointer\n' \
        "$1" "$1" "$1"
}
check_instrumented() {
    if ! nm -u "$1/rgrpc/libs/rgrpc.so" | grep -q "__$2"; then
        echo "FATAL: $1/rgrpc/libs/rgrpc.so has no __$2 references —"
        echo "the sanitizer flags did not reach the build"
        exit 1
    fi
}

## ---- AddressSanitizer ----
sanitizer_flags address > "$WORK/Makevars.asan"
mkdir -p "$WORK/lib-asan"
## --no-test-load: the instrumented .so only loads under the matching
## LD_PRELOAD, which the suite run below provides.
R_MAKEVARS_USER="$WORK/Makevars.asan" \
    R CMD INSTALL --preclean --no-test-load -l "$WORK/lib-asan" .
check_instrumented "$WORK/lib-asan" asan
## detect_leaks=0: valgrind above owns leak checking; R's exit-time
## reachable allocations drown LSan otherwise. ASan halts on its first
## error, so a nonzero exit here is fatal via set -e.
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.8 \
    ASAN_OPTIONS=detect_leaks=0 \
    R_LIBS="$WORK/lib-asan" GRPC_SANITIZE_LIB="$WORK/lib-asan" \
    r -l rgrpc -e "$SUITE"

## ---- ThreadSanitizer ----
sanitizer_flags thread > "$WORK/Makevars.tsan"
mkdir -p "$WORK/lib-tsan"
R_MAKEVARS_USER="$WORK/Makevars.tsan" \
    R CMD INSTALL --preclean --no-test-load -l "$WORK/lib-tsan" .
check_instrumented "$WORK/lib-tsan" tsan
setarch "$(uname -m)" -R env \
    LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libtsan.so.2 \
    TSAN_OPTIONS="halt_on_error=0 report_thread_leaks=0" \
    R_LIBS="$WORK/lib-tsan" GRPC_SANITIZE_LIB="$WORK/lib-tsan" \
    r -l rgrpc -e "$SUITE" \
    > "$WORK/tsan.log" 2>&1 || true
grep -q "SUITE-ALL-PASS" "$WORK/tsan.log" || {
    echo "FATAL: TSan suite did not complete with all tests passing"
    tail -20 "$WORK/tsan.log"
    exit 1
}
echo "TSan reports: $(grep -c 'WARNING: ThreadSanitizer' "$WORK/tsan.log" || true)"
check_warning_types "$WORK/tsan.log"
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
    R_LIBS="$WORK/lib-tsan" GRPC_SANITIZE_LIB="$WORK/lib-tsan" \
    CERTDIR="$CERTDIR" \
    r tools/tls-exercise.R \
    > "$WORK/tsan_tls.log" 2>&1 || true
grep -q "TLS-TSAN-OK" "$WORK/tsan_tls.log" || {
    echo "FATAL: TLS exercise did not complete"
    tail -20 "$WORK/tsan_tls.log"
    exit 1
}
echo "TSan TLS reports: $(grep -c 'WARNING: ThreadSanitizer' "$WORK/tsan_tls.log" || true)"
check_warning_types "$WORK/tsan_tls.log"
check_frames "$WORK/tsan_tls.log"

echo "all passes clean; logs in $WORK"
