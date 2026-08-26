#!/bin/sh
# Verify the declared minimum R version against the R that Ubuntu noble
# actually ships. Run from the package root; needs docker.
#
#   tools/r-floor/check.sh
#
# Why this exists: DESCRIPTION's floor arrived with the package skeleton
# rather than from a decision, and nothing in the package needs anything
# newer than get0() (R 3.2). Noble ships R 4.3.x while r2u tracks current
# R, so both are in the wild on the same distro and CI runners land on
# either -- a too-high floor therefore fails intermittently, which reads
# as flakiness rather than as a wrong constraint.
#
# The check is empirical on purpose. Static inspection of which functions
# a package calls is good evidence and not proof: a floor can also be
# wrong because of C-level API changes, Makevars handling, or a
# dependency's own floor, none of which show up in a grep for base
# functions.
#
# Passing here licenses the floor at noble's R and no lower. Claiming
# support for older R would need older R to be tested, and older distros
# carry a different libgrpc++ ABI, which the platform commitment does not
# cover in any case.
set -eu

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SP="$(cd "$(dirname "$0")" && pwd)"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/pkg"
cp "$SP/Dockerfile" "$WORK/"
# tools/ is not needed inside and carries the build context weight.
rsync -a --exclude .git --exclude '*.o' --exclude '*.so' --exclude tools \
    "$ROOT/" "$WORK/pkg/"

echo "declared floor: $(grep '^Depends:' "$ROOT/DESCRIPTION")"
docker build -t grpc-r-floor "$WORK" > "$WORK/build.log" 2>&1 || {
    echo "FATAL: image build failed"; tail -20 "$WORK/build.log"; exit 1
}

docker run --rm grpc-r-floor sh -c '
set -e
R --version | head -1
R CMD INSTALL . > /tmp/inst.log 2>&1 || {
    echo "RESULT install FAILED"; tail -25 /tmp/inst.log; exit 1
}
echo "RESULT install OK"
Rscript -e "
library(rgrpc)
res <- tinytest::run_test_dir(\"inst/tinytest\", verbose = 0)
print(res)
if (!tinytest::all_pass(res)) { cat(\"RESULT tests FAILED\n\"); quit(status = 1L) }
cat(\"RESULT tests OK\n\")
" 2>&1 | grep -E "^(RESULT|All ok|[0-9]+ tests)"
'
