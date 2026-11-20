#!/usr/bin/env bash
# Runs the block-assembly reorg tests against each mutant in
# services/blockassembly/subtreeprocessor/testdata/mutants (issue 2269).
#
# A mutant is a patch that breaks one rule the reorg must keep. For each one the
# script applies the patch, runs the reorg tests, and expects them to fail; a
# mutant the tests do not kill is a gap in the tests. A patch that no longer
# applies is reported as stale: regenerate it against the current code.
#
# Exit status: 0 when every mutant is killed, 1 otherwise.
#
# Usage: scripts/reorg-mutants.sh [test regex]
#   REORG_MUTANT_TESTS  test regex (default: the reorg scenario, oracle and
#                       differential tests)
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

pkg=./services/blockassembly/subtreeprocessor
mutant_dir=services/blockassembly/subtreeprocessor/testdata/mutants
tests=${1:-${REORG_MUTANT_TESTS:-^(TestReorgScenario|TestReorgDifferential|TestOrderOracle)}}

if ! git diff --quiet -- services/blockassembly; then
	echo "services/blockassembly has uncommitted changes; commit or stash them first" >&2
	exit 1
fi

applied=""
cleanup() {
	if [[ -n "$applied" ]]; then
		git apply -R "$applied"
	fi
}
trap cleanup EXIT

killed=0
survived=()
stale=()

for patch in "$mutant_dir"/*.patch; do
	name=$(basename "$patch" .patch)

	if ! git apply --check "$patch" 2>/dev/null; then
		stale+=("$name")
		continue
	fi

	git apply "$patch"
	applied=$patch

	if go test -count=1 -tags testtxmetacache -run "$tests" "$pkg" >/dev/null 2>&1; then
		survived+=("$name")
		echo "SURVIVED  $name"
	else
		killed=$((killed + 1))
		echo "killed    $name"
	fi

	git apply -R "$patch"
	applied=""
done

echo
echo "killed: $killed  survived: ${#survived[@]}  stale: ${#stale[@]}"

for name in "${stale[@]}"; do
	echo "STALE     $name (patch no longer applies)"
done

[[ ${#survived[@]} -eq 0 && ${#stale[@]} -eq 0 ]]
