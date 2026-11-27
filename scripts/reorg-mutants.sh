#!/usr/bin/env bash
# Runs the block-assembly reorg tests against each mutant in
# services/blockassembly/subtreeprocessor/testdata/mutants (issue 2269).
#
# A mutant is a patch that breaks one rule the reorg must keep. The script first
# runs the tests on the clean tree, which must pass. Then for each mutant it
# applies the patch, checks that the package still compiles, runs the tests and
# expects them to fail; a mutant the tests do not kill is a gap in the tests. A
# patch that no longer applies or no longer compiles is reported as stale:
# regenerate it against the current code.
#
# Exit status: 0 when every mutant is killed, 1 otherwise.
#
# Usage: scripts/reorg-mutants.sh [test regex]
#   REORG_MUTANT_TESTS  test regex (default: the reorg scenario, oracle and
#                       differential tests, and the supersede-before-clear pin)
set -euo pipefail
shopt -s nullglob

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

pkg=./services/blockassembly/subtreeprocessor
mutant_dir=services/blockassembly/subtreeprocessor/testdata/mutants
tests=${1:-${REORG_MUTANT_TESTS:-^(TestReorgScenario|TestReorgDifferential|TestOrderOracle|TestPrepareForkBlock_(LetGoSets|UnfinishedSet)|TestResetSupersedes)}}

# run_tests returns 0 when the tests pass, 1 when a test failed and 2 when the run broke without
# a test failing (timeout, panic outside a test, build error): only 1 is a kill.
run_tests() {
	local out status=0
	out=$(TERANODE_HEAVY_TESTS=1 go test -count=1 -timeout 15m -tags testtxmetacache -run "$tests" "$pkg" 2>&1) || status=$?
	[[ $status -eq 0 ]] && return 0

	if grep -q '^--- FAIL' <<<"$out" && ! grep -q 'panic: test timed out' <<<"$out"; then
		return 1
	fi

	printf '%s\n' "$out" | tail -20 >&2
	return 2
}

compiles() {
	go test -count=1 -tags testtxmetacache -run '^$' "$pkg" >/dev/null 2>&1
}

if ! git diff --quiet HEAD -- services/blockassembly ||
	[[ -n $(git ls-files --others --exclude-standard -- services/blockassembly) ]]; then
	echo "services/blockassembly has staged, unstaged or untracked changes; commit or stash them first" >&2
	exit 1
fi

patches=("$mutant_dir"/*.patch)
if [[ ${#patches[@]} -eq 0 ]]; then
	echo "no mutants in $mutant_dir" >&2
	exit 1
fi

if run_tests; then :; else
	echo "the tests fail on the clean tree; fix them before running mutants" >&2
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

for patch in "${patches[@]}"; do
	name=$(basename "$patch" .patch)

	if ! git apply --check "$patch" 2>/dev/null; then
		stale+=("$name (does not apply)")
		continue
	fi

	applied=$patch
	git apply "$patch"

	if ! compiles; then
		stale+=("$name (does not compile)")
	else
		status=0
		run_tests || status=$?

		case $status in
		0)
			survived+=("$name")
			echo "SURVIVED  $name"
			;;
		1)
			killed=$((killed + 1))
			echo "killed    $name"
			;;
		*)
			stale+=("$name (the run broke without a test failing)")
			;;
		esac
	fi

	git apply -R "$patch"
	applied=""
done

echo
echo "killed: $killed  survived: ${#survived[@]}  stale: ${#stale[@]}"

for name in ${stale[@]+"${stale[@]}"}; do
	echo "STALE     $name"
done

[[ ${#survived[@]} -eq 0 && ${#stale[@]} -eq 0 ]]
