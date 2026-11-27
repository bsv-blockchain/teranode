#!/usr/bin/env bash
# Self-test of check-test-shards.sh and merge-coverage.sh. Both exist only to fail
# on drift, so each must pass on good input and fail on every case it targets.
#
# Usage: .github/scripts/test-ci-scripts.sh (from the repository root)
set -euo pipefail

root="$PWD"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
fails=0
pr=teranode_pr_tests.yaml
main=teranode_main_tests.yaml

report() { # <case> <want status> <got status>
  if [ "$2" = "$3" ]; then echo "ok   $1"; else echo "::error::FAIL $1: want exit status $2, got $3"; fails=1; fi
}

# Runs check-test-shards.sh on a copy of the repository whose named workflows
# have sed expression <expr> applied. An edit that matches nothing leaves the
# files valid, so a stale expression fails its case rather than passing it.
shard_case() { # <case> <want status> <sed expr> <workflow>...
  local name="$1" want="$2" expr="$3" d="$tmp/shards" got=0
  shift 3
  rm -rf "$d" && mkdir -p "$d"
  for e in "$root"/*; do ln -s "$e" "$d/"; done
  cp -R "$root/.github" "$d/"
  for wf in "$@"; do
    sed -E "$expr" "$d/.github/workflows/$wf" >"$d/edited" && mv "$d/edited" "$d/.github/workflows/$wf"
  done
  (cd "$d" && bash .github/scripts/check-test-shards.sh >/dev/null 2>&1) || got=1
  report "check-test-shards: $name" "$want" "$got"
}

shard_case "unedited workflows" 0 's/^//' "$pr"
shard_case "package excluded from unit but in no shard" 1 '/make test-pkgs /s#services/blockvalidation ##' "$pr" "$main"
shard_case "package in two shards" 1 's#TEST_PKGS=stores/utxo/aerospike #TEST_PKGS="stores/utxo/aerospike model" #' "$pr" "$main"
shard_case "shard missing from COVERAGE_SHARDS" 1 's#(COVERAGE_SHARDS: unit) aerospike#\1#' "$pr" "$main"
shard_case "test-build-pkgs list drifts from TEST_EXCLUDE" 1 '/make test-build-pkgs /s#stores/utxo/aerospike ##' "$main"
shard_case "workflows differ" 1 's#(coverage-|shard: |aerospike )slow#\1slower#g' "$pr"
shard_case "profile is not coverage-<shard>.out" 1 's#profile: coverage-slow.out#profile: coverage-s.out#' "$pr" "$main"
shard_case "TEST_COVERPROFILE is not coverage-<shard>.out" 1 's#TEST_COVERPROFILE=coverage-slow.out#TEST_COVERPROFILE=coverage-s.out#' "$pr" "$main"
shard_case "package that does not exist" 1 's#services/legacy/netsync#services/legacy/netsyncx#g' "$pr" "$main"
shard_case "no make test-build-pkgs step" 1 '/run: make test-build-pkgs /d' "$main"
shard_case "TEST_EXCLUDE set under a longer name" 1 's#run: make test TEST_EXCLUDE=#run: make test NOT_TEST_EXCLUDE=#' "$pr" "$main"
# The next three also move stores/utxo/aerospike back into the unit shard, so the
# lists still agree and only the targeted check can fail.
unaero='s#TEST_EXCLUDE="stores/utxo/aerospike #TEST_EXCLUDE="#; /make test-build-pkgs /s#stores/utxo/aerospike ##'
shard_case "shard with an empty TEST_PKGS" 1 "$unaero; s#TEST_PKGS=stores/utxo/aerospike #TEST_PKGS= #" "$pr" "$main"
shard_case "shard running neither make target" 1 "$unaero; s#make test-pkgs TEST_PKGS=stores/utxo/aerospike #make vet TEST_PKGS= #" "$pr" "$main"
shard_case "two shards running make test" 1 "$unaero; s#make test-pkgs TEST_PKGS=stores/utxo/aerospike #make test #" "$pr" "$main"

m="$tmp/coverage"
profile() { printf 'mode: %s\n%s\n' "$2" "$3" >"$m/coverage-$1.out"; }
reset_profiles() {
  rm -rf "$m" && mkdir -p "$m"
  profile a atomic "a.go:1.1,2.2 1 1"
  profile b atomic "b.go:1.1,2.2 1 0"
}
merge() { # [shard]...: prints merge-coverage.sh's exit status for those shards
  if bash "$root/.github/scripts/merge-coverage.sh" "$m" "$m/merged" "$@" >"$m.log" 2>&1; then echo 0; else echo 1; fi
}
merge_fails() { # <case> <error the merge must print> [shard]...
  local name="$1" msg="$2" got
  shift 2
  got="$(merge "$@")"
  grep -qF "$msg" "$m.log" || got="$got, but no \"$msg\" in its output"
  report "merge-coverage: $name" 1 "$got"
}

reset_profiles
report "merge-coverage: matching profiles" 0 "$(merge a b)"
want=$'mode: atomic\na.go:1.1,2.2 1 1\nb.go:1.1,2.2 1 0'
if [ "$(cat "$m/merged")" = "$want" ]; then got=0; else got=1; fi
report "merge-coverage: one mode line, then every shard's blocks" 0 "$got"

reset_profiles && rm "$m/coverage-b.out"
merge_fails "missing profile" "Missing coverage profile coverage-b.out" a b
reset_profiles && profile c atomic "c.go:1.1,2.2 1 1"
merge_fails "unexpected profile" "Unexpected coverage profile coverage-c.out" a b
reset_profiles && profile b set "b.go:1.1,2.2 1 0"
merge_fails "mismatched mode" "coverage-b.out has a different mode" a b
reset_profiles
merge_fails "no shards named" "No coverage shards named"
reset_profiles && printf 'x.go:1.1,2.2 1 1\n' >"$m/coverage-a.out" && printf 'x.go:1.1,2.2 1 1\n' >"$m/coverage-b.out"
merge_fails "profiles with no mode line" "coverage-a.out has no mode line" a b

exit "$fails"
