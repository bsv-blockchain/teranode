#!/usr/bin/env bash
# Checks the hand-kept lists of the Go test matrix in the PR and main workflows:
# the unit shard's TEST_EXCLUDE is exactly the other shards' TEST_PKGS, with each
# package in one shard only; every shard writes coverage-<shard>.out; COVERAGE_SHARDS
# names every shard; and both workflows define the same shards. A package dropped
# from one list but not the other would otherwise stop running with every job green.
#
# Usage: .github/scripts/check-test-shards.sh (from the repository root)
set -euo pipefail

workflows=(.github/workflows/teranode_pr_tests.yaml .github/workflows/teranode_main_tests.yaml)
fail=0
err() { echo "::error::$*"; fail=1; }

# One line per matrix shard, "<shard> <profile> <run>", then "coverage <shards>".
matrix() {
  awk '
    /^[[:space:]]*- shard:/ { shard = $3; run = ""; next }
    shard != "" && /^[[:space:]]*run:/ { run = $0; sub(/^[[:space:]]*run:[[:space:]]*/, "", run); next }
    shard != "" && /^[[:space:]]*profile:/ { print shard, $2, run; shard = ""; next }
    /^[[:space:]]*COVERAGE_SHARDS:/ { line = $0; sub(/^[[:space:]]*COVERAGE_SHARDS:[[:space:]]*/, "", line); print "coverage", line }
  ' "$1"
}

# The value of make variable $1 in run line $2, quoted or not, one word per line.
var() { sed -nE "s/.*$1=(\"[^\"]*\"|[^ ]*).*/\1/p" <<<"$2" | tr -d '"' | tr ' ' '\n' | sed '/^$/d'; }

for wf in "${workflows[@]}"; do
  shards="" excluded="" pkgs="" coverage="" units=0
  while read -r shard profile run; do
    if [ "$shard" = coverage ]; then coverage="$profile $run"; continue; fi
    shards+="$shard"$'\n'
    [ "$profile" = "coverage-$shard.out" ] || err "$wf: shard $shard has profile $profile, want coverage-$shard.out"
    [ "$(var TEST_COVERPROFILE "$run")" = "coverage-$shard.out" ] || err "$wf: shard $shard does not set TEST_COVERPROFILE=coverage-$shard.out"
    case "$run" in
      "make test "*) units=$((units + 1)); excluded+="$(var TEST_EXCLUDE "$run")"$'\n' ;;
      "make test-pkgs "*) p="$(var TEST_PKGS "$run")"; [ -n "$p" ] || err "$wf: shard $shard has no TEST_PKGS"; pkgs+="$p"$'\n' ;;
      *) err "$wf: shard $shard runs neither make test nor make test-pkgs" ;;
    esac
  done < <(matrix "$wf")

  [ "$units" -eq 1 ] || err "$wf: want one shard running make test, found $units"
  dups="$(sed '/^$/d' <<<"$pkgs" | sort | uniq -d)"
  [ -z "$dups" ] || err "$wf: packages in more than one shard: $(echo $dups)"
  for p in $(sed '/^$/d' <<<"$pkgs$excluded" | sort -u); do
    [ -d "$p" ] || err "$wf: package $p does not exist"
  done
  diff <(sed '/^$/d' <<<"$excluded" | sort -u) <(sed '/^$/d' <<<"$pkgs" | sort -u) >/dev/null ||
    err "$wf: the unit shard's TEST_EXCLUDE ($(sed '/^$/d' <<<"$excluded" | sort | xargs)) is not the other shards' TEST_PKGS ($(sed '/^$/d' <<<"$pkgs" | sort | xargs))"
  [ "$(tr ' ' '\n' <<<"$coverage" | sed '/^$/d' | sort | xargs)" = "$(sed '/^$/d' <<<"$shards" | sort | xargs)" ] ||
    err "$wf: COVERAGE_SHARDS ($(echo $coverage)) does not name exactly the matrix shards ($(echo $shards))"
done

diff <(matrix "${workflows[0]}") <(matrix "${workflows[1]}") >/dev/null ||
  err "${workflows[0]} and ${workflows[1]} define different test shards or COVERAGE_SHARDS"

exit "$fail"
