#!/usr/bin/env bash
# Merges the per-shard coverage profiles of the Go test matrix into one profile.
# Every expected shard must have written coverage-<shard>.out, no other profile
# may be present, and all must share one mode line, which is written once.
# Every profile comes from -race (mode: atomic) with -coverpkg=./..., so a block
# can appear in several; the coverage gate and Sonar count it covered when any
# copy of it is.
#
# Usage: .github/scripts/merge-coverage.sh <profile dir> <output file> <shard>...
set -euo pipefail

dir="$1"
out="$2"
shift 2
[ "$#" -gt 0 ] || { echo "::error::No coverage shards named"; exit 1; }

for shard in "$@"; do
  [ -f "$dir/coverage-$shard.out" ] || { echo "::error::Missing coverage profile coverage-$shard.out"; exit 1; }
done

for f in "$dir"/coverage-*.out; do
  shard="${f##*/coverage-}"
  shard="${shard%.out}"
  case " $* " in
    *" $shard "*) ;;
    *) echo "::error::Unexpected coverage profile ${f##*/}: add its shard to the expected list"; exit 1 ;;
  esac
done

mode="$(head -n 1 "$dir/coverage-$1.out")"
case "$mode" in
  "mode: "*) ;;
  *) echo "::error::coverage-$1.out has no mode line"; exit 1 ;;
esac

{
  echo "$mode"
  for shard in "$@"; do
    f="$dir/coverage-$shard.out"
    [ "$(head -n 1 "$f")" = "$mode" ] || { echo "::error::coverage-$shard.out has a different mode from coverage-$1.out" >&2; exit 1; }
    tail -n +2 "$f"
  done
} > "$out"
