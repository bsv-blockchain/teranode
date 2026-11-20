#!/usr/bin/env bash
# Consensus-library gate (bsv-blockchain/teranode#1423).
#
# If the working tree's go.mod changes the pin of a module listed in
# .github/consensus-modules.txt relative to BASE_REF, run the consensus vectors
# on a checkout of BASE_REF and on the working tree, and compare the outcomes.
#
# The base run uses the whole base tree, not the working tree with only the
# pins set back: a bump usually comes with code that uses the new library
# version, and that code does not build against the old one.
#
# Usage: scripts/consensus_lib_gate.sh BASE_REF [OUT_DIR]
#
# Writes OUT_DIR/summary.md (Markdown) and leaves the raw runs beside it.
# The pull request run also records which statements of each bumped module the
# vectors execute. A bump of a module they never execute cannot be checked this
# way, so it fails the gate too rather than pass unseen.
#
# Exit status: 0 no consensus pin changed, or no outcome the base run reported
#                differs in the working tree and the vectors execute every
#                bumped module;
#              3 an outcome differs, or a bumped module is never executed
#                (see summary.md);
#              1 the gate itself failed.
#
# CONSENSUS_PKGS overrides the packages run (default ./test/consensus/).

set -euo pipefail

BASE_REF=${1:?usage: $0 BASE_REF [OUT_DIR]}
OUT_DIR=${2:-$(mktemp -d)}
PKGS=${CONSENSUS_PKGS:-./test/consensus/}
MODULES_FILE=.github/consensus-modules.txt

mkdir -p "$OUT_DIR"
OUT_DIR=$(cd "$OUT_DIR" && pwd)
SUMMARY=$OUT_DIR/summary.md
: >"$SUMMARY"

cd "$(git rev-parse --show-toplevel)"

BASE_SHA=$(git rev-parse --verify "$BASE_REF^{commit}")
git show "$BASE_SHA:go.mod" >"$OUT_DIR/base.go.mod"

go build -o "$OUT_DIR/consensusdiff" ./cmd/consensusdiff
TOOL=$OUT_DIR/consensusdiff

"$TOOL" changed -modules "$MODULES_FILE" -base "$OUT_DIR/base.go.mod" -head go.mod >"$OUT_DIR/changed.tsv"

{
	echo "## Consensus-library gate"
	echo
	echo "Base: \`$(git rev-parse --short "$BASE_SHA")\`. Vectors: \`$PKGS\`."
	echo
} >>"$SUMMARY"

if [[ ! -s $OUT_DIR/changed.tsv ]]; then
	echo "No module in \`$MODULES_FILE\` changed its pin, so the vectors were not run." >>"$SUMMARY"
	cat "$SUMMARY"
	exit 0
fi

run_vectors() { # dir output [go test flags...]
	local dir=$1 out=$2
	shift 2
	# go test exits non-zero when a test fails; the outcomes are in the JSON.
	# shellcheck disable=SC2086 # PKGS is a list of packages
	(cd "$dir" && go test -count=1 -timeout=20m -json "$@" $PKGS) >"$out" 2>"$out.stderr" || true
}

BASE_TREE=$OUT_DIR/base-tree
cleanup() {
	git worktree remove --force "$BASE_TREE" >/dev/null 2>&1 || true
}
trap cleanup EXIT

cleanup
git worktree add --quiet --detach "$BASE_TREE" "$BASE_SHA"

echo "Running the vectors on the base commit" >&2
run_vectors "$BASE_TREE" "$OUT_DIR/base.json"

# Instrument every bumped module the pull request still requires.
COVER_MODULES=()
COVERPKG=
while IFS=$'\t' read -r mod _ head; do
	if [[ $head != "(none)" ]]; then
		COVER_MODULES+=("$mod")
		COVERPKG+="${COVERPKG:+,}$mod/..."
	fi
done <"$OUT_DIR/changed.tsv"

echo "Running the vectors on the pull request" >&2
COVER_FLAGS=()
if ((${#COVER_MODULES[@]} > 0)); then
	COVER_FLAGS=(-coverpkg="$COVERPKG" -coverprofile="$OUT_DIR/head.cover")
fi
run_vectors . "$OUT_DIR/head.json" "${COVER_FLAGS[@]}"

DIFFERENT=0

{
	echo "| Module | Base | Pull request | Statements the vectors execute |"
	echo "|---|---|---|---|"
} >>"$SUMMARY"

if ((${#COVER_MODULES[@]} > 0)); then
	if [[ ! -s $OUT_DIR/head.cover ]]; then
		echo "consensusdiff: the pull request run wrote no cover profile; see $OUT_DIR/head.json.stderr" >&2
		exit 1
	fi
	"$TOOL" coverage -profile "$OUT_DIR/head.cover" "${COVER_MODULES[@]}" >"$OUT_DIR/coverage.tsv"
else
	: >"$OUT_DIR/coverage.tsv"
fi

NOT_EXERCISED=()
while IFS=$'\t' read -r mod base head; do
	executed="n/a (removed)"
	if [[ $head != "(none)" ]]; then
		read -r _ covered total < <(grep -F -- "$mod"$'\t' "$OUT_DIR/coverage.tsv")
		executed="$covered of $total"
		if ((covered == 0)); then
			executed="**none** (of $total)"
			NOT_EXERCISED+=("\`$mod\`")
		fi
	fi
	echo "| \`$mod\` | $base | $head | $executed |" >>"$SUMMARY"
done <"$OUT_DIR/changed.tsv"
echo >>"$SUMMARY"

if ((${#NOT_EXERCISED[@]} > 0)); then
	DIFFERENT=1
	echo "**Not checked:** the vectors never execute ${NOT_EXERCISED[*]}, so this gate cannot see a change in its behaviour. Review the bump as a consensus change." >>"$SUMMARY"
	echo >>"$SUMMARY"
fi

status=0
"$TOOL" compare -base "$OUT_DIR/base.json" -head "$OUT_DIR/head.json" >>"$SUMMARY" || status=$?
case $status in
0) ;;
3) DIFFERENT=1 ;;
*) exit 1 ;;
esac

cat "$SUMMARY"

if ((DIFFERENT)); then
	exit 3
fi
