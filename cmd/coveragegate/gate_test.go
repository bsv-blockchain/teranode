package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const testModule = "example.com/mod"

func TestReadThresholds(t *testing.T) {
	thresholds, err := readThresholds(strings.NewReader(`
# consensus packages
services/a 88
services/b   92.5  # trailing comment
`))
	require.NoError(t, err)
	require.Equal(t, []threshold{{pkg: "services/a", min: 88}, {pkg: "services/b", min: 92.5}}, thresholds)
}

func TestReadThresholdsRejectsBadLines(t *testing.T) {
	for _, input := range []string{
		"services/a",
		"services/a eighty",
		"services/a 101",
		"services/a -1",
		"services/a 80 extra",
		"services/a 80\nservices/a 90",
	} {
		_, err := readThresholds(strings.NewReader(input))
		require.Error(t, err, "input %q must be rejected", input)
	}
}

// A block is covered when any test binary covered it. make test runs with
// -coverpkg=./..., so every test binary writes a line for every block, and the
// same block appears once per binary with its own count.
func TestPackageCoverageMergesBlocksAcrossTestBinaries(t *testing.T) {
	profile := `mode: atomic
example.com/mod/services/a/x.go:1.1,2.1 3 0
example.com/mod/services/a/x.go:3.1,4.1 1 0
example.com/mod/services/a/x.go:1.1,2.1 3 0
example.com/mod/services/a/x.go:3.1,4.1 1 5
example.com/mod/services/a/sub/y.go:1.1,2.1 7 9
example.com/mod/services/b/z.go:1.1,2.1 2 1
example.com/mod/services/c/w.go:1.1,2.1 4 1
`
	coverage, err := packageCoverage(strings.NewReader(profile), testModule, map[string]bool{"services/a": true, "services/b": true})
	require.NoError(t, err)

	require.Equal(t, map[string]counts{
		"services/a": {covered: 1, total: 4},
		"services/b": {covered: 2, total: 2},
	}, coverage, "subpackages and packages without a threshold are not counted")
}

func TestPackageCoverageRejectsAProfileWithoutAModeLine(t *testing.T) {
	_, err := packageCoverage(strings.NewReader("example.com/mod/a/x.go:1.1,2.1 1 1\n"), testModule, map[string]bool{"a": true})
	require.Error(t, err)
}

func TestPackageCoverageRejectsAMalformedLine(t *testing.T) {
	_, err := packageCoverage(strings.NewReader("mode: atomic\nexample.com/mod/a/x.go:1.1,2.1 one 1\n"), testModule, map[string]bool{"a": true})
	require.Error(t, err)
}

func TestEvaluate(t *testing.T) {
	thresholds := []threshold{{pkg: "services/a", min: 80}, {pkg: "services/b", min: 90}, {pkg: "services/c", min: 50}}
	coverage := map[string]counts{
		"services/a": {covered: 80, total: 100},
		"services/b": {covered: 899, total: 1000},
	}

	lines, failed := evaluate(thresholds, coverage)
	require.True(t, failed)
	require.Len(t, lines, 3)
	require.Contains(t, lines[0], "services/a")
	require.Contains(t, lines[0], "ok")
	require.Contains(t, lines[1], "services/b")
	require.Contains(t, lines[1], "FAIL")
	require.Contains(t, lines[2], "services/c")
	require.Contains(t, lines[2], "FAIL", "a package with no statements in the profile fails rather than passing unmeasured")

	_, failed = evaluate(thresholds[:1], coverage)
	require.False(t, failed, "coverage exactly at the threshold passes")
}

func TestPackageCoverageRejectsALineWithoutSeparators(t *testing.T) {
	for _, line := range []string{"example.com/mod/a/x.go", "example.com/mod/a/x.go 1", "example.com/mod/a/x.go 1 1"} {
		_, err := packageCoverage(strings.NewReader("mode: atomic\n"+line+"\n"), testModule, map[string]bool{"a": true})
		require.Error(t, err, "line %q must be rejected", line)
	}
}
