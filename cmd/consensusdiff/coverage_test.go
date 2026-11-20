package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadModuleCoverage(t *testing.T) {
	modules := []string{
		"github.com/bsv-blockchain/go-bt/v2",
		"github.com/bsv-blockchain/go-wire",
		"github.com/bsv-blockchain/go-bt", // a shorter path must not take go-bt/v2's blocks
	}

	t.Run("statements and executed statements per module", func(t *testing.T) {
		profile := strings.Join([]string{
			"mode: set",
			"github.com/bsv-blockchain/go-bt/v2/tx.go:10.2,12.3 2 1",
			"github.com/bsv-blockchain/go-bt/v2/tx.go:14.2,15.3 3 0",
			"github.com/bsv-blockchain/go-bt/v2/tx.go:14.2,15.3 3 1", // same block from another binary: ran
			"github.com/bsv-blockchain/go-bt/v2/input.go:1.1,2.2 4 0",
			"github.com/bsv-blockchain/go-wire/msg.go:5.1,6.2 7 0",
			"github.com/bsv-blockchain/teranode/model/block.go:1.1,2.2 9 1",
			"",
		}, "\n")

		coverage, err := readModuleCoverage(strings.NewReader(profile), modules)
		require.NoError(t, err)
		require.Equal(t, map[string]moduleCoverage{
			"github.com/bsv-blockchain/go-bt/v2": {covered: 5, total: 9},
			"github.com/bsv-blockchain/go-wire":  {covered: 0, total: 7},
			"github.com/bsv-blockchain/go-bt":    {},
		}, coverage)
	})

	t.Run("a malformed line is an error", func(t *testing.T) {
		_, err := readModuleCoverage(strings.NewReader("mode: set\ngithub.com/x/y.go:1.1,2.2 two 1\n"), modules)
		require.ErrorContains(t, err, "bad numbers")

		_, err = readModuleCoverage(strings.NewReader("mode: set\ngithub.com/x/y.go:1.1,2.2 1\n"), modules)
		require.ErrorContains(t, err, "want")
	})
}
