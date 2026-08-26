package settings

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseFeeSizeTiers(t *testing.T) {
	t.Run("empty input returns nil", func(t *testing.T) {
		require.Nil(t, parseFeeSizeTiers(nil))
		require.Nil(t, parseFeeSizeTiers([]string{}))
		require.Nil(t, parseFeeSizeTiers([]string{"", "  "}))
	})

	t.Run("single tier", func(t *testing.T) {
		tiers := parseFeeSizeTiers([]string{"1000000:10"})
		require.Equal(t, []FeeSizeTier{{SizeBytes: 1_000_000, SatoshisPerKB: 10}}, tiers)
	})

	t.Run("multiple tiers are sorted ascending by size", func(t *testing.T) {
		tiers := parseFeeSizeTiers([]string{"10000000:50", "1000000:10"})
		require.Equal(t, []FeeSizeTier{
			{SizeBytes: 1_000_000, SatoshisPerKB: 10},
			{SizeBytes: 10_000_000, SatoshisPerKB: 50},
		}, tiers)
	})

	t.Run("surrounding whitespace is tolerated", func(t *testing.T) {
		tiers := parseFeeSizeTiers([]string{" 1000000 : 10 "})
		require.Equal(t, []FeeSizeTier{{SizeBytes: 1_000_000, SatoshisPerKB: 10}}, tiers)
	})

	t.Run("missing separator panics", func(t *testing.T) {
		require.Panics(t, func() { parseFeeSizeTiers([]string{"1000000"}) })
	})

	t.Run("non-numeric size panics", func(t *testing.T) {
		require.Panics(t, func() { parseFeeSizeTiers([]string{"abc:10"}) })
	})

	t.Run("zero size panics", func(t *testing.T) {
		require.Panics(t, func() { parseFeeSizeTiers([]string{"0:10"}) })
	})

	t.Run("non-numeric rate panics", func(t *testing.T) {
		require.Panics(t, func() { parseFeeSizeTiers([]string{"1000000:xyz"}) })
	})

	t.Run("negative rate panics", func(t *testing.T) {
		require.Panics(t, func() { parseFeeSizeTiers([]string{"1000000:-1"}) })
	})

	t.Run("duplicate size threshold panics", func(t *testing.T) {
		require.Panics(t, func() { parseFeeSizeTiers([]string{"1000000:10", "1000000:20"}) })
	})

	t.Run("decreasing rate across ascending sizes panics", func(t *testing.T) {
		require.Panics(t, func() { parseFeeSizeTiers([]string{"1000000:50", "10000000:10"}) })
	})
}

func TestNewSettingsMinMiningTxFeeBySize(t *testing.T) {
	t.Run("unset means disabled", func(t *testing.T) {
		s := NewSettings()
		require.Empty(t, s.Policy.MinMiningTxFeeBySize)
	})

	t.Run("parsed from the pipe-separated setting", func(t *testing.T) {
		t.Setenv("minminingtxfeebysize", "1000000:10|10000000:50")

		s := NewSettings()
		require.Equal(t, []FeeSizeTier{
			{SizeBytes: 1_000_000, SatoshisPerKB: 10},
			{SizeBytes: 10_000_000, SatoshisPerKB: 50},
		}, s.Policy.MinMiningTxFeeBySize)
	})
}
