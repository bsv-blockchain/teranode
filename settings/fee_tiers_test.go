package settings

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseFeeTiers(t *testing.T) {
	t.Run("empty input returns nil", func(t *testing.T) {
		require.Nil(t, parseFeeTiers("minminingtxfeebyscriptsize", nil))
		require.Nil(t, parseFeeTiers("minminingtxfeebyscriptsize", []string{}))
		require.Nil(t, parseFeeTiers("minminingtxfeebyscriptsize", []string{"", "  "}))
	})

	t.Run("single tier", func(t *testing.T) {
		tiers := parseFeeTiers("minminingtxfeebyscriptsize", []string{"500000:10"})
		require.Equal(t, []FeeTier{{Threshold: 500_000, SatoshisPerK: 10}}, tiers)
	})

	t.Run("multiple tiers are sorted ascending by threshold", func(t *testing.T) {
		tiers := parseFeeTiers("minminingtxfeebyscriptops", []string{"10000000:50", "1000000:10"})
		require.Equal(t, []FeeTier{
			{Threshold: 1_000_000, SatoshisPerK: 10},
			{Threshold: 10_000_000, SatoshisPerK: 50},
		}, tiers)
	})

	t.Run("surrounding whitespace is tolerated", func(t *testing.T) {
		tiers := parseFeeTiers("minminingtxfeebyscriptsize", []string{" 500000 : 10 "})
		require.Equal(t, []FeeTier{{Threshold: 500_000, SatoshisPerK: 10}}, tiers)
	})

	t.Run("missing separator panics", func(t *testing.T) {
		require.Panics(t, func() { parseFeeTiers("minminingtxfeebyscriptsize", []string{"500000"}) })
	})

	t.Run("non-numeric threshold panics", func(t *testing.T) {
		require.Panics(t, func() { parseFeeTiers("minminingtxfeebyscriptsize", []string{"abc:10"}) })
	})

	t.Run("zero threshold panics", func(t *testing.T) {
		require.Panics(t, func() { parseFeeTiers("minminingtxfeebyscriptsize", []string{"0:10"}) })
	})

	t.Run("non-numeric rate panics", func(t *testing.T) {
		require.Panics(t, func() { parseFeeTiers("minminingtxfeebyscriptsize", []string{"500000:xyz"}) })
	})

	t.Run("negative rate panics", func(t *testing.T) {
		require.Panics(t, func() { parseFeeTiers("minminingtxfeebyscriptsize", []string{"500000:-1"}) })
	})

	t.Run("duplicate threshold panics", func(t *testing.T) {
		require.Panics(t, func() { parseFeeTiers("minminingtxfeebyscriptsize", []string{"500000:10", "500000:20"}) })
	})

	t.Run("decreasing rate across ascending thresholds panics", func(t *testing.T) {
		require.Panics(t, func() { parseFeeTiers("minminingtxfeebyscriptsize", []string{"500000:50", "5000000:10"}) })
	})
}

func TestNewSettingsScriptFeeTiers(t *testing.T) {
	t.Run("unset means disabled", func(t *testing.T) {
		s := NewSettings()
		require.Empty(t, s.Policy.MinMiningTxFeeByScriptSize)
		require.Empty(t, s.Policy.MinMiningTxFeeByScriptOps)
	})

	t.Run("parsed from the pipe-separated settings", func(t *testing.T) {
		t.Setenv("minminingtxfeebyscriptsize", "500000:10|10000000:50")
		t.Setenv("minminingtxfeebyscriptops", "1000000:10")

		s := NewSettings()
		require.Equal(t, []FeeTier{
			{Threshold: 500_000, SatoshisPerK: 10},
			{Threshold: 10_000_000, SatoshisPerK: 50},
		}, s.Policy.MinMiningTxFeeByScriptSize)
		require.Equal(t, []FeeTier{
			{Threshold: 1_000_000, SatoshisPerK: 10},
		}, s.Policy.MinMiningTxFeeByScriptOps)
	})
}
