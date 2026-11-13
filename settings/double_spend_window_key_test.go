package settings

import (
	"testing"
	"time"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestDoubleSpendWindow_PrefixedKeyResolution guards the naming fix: the key
// that drives the mining hold-back AND the subtree-processor drain bound was
// the one block-assembly knob without the blockassembly_ prefix, invisible to
// operators searching the config by service name. The prefixed key is now
// canonical; the unprefixed one remains a working deprecated alias, and the
// prefixed key wins when both are set.

func TestDoubleSpendWindow_LegacyAliasStillLoads(t *testing.T) {
	gocore.Config().Set("double_spend_window_millis", "250")
	t.Cleanup(func() { gocore.Config().Set("double_spend_window_millis", "") })

	s := NewSettings()
	require.Equal(t, 250*time.Millisecond, s.BlockAssembly.DoubleSpendWindow,
		"the unprefixed key must keep working - existing deployments set it")
}

func TestDoubleSpendWindow_PrefixedKeyWins(t *testing.T) {
	gocore.Config().Set("double_spend_window_millis", "250")
	gocore.Config().Set("blockassembly_doubleSpendWindowMillis", "900")
	t.Cleanup(func() {
		gocore.Config().Set("double_spend_window_millis", "")
		gocore.Config().Set("blockassembly_doubleSpendWindowMillis", "")
	})

	s := NewSettings()
	require.Equal(t, 900*time.Millisecond, s.BlockAssembly.DoubleSpendWindow,
		"the canonical prefixed key wins over the deprecated alias")
}

func TestDoubleSpendWindow_PrefixedOnly(t *testing.T) {
	gocore.Config().Set("blockassembly_doubleSpendWindowMillis", "750")
	t.Cleanup(func() { gocore.Config().Set("blockassembly_doubleSpendWindowMillis", "") })

	s := NewSettings()
	require.Equal(t, 750*time.Millisecond, s.BlockAssembly.DoubleSpendWindow)
}

func TestDoubleSpendWindow_DefaultIsZero(t *testing.T) {
	gocore.Config().Set("double_spend_window_millis", "")
	gocore.Config().Set("blockassembly_doubleSpendWindowMillis", "")

	s := NewSettings()
	require.Zero(t, s.BlockAssembly.DoubleSpendWindow,
		"no key set anywhere must stay at 0 - immediate dequeue, the shipped default")
}

func TestDoubleSpendWindow_NegativeClampsToZero(t *testing.T) {
	// The loader's "unset" sentinel is -1; a literal negative must never
	// leak through as a negative duration.
	gocore.Config().Set("blockassembly_doubleSpendWindowMillis", "-5")
	t.Cleanup(func() { gocore.Config().Set("blockassembly_doubleSpendWindowMillis", "") })

	s := NewSettings()
	require.Zero(t, s.BlockAssembly.DoubleSpendWindow)
}
