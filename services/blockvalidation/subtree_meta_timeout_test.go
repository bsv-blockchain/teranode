package blockvalidation

import (
	"testing"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// TestSubtreeMetaPeerFetchTimeout_ConstantsDoNotDrift pins the two copies of the
// regenerator's default peer-fetch budget together.
//
// model must not import settings, so the value exists twice:
// model.DefaultPeerFetchTimeout is the fallback the regenerator applies when it
// is handed a non-positive timeout, and settings.DefaultSubtreeMetaPeerFetchTimeout
// is what the loader substitutes for an unset key. Nothing but this test stops
// them drifting, and a drift would mean the documented default and the effective
// one disagree depending on whether the key is present.
func TestSubtreeMetaPeerFetchTimeout_ConstantsDoNotDrift(t *testing.T) {
	require.Equal(t, settings.DefaultSubtreeMetaPeerFetchTimeout, model.DefaultPeerFetchTimeout,
		"the regenerator's own fallback and the settings default must be the same duration")
}

// TestSubtreeMetaPeerFetchTimeout_IsWired proves the key reaches the struct.
// A struct tag alone does not load a setting — only an entry in settings.go
// does — so without this a rename or a dropped loader line would silently leave
// operators unable to change the bound.
func TestSubtreeMetaPeerFetchTimeout_IsWired(t *testing.T) {
	tSettings := settings.NewSettings()

	require.Equal(t, settings.DefaultSubtreeMetaPeerFetchTimeout, tSettings.BlockValidation.SubtreeMetaPeerFetchTimeout,
		"committed settings.conf must carry the documented default through the loader")
}
