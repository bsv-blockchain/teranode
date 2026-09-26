package legacy

import (
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// TestLegacyPeerPoolHeaders pins pushBlockMsg's internal-pool header decision:
// the shared-secret header is sent only when asset_legacyPeerPoolToken is
// configured, matching Asset's httpimpl.GetLegacyBlock gate exactly (empty
// configured token => no header => Asset's anonymous pool, today's
// single-pool behaviour).
func TestLegacyPeerPoolHeaders(t *testing.T) {
	t.Run("configured token is sent in the internal token header", func(t *testing.T) {
		tSettings := &settings.Settings{}
		tSettings.Asset.LegacyPeerPoolToken = "a-shared-secret"

		headers := legacyPeerPoolHeaders(tSettings)

		require.Equal(t, map[string]string{legacyInternalTokenHeader: "a-shared-secret"}, headers)
	})

	t.Run("empty (default) token sends no header", func(t *testing.T) {
		tSettings := &settings.Settings{}

		headers := legacyPeerPoolHeaders(tSettings)

		require.Nil(t, headers)
	})
}
