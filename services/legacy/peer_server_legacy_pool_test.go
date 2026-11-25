package legacy

import (
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
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

// warnCountingLogger counts Warnf calls; everything else is the no-op TestLogger.
type warnCountingLogger struct {
	ulogger.TestLogger
	warns int
}

func (l *warnCountingLogger) Warnf(string, ...interface{}) {
	l.warns++
}

// TestWarnIfLegacyPeerPoolTokenUnset pins when the legacy service warns at startup that the
// blocks it serves to SV peers are fetched under Asset's per-IP heavy rate limit: only when the
// token that exempts it is unset and that limit is on.
func TestWarnIfLegacyPeerPoolTokenUnset(t *testing.T) {
	tests := []struct {
		name      string
		token     string
		heavyRate int
		wantWarns int
	}{
		{"token unset, heavy limit on", "", 10, 1},
		{"token set, heavy limit on", "a-shared-secret", 10, 0},
		{"token unset, heavy limit off", "", 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tSettings := &settings.Settings{}
			tSettings.Asset.LegacyPeerPoolToken = tt.token
			tSettings.Asset.HTTPHeavyRateLimit = tt.heavyRate

			logger := &warnCountingLogger{}
			warnIfLegacyPeerPoolTokenUnset(logger, tSettings)

			require.Equal(t, tt.wantWarns, logger.warns)
		})
	}
}
