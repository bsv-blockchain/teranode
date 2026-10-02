package settings

import (
	"testing"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestAssetSettings_LoaderReadsAllRateLimitKeys guards against the same class
// of bug as #933 / #643 PR review: a struct field exists with a `key:` tag but
// the hand-rolled loader doesn't call getInt/getString for it, so the value
// stays at Go zero and the documented setting is silently unreadable.
//
// Defaults for HTTPMinerRateLimit (0) and PeerAuthAllowlist ("") happen to
// equal Go zero, so a default-value assertion would pass spuriously. The
// only honest test is: set a non-zero override, call NewSettings(), assert
// the field changed.
func TestAssetSettings_LoaderReadsAllRateLimitKeys(t *testing.T) {
	type kv struct {
		key      string
		override string
		check    func(t *testing.T, s *Settings)
	}

	cases := []kv{
		{
			key:      "asset_httpRateLimit",
			override: "777",
			check: func(t *testing.T, s *Settings) {
				require.Equal(t, 777, s.Asset.HTTPRateLimit)
			},
		},
		{
			key:      "asset_httpHeavyRateLimit",
			override: "33",
			check: func(t *testing.T, s *Settings) {
				require.Equal(t, 33, s.Asset.HTTPHeavyRateLimit)
			},
		},
		{
			key:      "asset_httpPeerRateMultiplier",
			override: "9",
			check: func(t *testing.T, s *Settings) {
				require.Equal(t, 9, s.Asset.HTTPPeerRateMultiplier)
			},
		},
		{
			key:      "asset_httpMinerRateLimit",
			override: "12345",
			check: func(t *testing.T, s *Settings) {
				require.Equal(t, 12345, s.Asset.HTTPMinerRateLimit,
					"loader must read asset_httpMinerRateLimit; otherwise the M3 miner cap is permanently unfastenable")
			},
		},
		{
			key:      "asset_httpBodyLimit",
			override: "42MB",
			check: func(t *testing.T, s *Settings) {
				require.Equal(t, "42MB", s.Asset.HTTPBodyLimit)
			},
		},
		{
			key:      "asset_trustedProxyCIDRs",
			override: "10.0.0.0/8",
			check: func(t *testing.T, s *Settings) {
				require.Equal(t, "10.0.0.0/8", s.Asset.TrustedProxyCIDRs)
			},
		},
		{
			key:      "asset_peerMinerReputationThreshold",
			override: "75.5",
			check: func(t *testing.T, s *Settings) {
				require.InDelta(t, 75.5, s.Asset.PeerMinerReputationThreshold, 0.001)
			},
		},
		{
			key:      "asset_peerAuthAllowlist",
			override: "12D3KooWAFXWuxgdJoRsaA4J4RRRr8yu6WCrAPf8FaS7UfZg3ceG",
			check: func(t *testing.T, s *Settings) {
				require.Equal(t, "12D3KooWAFXWuxgdJoRsaA4J4RRRr8yu6WCrAPf8FaS7UfZg3ceG", s.Asset.PeerAuthAllowlist,
					"loader must read asset_peerAuthAllowlist; otherwise the C3 allowlist gate cannot be turned on")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			gocore.Config().Set(tc.key, tc.override)
			t.Cleanup(func() { gocore.Config().Set(tc.key, "") })

			s := NewSettings()
			tc.check(t, s)
		})
	}
}

// TestAssetSettings_BatchAndResponseBudgets is the P0 prep PR guard: every new
// admission-budget key added ahead of the follow-up security PRs must (a) parse
// into the struct via NewSettings(), the same class of bug as above, and (b)
// default to today's behaviour (unlimited/fail-open/verbose) so this PR is a
// pure no-op at runtime.
func TestAssetSettings_BatchAndResponseBudgets(t *testing.T) {
	t.Run("defaults preserve current behaviour", func(t *testing.T) {
		s := NewSettings()

		require.Equal(t, 0, s.Asset.MaxBatchRecords)
		require.Equal(t, int64(0), s.Asset.MaxBatchResponseBytes)
		require.Equal(t, 0, s.Asset.MaxUTXOsPerTx)
		require.Equal(t, 0, s.Asset.MaxBlockHeaders)
		require.Equal(t, 0, s.Asset.MaxLastNBlocks)
		require.Equal(t, 0, s.Asset.MaxNBlocks)
		require.False(t, s.Asset.RequireAuthCredentials)
		require.False(t, s.Asset.SecureCookies)
		require.Equal(t, "", s.Asset.CORSAllowedOrigins)
		require.False(t, s.Asset.EnforcePostAuth)
		require.Equal(t, 0, s.Asset.MaxWebsocketConnections)
		require.Equal(t, int64(0), s.Asset.WebsocketReadLimit)
		require.Equal(t, 0, s.Asset.SubtreeStreamConcurrency)
		require.True(t, s.Asset.PublicErrorDetail)
		require.True(t, s.Asset.PublicHealthDetail)
		require.False(t, s.Asset.HealthStrictStatus)
		require.True(t, s.Asset.PublicPeersDetail)
		require.True(t, s.Asset.TxMetaRawEnabled)
		require.Equal(t, 0, s.Asset.MaxBlockGraphPoints)
		require.Equal(t, 0, s.Asset.MaxLocatorWalkDepth)
	})

	type kv struct {
		key      string
		override string
		check    func(t *testing.T, s *Settings)
	}

	cases := []kv{
		{"asset_maxBatchRecords", "5000", func(t *testing.T, s *Settings) {
			require.Equal(t, 5000, s.Asset.MaxBatchRecords)
		}},
		{"asset_maxBatchResponseBytes", "104857600", func(t *testing.T, s *Settings) {
			require.Equal(t, int64(104857600), s.Asset.MaxBatchResponseBytes)
		}},
		{"asset_maxUTXOsPerTx", "1000", func(t *testing.T, s *Settings) {
			require.Equal(t, 1000, s.Asset.MaxUTXOsPerTx)
		}},
		{"asset_maxBlockHeaders", "2000", func(t *testing.T, s *Settings) {
			require.Equal(t, 2000, s.Asset.MaxBlockHeaders)
		}},
		{"asset_maxLastNBlocks", "500", func(t *testing.T, s *Settings) {
			require.Equal(t, 500, s.Asset.MaxLastNBlocks)
		}},
		{"asset_maxNBlocks", "500", func(t *testing.T, s *Settings) {
			require.Equal(t, 500, s.Asset.MaxNBlocks)
		}},
		{"asset_requireAuthCredentials", "true", func(t *testing.T, s *Settings) {
			require.True(t, s.Asset.RequireAuthCredentials)
		}},
		{"asset_secureCookies", "true", func(t *testing.T, s *Settings) {
			require.True(t, s.Asset.SecureCookies)
		}},
		{"asset_corsAllowedOrigins", "https://dashboard.example.com", func(t *testing.T, s *Settings) {
			require.Equal(t, "https://dashboard.example.com", s.Asset.CORSAllowedOrigins)
		}},
		{"asset_enforcePostAuth", "true", func(t *testing.T, s *Settings) {
			require.True(t, s.Asset.EnforcePostAuth)
		}},
		{"asset_maxWebsocketConnections", "250", func(t *testing.T, s *Settings) {
			require.Equal(t, 250, s.Asset.MaxWebsocketConnections)
		}},
		{"asset_websocketReadLimit", "1048576", func(t *testing.T, s *Settings) {
			require.Equal(t, int64(1048576), s.Asset.WebsocketReadLimit)
		}},
		{"asset_subtreeStreamConcurrency", "8", func(t *testing.T, s *Settings) {
			require.Equal(t, 8, s.Asset.SubtreeStreamConcurrency)
		}},
		{"asset_publicErrorDetail", "false", func(t *testing.T, s *Settings) {
			require.False(t, s.Asset.PublicErrorDetail)
		}},
		{"asset_publicHealthDetail", "false", func(t *testing.T, s *Settings) {
			require.False(t, s.Asset.PublicHealthDetail)
		}},
		{"asset_healthStrictStatus", "true", func(t *testing.T, s *Settings) {
			require.True(t, s.Asset.HealthStrictStatus)
		}},
		{"asset_publicPeersDetail", "false", func(t *testing.T, s *Settings) {
			require.False(t, s.Asset.PublicPeersDetail)
		}},
		{"asset_txMetaRawEnabled", "false", func(t *testing.T, s *Settings) {
			require.False(t, s.Asset.TxMetaRawEnabled)
		}},
		{"asset_maxBlockGraphPoints", "3000", func(t *testing.T, s *Settings) {
			require.Equal(t, 3000, s.Asset.MaxBlockGraphPoints)
		}},
		{"asset_maxLocatorWalkDepth", "1500", func(t *testing.T, s *Settings) {
			require.Equal(t, 1500, s.Asset.MaxLocatorWalkDepth)
		}},
	}

	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			gocore.Config().Set(tc.key, tc.override)
			t.Cleanup(func() { gocore.Config().Set(tc.key, "") })

			s := NewSettings()
			tc.check(t, s)
		})
	}
}
