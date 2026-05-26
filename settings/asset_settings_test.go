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
