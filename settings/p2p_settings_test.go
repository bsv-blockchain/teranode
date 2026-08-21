package settings

import (
	"testing"
	"time"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestP2PPeerMapSettings_LoaderReadsAllKeys guards the exact bug these three
// keys were shipped to fix. All three carried `key:` and `default:` struct tags
// and appeared in the reference docs, but settings are populated by explicit
// getInt/getDuration calls rather than by reflection over those tags, and no
// call existed — so every value stayed at Go zero, the p2p service always fell
// back to its own constants, and setting the key in settings.conf did nothing.
//
// A default-value assertion cannot catch a relapse on its own: the p2p service
// falls back to the same numbers the loader defaults to, deliberately, so the
// node behaves identically whether the loader reads the key or not. Only an
// override proves the key is read.
func TestP2PPeerMapSettings_LoaderReadsAllKeys(t *testing.T) {
	cases := []struct {
		key      string
		override string
		check    func(t *testing.T, s *Settings)
	}{
		{
			key:      "p2p_peer_map_max_size",
			override: "4242",
			check: func(t *testing.T, s *Settings) {
				require.Equal(t, 4242, s.P2P.PeerMapMaxSize,
					"loader must read p2p_peer_map_max_size; otherwise the attribution cap is unconfigurable")
			},
		},
		{
			key:      "p2p_peer_map_ttl",
			override: "7m",
			check: func(t *testing.T, s *Settings) {
				require.Equal(t, 7*time.Minute, s.P2P.PeerMapTTL,
					"loader must read p2p_peer_map_ttl; otherwise the attribution TTL is unconfigurable")
			},
		},
		{
			key:      "p2p_peer_map_cleanup_interval",
			override: "23s",
			check: func(t *testing.T, s *Settings) {
				require.Equal(t, 23*time.Second, s.P2P.PeerMapCleanupInterval,
					"loader must read p2p_peer_map_cleanup_interval; otherwise the sweep interval is unconfigurable")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			gocore.Config().Set(tc.key, tc.override)
			t.Cleanup(func() { gocore.Config().Set(tc.key, "") })

			tc.check(t, NewSettings())
		})
	}
}

// TestP2PSettings_LoaderReadsWebsocketHardeningKeys guards against the same
// class of bug as settings/asset_settings_test.go's
// TestAssetSettings_LoaderReadsAllRateLimitKeys: a struct field exists with a
// `key:` tag but the hand-rolled loader in settings.go doesn't call
// getInt/getMultiString for it, so the value stays at Go zero and the
// documented setting (and its advertised default) is silently unreadable.
//
// This must go through NewSettings(), not a hand-built Settings{} literal -
// a hand-built literal is exactly how this class of bug slipped through
// review previously (see PR #1566 / services/p2p/HandleWebsocket_test.go's
// TestHandleWebSocket_ConnectionCap).
func TestP2PSettings_LoaderReadsWebsocketHardeningKeys(t *testing.T) {
	type kv struct {
		key      string
		override string
		check    func(t *testing.T, s *Settings)
	}

	cases := []kv{
		{
			key:      "p2p_wsMaxConnections",
			override: "555",
			check: func(t *testing.T, s *Settings) {
				require.Equal(t, 555, s.P2P.WSMaxConnections)
			},
		},
		{
			key:      "p2p_wsMaxConnectionsPerIP",
			override: "9",
			check: func(t *testing.T, s *Settings) {
				require.Equal(t, 9, s.P2P.WSMaxConnectionsPerIP)
			},
		},
		{
			key:      "p2p_wsAllowedOrigins",
			override: "https://dashboard.example.com|https://ops.example.com",
			check: func(t *testing.T, s *Settings) {
				require.Equal(t, []string{"https://dashboard.example.com", "https://ops.example.com"}, s.P2P.WSAllowedOrigins)
			},
		},
		{
			key:      "p2p_httpRateLimit",
			override: "77",
			check: func(t *testing.T, s *Settings) {
				require.Equal(t, 77, s.P2P.HTTPRateLimit)
			},
		},
		{
			key:      "p2p_trustedProxyCIDRs",
			override: "10.0.0.0/8",
			check: func(t *testing.T, s *Settings) {
				require.Equal(t, "10.0.0.0/8", s.P2P.TrustedProxyCIDRs)
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

// TestP2PPeerMapSettings_DefaultsMatchTheServiceConstants pins the other half of
// wiring keys that were previously inert: the defaults have to equal what the
// node ran while they were dead, or turning them on is itself a behaviour
// change for every deployment that never set them.
//
// The numbers are duplicated rather than imported because services/p2p depends
// on settings, so the constants cannot be referenced from here without a cycle.
// They must stay equal to defaultPeerMapMaxSize, defaultPeerMapTTL and
// defaultPeerMapCleanupInterval in services/p2p/Server.go.
func TestP2PPeerMapSettings_DefaultsMatchTheServiceConstants(t *testing.T) {
	for _, key := range []string{"p2p_peer_map_max_size", "p2p_peer_map_ttl", "p2p_peer_map_cleanup_interval"} {
		gocore.Config().Set(key, "")
	}

	s := NewSettings()

	require.Equal(t, 10000, s.P2P.PeerMapMaxSize)
	require.Equal(t, 10*time.Minute, s.P2P.PeerMapTTL)
	require.Equal(t, time.Minute, s.P2P.PeerMapCleanupInterval)
}

// TestP2PSettings_LoaderDefaults asserts that the documented defaults from
// the `key:` struct tags actually land on the struct when no override is
// present, not just Go's zero value.
func TestP2PSettings_LoaderDefaults(t *testing.T) {
	s := NewSettings()

	require.Equal(t, 10000, s.P2P.WSMaxConnections)
	require.Equal(t, 100, s.P2P.WSMaxConnectionsPerIP)
	require.Equal(t, 100, s.P2P.HTTPRateLimit)
	require.Empty(t, s.P2P.WSAllowedOrigins)
	require.Empty(t, s.P2P.TrustedProxyCIDRs)
}
