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
