package settings

import (
	"testing"
	"time"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestBlockChainSettings_LoaderReadsAllKeys guards against the field-exists-
// but-loader-never-reads-it bug: each of these fields has a `key:` struct
// tag, but if NewSettings() does not call the matching getX helper for it,
// the value stays at Go zero and the documented setting is silently
// unreadable. All three default to the Go zero value, so a default-value
// assertion alone would pass spuriously — the honest test is: set an
// override via gocore, call NewSettings(), assert the field reflects it.
func TestBlockChainSettings_LoaderReadsAllKeys(t *testing.T) {
	type kv struct {
		key      string
		override string
		check    func(t *testing.T, s *Settings)
	}

	cases := []kv{
		{
			key:      "blockchain_raw_miner_tag",
			override: "true",
			check: func(t *testing.T, s *Settings) {
				require.True(t, s.BlockChain.RawMinerTag)
			},
		},
		{
			key:      "blockchain_peerRegistryStore",
			override: "file:///tmp/peer-registry",
			check: func(t *testing.T, s *Settings) {
				require.NotNil(t, s.BlockChain.PeerRegistryStore)
				require.Equal(t, "file:///tmp/peer-registry", s.BlockChain.PeerRegistryStore.String())
			},
		},
		{
			key:      "blockchain_peerRegistrySaveInterval",
			override: "5s",
			check: func(t *testing.T, s *Settings) {
				require.Equal(t, 5*time.Second, s.BlockChain.PeerRegistrySaveInterval)
			},
		},
	}

	// All three default to the disabled/off Go zero value, so wiring them is
	// a no-op until an operator opts in.
	def := NewSettings()
	require.False(t, def.BlockChain.RawMinerTag)
	require.Nil(t, def.BlockChain.PeerRegistryStore)
	require.Equal(t, 60*time.Second, def.BlockChain.PeerRegistrySaveInterval,
		"default save interval must stay 60s even though it only takes effect once PeerRegistryStore is set")

	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			gocore.Config().Set(tc.key, tc.override)
			t.Cleanup(func() { gocore.Config().Set(tc.key, "") })

			s := NewSettings()
			tc.check(t, s)
		})
	}
}
