package p2p

import (
	"crypto/rand"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/stretchr/testify/require"
)

// TestBuildP2PMessageBusConfig_MeshProtection guards the GossipSub Sybil-defence
// wiring from settings into the message bus config. The riskiest line is the
// enable->disable inversion for peer exchange: flipping it silently disables PX
// in production while every other test stays green.
func TestBuildP2PMessageBusConfig_MeshProtection(t *testing.T) {
	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)

	build := func(scoring, px, privateIPs bool) *settings.Settings {
		s := settings.NewSettings()
		s.P2P.EnablePeerScoring = scoring
		s.P2P.EnablePeerExchange = px
		s.P2P.AllowPrivateIPs = privateIPs
		return s
	}

	t.Run("scoring and PX flags pass through, PX inverted", func(t *testing.T) {
		for _, scoring := range []bool{true, false} {
			for _, px := range []bool{true, false} {
				conf := buildP2PMessageBusConfig(ulogger.TestLogger{}, build(scoring, px, false), privKey, "proto", "off", nil)
				require.Equal(t, scoring, conf.EnablePeerScoring)
				require.Equal(t, !px, conf.DisablePeerExchange,
					"bus DisablePeerExchange must be the inverse of the EnablePeerExchange setting")
			}
		}
	})

	t.Run("private-IP deployments whitelist local ranges from the colocation penalty", func(t *testing.T) {
		conf := buildP2PMessageBusConfig(ulogger.TestLogger{}, build(true, true, true), privKey, "proto", "off", nil)
		require.NotNil(t, conf.PeerScoreParams)
		require.NotNil(t, conf.PeerScoreThresholds)
		require.NotEmpty(t, conf.PeerScoreParams.IPColocationFactorWhitelist)
	})

	t.Run("scoring disabled must not set score params", func(t *testing.T) {
		// PeerScoreParams being non-nil force-enables scoring in the bus even when
		// EnablePeerScoring is false, so the disabled path must leave them nil.
		conf := buildP2PMessageBusConfig(ulogger.TestLogger{}, build(false, true, true), privKey, "proto", "off", nil)
		require.Nil(t, conf.PeerScoreParams)
		require.Nil(t, conf.PeerScoreThresholds)
		require.Nil(t, conf.PeerScoreInspect)
	})

	t.Run("score inspection is wired when scoring is on", func(t *testing.T) {
		conf := buildP2PMessageBusConfig(ulogger.TestLogger{}, build(true, true, false), privKey, "proto", "off", nil)
		require.NotNil(t, conf.PeerScoreInspect)
		// Public deployments keep the library defaults (no explicit params).
		require.Nil(t, conf.PeerScoreParams)
	})
}
