package p2p

import (
	"crypto/rand"
	"fmt"
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

// captureLogger records Warnf/Debugf lines so tests can assert on the score
// inspection output; everything else is inherited no-op TestLogger behaviour.
type captureLogger struct {
	ulogger.TestLogger
	mu     sync.Mutex
	warns  []string
	debugs []string
}

func (l *captureLogger) Warnf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, fmt.Sprintf(format, args...))
}

func (l *captureLogger) Debugf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.debugs = append(l.debugs, fmt.Sprintf(format, args...))
}

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

	t.Run("score inspection logs negative and graylisted peers", func(t *testing.T) {
		newPeerID := func() peer.ID {
			_, pub, err := crypto.GenerateEd25519Key(rand.Reader)
			require.NoError(t, err)
			pid, err := peer.IDFromPublicKey(pub)
			require.NoError(t, err)
			return pid
		}
		healthy, negative, graylisted := newPeerID(), newPeerID(), newPeerID()

		logger := &captureLogger{}
		conf := buildP2PMessageBusConfig(logger, build(true, true, false), privKey, "proto", "off", nil)
		require.NotNil(t, conf.PeerScoreInspect)

		// Healthy mesh (positive scores, nil snapshots): silence.
		conf.PeerScoreInspect(map[peer.ID]*pubsub.PeerScoreSnapshot{
			healthy:  {Score: 1},
			negative: nil,
		})
		require.Empty(t, logger.warns)
		require.Empty(t, logger.debugs)

		// Negative but above the graylist threshold: debug only.
		conf.PeerScoreInspect(map[peer.ID]*pubsub.PeerScoreSnapshot{
			healthy:  {Score: 1},
			negative: {Score: -50},
		})
		require.Empty(t, logger.warns)
		require.Len(t, logger.debugs, 1)
		require.Contains(t, logger.debugs[0], negative.String(), "worst offender must be named")

		// Below the graylist threshold: warn, naming the worst offender.
		conf.PeerScoreInspect(map[peer.ID]*pubsub.PeerScoreSnapshot{
			negative:   {Score: -50},
			graylisted: {Score: -9000},
		})
		require.Len(t, logger.warns, 1)
		require.Contains(t, logger.warns[0], graylisted.String(), "worst offender must be named")
	})

	t.Run("advertise addresses set announce addrs and port", func(t *testing.T) {
		s := build(true, true, false)
		s.P2P.Port = 9906
		addrs := []string{"/ip4/203.0.113.7/tcp/9906"}
		conf := buildP2PMessageBusConfig(ulogger.TestLogger{}, s, privKey, "proto", "off", addrs)
		require.Equal(t, addrs, conf.AnnounceAddrs)
		require.Equal(t, 9906, conf.Port)
	})
}
