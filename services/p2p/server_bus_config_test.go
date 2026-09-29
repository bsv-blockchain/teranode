package p2p

import (
	"crypto/rand"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
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
		cases := []struct{ scoring, px bool }{
			{scoring: true, px: true},
			{scoring: true, px: false},
			{scoring: false, px: false},
		}
		for _, tc := range cases {
			conf, err := buildP2PMessageBusConfig(ulogger.TestLogger{}, build(tc.scoring, tc.px, false), privKey, "proto", "off", nil)
			require.NoError(t, err)
			require.Equal(t, tc.scoring, conf.EnablePeerScoring)
			require.Equal(t, !tc.px, conf.DisablePeerExchange,
				"bus DisablePeerExchange must be the inverse of the EnablePeerExchange setting")
		}
	})

	t.Run("PX without scoring is a configuration error", func(t *testing.T) {
		// PX on with scoring off is the spec-violating state the wiring exists to
		// eliminate; it must refuse to start rather than warn into a log nobody greps.
		_, err := buildP2PMessageBusConfig(ulogger.TestLogger{}, build(false, true, false), privKey, "proto", "off", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "p2p_enable_peer_exchange requires p2p_enable_peer_scoring")
	})

	t.Run("scoring sets explicit params and refuses PX records", func(t *testing.T) {
		conf, err := buildP2PMessageBusConfig(ulogger.TestLogger{}, build(true, true, false), privKey, "proto", "off", nil)
		require.NoError(t, err)
		require.NotNil(t, conf.PeerScoreParams)
		require.NotNil(t, conf.PeerScoreThresholds)
		// Penalty-only params cap every score at 0: a positive AcceptPXThreshold
		// means PRUNE-supplied peer records are never accepted, closing the
		// pxConnect-marks-outbound bypass of gossipsub's Dhi/Dout guards.
		require.Greater(t, conf.PeerScoreThresholds.AcceptPXThreshold, 0.0)
		// Public deployments must not whitelist private ranges.
		require.Empty(t, conf.PeerScoreParams.IPColocationFactorWhitelist)
	})

	t.Run("colocation threshold setting is honoured", func(t *testing.T) {
		s := build(true, true, false)
		s.P2P.PeerScoreIPColocationThreshold = 3
		conf, err := buildP2PMessageBusConfig(ulogger.TestLogger{}, s, privKey, "proto", "off", nil)
		require.NoError(t, err)
		require.Equal(t, 3, conf.PeerScoreParams.IPColocationFactorThreshold)

		// 0 keeps the library default rather than disabling the threshold.
		s.P2P.PeerScoreIPColocationThreshold = 0
		conf, err = buildP2PMessageBusConfig(ulogger.TestLogger{}, s, privKey, "proto", "off", nil)
		require.NoError(t, err)
		require.Equal(t, 10, conf.PeerScoreParams.IPColocationFactorThreshold)
	})

	t.Run("private-IP deployments whitelist local ranges from the colocation penalty", func(t *testing.T) {
		conf, err := buildP2PMessageBusConfig(ulogger.TestLogger{}, build(true, true, true), privKey, "proto", "off", nil)
		require.NoError(t, err)
		require.NotNil(t, conf.PeerScoreParams)
		require.NotEmpty(t, conf.PeerScoreParams.IPColocationFactorWhitelist)
	})

	t.Run("scoring disabled must not set score params", func(t *testing.T) {
		// PeerScoreParams being non-nil force-enables scoring in the bus even when
		// EnablePeerScoring is false, so the disabled path must leave them nil.
		conf, err := buildP2PMessageBusConfig(ulogger.TestLogger{}, build(false, false, true), privKey, "proto", "off", nil)
		require.NoError(t, err)
		require.Nil(t, conf.PeerScoreParams)
		require.Nil(t, conf.PeerScoreThresholds)
		require.Nil(t, conf.PeerScoreInspect)
	})

	t.Run("score inspection logs negative and graylisted peers, warns on change only", func(t *testing.T) {
		newPeerID := func() peer.ID {
			_, pub, err := crypto.GenerateEd25519Key(rand.Reader)
			require.NoError(t, err)
			pid, err := peer.IDFromPublicKey(pub)
			require.NoError(t, err)
			return pid
		}
		healthy, negative, graylisted := newPeerID(), newPeerID(), newPeerID()

		logger := &captureLogger{}
		conf, err := buildP2PMessageBusConfig(logger, build(true, true, false), privKey, "proto", "off", nil)
		require.NoError(t, err)
		require.NotNil(t, conf.PeerScoreInspect)
		require.Empty(t, logger.warns, "building the config must not warn when scoring is enabled")

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
		graylistSnapshot := map[peer.ID]*pubsub.PeerScoreSnapshot{
			negative:   {Score: -50},
			graylisted: {Score: -9000},
		}
		conf.PeerScoreInspect(graylistSnapshot)
		require.Len(t, logger.warns, 1)
		require.Contains(t, logger.warns[0], graylisted.String(), "worst offender must be named")

		// Unchanged graylisted state on the next tick: no repeat warn (debug only),
		// so a persistent swarm cannot flood the warn level.
		conf.PeerScoreInspect(graylistSnapshot)
		require.Len(t, logger.warns, 1)

		// State change (swarm shrinks below graylist): warn again on recovery-relevant change.
		conf.PeerScoreInspect(map[peer.ID]*pubsub.PeerScoreSnapshot{
			negative:   {Score: -50},
			graylisted: {Score: -9000},
			healthy:    {Score: -8500},
		})
		require.Len(t, logger.warns, 2)
	})

	t.Run("advertise addresses set announce addrs and port", func(t *testing.T) {
		s := build(true, true, false)
		s.P2P.Port = 9906
		addrs := []string{"/ip4/203.0.113.7/tcp/9906"}
		conf, err := buildP2PMessageBusConfig(ulogger.TestLogger{}, s, privKey, "proto", "off", addrs)
		require.NoError(t, err)
		require.Equal(t, addrs, conf.AnnounceAddrs)
		require.Equal(t, 9906, conf.Port)
	})

	// Committed settings ship with no advertise addresses. Port used to be set
	// only alongside them, so the default deployment bound a random ephemeral
	// port and every firewall rule written for p2p_port pointed at nothing.
	t.Run("port is set without advertise addresses", func(t *testing.T) {
		s := build(true, true, false)
		s.P2P.Port = 9906

		for _, addrs := range [][]string{nil, {}} {
			conf, err := buildP2PMessageBusConfig(ulogger.TestLogger{}, s, privKey, "proto", "off", addrs)
			require.NoError(t, err)
			require.Equal(t, 9906, conf.Port)
			require.Empty(t, conf.AnnounceAddrs)
		}
	})

	// AllowedPublisherIDs must reach the bus config unmodified: the bus, not
	// this wiring, is responsible for augmenting it with the node's own peer ID
	// and StaticPeers' publisher IDs. If this line were dropped, the setting
	// would be fully plumbed through settings.go and documented, yet silently
	// have no effect - every peer's pubsub messages would keep being accepted.
	t.Run("allowed publisher IDs pass through to the bus config", func(t *testing.T) {
		s := build(true, true, false)
		s.P2P.AllowedPublisherIDs = []string{"12D3KooWA1b2C3", "12D3KooWD4e5F6"}
		conf, err := buildP2PMessageBusConfig(ulogger.TestLogger{}, s, privKey, "proto", "off", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"12D3KooWA1b2C3", "12D3KooWD4e5F6"}, conf.AllowedPublisherIDs)
	})

	// The default (empty) allowlist must reach the bus as empty too, since an
	// empty Config.AllowedPublisherIDs is what disables the bus-side filter.
	t.Run("empty allowed publisher IDs default reaches the bus config", func(t *testing.T) {
		conf, err := buildP2PMessageBusConfig(ulogger.TestLogger{}, build(true, true, false), privKey, "proto", "off", nil)
		require.NoError(t, err)
		require.Empty(t, conf.AllowedPublisherIDs)
	})
}

// TestBuildP2PMessageBusConfig_RegtestBootstrap guards against regtest nodes
// dialling the BSVA-managed bootstrap domain or the public IPFS network.
// settings.conf templates p2p_bootstrap_peers from ${network}, but BSVA
// publishes no regtest record. Dropping the entry alone is not enough: the
// message bus treats an empty list as "use the IPFS bootstrap peers" unless
// DisableDefaultBootstrapPeers is set. The bus skips that fallback under
// testing.Testing(), so asserting the flag here is the only guard.
func TestBuildP2PMessageBusConfig_RegtestBootstrap(t *testing.T) {
	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)

	const (
		bsvaRegtest = "/dnsaddr/regtest.bootstrap.teranode.bsvb.tech"
		custom      = "/ip4/10.0.0.5/tcp/9905/p2p/12D3KooWSoovh3vMWJpTMqC1Fj4X7Ywb58L2Wmaa968PG3oHrYjq"
	)

	build := func(params *chaincfg.Params, peers ...string) *settings.Settings {
		s := settings.NewSettings()
		s.ChainCfgParams = params
		s.P2P.BootstrapPeers = peers
		return s
	}

	t.Run("regtest drops BSVA bootstrap domain, keeps operator peers", func(t *testing.T) {
		conf, err := buildP2PMessageBusConfig(ulogger.TestLogger{}, build(&chaincfg.RegressionNetParams, bsvaRegtest, custom), privKey, "proto", "off", nil)
		require.NoError(t, err)
		require.Equal(t, []string{custom}, conf.BootstrapPeers)
		require.True(t, conf.DisableDefaultBootstrapPeers)
	})

	t.Run("regtest with only the default disables the IPFS fallback", func(t *testing.T) {
		conf, err := buildP2PMessageBusConfig(ulogger.TestLogger{}, build(&chaincfg.RegressionNetParams, bsvaRegtest), privKey, "proto", "off", nil)
		require.NoError(t, err)
		require.Empty(t, conf.BootstrapPeers)
		require.True(t, conf.DisableDefaultBootstrapPeers)
	})

	t.Run("regtest drops BSVA entry variants", func(t *testing.T) {
		variants := []string{
			"/dnsaddr/regtest.bootstrap.teranode.bsvb.tech.",
			"/dnsaddr/REGTEST.Bootstrap.Teranode.BSVB.tech",
			"/dnsaddr/regtest.bootstrap.teranode.bsvb.tech/p2p/12D3KooWSoovh3vMWJpTMqC1Fj4X7Ywb58L2Wmaa968PG3oHrYjq",
			"/dnsaddr/bootstrap.teranode.bsvb.tech",
		}
		for _, v := range variants {
			conf, err := buildP2PMessageBusConfig(ulogger.TestLogger{}, build(&chaincfg.RegressionNetParams, v), privKey, "proto", "off", nil)
			require.NoError(t, err)
			require.Empty(t, conf.BootstrapPeers, v)
		}
	})

	t.Run("regtest keeps lookalike and operator dnsaddr entries", func(t *testing.T) {
		keep := []string{
			"/dnsaddr/evilbootstrap.teranode.bsvb.tech",
			"/dnsaddr/seed.example.com",
			"/dns4/regtest.bootstrap.teranode.bsvb.tech/tcp/9905",
		}
		conf, err := buildP2PMessageBusConfig(ulogger.TestLogger{}, build(&chaincfg.RegressionNetParams, keep...), privKey, "proto", "off", nil)
		require.NoError(t, err)
		require.Equal(t, keep, conf.BootstrapPeers)
	})

	t.Run("public networks keep BSVA bootstrap domain and the fallback", func(t *testing.T) {
		mainnet := "/dnsaddr/mainnet.bootstrap.teranode.bsvb.tech"
		conf, err := buildP2PMessageBusConfig(ulogger.TestLogger{}, build(&chaincfg.MainNetParams, mainnet), privKey, "proto", "off", nil)
		require.NoError(t, err)
		require.Equal(t, []string{mainnet}, conf.BootstrapPeers)
		require.False(t, conf.DisableDefaultBootstrapPeers)
	})

	t.Run("nil chain params are left untouched", func(t *testing.T) {
		conf, err := buildP2PMessageBusConfig(ulogger.TestLogger{}, build(nil, bsvaRegtest), privKey, "proto", "off", nil)
		require.NoError(t, err)
		require.Equal(t, []string{bsvaRegtest}, conf.BootstrapPeers)
		require.False(t, conf.DisableDefaultBootstrapPeers)
	})
}

// TestPrivateIPColocationWhitelist pins the whitelist contents: dual-stack local
// clusters (IPv6 ULA/link-local) and cloud CNI pod ranges (RFC6598) must be
// exempt, or AllowPrivateIPs deployments silently lose mesh eligibility.
func TestPrivateIPColocationWhitelist(t *testing.T) {
	nets := privateIPColocationWhitelist()
	require.Len(t, nets, 9)

	contains := func(ipStr string) bool {
		ip := net.ParseIP(ipStr)
		require.NotNil(t, ip, "test IP %q must parse", ipStr)
		for _, n := range nets {
			if n.Contains(ip) {
				return true
			}
		}
		return false
	}
	for _, ip := range []string{"127.0.0.1", "10.1.2.3", "172.20.0.5", "192.168.1.9", "100.72.0.1", "169.254.10.10", "::1", "fd00::1", "fe80::1"} {
		require.True(t, contains(ip), "whitelist must cover %s", ip)
	}
	for _, ip := range []string{"8.8.8.8", "2001:4860:4860::8888"} {
		require.False(t, contains(ip), "whitelist must not cover public IP %s", ip)
	}
}
