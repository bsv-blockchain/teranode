package parity

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/svp2p/svp2ptest"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// Scenario is one parity case: an identical scripted peer set is built for each
// implementation, the node is driven, observed and torn down, and the two
// observations are compared.
type Scenario struct {
	Name string
	// Chain is the fixture chain height.
	Chain int
	// Pad is OP_RETURN bytes per coinbase (BuildFixtureChainPadded); 0 = none.
	Pad int
	// Tweaks adjust the settings of BOTH legs before the peers and the node are
	// built. They may replace ChainCfgParams (for instance to add a checkpoint on
	// the fixture chain); the peers are then built with the tweaked wire magic.
	Tweaks []func(impl Impl, chain *svp2ptest.FixtureChain, s *settings.Settings)
	// Peers builds the peer set for one leg. Index i is reported as "peer<i>".
	Peers func(t *testing.T, chain *svp2ptest.FixtureChain, net wire.BitcoinNet) []*svp2ptest.ScriptedPeer
	// Connect picks which peers the node dials (legacy_connect_peers); nil
	// means all of them. Peers left out are expected to Dial the node in Drive.
	Connect func(peers []*svp2ptest.ScriptedPeer) []string
	// Drive runs the scenario against a started node.
	Drive func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer)
	// Observe reads the externally visible facts; ObserveDefault covers most.
	Observe func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) Observation
	// Accepted lists fields expected to differ, with the reason.
	Accepted []Divergence
	// Only restricts the legs run; empty means both. With one leg there is no
	// comparison and the returned Result is empty.
	Only []Impl
}

// RunParity runs the scenario for each implementation, compares the results,
// writes the report and fails the test on any unaccepted difference.
func RunParity(t *testing.T, s Scenario) (map[Impl]Observation, Result) {
	t.Helper()

	impls := s.Only
	if len(impls) == 0 {
		impls = []Impl{Legacy, Svp2p}
	}

	obs := make(map[Impl]Observation, len(impls))
	transcripts := make(map[Impl][]*svp2ptest.Transcript, len(impls))

	for _, impl := range impls {
		obs[impl], transcripts[impl] = runLeg(t, s, impl)
	}

	var res Result

	if len(impls) == 2 {
		res = Compare(obs[Legacy], obs[Svp2p], s.Accepted)
	}

	WriteReport(t, s.Name, obs[Legacy], obs[Svp2p], res, transcripts)

	require.Empty(t, res.Diffs, "parity diffs for %s", s.Name)

	return obs, res
}

func runLeg(t *testing.T, s Scenario, impl Impl) (Observation, []*svp2ptest.Transcript) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	chain := svp2ptest.BuildFixtureChainPadded(t, tSettings, s.Chain, s.Pad)

	for _, tweak := range s.Tweaks {
		tweak(impl, chain, tSettings)
	}

	peers := s.Peers(t, chain, tSettings.ChainCfgParams.Net)

	var addrs []string

	if s.Connect != nil {
		addrs = s.Connect(peers)
	} else {
		for _, p := range peers {
			addrs = append(addrs, p.Addr)
		}
	}

	n := newNode(t, impl, addrs, func(dst *settings.Settings) {
		dst.ChainCfgParams = tSettings.ChainCfgParams

		for _, tweak := range s.Tweaks {
			tweak(impl, chain, dst)
		}
	})

	start := time.Now()

	s.Drive(t, n, peers)

	o := s.Observe(t, n, peers)
	o.WallClock = time.Since(start)

	transcripts := make([]*svp2ptest.Transcript, len(peers))
	for i, p := range peers {
		transcripts[i] = p.Transcript
	}

	n.Stop()

	for _, p := range peers {
		p.Close()
	}

	return o, transcripts
}

// ObserveDefault reads the facts every scenario can see: the chain height and,
// per peer, the block requests it received, the blocks it served and who closed
// its connection. Scores are left to scenarios that read them.
func ObserveDefault(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) Observation {
	t.Helper()

	o := Observation{
		BlocksAccepted: n.BestHeight(t),
		Requests:       make(map[string]int, len(peers)),
		Served:         make(map[string]int, len(peers)),
		Disconnected:   make(map[string]string),
		Scores:         make(map[string]int),
		Connections:    make(map[string]int, len(peers)),
	}

	for i, p := range peers {
		name := peerName(i)
		o.Requests[name] = p.RequestedCount()
		o.GetHeadersIn += p.Transcript.Count(svp2ptest.In, "getheaders")
		o.Served[name] = p.ServedBlocks()
		o.Connections[name] = p.Connections()

		if who := p.Transcript.ClosedBy(); who != "" {
			o.Disconnected[name] = who
		}
	}

	return o
}

func peerName(i int) string { return "peer" + string(rune('0'+i)) }
