package wirepeer_test

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// legacyDaemon starts a daemon configured for wire-peer tests. It goes through
// the exported helper deliberately, so these self-tests exercise the same
// construction path every port uses.
func legacyDaemon(t *testing.T) *daemon.TestDaemon {
	t.Helper()

	return wirepeer.NewLegacyDaemon(t)
}

// TestWirePeerHandshake is the harness self-test for the outbound direction: it
// proves a wirepeer.Peer can complete a BSV-wire handshake against a
// legacy-enabled TestDaemon. Every ported P2P test depends on this, so it is
// worth keeping separate and minimal — when a port fails, this tells you whether
// the harness or the port is at fault.
func TestWirePeerHandshake(t *testing.T) {
	td := legacyDaemon(t)
	defer td.Stop(t)

	p := wirepeer.Connect(t, td)
	defer p.Close()

	// Connect only returns after verack, so reaching here means version/verack
	// completed in both directions.
	require.True(t, p.Connected(), "peer should be connected after handshake")
	require.NotZero(t, p.Count(wire.CmdVersion), "should have recorded the node's version message")
	require.NotNil(t, p.RemoteVersion(), "should have captured the node's version message")

	t.Logf("node user agent: %q, advertised height: %d",
		p.RemoteVersion().UserAgent, p.RemoteVersion().LastBlock)
	t.Logf("node sent: %s", p.Summary())
}

// TestWirePeerMessageIndexOrdersByArrival is the self-test for MessageIndex,
// which exists so ports can assert that one message arrived before another.
// Nothing at a call site would reveal if it started reporting, say, the LAST
// occurrence or an index into a per-command slice: both would still return
// plausible small integers, and an ordering assertion built on either would pass
// or fail for reasons unrelated to the node. The handshake is a fixed known
// order, so it is enough to pin the meaning.
func TestWirePeerMessageIndexOrdersByArrival(t *testing.T) {
	td := legacyDaemon(t)
	defer td.Stop(t)

	p := wirepeer.Connect(t, td)
	defer p.Close()

	// Connect returns only after both, and the node cannot ack a version it has
	// not yet sent, so this order is guaranteed rather than merely observed.
	versionAt := p.MessageIndex(wire.CmdVersion)
	verackAt := p.MessageIndex(wire.CmdVerAck)

	require.NotEqual(t, -1, versionAt, "version should be recorded, got: %s", p.Summary())
	require.NotEqual(t, -1, verackAt, "verack should be recorded, got: %s", p.Summary())
	require.Less(t, versionAt, verackAt, "version arrived before verack, got: %s", p.Summary())

	require.Equal(t, -1, p.MessageIndex("nosuchcommand"),
		"MessageIndex should report -1 for a command that never arrived")
}

// TestWirePeerGetHeadersRoundTrip is the harness self-test for the inbound
// direction: a request we send produces a response the recorder observes. It
// uses getheaders deliberately, because the node serves it directly from the
// blockchain store with no Kafka or announcement machinery involved — so a
// failure here is unambiguously the harness.
func TestWirePeerGetHeadersRoundTrip(t *testing.T) {
	td := legacyDaemon(t)
	defer td.Stop(t)

	// Mine a few blocks so there is something to return beyond genesis.
	td.MineBlocks(t, 3)

	p := wirepeer.Connect(t, td)
	defer p.Close()

	msg := wire.NewMsgGetHeaders()
	msg.ProtocolVersion = wire.ProtocolVersion

	// The locator must name a block the node actually has, or it cannot find a
	// common ancestor. Genesis is the one hash we know exists. A zero stop hash
	// means "send as many as you have after that".
	msg.BlockLocatorHashes = append(msg.BlockLocatorHashes, td.Settings.ChainCfgParams.GenesisHash)
	msg.HashStop = chainhash.Hash{}

	p.Send(t, msg)

	headers := p.WaitForHeaders(t, 30*time.Second)

	require.NotEmpty(t, headers.Headers, "node should return at least one header")
	t.Logf("node returned %d headers; sent: %s", len(headers.Headers), p.Summary())
}

// TestWirePeerRejectsNonBSVUserAgent covers the harness's ability to observe a
// reject, and documents the legacy service's user-agent ban rule that every
// other test in this package has to work around. It is the mechanism the
// upstream bsv-ban-useragents.py port will use.
func TestWirePeerRejectsNonBSVUserAgent(t *testing.T) {
	td := legacyDaemon(t)
	defer td.Stop(t)

	// SkipHandshakeWait because the node answers this version with a reject and a
	// disconnect rather than a verack.
	p := wirepeer.Connect(t, td,
		wirepeer.WithUserAgent("Bitcoin ABC", "0.1"),
		wirepeer.SkipHandshakeWait())
	defer p.Close()

	reject := p.WaitForReject(t, 30*time.Second)

	require.Equal(t, wire.CmdVersion, reject.Cmd, "reject should name the version message")
	require.Contains(t, reject.Reason, "BSV", "reject reason should explain the BSV-only rule")

	t.Logf("node rejected non-BSV agent: %s/%s %q", reject.Cmd, reject.Code, reject.Reason)
}

// TestWirePeerChainParamsAreIsolated proves that moving an activation height for
// one daemon does not move it for anything else in the process.
//
// The isolation is not this package's doing: daemon.NewTestDaemon copies
// RegressionNetParams and repoints ChainCfgParams at the copy before it applies
// SettingsOverrideFunc. But chaincfg.GetChainParams does hand back a pointer to a
// package-level struct, so without that copy a single port setting
// -genesisactivationheight would silently rewrite consensus rules for every other
// test in the binary - a bug that does not fail where it is written, but somewhere
// else, later, looking like flakiness.
//
// So this is a regression lock on someone else's invariant, which is exactly the
// kind worth having: nothing at a call site would reveal if that copy went away,
// and every activation-height port depends on it.
func TestWirePeerChainParamsAreIsolated(t *testing.T) {
	// Read the global before touching anything, so the assertion compares against
	// what the process actually started with rather than a hardcoded expectation
	// that a chaincfg upgrade could silently invalidate.
	globalBefore := chaincfg.RegressionNetParams.GenesisActivationHeight
	require.NotEqual(t, movedGenesisHeight, globalBefore,
		"the test height must differ from the regtest default or this proves nothing")

	td := wirepeer.NewLegacyDaemon(t, wirepeer.WithGenesisActivationHeight(movedGenesisHeight))
	defer td.Stop(t)

	require.Equal(t, movedGenesisHeight, td.Settings.ChainCfgParams.GenesisActivationHeight,
		"the daemon should see the moved height")

	require.Equal(t, globalBefore, chaincfg.RegressionNetParams.GenesisActivationHeight,
		"the shared chaincfg.RegressionNetParams must be untouched; if this fails, every "+
			"other test in this binary is running with rewritten consensus parameters")

	// And a second daemon, built after the first, must still see the default -
	// which is the failure mode a caller would actually hit.
	other := wirepeer.NewLegacyDaemon(t)
	defer other.Stop(t)

	require.Equal(t, globalBefore, other.Settings.ChainCfgParams.GenesisActivationHeight,
		"a daemon that did not ask for a moved height should see the network default")
}

// movedGenesisHeight is an arbitrary height that is not the regtest default. 104
// is upstream's choice in bsv-genesis-pushonly.py, kept for recognisability.
const movedGenesisHeight uint32 = 104
