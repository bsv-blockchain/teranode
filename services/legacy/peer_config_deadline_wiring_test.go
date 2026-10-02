package legacy

import (
	"net"
	"testing"

	"github.com/bsv-blockchain/teranode/services/legacy/addrmgr"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestNewPeerConfigWiresDownloadDeadlineCallbacks pins the wiring, not the maths.
//
// The block-download ceiling widens while we are catching up and widens further
// per peer we are downloading from, but the peer layer can only know either of
// those things through callbacks supplied here. If this wiring is dropped, both
// callbacks are nil, the peer silently falls back to the shortest ceiling, and
// nothing else in the tree notices — the package stays green and the feature is
// dead. That has already happened twice in this series of changes, once with
// four settings that no configuration could ever activate, so it is worth a test
// of its own rather than trusting the call site to stay put.
func TestNewPeerConfigWiresDownloadDeadlineCallbacks(t *testing.T) {
	// newPeerConfig reads the package-level cfg global, which unit tests leave nil.
	// Set it for the duration of this test and put it back afterwards.
	prev := cfg
	cfg = &config{}

	t.Cleanup(func() { cfg = prev })

	amgr := addrmgr.New(ulogger.TestLogger{}, t.TempDir(), func(string) ([]net.IP, error) { return nil, nil })
	sp := &serverPeer{server: &server{settings: settings.NewSettings(), addrManager: amgr}}

	// Deliberately not named cfg: that is the package-level global this test is
	// swapping out above, and shadowing it here would hide the restore.
	peerCfg := newPeerConfig(sp)
	require.NotNil(t, peerCfg)

	require.NotNil(t, peerCfg.CatchingUp,
		"CatchingUp must be wired or the catch-up ceiling can never apply")
	require.NotNil(t, peerCfg.PeersWithBlockDownloads,
		"PeersWithBlockDownloads must be wired or the per-peer compensation can never apply")

	// With no sync manager attached both must answer safely rather than panic:
	// "not catching up" and "no peers", which together give the shortest ceiling.
	require.False(t, peerCfg.CatchingUp(),
		"a peer with no sync manager must not claim to be catching up")
	require.Zero(t, peerCfg.PeersWithBlockDownloads(),
		"a peer with no sync manager must not report downloading peers")
}
