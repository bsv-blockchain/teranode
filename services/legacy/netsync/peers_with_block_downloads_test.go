package netsync

import (
	"strconv"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func newTestPeer(t *testing.T, addr string) *peerpkg.Peer {
	t.Helper()

	p, err := peerpkg.NewOutboundPeer(ulogger.TestLogger{}, settings.NewSettings(), &peerpkg.Config{}, addr)
	require.NoError(t, err)

	return p
}

// TestPeersWithBlockDownloads pins the count that drives the per-peer widening
// of a block's download deadline. The number this returns is multiplied by
// BlockDownloadTimeoutPerPeerPercent, so counting a peer that is not actually
// downloading hands every peer extra patience it did not earn, and missing one
// that is disconnects peers that are only slow because we are sharing our own
// downstream link between them.
func TestPeersWithBlockDownloads(t *testing.T) {
	tests := []struct {
		name string
		// requested holds the outstanding block count per peer.
		requested []int
		want      int
	}{
		{name: "no peers at all", requested: nil, want: 0},
		{name: "one peer, nothing outstanding", requested: []int{0}, want: 0},
		{name: "one peer, one block outstanding", requested: []int{1}, want: 1},
		{name: "one peer, several blocks outstanding counts once", requested: []int{5}, want: 1},
		{name: "three peers all downloading", requested: []int{1, 2, 3}, want: 3},
		{name: "only the downloading peers are counted", requested: []int{0, 3, 0, 1}, want: 2},
		{name: "every peer idle", requested: []int{0, 0, 0}, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := &SyncManager{
				peerStates:     txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
				blockDownloads: newBlockDownloadTracker(blockRequestAssignmentTTL),
			}

			for i, count := range tt.requested {
				// Distinct ports so each peer is a distinct map key.
				p := newTestPeer(t, "localhost:"+strconv.Itoa(8333+i))
				sm.peerStates.Set(p, &peerSyncState{})

				for j := 0; j < count; j++ {
					sm.blockDownloads.Add(p, chainhash.Hash{byte(i), byte(j)})
				}
			}

			require.Equal(t, tt.want, sm.PeersWithBlockDownloads())
		})
	}
}

// TestPeersWithBlockDownloadsWithoutALedger covers a manager built as a struct
// literal that never got a download ledger. The stall handler reads this count
// from its own goroutine and must get a number back rather than panic.
func TestPeersWithBlockDownloadsWithoutALedger(t *testing.T) {
	sm := &SyncManager{
		peerStates: txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
	}

	sm.peerStates.Set(newTestPeer(t, "localhost:8333"), &peerSyncState{})

	require.NotPanics(t, func() {
		require.Equal(t, 0, sm.PeersWithBlockDownloads())
	})
}

// TestPeersWithBlockDownloadsIgnoresUnregisteredPeers pins that the count comes
// from the ledger, not from peerStates. A peer that is registered but has never
// been asked for anything must not count, and a peer that has been asked counts
// even though the two are tracked in different places.
func TestPeersWithBlockDownloadsIgnoresUnregisteredPeers(t *testing.T) {
	sm := &SyncManager{
		peerStates:     txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		blockDownloads: newBlockDownloadTracker(blockRequestAssignmentTTL),
	}

	idle := newTestPeer(t, "localhost:8333")
	busy := newTestPeer(t, "localhost:8334")

	sm.peerStates.Set(idle, &peerSyncState{})
	sm.peerStates.Set(busy, &peerSyncState{})
	sm.blockDownloads.Add(busy, chainhash.Hash{0x01})
	sm.blockDownloads.Add(busy, chainhash.Hash{0x02})

	require.Equal(t, 1, sm.PeersWithBlockDownloads())
}

// TestIsCurrentCached_AsksTheBlockchainNothing pins the other half of the deadline
// plumbing. The peer layer needs to know whether we are catching up in order to
// size a block download deadline, and it asks on a fifteen-second timer from
// every peer's stall handler.
//
// That goroutine is the only consumer of the peer's stallControl channel, which
// is buffered to one and whose sends from inHandler are blocking. So a blockchain
// round trip on this path — and GetBestBlockHeader can block for minutes during
// initial sync, which is why headerMu is dropped around it — stops that peer
// reading its socket, and stops the stall detector disconnecting anybody. It
// mutes the very peers it is meant to police.
//
// The read must therefore cost nothing, and must still tell the truth that
// current() last worked out.
func TestIsCurrentCached_AsksTheBlockchainNothing(t *testing.T) {
	running := blockchain2.FSMStateRUNNING
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	client := &blockchain2.Mock{}
	client.Mock.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)
	// Height 100 is far below mainnet's last checkpoint and the block time is
	// ancient, so current() computes false — the state a node is in during IBD.
	client.Mock.On("GetBestBlockHeader", mock.Anything).
		Return(bestHeader, &model.BlockHeaderMeta{Height: 100}, nil)

	sm := &SyncManager{
		logger:           ulogger.TestLogger{},
		settings:         test.CreateBaseTestSettings(t),
		chainParams:      &chaincfg.MainNetParams,
		blockchainClient: client,
		peerStates:       txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
	}

	calls := func() int {
		n := 0

		for _, c := range client.Mock.Calls {
			if c.Method == "GetBestBlockHeader" {
				n++
			}
		}

		return n
	}

	// Before anything has computed it, the answer reads as "still catching up".
	// That is the safe direction: the wider deadline it produces does not
	// disconnect a peer early.
	require.False(t, sm.IsCurrentCached())
	require.Zero(t, calls(), "the cached read must not ask the blockchain anything")

	require.False(t, sm.current(), "sanity: a node at height 100 is not current")
	require.Equal(t, 1, calls(), "current() itself does make the call")

	for i := 0; i < 50; i++ {
		require.False(t, sm.IsCurrentCached())
	}

	require.Equal(t, 1, calls(), "no number of cached reads may add a blockchain call")

	// And it carries the answer, rather than being hardwired to one.
	sm.currentCached.Store(true)
	require.True(t, sm.IsCurrentCached())
	require.Equal(t, 1, calls())
}
