package protocol

import (
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// downloadFixture is one genesis-rooted index, the headers-first machine that
// shares it, and the downloader under test. The headers here carry no valid
// proof of work on purpose: nothing in this file reaches OnHeaders, which is
// the only code path that checks it, so the cheap headerindex_test helpers are
// used instead of the ground ones from headersync_test.
type downloadFixture struct {
	idx     *HeaderIndex
	hs      *HeaderSync
	bd      *BlockDownloader
	genesis *wire.BlockHeader
	chain   []*wire.BlockHeader // chain[i] has height i+1
	nonces  *nonceCounter
}

func newDownloadFixture(t *testing.T, height int) *downloadFixture {
	t.Helper()

	genesis := testGenesis()

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	nonces := &nonceCounter{}
	chain := buildChain(t, idx, nonces, genesis, height)

	// A checkpoint far above the fixture height keeps headers-first mode on
	// once a peer takes the sync slot, which is the state the rotation rules
	// are exercised in.
	cpHash := chainhash.Hash{0xC0}

	hs, err := NewHeaderSync(HeaderSyncConfig{
		Index:  idx,
		Params: syncTestParams([]chaincfg.Checkpoint{{Height: 100000, Hash: &cpHash}}),
	})
	require.NoError(t, err)

	bd, err := NewBlockDownloader(idx, hs)
	require.NoError(t, err)

	return &downloadFixture{idx: idx, hs: hs, bd: bd, genesis: genesis, chain: chain, nonces: nonces}
}

// node returns the indexed header at height h, where 0 is genesis.
func (f *downloadFixture) node(t *testing.T, h int) HeaderNode {
	t.Helper()

	hash := f.genesis.BlockHash()
	if h > 0 {
		hash = f.chain[h-1].BlockHash()
	}

	n, ok := f.idx.Lookup(hash)
	require.True(t, ok)

	return n
}

// peerAt returns a full-node peer whose pindexBestKnownBlock is the fixture
// header at height h.
func (f *downloadFixture) peerAt(t *testing.T, addr string, h int) *SyncPeer {
	t.Helper()

	peer := fullNodePeer(addr)
	best := f.node(t, h)
	peer.State.pindexBestKnownBlock = &best

	return peer
}

// micros converts a duration to the microsecond unit every timestamp in this
// machine uses, the port of SVNode's GetTimeMicros.
// noIngest is the "no block is being ingested from this peer" observation,
// which is what every case in this file exercises: the ingest-aware
// suppression has its own tests.
var noIngest = IngestSnapshot{}

func micros(d time.Duration) int64 { return int64(d / time.Microsecond) }

// testNow is an arbitrary fixed microsecond timestamp. Every clock value in
// this file is derived from it: the machine reads no clock of its own.
const testNow = int64(1_700_000_000_000_000)

func invMsg(t *testing.T, typ wire.InvType, hashes ...chainhash.Hash) *wire.MsgInv {
	t.Helper()

	msg := wire.NewMsgInv()

	for i := range hashes {
		hash := hashes[i]
		require.NoError(t, msg.AddInvVect(wire.NewInvVect(typ, &hash)))
	}

	return msg
}

// TestFindNextBlocksToDownload_PeerWithNothingUseful covers the
// net_processing.cpp "This peer has nothing interesting" returns, including the
// IBD world where every peer except the sync peer carries only
// hashLastUnknownBlock.
func TestFindNextBlocksToDownload_PeerWithNothingUseful(t *testing.T) {
	f := newDownloadFixture(t, 3)
	activeTip := f.node(t, 3)

	tests := []struct {
		name  string
		setup func(peer *SyncPeer)
	}{
		{
			name: "only an unknown announcement parked: pindexBestKnownBlock is still nullptr",
			setup: func(peer *SyncPeer) {
				peer.State.hashLastUnknownBlock = chainhash.Hash{0xAB}
			},
		},
		{
			name:  "nothing announced at all",
			setup: func(_ *SyncPeer) {},
		},
		{
			name: "best known block below our own tip",
			setup: func(peer *SyncPeer) {
				n := f.node(t, 1)
				peer.State.pindexBestKnownBlock = &n
			},
		},
		{
			name: "best known block equal to our own tip",
			setup: func(peer *SyncPeer) {
				n := f.node(t, 3)
				peer.State.pindexBestKnownBlock = &n
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			peer := fullNodePeer("1.2.3.4:8333")
			tc.setup(peer)

			blocks, staller := f.bd.FindNextBlocksToDownload(peer, activeTip, MaxBlocksInTransitPerPeer, testNow)

			require.Empty(t, blocks)
			require.Nil(t, staller)
		})
	}
}

func TestFindNextBlocksToDownload_ReturnsSuccessorsInOrder(t *testing.T) {
	f := newDownloadFixture(t, 10)
	activeTip := f.node(t, 4)
	peer := f.peerAt(t, "1.2.3.4:8333", 10)

	t.Run("every missing successor up to the peer's best known block", func(t *testing.T) {
		blocks, staller := f.bd.FindNextBlocksToDownload(peer, activeTip, MaxBlocksInTransitPerPeer, testNow)

		require.Nil(t, staller)
		require.Len(t, blocks, 6)

		for i := range blocks {
			require.Equal(t, int32(5+i), blocks[i].Height)
			require.Equal(t, f.node(t, 5+i).Hash, blocks[i].Hash)
		}
	})

	t.Run("count caps the batch", func(t *testing.T) {
		peer.State.pindexLastCommonBlock = nil

		blocks, staller := f.bd.FindNextBlocksToDownload(peer, activeTip, 3, testNow)

		require.Nil(t, staller)
		require.Len(t, blocks, 3)
		require.Equal(t, int32(5), blocks[0].Height)
		require.Equal(t, int32(7), blocks[2].Height)
	})
}

func TestFindNextBlocksToDownload_SkipsBlocksAlreadyInFlight(t *testing.T) {
	f := newDownloadFixture(t, 10)
	activeTip := f.node(t, 4)

	holder := f.peerAt(t, "5.6.7.8:8333", 10)
	require.True(t, f.bd.MarkBlockAsInFlight(holder, f.node(t, 6), testNow))

	peer := f.peerAt(t, "1.2.3.4:8333", 10)

	blocks, staller := f.bd.FindNextBlocksToDownload(peer, activeTip, MaxBlocksInTransitPerPeer, testNow)

	// The window never ran out, so nobody is named as a staller.
	require.Nil(t, staller)
	require.Len(t, blocks, 5)
	require.Equal(t, int32(5), blocks[0].Height)
	require.Equal(t, int32(7), blocks[1].Height, "the in-flight height 6 must be skipped, not re-requested")
}

// TestFindNextBlocksToDownload_ComparesChainWorkNotHeight pins the
// net_processing.cpp:362-368 gate: "This peer has nothing interesting" fires
// when the peer's best known block carries less work than our own tip, whatever
// the two heights are. The Phase 2 port compared heights here, which scheduled
// a whole low-work branch off a peer that had nothing better than what we hold.
func TestFindNextBlocksToDownload_ComparesChainWorkNotHeight(t *testing.T) {
	// Two branches off the same genesis, weights from the per-header proofs
	// pinned by TestBlockProof:
	//   heavy, 3 headers: 4_295_032_833 + 3 * 8_589_934_591 = 30_064_836_606
	//   light, 5 headers: 6 * 4_295_032_833                 = 25_770_196_998
	// The light branch stands two blocks taller and carries less work.
	const (
		heavyLen = 3
		lightLen = 5
	)

	newFixture := func(t *testing.T) (*BlockDownloader, HeaderNode, HeaderNode) {
		t.Helper()

		genesis := testGenesis()

		idx, err := NewHeaderIndex(genesis)
		require.NoError(t, err)

		nc := &nonceCounter{}
		heavy := buildChainBits(t, idx, nc, genesis, heavyLen, heavyBits)
		light := buildChainBits(t, idx, nc, genesis, lightLen, difficulty1Bits)

		cpHash := chainhash.Hash{0xC0}

		hs, err := NewHeaderSync(HeaderSyncConfig{
			Index:  idx,
			Params: syncTestParams([]chaincfg.Checkpoint{{Height: 100000, Hash: &cpHash}}),
		})
		require.NoError(t, err)

		bd, err := NewBlockDownloader(idx, hs)
		require.NoError(t, err)

		heavyTip, ok := idx.Lookup(heavy[len(heavy)-1].BlockHash())
		require.True(t, ok)
		require.Equal(t, int32(heavyLen), heavyTip.Height)

		lightTip, ok := idx.Lookup(light[len(light)-1].BlockHash())
		require.True(t, ok)
		require.Equal(t, int32(lightLen), lightTip.Height)

		require.Positive(t, heavyTip.ChainWork.Cmp(lightTip.ChainWork),
			"test setup: the shorter branch must be the heavier one")

		return bd, heavyTip, lightTip
	}

	t.Run("a taller but lighter best known block has nothing interesting", func(t *testing.T) {
		bd, heavyTip, lightTip := newFixture(t)

		peer := fullNodePeer("1.2.3.4:8333")
		peer.State.pindexBestKnownBlock = &lightTip

		blocks, staller := bd.FindNextBlocksToDownload(peer, heavyTip, MaxBlocksInTransitPerPeer, testNow)

		require.Empty(t, blocks, "a branch with less work than our tip must not be scheduled")
		require.Nil(t, staller)
	})

	t.Run("a shorter but heavier best known block is scheduled", func(t *testing.T) {
		bd, heavyTip, lightTip := newFixture(t)

		peer := fullNodePeer("1.2.3.4:8333")
		peer.State.pindexBestKnownBlock = &heavyTip

		blocks, staller := bd.FindNextBlocksToDownload(peer, lightTip, MaxBlocksInTransitPerPeer, testNow)

		require.Nil(t, staller)
		require.Len(t, blocks, heavyLen, "the whole heavier branch is missing and must be fetched")

		for i := range blocks {
			require.Equal(t, int32(1+i), blocks[i].Height)
		}
	})
}

// TestFindNextBlocksToDownload_FollowsAPeerReorg pins the net_processing.cpp
// "If the peer reorganized, our previous pindexLastCommonBlock may not be an
// ancestor of its current tip anymore" rewind.
func TestFindNextBlocksToDownload_FollowsAPeerReorg(t *testing.T) {
	f := newDownloadFixture(t, 6)
	activeTip := f.node(t, 2)

	// A branch forking off height 2 and reaching height 7.
	side := buildChain(t, f.idx, &nonceCounter{next: 1000}, f.chain[1], 5)

	sideTip, ok := f.idx.Lookup(side[len(side)-1].BlockHash())
	require.True(t, ok)
	require.Equal(t, int32(7), sideTip.Height)

	peer := fullNodePeer("1.2.3.4:8333")
	peer.State.pindexBestKnownBlock = &sideTip

	// A stale last-common from before the peer reorged, on the main branch and
	// above the fork point.
	stale := f.node(t, 5)
	peer.State.pindexLastCommonBlock = &stale

	blocks, staller := f.bd.FindNextBlocksToDownload(peer, activeTip, MaxBlocksInTransitPerPeer, testNow)

	require.Nil(t, staller)
	require.Equal(t, int32(2), peer.State.pindexLastCommonBlock.Height, "last common must rewind to the fork point")
	require.Len(t, blocks, 5)

	for i := range blocks {
		require.Equal(t, int32(3+i), blocks[i].Height)
		require.Equal(t, side[i].BlockHash(), blocks[i].Hash, "the peer's branch must be fetched, not ours")
	}
}

// TestFindNextBlocksToDownload_WindowAdvancesOnlyOnContiguousCompletion is the
// moving-window rule. A block completing in the middle of the window frees its
// own slot but must not extend the window: net_processing.cpp advances
// pindexLastCommonBlock only over blocks whose ancestors are all downloaded
// (the GetChainTx() guard), so the window end moves only when the prefix below
// the hole closes.
func TestFindNextBlocksToDownload_WindowAdvancesOnlyOnContiguousCompletion(t *testing.T) {
	const peerHeight = BlockDownloadWindow + 6

	f := newDownloadFixture(t, peerHeight)
	activeTip := f.node(t, 0)
	peer := f.peerAt(t, "1.2.3.4:8333", peerHeight)

	// The first sweep offers exactly the window: heights 1..1024, never the
	// blocks above it, even though the peer has them.
	blocks, staller := f.bd.FindNextBlocksToDownload(peer, activeTip, 4096, testNow)
	require.Nil(t, staller)
	require.Len(t, blocks, BlockDownloadWindow)
	require.Equal(t, int32(1), blocks[0].Height)
	require.Equal(t, int32(BlockDownloadWindow), blocks[len(blocks)-1].Height)

	for _, b := range blocks {
		require.True(t, f.bd.MarkBlockAsInFlight(peer, b, testNow))
	}

	// Out-of-order completion in the middle of the window.
	require.True(t, f.bd.BlockReceived(peer, f.node(t, 500).Hash, testNow))

	blocks, staller = f.bd.FindNextBlocksToDownload(peer, activeTip, 4096, testNow)
	require.Nil(t, staller, "the peer is waiting on its own in-flight blocks, so it is not its own staller")
	require.Empty(t, blocks, "a mid-window completion must not release blocks beyond the window end")
	require.Equal(t, int32(0), peer.State.pindexLastCommonBlock.Height, "the window must not move over a hole")

	// Closing the prefix below the hole moves the window.
	for h := 1; h < 500; h++ {
		require.True(t, f.bd.BlockReceived(peer, f.node(t, h).Hash, testNow))
	}

	// net_processing.cpp computes nWindowEnd once, before the walk that
	// advances pindexLastCommonBlock, so this pass records the advance and the
	// next one gets the wider window.
	blocks, staller = f.bd.FindNextBlocksToDownload(peer, activeTip, 4096, testNow)
	require.Nil(t, staller)
	require.Empty(t, blocks)
	require.Equal(t, int32(500), peer.State.pindexLastCommonBlock.Height)

	blocks, staller = f.bd.FindNextBlocksToDownload(peer, activeTip, 4096, testNow)
	require.Nil(t, staller)
	require.Len(t, blocks, 6, "the window end moved to 500+1024, exposing the remaining blocks")
	require.Equal(t, int32(BlockDownloadWindow+1), blocks[0].Height)
	require.Equal(t, int32(peerHeight), blocks[len(blocks)-1].Height)
}

// TestSendGetDataBlocks_InTransitCapHoldsAtSixteen pins
// MAX_BLOCKS_IN_TRANSIT_PER_PEER: the gate closes at exactly 16 and reopens by
// exactly one slot per receipt.
func TestSendGetDataBlocks_InTransitCapHoldsAtSixteen(t *testing.T) {
	f := newDownloadFixture(t, 100)
	activeTip := f.node(t, 0)
	peer := f.peerAt(t, "1.2.3.4:8333", 100)

	msgs := f.bd.SendGetDataBlocks(peer, activeTip, testNow)
	require.Len(t, msgs, 1)

	getData, ok := msgs[0].(*wire.MsgGetData)
	require.True(t, ok)
	require.Len(t, getData.InvList, MaxBlocksInTransitPerPeer)
	require.Equal(t, MaxBlocksInTransitPerPeer, peer.State.nBlocksInFlight)

	for i, inv := range getData.InvList {
		require.Equal(t, wire.InvTypeBlock, inv.Type)
		require.Equal(t, f.node(t, i+1).Hash, inv.Hash)
	}

	require.Empty(t, f.bd.SendGetDataBlocks(peer, activeTip, testNow), "the cap must hold at exactly 16")
	require.Equal(t, MaxBlocksInTransitPerPeer, peer.State.nBlocksInFlight)

	require.True(t, f.bd.BlockReceived(peer, getData.InvList[0].Hash, testNow))
	require.Equal(t, MaxBlocksInTransitPerPeer-1, peer.State.nBlocksInFlight)

	msgs = f.bd.SendGetDataBlocks(peer, activeTip, testNow)
	require.Len(t, msgs, 1)

	next, ok := msgs[0].(*wire.MsgGetData)
	require.True(t, ok)
	require.Len(t, next.InvList, 1, "one receipt frees exactly one slot")
	require.Equal(t, f.node(t, MaxBlocksInTransitPerPeer+1).Hash, next.InvList[0].Hash)
	require.Equal(t, MaxBlocksInTransitPerPeer, peer.State.nBlocksInFlight)
}

// TestFindNextBlocksToDownload_PrunesReceivedBlocksBehindTheActiveTip pins the
// haveData watermark. Without it every received block stays recorded for the
// lifetime of the process, because the download walk starts above the last
// common block and the steady-state calls return before ever reaching it.
func TestFindNextBlocksToDownload_PrunesReceivedBlocksBehindTheActiveTip(t *testing.T) {
	f := newDownloadFixture(t, 40)
	peer := f.peerAt(t, "1.2.3.4:8333", 40)

	// Our chain is still at genesis while 20 blocks arrive.
	activeTip := f.node(t, 0)

	blocks, _ := f.bd.FindNextBlocksToDownload(peer, activeTip, 20, testNow)
	require.Len(t, blocks, 20)

	for _, b := range blocks {
		require.True(t, f.bd.MarkBlockAsInFlight(peer, b, testNow))
		require.True(t, f.bd.BlockReceived(peer, b.Hash, testNow))
	}

	require.Equal(t, 20, len(f.bd.haveData), "every delivered block is recorded until our chain covers it")

	// Our chain connects the first 12 of them.
	f.bd.FindNextBlocksToDownload(peer, f.node(t, 12), MaxBlocksInTransitPerPeer, testNow)

	require.Equal(t, 8, len(f.bd.haveData), "everything at or below the active tip must be dropped")

	for h := 13; h <= 20; h++ {
		require.Contains(t, f.bd.haveData, f.node(t, h).Hash, "blocks above the active tip are still ours to remember")
	}

	// The whole run connects.
	f.bd.FindNextBlocksToDownload(peer, f.node(t, 20), MaxBlocksInTransitPerPeer, testNow)
	require.Empty(t, f.bd.haveData)

	// A steady-state call that returns early must still prune: this peer has
	// nothing interesting, which is the path that made the leak unbounded.
	idle := fullNodePeer("9.9.9.9:8333")
	require.True(t, f.bd.MarkBlockAsInFlight(peer, f.node(t, 25), testNow))
	require.True(t, f.bd.BlockReceived(peer, f.node(t, 25).Hash, testNow))
	require.Len(t, f.bd.haveData, 1)

	f.bd.FindNextBlocksToDownload(idle, f.node(t, 25), MaxBlocksInTransitPerPeer, testNow)
	require.Empty(t, f.bd.haveData, "the prune must run before the nothing-interesting return")
}

// TestFindNextBlocksToDownload_RefusesAnActiveTipOutsideTheIndex pins the
// contract that activeTip must be a header this index holds. Guessing would
// mean treating our own blocks as missing and re-requesting the whole branch.
func TestFindNextBlocksToDownload_RefusesAnActiveTipOutsideTheIndex(t *testing.T) {
	f := newDownloadFixture(t, 10)
	peer := f.peerAt(t, "1.2.3.4:8333", 10)

	stranger := HeaderNode{Hash: chainhash.Hash{0xFE}, Height: 4}

	t.Run("on the bootstrap path", func(t *testing.T) {
		blocks, staller := f.bd.FindNextBlocksToDownload(peer, stranger, MaxBlocksInTransitPerPeer, testNow)

		require.Empty(t, blocks)
		require.Nil(t, staller)
	})

	t.Run("on the steady-state path, with last-common already set", func(t *testing.T) {
		known := f.node(t, 2)
		peer.State.pindexLastCommonBlock = &known

		blocks, staller := f.bd.FindNextBlocksToDownload(peer, stranger, MaxBlocksInTransitPerPeer, testNow)

		require.Empty(t, blocks, "an unplaceable active tip must not re-request the branch")
		require.Nil(t, staller)
	})
}

// TestSendGetDataBlocks_SkipsAPeerThatCannotServeBlocks pins the C++ !fClient
// gate, which this port reads through isSyncCandidate.
func TestSendGetDataBlocks_SkipsAPeerThatCannotServeBlocks(t *testing.T) {
	f := newDownloadFixture(t, 10)
	activeTip := f.node(t, 0)

	peer := NewSyncPeer("1.2.3.4:8333", 0, newPeerSyncState())
	best := f.node(t, 10)
	peer.State.pindexBestKnownBlock = &best

	require.Empty(t, f.bd.SendGetDataBlocks(peer, activeTip, testNow))
	require.Equal(t, 0, peer.State.nBlocksInFlight)
	require.Equal(t, 0, f.bd.BlocksInFlight())
}

// TestSendGetDataBlocks_NamesTheStaller pins the nodeStaller path: a peer whose
// whole window is held in flight by someone else cannot move, and the peer
// holding it starts its nStallingSince clock.
func TestSendGetDataBlocks_NamesTheStaller(t *testing.T) {
	const peerHeight = BlockDownloadWindow + 6

	f := newDownloadFixture(t, peerHeight)
	activeTip := f.node(t, 0)

	holder := f.peerAt(t, "5.6.7.8:8333", peerHeight)
	for h := 1; h <= BlockDownloadWindow; h++ {
		require.True(t, f.bd.MarkBlockAsInFlight(holder, f.node(t, h), testNow))
	}

	waiter := f.peerAt(t, "1.2.3.4:8333", peerHeight)

	require.Empty(t, f.bd.SendGetDataBlocks(waiter, activeTip, testNow))
	require.Equal(t, testNow, holder.State.nStallingSince, "the peer blocking the window starts stalling")
	require.Equal(t, int64(0), waiter.State.nStallingSince, "the waiting peer is not the staller")

	// net_processing.cpp only starts the clock once. The second pass stays inside
	// the slow-fetch fuse, because past it the walk races the contested block and
	// the waiter is handed work instead of naming a staller — the case
	// TestSendGetDataBlocks_SchedulesAcrossEveryUsefulPeer covers.
	require.Empty(t, f.bd.SendGetDataBlocks(waiter, activeTip, testNow+micros(BlockDownloadSlowFetchTimeout/2)))
	require.Equal(t, testNow, holder.State.nStallingSince)
}

func TestCheckStall_DisconnectsAStallingPeer(t *testing.T) {
	f := newDownloadFixture(t, 3)
	peer := f.peerAt(t, "1.2.3.4:8333", 3)

	peer.State.nStallingSince = testNow

	require.Equal(t, StallActionNone, f.bd.CheckStall(peer, noIngest, testNow+micros(BlockStallingTimeout)))
	require.Equal(t, StallActionDisconnect, f.bd.CheckStall(peer, noIngest, testNow+micros(BlockStallingTimeout)+1))
}

// TestCheckStall_DisconnectsAStallerThatHoldsNoSyncSlot pins the ORDER of the
// two clauses in CheckStall. DetectStalling runs before the fSyncStarted early
// return, so a peer a rotation stripped of the sync slot is still judged on the
// blocks it holds. Reverse the two and a rotated-but-connected peer becomes
// ungovernable: every block the scheduler re-hands it is lost until it
// disconnects on its own.
func TestCheckStall_DisconnectsAStallerThatHoldsNoSyncSlot(t *testing.T) {
	f := newDownloadFixture(t, 3)

	peer := f.peerAt(t, "1.2.3.4:8333", 3)
	require.False(t, peer.State.fSyncStarted, "this is the state a rotation leaves behind")

	peer.State.nStallingSince = testNow

	require.Equal(t, StallActionDisconnect, f.bd.CheckStall(peer, noIngest, testNow+micros(BlockStallingTimeout)+1))
}

func TestCheckStall_IgnoresAPeerThatIsNotStalling(t *testing.T) {
	f := newDownloadFixture(t, 3)
	peer := f.peerAt(t, "1.2.3.4:8333", 3)

	require.Equal(t, StallActionNone, f.bd.CheckStall(peer, noIngest, testNow+micros(time.Hour)))
}

// TestCheckStall_RotatesTheSyncPeer pins the Teranode rotation carried from
// legacy netsync handleCheckSyncPeer: a sync peer that makes no progress for
// maxLastBlockTime loses the slot. It also covers the headers-first case, where
// no block ever arrives and the SVNode nStallingSince clock never starts.
func TestCheckStall_RotatesTheSyncPeer(t *testing.T) {
	f := newDownloadFixture(t, 3)

	peer := fullNodePeer("1.2.3.4:8333")
	require.Len(t, f.hs.PeerEstablished(peer), 1)
	require.True(t, peer.State.fSyncStarted)
	require.True(t, f.hs.IsHeadersFirstMode())

	best := f.node(t, 3)
	peer.State.pindexBestKnownBlock = &best
	peer.State.pindexLastCommonBlock = &best

	require.True(t, f.bd.MarkBlockAsInFlight(peer, best, testNow))

	require.Equal(t, StallActionNone, f.bd.CheckStall(peer, noIngest, testNow))
	require.Equal(t, StallActionNone, f.bd.CheckStall(peer, noIngest, testNow+micros(MaxLastBlockTime)))

	require.Equal(t, StallActionRotateSyncPeer, f.bd.CheckStall(peer, noIngest, testNow+micros(MaxLastBlockTime)+1))

	require.False(t, peer.State.fSyncStarted, "the sync slot must be released through HeaderSync.SyncPeerTimedOut")
	require.False(t, f.hs.IsHeadersFirstMode(), "the header state must reset with the round")
	require.False(t, f.bd.IsInFlight(best.Hash), "the rotated peer's in-flight blocks must be released")
	require.Equal(t, 0, peer.State.nBlocksInFlight)
	require.Nil(t, peer.State.pindexLastCommonBlock)
}

// syncingPeer returns a peer holding the sync slot with one block in flight,
// which is the state the rotation rules are written against.
func (f *downloadFixture) syncingPeer(t *testing.T, addr string) *SyncPeer {
	t.Helper()

	peer := fullNodePeer(addr)
	require.Len(t, f.hs.PeerEstablished(peer), 1)

	best := f.node(t, len(f.chain))
	peer.State.pindexBestKnownBlock = &best
	peer.State.pindexLastCommonBlock = &best

	require.True(t, f.bd.MarkBlockAsInFlight(peer, best, testNow))

	return peer
}

// TestCheckStall_KeepsAPeerStillDeliveringALargeBlock covers the suppression
// carried from legacy netsync manager.go:1052-1068: a block big enough to
// outlast maxLastBlockTime must not cost the peer that is still streaming it
// the sync slot. The rate floor and the wall-clock cap are both exercised.
func TestCheckStall_KeepsAPeerStillDeliveringALargeBlock(t *testing.T) {
	f := newDownloadFixture(t, 3)
	peer := f.syncingPeer(t, "1.2.3.4:8333")

	const tick = 5 * time.Second

	// Bytes arriving comfortably above MinBlockDownloadBytesPerSec.
	perTick := uint64(MinBlockDownloadBytesPerSec) * uint64(tick/time.Second) * 2

	ingest := IngestSnapshot{Active: true, StartedMicros: testNow}

	// Seed the first sample, then run past the rotation window while the
	// ingest keeps pulling bytes.
	require.Equal(t, StallActionNone, f.bd.CheckStall(peer, ingest, testNow))

	for elapsed := tick; elapsed <= MaxLastBlockTime+2*tick; elapsed += tick {
		ingest.BytesRead += perTick

		require.Equal(t, StallActionNone, f.bd.CheckStall(peer, ingest, testNow+micros(elapsed)),
			"a peer still delivering at %d B/s must keep the sync slot at %s", MinBlockDownloadBytesPerSec, elapsed)
	}

	require.True(t, peer.State.fSyncStarted, "the sync slot must survive a long but healthy download")
}

func TestCheckStall_RotatesAnIngestThatStoppedMoving(t *testing.T) {
	f := newDownloadFixture(t, 3)
	peer := f.syncingPeer(t, "1.2.3.4:8333")

	// An ingest that took some bytes and then went quiet: the byte count stops
	// rising, so it is a stalled peer, not a large block.
	ingest := IngestSnapshot{Active: true, StartedMicros: testNow, BytesRead: 1 << 20}

	require.Equal(t, StallActionNone, f.bd.CheckStall(peer, ingest, testNow))
	require.Equal(t, StallActionNone, f.bd.CheckStall(peer, ingest, testNow+micros(MaxLastBlockTime)))

	require.Equal(t, StallActionRotateSyncPeer, f.bd.CheckStall(peer, ingest, testNow+micros(MaxLastBlockTime)+1),
		"an ingest whose byte count stopped rising is a stall, whatever it read earlier")
}

func TestCheckStall_NeverRotatesANonSyncPeer(t *testing.T) {
	f := newDownloadFixture(t, 3)
	peer := f.peerAt(t, "1.2.3.4:8333", 3)

	require.Equal(t, StallActionNone, f.bd.CheckStall(peer, noIngest, testNow))
	require.Equal(t, StallActionNone, f.bd.CheckStall(peer, noIngest, testNow+micros(10*MaxLastBlockTime)))
}

// TestCheckStall_ProgressKeepsTheSyncPeer covers both progress sources the
// rotation clock accepts: a delivered block, and a headers-first round that
// pushes the header index tip forward.
func TestCheckStall_ProgressKeepsTheSyncPeer(t *testing.T) {
	t.Run("a delivered block refreshes the clock", func(t *testing.T) {
		f := newDownloadFixture(t, 3)

		peer := fullNodePeer("1.2.3.4:8333")
		require.Len(t, f.hs.PeerEstablished(peer), 1)

		best := f.node(t, 3)
		peer.State.pindexBestKnownBlock = &best
		require.True(t, f.bd.MarkBlockAsInFlight(peer, best, testNow))

		require.Equal(t, StallActionNone, f.bd.CheckStall(peer, noIngest, testNow))

		delivered := testNow + micros(100*time.Second)
		require.True(t, f.bd.BlockReceived(peer, best.Hash, delivered))

		require.Equal(t, StallActionNone, f.bd.CheckStall(peer, noIngest, delivered+micros(MaxLastBlockTime)))
		require.Equal(t, StallActionRotateSyncPeer, f.bd.CheckStall(peer, noIngest, delivered+micros(MaxLastBlockTime)+1))
	})

	t.Run("a headers-first round advancing the index tip refreshes the clock", func(t *testing.T) {
		f := newDownloadFixture(t, 3)

		peer := fullNodePeer("1.2.3.4:8333")
		require.Len(t, f.hs.PeerEstablished(peer), 1)
		require.True(t, f.hs.IsHeadersFirstMode())

		require.Equal(t, StallActionNone, f.bd.CheckStall(peer, noIngest, testNow))

		// The round delivers headers: the index tip rises, which is the only
		// progress signal available while no block is being downloaded.
		connected, err := f.idx.AddHeader(childOf(f.chain[2], 9999))
		require.NoError(t, err)
		require.True(t, connected)

		advanced := testNow + micros(MaxLastBlockTime) - 1
		require.Equal(t, StallActionNone, f.bd.CheckStall(peer, noIngest, advanced))
		require.True(t, peer.State.fSyncStarted)

		// The clock restarted from the observation, not from testNow.
		require.Equal(t, StallActionNone, f.bd.CheckStall(peer, noIngest, advanced+micros(MaxLastBlockTime)))
		require.Equal(t, StallActionRotateSyncPeer, f.bd.CheckStall(peer, noIngest, advanced+micros(MaxLastBlockTime)+1))
	})

	// The tip is chosen by cumulative work, so a heavier branch can take it
	// while standing shorter than the branch it displaces. The rotation clock
	// must read that as progress: the peer delivered the headers that moved
	// the tip, and a height watermark would have missed it and rotated a peer
	// that was doing exactly what we wanted.
	t.Run("a tip moving to a shorter heavier branch refreshes the clock", func(t *testing.T) {
		f := newDownloadFixture(t, 3)

		peer := fullNodePeer("1.2.3.4:8333")
		require.Len(t, f.hs.PeerEstablished(peer), 1)
		require.True(t, f.hs.IsHeadersFirstMode())

		require.Equal(t, StallActionNone, f.bd.CheckStall(peer, noIngest, testNow))

		// Two heavy headers off genesis outweigh the fixture's three light
		// ones (4_295_032_833 + 2 * 8_589_934_591 = 21_474_902_015 against
		// 4 * 4_295_032_833 = 17_180_131_332) one block shorter.
		heavy := buildChainBits(t, f.idx, &nonceCounter{next: 7000}, f.genesis, 2, heavyBits)

		tipHash, tipHeight := f.idx.Tip()
		require.Equal(t, heavy[len(heavy)-1].BlockHash(), tipHash)
		require.Equal(t, int32(2), tipHeight, "the new tip stands below the branch it displaced")

		advanced := testNow + micros(MaxLastBlockTime) - 1
		require.Equal(t, StallActionNone, f.bd.CheckStall(peer, noIngest, advanced))
		require.True(t, peer.State.fSyncStarted)

		require.Equal(t, StallActionNone, f.bd.CheckStall(peer, noIngest, advanced+micros(MaxLastBlockTime)))
		require.Equal(t, StallActionRotateSyncPeer, f.bd.CheckStall(peer, noIngest, advanced+micros(MaxLastBlockTime)+1))
	})

	// Header progress may only stand in for block progress when there is no
	// block download to judge instead. A peer that trickles headers while
	// sitting on everything we asked it for is withholding blocks, and must
	// still be rotated.
	t.Run("header progress does not excuse a peer withholding blocks", func(t *testing.T) {
		f := newDownloadFixture(t, MaxBlocksInTransitPerPeer+4)

		peer := fullNodePeer("1.2.3.4:8333")
		require.Len(t, f.hs.PeerEstablished(peer), 1)
		require.True(t, f.hs.IsHeadersFirstMode())

		best := f.node(t, MaxBlocksInTransitPerPeer+4)
		peer.State.pindexBestKnownBlock = &best

		msgs := f.bd.SendGetDataBlocks(peer, f.node(t, 0), testNow)
		require.Len(t, msgs, 1)
		require.Equal(t, MaxBlocksInTransitPerPeer, peer.State.nBlocksInFlight)

		require.Equal(t, StallActionNone, f.bd.CheckStall(peer, noIngest, testNow))

		// The peer keeps the header round moving but delivers no block.
		prev := f.chain[len(f.chain)-1]
		for i := uint32(0); i < 3; i++ {
			header := childOf(prev, 5000+i)

			connected, err := f.idx.AddHeader(header)
			require.NoError(t, err)
			require.True(t, connected)

			prev = header

			require.Equal(t, StallActionNone, f.bd.CheckStall(peer, noIngest, testNow+micros(time.Duration(i+1)*time.Second)))
		}

		require.Equal(t, StallActionRotateSyncPeer, f.bd.CheckStall(peer, noIngest, testNow+micros(MaxLastBlockTime)+1),
			"headers must not hold the sync slot while the peer sits on in-flight blocks")
		require.False(t, peer.State.fSyncStarted)
	})
}

func TestOnInv_UnknownBlockAsksForHeaders(t *testing.T) {
	f := newDownloadFixture(t, 3)
	peer := fullNodePeer("1.2.3.4:8333")

	unknown := chainhash.Hash{0xAB}

	msgs, txHashes, err := f.bd.OnInv(peer, invMsg(t, wire.InvTypeBlock, unknown))
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Empty(t, txHashes)

	getHeaders, ok := msgs[0].(*wire.MsgGetHeaders)
	require.True(t, ok)
	require.Equal(t, unknown, getHeaders.HashStop)

	locator := f.idx.Locator()
	require.Len(t, getHeaders.BlockLocatorHashes, len(locator))

	for i := range locator {
		require.Equal(t, locator[i], *getHeaders.BlockLocatorHashes[i])
	}

	require.Equal(t, unknown, peer.State.hashLastUnknownBlock)
	require.Nil(t, peer.State.pindexBestKnownBlock)
}

func TestOnInv_KnownBlockOnlyUpdatesAvailability(t *testing.T) {
	f := newDownloadFixture(t, 3)
	peer := fullNodePeer("1.2.3.4:8333")

	known := f.node(t, 3)

	msgs, txHashes, err := f.bd.OnInv(peer, invMsg(t, wire.InvTypeBlock, known.Hash))
	require.NoError(t, err)
	require.Empty(t, msgs, "a block we already have a header for needs no getheaders")
	require.Empty(t, txHashes)

	require.NotNil(t, peer.State.pindexBestKnownBlock)
	require.Equal(t, int32(3), peer.State.pindexBestKnownBlock.Height)
	require.Equal(t, known.Hash, peer.State.pindexBestKnownBlock.Hash)
}

func TestOnInv_DoesNotRequestTheSameBlockTwice(t *testing.T) {
	f := newDownloadFixture(t, 3)
	peer := fullNodePeer("1.2.3.4:8333")

	unknown := chainhash.Hash{0xAB}

	msgs, txHashes, err := f.bd.OnInv(peer, invMsg(t, wire.InvTypeBlock, unknown, unknown, unknown))
	require.NoError(t, err)
	require.Len(t, msgs, 1, "a hash repeated inside one inv message draws one getheaders")
	require.Empty(t, txHashes)
}

func TestOnInv_CollectsTxHashesForKafka(t *testing.T) {
	f := newDownloadFixture(t, 3)
	peer := fullNodePeer("1.2.3.4:8333")

	msgs, txHashes, err := f.bd.OnInv(peer, invMsg(t, wire.InvTypeTx, chainhash.Hash{0x01}, chainhash.Hash{0x02}))
	require.NoError(t, err)
	require.Empty(t, msgs, "a tx inv never draws a getheaders")
	require.Equal(t, []chainhash.Hash{{0x01}, {0x02}}, txHashes, "collected in order, for the caller to produce to Kafka (Task 16)")

	require.Equal(t, chainhash.Hash{}, peer.State.hashLastUnknownBlock, "a tx inv must not touch block availability")
	require.Nil(t, peer.State.pindexBestKnownBlock)
}

func TestOnInv_UnsupportedInvTypeIsAProtocolViolation(t *testing.T) {
	tests := []struct {
		name string
		typ  wire.InvType
	}{
		{name: "error type", typ: wire.InvTypeError},
		{name: "filtered block", typ: wire.InvTypeFilteredBlock},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newDownloadFixture(t, 3)
			peer := fullNodePeer("1.2.3.4:8333")

			msgs, txHashes, err := f.bd.OnInv(peer, invMsg(t, tc.typ, chainhash.Hash{0x01}))
			require.Error(t, err)
			require.ErrorIs(t, err, ErrProtocolViolation)
			require.Empty(t, msgs)
			require.Empty(t, txHashes)
		})
	}
}

func TestMarkBlockAsInFlight_RefusesTheSamePeerAndAdmitsAnother(t *testing.T) {
	f := newDownloadFixture(t, 3)
	peer := f.peerAt(t, "1.2.3.4:8333", 3)
	other := f.peerAt(t, "5.6.7.8:8333", 3)

	block := f.node(t, 3)

	require.True(t, f.bd.MarkBlockAsInFlight(peer, block, testNow))
	require.Equal(t, 1, peer.State.nBlocksInFlight)

	require.False(t, f.bd.MarkBlockAsInFlight(peer, block, testNow), "the same block from the same peer is a no-op")
	require.Equal(t, 1, peer.State.nBlocksInFlight)

	// The C++ short-circuit is per (block, peer), not per block
	// (block_download_tracker.cpp:18-30): a second peer may be asked for the
	// same block, which is what the parallel-fetch branch does. Task 6b reversed
	// the single-assignment rule Phase 2 asserted here.
	later := testNow + micros(time.Minute)

	require.True(t, f.bd.MarkBlockAsInFlight(other, block, later), "a second peer may hold the same block")
	require.Equal(t, 1, other.State.nBlocksInFlight)

	// Both holders are recorded, in the order they were asked, each with its own
	// clock. The order decides which of them the walk may name as the staller.
	require.Equal(t, []inFlightHolder{{peer: peer, since: testNow}, {peer: other, since: later}},
		f.bd.inFlight[block.Hash])

	require.True(t, f.bd.IsInFlightFrom(peer, block.Hash))
	require.True(t, f.bd.IsInFlightFrom(other, block.Hash))
	require.Equal(t, 1, f.bd.BlocksInFlight(), "one block, two holders")

	// Releasing one holder leaves the other in flight.
	require.True(t, f.bd.BlockNotDelivered(peer, block.Hash, later))
	require.False(t, f.bd.IsInFlightFrom(peer, block.Hash))
	require.True(t, f.bd.IsInFlightFrom(other, block.Hash))
	require.True(t, f.bd.IsInFlight(block.Hash))
	require.Equal(t, 0, peer.State.nBlocksInFlight)
	require.Equal(t, 1, other.State.nBlocksInFlight)
}

func TestBlockReceived_ClearsInFlightAndTheStallClock(t *testing.T) {
	f := newDownloadFixture(t, 3)
	peer := f.peerAt(t, "1.2.3.4:8333", 3)

	block := f.node(t, 3)
	require.True(t, f.bd.MarkBlockAsInFlight(peer, block, testNow))

	peer.State.nStallingSince = testNow

	require.True(t, f.bd.BlockReceived(peer, block.Hash, testNow))
	require.Equal(t, 0, peer.State.nBlocksInFlight)
	require.Equal(t, int64(0), peer.State.nStallingSince)
	require.False(t, f.bd.IsInFlight(block.Hash))

	require.False(t, f.bd.BlockReceived(peer, block.Hash, testNow), "a second receipt is a no-op")
	require.Equal(t, 0, peer.State.nBlocksInFlight)
}

func TestBlockFailed_ReleasesTheBlockForAnotherPeer(t *testing.T) {
	f := newDownloadFixture(t, 10)
	activeTip := f.node(t, 4)

	holder := f.peerAt(t, "5.6.7.8:8333", 10)
	block := f.node(t, 5)
	require.True(t, f.bd.MarkBlockAsInFlight(holder, block, testNow))

	require.True(t, f.bd.BlockFailed(holder, block.Hash, testNow))
	require.False(t, f.bd.IsInFlight(block.Hash))
	require.Equal(t, 0, holder.State.nBlocksInFlight)

	peer := f.peerAt(t, "1.2.3.4:8333", 10)
	blocks, _ := f.bd.FindNextBlocksToDownload(peer, activeTip, MaxBlocksInTransitPerPeer, testNow)

	require.NotEmpty(t, blocks)
	require.Equal(t, block.Hash, blocks[0].Hash, "a failed download must be offered again")
}

func TestPeerDisconnected_ReleasesEverythingThePeerHeld(t *testing.T) {
	f := newDownloadFixture(t, 10)
	activeTip := f.node(t, 4)

	peer := f.peerAt(t, "1.2.3.4:8333", 10)
	other := f.peerAt(t, "5.6.7.8:8333", 10)

	require.True(t, f.bd.MarkBlockAsInFlight(peer, f.node(t, 5), testNow))
	require.True(t, f.bd.MarkBlockAsInFlight(peer, f.node(t, 6), testNow))
	require.True(t, f.bd.MarkBlockAsInFlight(other, f.node(t, 7), testNow))

	peer.State.nStallingSince = testNow
	peer.State.pindexLastCommonBlock = &activeTip

	f.bd.PeerDisconnected(peer)

	require.False(t, f.bd.IsInFlight(f.node(t, 5).Hash))
	require.False(t, f.bd.IsInFlight(f.node(t, 6).Hash))
	require.True(t, f.bd.IsInFlight(f.node(t, 7).Hash), "another peer's downloads must survive")

	require.Equal(t, 0, peer.State.nBlocksInFlight)
	require.Equal(t, int64(0), peer.State.nStallingSince)
	require.Nil(t, peer.State.pindexLastCommonBlock)
	require.Equal(t, 1, other.State.nBlocksInFlight)
	require.Equal(t, 1, f.bd.BlocksInFlight())
}

// multiPeerFixture is the world all-peer block scheduling happens in: three
// branches off one genesis.
//
//   - main is the fixture chain, BlockDownloadWindow + 6 headers at difficulty 1.
//     It is what every USEFUL peer announces, and none of it is downloaded.
//   - ours is three heavy headers and stands for our own active chain.
//   - light is five difficulty-1 headers. It stands TALLER than ours and carries
//     LESS work.
//
// Three branches rather than one chain, because the useless-peer case is a WORK
// test: Phase 3 Task 1 put ChainWork on HeaderNode and made the download gate
// order by work. A peer whose best known block merely stood lower than our tip
// would be refused by the last-common test instead, which proves nothing about
// the gate.
type multiPeerFixture struct {
	*downloadFixture

	ourTip   HeaderNode
	mainTip  HeaderNode
	lightTip HeaderNode
}

func newMultiPeerFixture(t *testing.T) *multiPeerFixture {
	t.Helper()

	f := newDownloadFixture(t, BlockDownloadWindow+6)

	ours := buildChainBits(t, f.idx, &nonceCounter{next: 90000}, f.genesis, 3, heavyBits)
	light := buildChainBits(t, f.idx, &nonceCounter{next: 95000}, f.genesis, 5, difficulty1Bits)

	ourTip, ok := f.idx.Lookup(ours[len(ours)-1].BlockHash())
	require.True(t, ok)

	lightTip, ok := f.idx.Lookup(light[len(light)-1].BlockHash())
	require.True(t, ok)

	require.Positive(t, ourTip.ChainWork.Cmp(lightTip.ChainWork),
		"test setup: the useless peer's branch must carry less work than our own tip")
	require.Greater(t, lightTip.Height, ourTip.Height,
		"test setup: the useless peer's branch must stand taller than our own tip, so height cannot be what refuses it")

	return &multiPeerFixture{
		downloadFixture: f,
		ourTip:          ourTip,
		mainTip:         f.node(t, len(f.chain)),
		lightTip:        lightTip,
	}
}

// usefulPeer returns a full-node peer whose best known block is the main
// branch tip, which is the availability Task 4's promotion sweep supplies to a
// peer that never held the sync slot.
func (f *multiPeerFixture) usefulPeer(addr string) *SyncPeer {
	peer := fullNodePeer(addr)
	best := f.mainTip
	peer.State.pindexBestKnownBlock = &best

	return peer
}

// requestedBlocks runs SendGetDataBlocks once for each peer in turn, which is
// the shape one sync tick has: net_processing.cpp drives SendMessages per peer
// (net_processing.cpp:5865) and each peer takes its own pass over the same
// window. It returns the hashes each peer was asked for, index-aligned with
// peers.
func requestedBlocks(t *testing.T, bd *BlockDownloader, activeTip HeaderNode, now int64, peers []*SyncPeer) [][]chainhash.Hash {
	t.Helper()

	asked := make([][]chainhash.Hash, len(peers))

	for i, peer := range peers {
		hashes := make([]chainhash.Hash, 0)

		for _, msg := range bd.SendGetDataBlocks(peer, activeTip, now) {
			getData, ok := msg.(*wire.MsgGetData)
			require.True(t, ok, "the block pass may only produce getdata, got %T", msg)

			for _, inv := range getData.InvList {
				require.Equal(t, wire.InvTypeBlock, inv.Type)

				hashes = append(hashes, inv.Hash)
			}
		}

		asked[i] = hashes
	}

	return asked
}

// heightRange is the inclusive height slice from..to. It pairs with heightsOf
// (headerindex_test.go) so a case can state the window slice it expects instead
// of a list of hashes.
func heightRange(from, to int32) []int32 {
	out := make([]int32, 0, to-from+1)

	for h := from; h <= to; h++ {
		out = append(out, h)
	}

	return out
}

// TestSendGetDataBlocks_SchedulesAcrossEveryUsefulPeer is the multi-peer
// contract: one sync tick offers the download window to EVERY peer that can
// serve blocks, and the window walk itself is what keeps the offers apart —
// peer B's walk skips whatever peer A took and carries on from there.
//
// It is the port of the per-peer getdata pass in net_processing.cpp SendMessages
// (net_processing.cpp:5865 drives it per peer; the pass itself is
// SendGetDataBlocks, net_processing.cpp:5662-5701, gated on
// nBlocksInFlight < MAX_BLOCKS_IN_TRANSIT_PER_PEER). Phase 2 could not exercise
// it: while a headers-first round ran, only the sync peer had a useful
// pindexBestKnownBlock. Phase 3 Task 4's promotion sweep gives every peer one,
// so the pass has more than one peer to distribute across.
func TestSendGetDataBlocks_SchedulesAcrossEveryUsefulPeer(t *testing.T) {
	tests := []struct {
		name string
		// peers builds the peers this tick runs over, in the order it reaches
		// them.
		peers func(t *testing.T, f *multiPeerFixture) []*SyncPeer
		// prepare runs before the tick, on the peers peers returned.
		prepare func(t *testing.T, f *multiPeerFixture, peers []*SyncPeer)
		// wantHeights is the window slice each peer must be asked for.
		wantHeights [][]int32
		// wantStaller is the index of the peer whose nStallingSince clock this
		// tick must start, or -1 when nobody may be named.
		wantStaller int
	}{
		{
			name: "two peers with the same best known block split the window",
			peers: func(_ *testing.T, f *multiPeerFixture) []*SyncPeer {
				return []*SyncPeer{f.usefulPeer("1.1.1.1:8333"), f.usefulPeer("2.2.2.2:8333")}
			},
			wantHeights: [][]int32{
				heightRange(1, MaxBlocksInTransitPerPeer),
				heightRange(MaxBlocksInTransitPerPeer+1, 2*MaxBlocksInTransitPerPeer),
			},
			wantStaller: -1,
		},
		{
			name: "a taller peer carrying less work than our own tip gets nothing",
			peers: func(_ *testing.T, f *multiPeerFixture) []*SyncPeer {
				useless := fullNodePeer("3.3.3.3:8333")
				best := f.lightTip
				useless.State.pindexBestKnownBlock = &best

				return []*SyncPeer{f.usefulPeer("1.1.1.1:8333"), useless, f.usefulPeer("2.2.2.2:8333")}
			},
			wantHeights: [][]int32{
				heightRange(1, MaxBlocksInTransitPerPeer),
				{},
				heightRange(MaxBlocksInTransitPerPeer+1, 2*MaxBlocksInTransitPerPeer),
			},
			wantStaller: -1,
		},
		{
			name: "the peer holding the window head is named the staller",
			peers: func(_ *testing.T, f *multiPeerFixture) []*SyncPeer {
				return []*SyncPeer{f.usefulPeer("1.1.1.1:8333"), f.usefulPeer("2.2.2.2:8333")}
			},
			prepare: func(t *testing.T, f *multiPeerFixture, peers []*SyncPeer) {
				t.Helper()

				// The first peer takes the head of the window and holds it: 16
				// blocks is where a silent peer comes to rest, because
				// SendGetDataBlocks is the only thing that marks blocks in flight
				// and nothing decrements the count until a block arrives.
				//
				// It is asked one second before the pass under test, INSIDE the
				// slow-fetch fuse, so the parallel fetch does not fire and the
				// staller rule is what the case exercises. The case below is the
				// same window with an older claim, where racing fires instead.
				head := requestedBlocks(t, f.bd, f.ourTip, testNow-micros(time.Second), peers[:1])
				require.Len(t, head[0], MaxBlocksInTransitPerPeer)

				// The second peer then downloads the whole rest of the window and
				// still cannot move, which is the only thing that names a staller.
				for h := MaxBlocksInTransitPerPeer + 1; h <= BlockDownloadWindow; h++ {
					node := f.node(t, h)

					require.True(t, f.bd.MarkBlockAsInFlight(peers[1], node, testNow))
					require.True(t, f.bd.BlockReceived(peers[1], node.Hash, testNow-micros(time.Minute)))
				}
			},
			wantHeights: [][]int32{{}, {}},
			wantStaller: 0,
		},
		{
			// The same shut window as the case above, with the head claimed long
			// enough ago to be raced. Two things follow, and they are the point
			// of the parallel fetch: the waiting peer is handed the contested
			// block instead of nothing, and NO staller is named — C++ sets
			// nodeStaller only when it reaches the window end with vBlocks empty
			// (net_processing.cpp FetchBlock), and the raced block is in vBlocks.
			// So the race replaces the disconnect rather than preceding it.
			name: "a stale claim on the window head is raced instead of naming a staller",
			peers: func(_ *testing.T, f *multiPeerFixture) []*SyncPeer {
				return []*SyncPeer{f.usefulPeer("1.1.1.1:8333"), f.usefulPeer("2.2.2.2:8333")}
			},
			prepare: func(t *testing.T, f *multiPeerFixture, peers []*SyncPeer) {
				t.Helper()

				head := requestedBlocks(t, f.bd, f.ourTip, testNow-micros(time.Minute), peers[:1])
				require.Len(t, head[0], MaxBlocksInTransitPerPeer)

				for h := MaxBlocksInTransitPerPeer + 1; h <= BlockDownloadWindow; h++ {
					node := f.node(t, h)

					require.True(t, f.bd.MarkBlockAsInFlight(peers[1], node, testNow))
					require.True(t, f.bd.BlockReceived(peers[1], node.Hash, testNow-micros(time.Minute)))
				}
			},
			wantHeights: [][]int32{{}, {1}},
			wantStaller: -1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newMultiPeerFixture(t)
			peers := tc.peers(t, f)

			require.Len(t, tc.wantHeights, len(peers), "test setup: one expectation per peer")

			if tc.prepare != nil {
				tc.prepare(t, f, peers)
			}

			asked := requestedBlocks(t, f.bd, f.ourTip, testNow, peers)

			for i := range peers {
				require.Equal(t, tc.wantHeights[i], heightsOf(t, f.idx, asked[i]), "peer %d was asked for the wrong window slice", i)
			}

			// The invariants every tick owes, whatever the case: no block is
			// requested from two peers, no peer is asked for more than the
			// in-transit cap, and nothing outside the download window is asked
			// for at all.
			holders := make(map[chainhash.Hash]int, 2*MaxBlocksInTransitPerPeer)

			for i, peer := range peers {
				require.LessOrEqual(t, len(asked[i]), MaxBlocksInTransitPerPeer,
					"peer %d was asked for more than MAX_BLOCKS_IN_TRANSIT_PER_PEER", i)

				for _, hash := range asked[i] {
					previous, dup := holders[hash]
					require.False(t, dup, "block %s was requested from peer %d and peer %d", hash, previous, i)

					holders[hash] = i

					require.True(t, f.bd.IsInFlightFrom(peer, hash),
						"a requested block must be recorded in flight from the peer it was requested from")

					node, ok := f.idx.Lookup(hash)
					require.True(t, ok)
					require.Positive(t, node.Height)
					require.LessOrEqual(t, node.Height, int32(BlockDownloadWindow),
						"the download window must bound every peer's batch, not just the first peer's")
				}
			}

			for i, peer := range peers {
				if i == tc.wantStaller {
					require.Equal(t, testNow, peer.State.nStallingSince, "peer %d must be named the staller", i)
					continue
				}

				require.Zero(t, peer.State.nStallingSince, "peer %d must not be named the staller", i)
			}
		})
	}
}

// TestSendGetDataBlocks_AdvancesEachPeersLastCommonBlockIndependently pins that
// pindexLastCommonBlock is per-peer state, not shared. It is a CNodeState field
// in the source (net_processing.cpp) and a peerSyncState field here, and the
// multi-peer contract depends on it: two peers walk the same window from
// different starting points, so one peer's progress must not move where another
// peer's walk resumes.
func TestSendGetDataBlocks_AdvancesEachPeersLastCommonBlockIndependently(t *testing.T) {
	f := newMultiPeerFixture(t)

	deliverer := f.usefulPeer("1.1.1.1:8333")
	silent := f.usefulPeer("2.2.2.2:8333")
	peers := []*SyncPeer{deliverer, silent}

	asked := requestedBlocks(t, f.bd, f.ourTip, testNow, peers)
	require.Len(t, asked[0], MaxBlocksInTransitPerPeer)
	require.Len(t, asked[1], MaxBlocksInTransitPerPeer)

	// Only the first peer delivers. Its whole batch is the unbroken run above
	// the fork point, so its own last-common block may advance over it.
	for _, hash := range asked[0] {
		require.True(t, f.bd.BlockReceived(deliverer, hash, testNow))
	}

	requestedBlocks(t, f.bd, f.ourTip, testNow+micros(time.Second), peers)

	require.NotNil(t, deliverer.State.pindexLastCommonBlock)
	require.Equal(t, int32(MaxBlocksInTransitPerPeer), deliverer.State.pindexLastCommonBlock.Height,
		"the delivering peer's window must move up over the run it completed")

	// The silent peer's walk still resumes where it started: the fork point
	// between our own branch and the branch both peers announced, which is
	// genesis here.
	require.NotNil(t, silent.State.pindexLastCommonBlock)
	require.Equal(t, f.genesis.BlockHash(), silent.State.pindexLastCommonBlock.Hash,
		"another peer's deliveries must not move where this peer's walk resumes")
}

// TestSendGetDataBlocks_ReSchedulesBlocksAPeerDisconnectReleased pins the
// RELEASE path only: ClearPeer puts everything a departed peer was downloading
// back on offer, and the next tick's pass hands those blocks to a peer that is
// still here.
//
// It is NOT a claim that a silent peer's blocks come back on their own. A peer
// that keeps its connection and answers nothing holds
// MaxBlocksInTransitPerPeer blocks until something disconnects it — see the
// cost note on CheckStall and
// TestSyncPass_ReHandedBlocksToASilentRotatedPeerAreReleasedAgain. Taking single
// blocks back from a peer that is still connected needs the per-block download
// timeout, which this task does not carry.
func TestSendGetDataBlocks_ReSchedulesBlocksAPeerDisconnectReleased(t *testing.T) {
	f := newMultiPeerFixture(t)

	going := f.usefulPeer("1.1.1.1:8333")
	staying := f.usefulPeer("2.2.2.2:8333")
	peers := []*SyncPeer{going, staying}

	asked := requestedBlocks(t, f.bd, f.ourTip, testNow, peers)

	released := asked[0]
	require.Equal(t, heightRange(1, MaxBlocksInTransitPerPeer), heightsOf(t, f.idx, released))

	// The peer that is staying finishes its own batch, so the in-transit cap is
	// not what stops it taking the released blocks.
	for _, hash := range asked[1] {
		require.True(t, f.bd.BlockReceived(staying, hash, testNow))
	}

	f.bd.PeerDisconnected(going)

	for _, hash := range released {
		require.False(t, f.bd.IsInFlight(hash), "a departed peer's downloads must go back on offer")
	}

	next := requestedBlocks(t, f.bd, f.ourTip, testNow+micros(time.Second), []*SyncPeer{staying})

	require.Equal(t, released, next[0],
		"the blocks the disconnect released must be the head of the window the remaining peer takes next")

	for _, hash := range released {
		require.True(t, f.bd.IsInFlightFrom(staying, hash))
	}
}

// ---------------------------------------------------------------------------
// Task 6: per-block download timeout (nDownloadingSince)
// ---------------------------------------------------------------------------

// ibd forces the fixture out of the near-tip window, so CheckStall reads the
// initial-block-download timeout base. The fixture's headers carry
// wire.NewBlockHeader's own timestamp, which is the moment the test built them,
// so the default fixture is always in the STEADY state; moving the adjusted
// clock two days forward is what makes the tip look old.
func (f *downloadFixture) ibd(t *testing.T) {
	t.Helper()

	require.True(t, f.hs.tipIsNearAdjustedTime(), "test setup: the fixture starts in the steady state")

	real := f.hs.cfg.AdjustedTime
	f.hs.cfg.AdjustedTime = func() int64 { return real() + 2*NearTipHeaderSyncWindow }

	require.False(t, f.hs.tipIsNearAdjustedTime(), "test setup: the tip must now look older than the window")
}

// downloadingFrom marks one block in flight from a fresh peer at each of the
// given heights, so the peer under test has that many OTHER peers downloading
// validated blocks alongside it. It returns them.
func (f *downloadFixture) downloadingFrom(t *testing.T, heights ...int) []*SyncPeer {
	t.Helper()

	peers := make([]*SyncPeer, 0, len(heights))

	for i, h := range heights {
		peer := f.peerAt(t, fmt.Sprintf("10.0.0.%d:8333", i+1), len(f.chain))
		require.True(t, f.bd.MarkBlockAsInFlight(peer, f.node(t, h), testNow))

		peers = append(peers, peer)
	}

	return peers
}

// TestCheckStall_TimesOutTheFrontBlock is the DetectStalling second half
// (net_processing.cpp:5629-5661): a block sitting at the head of a peer's
// in-flight queue past maxDownloadTime costs the peer its connection, whether
// or not any other peer has named it a staller.
//
// The expected windows are derived by hand from the C++ formula, not from the
// implementation:
//
//	maxDownloadTime = nPowTargetSpacing * (timeoutBase + 50 * otherPeers) * 10000 microseconds
//
// with nPowTargetSpacing = 600 (mainnet, syncTestParams), timeoutBase = 100
// percent in the steady state and 600 percent during initial block download
// (validation.h:177-185). So 600 * 100 * 10000 microseconds is 10 minutes, and
// every other downloading peer adds 600 * 50 * 10000, which is 5 minutes.
func TestCheckStall_TimesOutTheFrontBlock(t *testing.T) {
	tests := []struct {
		name string
		// ibd runs the case against the initial-block-download base.
		ibd bool
		// others is how many OTHER peers hold a block in flight.
		others int
		want   time.Duration
	}{
		{name: "steady state, no other downloading peer", want: 10 * time.Minute},
		{name: "steady state, one other downloading peer", others: 1, want: 15 * time.Minute},
		{name: "steady state, two other downloading peers", others: 2, want: 20 * time.Minute},
		{name: "initial block download, no other downloading peer", ibd: true, want: 60 * time.Minute},
		{name: "initial block download, one other downloading peer", ibd: true, others: 1, want: 65 * time.Minute},
		{name: "initial block download, two other downloading peers", ibd: true, others: 2, want: 70 * time.Minute},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newDownloadFixture(t, 10)

			if tc.ibd {
				f.ibd(t)
			}

			// Heights 8 and 9 keep the other peers' blocks clear of the one
			// under test, since a block is only ever in flight from one peer.
			f.downloadingFrom(t, []int{8, 9}[:tc.others]...)

			peer := f.peerAt(t, "1.2.3.4:8333", 10)
			require.True(t, f.bd.MarkBlockAsInFlight(peer, f.node(t, 1), testNow))
			require.Equal(t, testNow, peer.State.nDownloadingSince,
				"the first block of a batch arms the clock (block_download_tracker.cpp:46-50)")

			require.Equal(t, StallActionNone, f.bd.CheckStall(peer, noIngest, testNow+micros(tc.want)),
				"the peer survives up to and including the timeout")

			require.Equal(t, StallActionDisconnectTimeout, f.bd.CheckStall(peer, noIngest, testNow+micros(tc.want)+1),
				"one microsecond past it, the front block has timed out")
		})
	}
}

// TestCheckStall_IgnoresAPeerOwingNothing pins the vBlocksInFlight.size() > 0
// guard: the clock is meaningless for a peer with an empty queue, and a stale
// nDownloadingSince from an earlier batch must not disconnect it.
func TestCheckStall_IgnoresAPeerOwingNothing(t *testing.T) {
	f := newDownloadFixture(t, 3)
	peer := f.peerAt(t, "1.2.3.4:8333", 3)

	block := f.node(t, 1)
	require.True(t, f.bd.MarkBlockAsInFlight(peer, block, testNow))
	require.True(t, f.bd.BlockReceived(peer, block.Hash, testNow))
	require.Empty(t, peer.State.vBlocksInFlight)

	require.Equal(t, StallActionNone, f.bd.CheckStall(peer, noIngest, testNow+micros(10*time.Hour)),
		"a peer that owes us nothing cannot time out on a block")
}

// TestBlockReceived_ReArmsTheClockOnlyForTheFrontBlock is
// removeFromBlockMapNL's nDownloadingSince half
// (block_download_tracker.cpp:311-315): the clock measures the FRONT of the
// queue, so it restarts when the front leaves and stands still when anything
// else does. Without the second half a peer could deliver its later blocks for
// ever while withholding the one at the head and never time out; without the
// first, a peer that delivers every block promptly would eventually be
// disconnected on the age of a queue that has fully turned over.
func TestBlockReceived_ReArmsTheClockOnlyForTheFrontBlock(t *testing.T) {
	t.Run("the front block re-arms it", func(t *testing.T) {
		f := newDownloadFixture(t, 5)
		peer := f.peerAt(t, "1.2.3.4:8333", 5)

		first, second := f.node(t, 1), f.node(t, 2)
		require.True(t, f.bd.MarkBlockAsInFlight(peer, first, testNow))
		require.True(t, f.bd.MarkBlockAsInFlight(peer, second, testNow))
		require.Equal(t, testNow, peer.State.nDownloadingSince)

		later := testNow + micros(9*time.Minute)
		require.True(t, f.bd.BlockReceived(peer, first.Hash, later))

		require.Equal(t, later, peer.State.nDownloadingSince,
			"the next block's clock starts when the one ahead of it arrived")
		require.Equal(t, []chainhash.Hash{second.Hash}, peer.State.vBlocksInFlight)

		// The re-arm is what buys the second block its own full window.
		require.Equal(t, StallActionNone, f.bd.CheckStall(peer, noIngest, later+micros(10*time.Minute)))
		require.Equal(t, StallActionDisconnectTimeout, f.bd.CheckStall(peer, noIngest, later+micros(10*time.Minute)+1))
	})

	t.Run("a block behind the front does not", func(t *testing.T) {
		f := newDownloadFixture(t, 5)
		peer := f.peerAt(t, "1.2.3.4:8333", 5)

		first, second := f.node(t, 1), f.node(t, 2)
		require.True(t, f.bd.MarkBlockAsInFlight(peer, first, testNow))
		require.True(t, f.bd.MarkBlockAsInFlight(peer, second, testNow))

		later := testNow + micros(9*time.Minute)
		require.True(t, f.bd.BlockReceived(peer, second.Hash, later))

		require.Equal(t, testNow, peer.State.nDownloadingSince,
			"delivering a later block must not buy the withheld front block more time")
		require.Equal(t, []chainhash.Hash{first.Hash}, peer.State.vBlocksInFlight)

		require.Equal(t, StallActionDisconnectTimeout, f.bd.CheckStall(peer, noIngest, testNow+micros(10*time.Minute)+1))
	})

	t.Run("the clock never moves backwards", func(t *testing.T) {
		f := newDownloadFixture(t, 5)
		peer := f.peerAt(t, "1.2.3.4:8333", 5)

		first, second := f.node(t, 1), f.node(t, 2)
		require.True(t, f.bd.MarkBlockAsInFlight(peer, first, testNow))
		require.True(t, f.bd.MarkBlockAsInFlight(peer, second, testNow))

		// C++ takes std::max of the old value and now, so an out-of-order or
		// duplicate clock reading cannot lengthen the next block's window.
		require.True(t, f.bd.BlockReceived(peer, first.Hash, testNow-micros(time.Minute)))

		require.Equal(t, testNow, peer.State.nDownloadingSince)
	})
}

// TestCheckStall_TimesOutARotatedPeerHoldingBlocks is the case Task 3 left
// pending and the reason this timeout is load-bearing rather than a refinement.
// A rotation clears fSyncStarted, so the rotation clause can never judge that
// peer again while it holds no slot, and until now only another peer naming it
// the staller could take its blocks back — which needs the whole rest of the
// download window drained first, and a second eligible peer to do the draining.
// The timeout reaches it with neither.
func TestCheckStall_TimesOutARotatedPeerHoldingBlocks(t *testing.T) {
	f := newDownloadFixture(t, 10)

	peer := f.peerAt(t, "1.2.3.4:8333", 10)
	require.False(t, peer.State.fSyncStarted, "this is the state a rotation leaves behind")

	require.True(t, f.bd.MarkBlockAsInFlight(peer, f.node(t, 1), testNow))

	require.Equal(t, int64(0), peer.State.nStallingSince, "no other peer has named it a staller")

	require.Equal(t, StallActionDisconnectTimeout, f.bd.CheckStall(peer, noIngest, testNow+micros(10*time.Minute)+1),
		"the timeout must run before the fSyncStarted early return")
}

// TestCheckStall_TheTimeoutIgnoresThroughput pins the clause's defining
// property: it is a pure wall clock. A peer delivering bytes at any rate is
// disconnected when the block at the head of its queue runs out of time, and
// the partial download is discarded.
//
// That is deliberate in SVNode and it is kept deliberately here. Every other
// rule governing a slow peer weighs throughput — the staller clause, SVNode's
// parallel-fetch trigger, the rotation suppression below — and throughput
// evidence is gameable: a peer trickling bytes at the floor satisfies all of
// them for ever. This clause is the one bound that cannot be talked out of.
// SVNode discards the bytes even when another peer has already delivered the
// same block, because keeping the download window moving is worth more than the
// bytes already spent.
//
// The operator's relief is the window itself, not an exception to it:
// TestCheckStall_HonoursTheConfiguredTimeoutWindow covers that.
func TestCheckStall_TheTimeoutIgnoresThroughput(t *testing.T) {
	f := newDownloadFixture(t, 10)
	peer := f.peerAt(t, "1.2.3.4:8333", 10)

	require.True(t, f.bd.MarkBlockAsInFlight(peer, f.node(t, 1), testNow))

	const tick = 30 * time.Second

	// Bytes arriving at ten times the rate floor, every tick, without a pause.
	perTick := uint64(MinBlockDownloadBytesPerSec) * uint64(tick/time.Second) * 10
	ingest := IngestSnapshot{Active: true, StartedMicros: testNow}

	var last StallAction

	for elapsed := time.Duration(0); elapsed <= 10*time.Minute; elapsed += tick {
		ingest.BytesRead += perTick
		last = f.bd.CheckStall(peer, ingest, testNow+micros(elapsed))

		require.Equal(t, StallActionNone, last, "inside the window the peer is kept, at %s", elapsed)
	}

	ingest.BytesRead += perTick

	require.Equal(t, StallActionDisconnectTimeout, f.bd.CheckStall(peer, ingest, testNow+micros(10*time.Minute)+1),
		"past the window a peer in full flow goes anyway, partial block and all")
}

// TestCheckStall_HonoursTheConfiguredTimeoutWindow is the operator's relief
// valve, and the reason these three percentages are settings rather than
// constants like BlockDownloadWindow beside them.
//
// SVNode can disconnect a slow peer cheaply because it races the block to
// several peers at once, so the block still arrives. This port does not race,
// so a disconnect restarts the download from zero with no more time than the
// attempt before it — and in the steady state the window is one bare block
// interval, because one block in flight means no other downloading peer to be
// compensated for. Ten minutes is 6.7 MB/s for a 4 GB block. An operator whose
// blocks outgrow that widens the window, exactly as an SVNode operator does
// with -blockdownloadtimeoutbasepercent.
func TestCheckStall_HonoursTheConfiguredTimeoutWindow(t *testing.T) {
	tests := []struct {
		name    string
		base    int64
		ibdBase int64
		perPeer int64
		ibd     bool
		// others is how many OTHER peers hold a block in flight.
		others int
		want   time.Duration
	}{
		{name: "the default steady-state window", want: 10 * time.Minute},
		{name: "a base of 300 percent triples it", base: 300, want: 30 * time.Minute},
		{name: "the initial-block-download base is separate", base: 300, ibdBase: 1200, ibd: true, want: 120 * time.Minute},
		{name: "the per-peer term is configurable too", perPeer: 200, others: 2, want: 50 * time.Minute},
		{name: "zero reads as unset, never as no window", base: 0, want: 10 * time.Minute},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newDownloadFixture(t, 10)

			// The path an operator's settings actually travel: SyncConfig into
			// ConfigureSync. Applied to the fixture's own downloader here, by the
			// same rules, so the case reads as a window rather than as plumbing;
			// TestConfigureSync_CarriesTheBlockDownloadTimeoutSettings
			// (manager_test.go) walks the plumbing itself.
			if tc.base > 0 {
				f.bd.timeoutBasePercent = tc.base
			}

			if tc.ibdBase > 0 {
				f.bd.timeoutBaseIBDPercent = tc.ibdBase
			}

			if tc.perPeer > 0 {
				f.bd.timeoutPerPeerPercent = tc.perPeer
			}

			if tc.ibd {
				f.ibd(t)
			}

			f.downloadingFrom(t, []int{8, 9}[:tc.others]...)

			peer := f.peerAt(t, "1.2.3.4:8333", 10)
			require.True(t, f.bd.MarkBlockAsInFlight(peer, f.node(t, 1), testNow))

			require.Equal(t, StallActionNone, f.bd.CheckStall(peer, noIngest, testNow+micros(tc.want)))
			require.Equal(t, StallActionDisconnectTimeout, f.bd.CheckStall(peer, noIngest, testNow+micros(tc.want)+1))
		})
	}
}

// TestCheckStall_TimeoutAndRotationOrder pins which of the two give-up rules
// reaches a dribbling peer first, in both regimes.
//
// The split between them: the SVNode timeout judges the FRONT block's wall
// clock and nothing else; the Teranode rotation judges a live ingest's byte
// rate and gives up on it at MaxBlockDownloadTime. So which one fires is
// decided by whether the front block's window is shorter than that cap.
//
// Steady state, one block interval: the timeout is due at 10 minutes and the
// cap is irrelevant — the peer is disconnected while still dribbling, which is
// the whole point of an unconditional clause. Initial block download, six
// intervals: the 60 minute window outlasts the 30 minute cap, so the rotation
// fires first, exactly as it did before this timeout existed. Both release the
// peer's blocks; the disconnect also costs it the connection.
func TestCheckStall_TimeoutAndRotationOrder(t *testing.T) {
	// dribble runs a healthy-but-slow ingest from a sync peer, tick by tick,
	// and returns the first action that is not None along with when it came.
	dribble := func(t *testing.T, f *downloadFixture, until time.Duration) (StallAction, time.Duration) {
		t.Helper()

		peer := f.syncingPeer(t, "1.2.3.4:8333")

		const tick = time.Minute

		perTick := uint64(MinBlockDownloadBytesPerSec) * uint64(tick/time.Second) * 2
		ingest := IngestSnapshot{Active: true, StartedMicros: testNow}

		require.Equal(t, StallActionNone, f.bd.CheckStall(peer, ingest, testNow))

		for elapsed := tick; elapsed <= until; elapsed += tick {
			ingest.BytesRead += perTick

			if action := f.bd.CheckStall(peer, ingest, testNow+micros(elapsed)); action != StallActionNone {
				return action, elapsed
			}
		}

		return StallActionNone, 0
	}

	t.Run("steady state: the timeout fires at one block interval", func(t *testing.T) {
		f := newDownloadFixture(t, 3)

		action, when := dribble(t, f, 2*MaxBlockDownloadTime)

		require.Equal(t, StallActionDisconnectTimeout, action,
			"a dribbling peer goes on the front block's clock, not on the throughput cap")
		require.Equal(t, 11*time.Minute, when,
			"the first tick past the 10 minute window is what catches it")
	})

	t.Run("initial block download: the rotation fires at the throughput cap", func(t *testing.T) {
		f := newDownloadFixture(t, 3)
		f.ibd(t)

		action, when := dribble(t, f, 2*MaxBlockDownloadTime)

		require.Equal(t, StallActionRotateSyncPeer, action,
			"the 60 minute window has not expired, so the legacy cap is what gives up")
		require.Equal(t, MaxBlockDownloadTime, when)
	})
}

// TestPeerDisconnected_ClearsTheDownloadClock pins that the timeout state goes
// with everything else a peer held, so a peer whose blocks were all released
// cannot be judged on a clock that measures a queue it no longer owns. It is
// the rotation path too: clearPeer serves both.
func TestPeerDisconnected_ClearsTheDownloadClock(t *testing.T) {
	f := newDownloadFixture(t, 5)
	peer := f.peerAt(t, "1.2.3.4:8333", 5)

	require.True(t, f.bd.MarkBlockAsInFlight(peer, f.node(t, 1), testNow))
	require.True(t, f.bd.MarkBlockAsInFlight(peer, f.node(t, 2), testNow))

	f.bd.PeerDisconnected(peer)

	require.Empty(t, peer.State.vBlocksInFlight)
	require.Equal(t, int64(0), peer.State.nDownloadingSince)

	require.Equal(t, StallActionNone, f.bd.CheckStall(peer, noIngest, testNow+micros(10*time.Hour)))
}

// TestBlockFailed_ReArmsTheClockLikeADelivery pins that a released front block
// re-arms the clock the same way a delivered one does. C++ runs both through
// removeFromBlockMapNL, so a rejected or cancelled front block must not leave
// the next block in the queue measured from the failed one's request time.
func TestBlockFailed_ReArmsTheClockLikeADelivery(t *testing.T) {
	f := newDownloadFixture(t, 5)
	peer := f.peerAt(t, "1.2.3.4:8333", 5)

	first, second := f.node(t, 1), f.node(t, 2)
	require.True(t, f.bd.MarkBlockAsInFlight(peer, first, testNow))
	require.True(t, f.bd.MarkBlockAsInFlight(peer, second, testNow))

	later := testNow + micros(9*time.Minute)
	require.True(t, f.bd.BlockFailed(peer, first.Hash, later))

	require.Equal(t, later, peer.State.nDownloadingSince)
	require.Equal(t, []chainhash.Hash{second.Hash}, peer.State.vBlocksInFlight)
}

// ---------------------------------------------------------------------------
// Task 6b: parallel block fetch
// ---------------------------------------------------------------------------

// deliveringPeer returns a peer whose cached delivery rate is bytesPerSec as of
// nowMicros, arranged the way production arranges it: two CheckStall ticks with
// an ingest whose byte count rose between them.
func (f *downloadFixture) deliveringPeer(t *testing.T, addr string, bytesPerSec uint64, nowMicros int64) *SyncPeer {
	t.Helper()

	peer := f.peerAt(t, addr, len(f.chain))

	const tick = time.Second

	ingest := IngestSnapshot{Active: true, StartedMicros: nowMicros - micros(tick)}

	f.bd.CheckStall(peer, ingest, nowMicros-micros(tick))

	ingest.BytesRead = bytesPerSec

	f.bd.CheckStall(peer, ingest, nowMicros)

	return peer
}

// TestCheckStall_CachesThePeerDeliveryRate pins the meter the parallel-fetch
// branch reads. SVNode asks the association for a per-peer block bandwidth
// average (net_processing.cpp:105-109); this port has no such accessor, so
// CheckStall computes the rate from the ingest it is already given and leaves it
// on the peer's state, where any later reader can find it.
func TestCheckStall_CachesThePeerDeliveryRate(t *testing.T) {
	t.Run("a delivering peer's rate is readable afterwards", func(t *testing.T) {
		f := newDownloadFixture(t, 3)

		peer := f.deliveringPeer(t, "1.2.3.4:8333", 4*BlockStallingMinDownloadRate, testNow)

		require.Equal(t, uint64(4*BlockStallingMinDownloadRate), peer.State.nIngestBytesPerSec)
		require.Equal(t, testNow, peer.State.nIngestRateMicros)
		require.False(t, f.bd.downloadStallingFrom(peer, testNow))
	})

	t.Run("a peer with no ingest is stalling", func(t *testing.T) {
		f := newDownloadFixture(t, 3)
		peer := f.peerAt(t, "1.2.3.4:8333", 3)

		require.Equal(t, StallActionNone, f.bd.CheckStall(peer, noIngest, testNow))

		require.Equal(t, uint64(0), peer.State.nIngestBytesPerSec)
		require.True(t, f.bd.downloadStallingFrom(peer, testNow),
			"a peer delivering nothing is what the C++ zero-bandwidth case is")
	})

	t.Run("a rate below the floor is stalling", func(t *testing.T) {
		f := newDownloadFixture(t, 3)

		peer := f.deliveringPeer(t, "1.2.3.4:8333", BlockStallingMinDownloadRate-1, testNow)

		require.True(t, f.bd.downloadStallingFrom(peer, testNow))
	})

	t.Run("a stale sample reads as stalling, not as its old rate", func(t *testing.T) {
		f := newDownloadFixture(t, 3)

		peer := f.deliveringPeer(t, "1.2.3.4:8333", 10*BlockStallingMinDownloadRate, testNow)

		require.False(t, f.bd.downloadStallingFrom(peer, testNow))
		require.True(t, f.bd.downloadStallingFrom(peer, testNow+micros(BlockDownloadSlowFetchTimeout)+1),
			"a rate nobody has measured this recently is not evidence the peer is alive")
	})

	t.Run("the rotation floor is not the racing floor", func(t *testing.T) {
		// A peer between the two thresholds: above legacy's rotation floor, so
		// it keeps the sync slot, and below SVNode's racing floor, so its blocks
		// may be raced. Both rules are meant to be true at once here.
		require.Less(t, uint64(MinBlockDownloadBytesPerSec), uint64(BlockStallingMinDownloadRate))

		f := newDownloadFixture(t, 3)

		peer := f.deliveringPeer(t, "1.2.3.4:8333", MinBlockDownloadBytesPerSec*3/2, testNow)

		require.True(t, f.bd.downloadStallingFrom(peer, testNow))
	})
}

// TestFindNextBlocksToDownload_RacesASlowBlock is the parallel-fetch branch,
// net_processing.cpp:461-506. It fires on the first already-in-flight block a
// walk meets, and only when EVERY holder of it has been sitting on it past the
// slow-fetch timeout while delivering below the rate floor. One holder inside
// the timeout, or one holder still delivering, cancels the race.
func TestFindNextBlocksToDownload_RacesASlowBlock(t *testing.T) {
	// The contested block is the first one above our tip, so it is the first
	// already-in-flight block any walk meets.
	const contested = 1

	tests := []struct {
		name string
		// holders describes the peers already fetching the contested block:
		// how long ago each was asked, and what rate each is delivering at.
		holders []struct {
			askedAgo time.Duration
			rate     uint64
		}
		maxParallel int
		wantRaced   bool
		reason      string
	}{
		{
			name: "one silent holder past the timeout is raced",
			holders: []struct {
				askedAgo time.Duration
				rate     uint64
			}{{askedAgo: BlockDownloadSlowFetchTimeout + time.Second, rate: 0}},
			wantRaced: true,
			reason:    "this is the case the branch exists for",
		},
		{
			name: "a holder inside the timeout is given more time",
			holders: []struct {
				askedAgo time.Duration
				rate     uint64
			}{{askedAgo: BlockDownloadSlowFetchTimeout - time.Second, rate: 0}},
			wantRaced: false,
			reason:    "C++ breaks out of the loop: give this peer more time",
		},
		{
			name: "a holder still delivering is not raced however long it has held it",
			holders: []struct {
				askedAgo time.Duration
				rate     uint64
			}{{askedAgo: 10 * BlockDownloadSlowFetchTimeout, rate: 4 * BlockStallingMinDownloadRate}},
			wantRaced: false,
			reason:    "this peer seems active currently",
		},
		{
			name: "one live holder among two stalled ones cancels the race",
			holders: []struct {
				askedAgo time.Duration
				rate     uint64
			}{
				{askedAgo: 2 * BlockDownloadSlowFetchTimeout, rate: 0},
				{askedAgo: 2 * BlockDownloadSlowFetchTimeout, rate: 4 * BlockStallingMinDownloadRate},
			},
			wantRaced: false,
			reason:    "the block is coming; a second copy would be waste",
		},
		{
			name: "two stalled holders are raced, since the cap is three",
			holders: []struct {
				askedAgo time.Duration
				rate     uint64
			}{
				{askedAgo: 2 * BlockDownloadSlowFetchTimeout, rate: 0},
				{askedAgo: 2 * BlockDownloadSlowFetchTimeout, rate: 0},
			},
			wantRaced: true,
		},
		{
			name: "the cap is honoured",
			holders: []struct {
				askedAgo time.Duration
				rate     uint64
			}{
				{askedAgo: 2 * BlockDownloadSlowFetchTimeout, rate: 0},
				{askedAgo: 2 * BlockDownloadSlowFetchTimeout, rate: 0},
				{askedAgo: 2 * BlockDownloadSlowFetchTimeout, rate: 0},
			},
			wantRaced: false,
			reason:    "three holders already, and DEFAULT_MAX_BLOCK_PARALLEL_FETCH is three",
		},
		{
			name: "a lower cap bites sooner",
			holders: []struct {
				askedAgo time.Duration
				rate     uint64
			}{{askedAgo: 2 * BlockDownloadSlowFetchTimeout, rate: 0}},
			maxParallel: 1,
			wantRaced:   false,
			reason:      "one staller already reaches a cap of one",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newDownloadFixture(t, 10)

			if tc.maxParallel > 0 {
				f.bd.maxParallelFetch = tc.maxParallel
			}

			block := f.node(t, contested)

			for i, h := range tc.holders {
				holder := f.deliveringPeer(t, fmt.Sprintf("10.0.0.%d:8333", i+1), h.rate, testNow)
				require.True(t, f.bd.MarkBlockAsInFlight(holder, block, testNow-micros(h.askedAgo)))
			}

			// The peer doing the walk has an empty ingest of its own, which is
			// no bar to it being handed work.
			walker := f.peerAt(t, "1.2.3.4:8333", 10)

			blocks, _ := f.bd.FindNextBlocksToDownload(walker, f.node(t, 0), MaxBlocksInTransitPerPeer, testNow)

			raced := false

			for _, b := range blocks {
				if b.Hash == block.Hash {
					raced = true
				}
			}

			require.Equal(t, tc.wantRaced, raced, tc.reason)

			// Whatever it decided about the contested block, the walk carries on
			// past it: the blocks above are not in flight and must be offered.
			require.NotEmpty(t, blocks)
		})
	}
}

// TestFindNextBlocksToDownload_NeverRacesABlockToItsOwnHolder pins the
// IsInFlight({hash, nodeid}) half of the C++ guard. Without it a peer sitting on
// a block past the timeout would be asked for the very block it is failing to
// deliver, every tick, for ever.
func TestFindNextBlocksToDownload_NeverRacesABlockToItsOwnHolder(t *testing.T) {
	f := newDownloadFixture(t, 10)

	block := f.node(t, 1)

	// One silent holder, which is also the peer taking the walk.
	holder := f.deliveringPeer(t, "1.2.3.4:8333", 0, testNow)
	require.True(t, f.bd.MarkBlockAsInFlight(holder, block, testNow-micros(2*BlockDownloadSlowFetchTimeout)))

	blocks, _ := f.bd.FindNextBlocksToDownload(holder, f.node(t, 0), MaxBlocksInTransitPerPeer, testNow)

	for _, b := range blocks {
		require.NotEqual(t, block.Hash, b.Hash, "a peer must not be raced against itself")
	}
}

// TestSendGetDataBlocks_ARacedBlockCostsAnInTransitSlot pins that racing is not
// free of the in-transit cap: the raced block goes through the same
// MarkBlockAsInFlight the walk's own blocks do, so it counts against
// MaxBlocksInTransitPerPeer like any other.
func TestSendGetDataBlocks_ARacedBlockCostsAnInTransitSlot(t *testing.T) {
	f := newDownloadFixture(t, BlockDownloadWindow+6)

	block := f.node(t, 1)

	holder := f.deliveringPeer(t, "5.6.7.8:8333", 0, testNow)
	require.True(t, f.bd.MarkBlockAsInFlight(holder, block, testNow-micros(2*BlockDownloadSlowFetchTimeout)))

	walker := f.peerAt(t, "1.2.3.4:8333", BlockDownloadWindow+6)

	msgs := f.bd.SendGetDataBlocks(walker, f.node(t, 0), testNow)
	require.Len(t, msgs, 1)

	getData, ok := msgs[0].(*wire.MsgGetData)
	require.True(t, ok)

	require.Len(t, getData.InvList, MaxBlocksInTransitPerPeer)
	require.Equal(t, MaxBlocksInTransitPerPeer, walker.State.nBlocksInFlight)
	require.Equal(t, block.Hash, getData.InvList[0].Hash, "the raced block leads the batch, being lowest")
	require.True(t, f.bd.IsInFlightFrom(walker, block.Hash))
	require.True(t, f.bd.IsInFlightFrom(holder, block.Hash), "the original holder keeps its claim")
}

// ---------------------------------------------------------------------------
// Task 21: defer re-request of parent-missing blocks
// ---------------------------------------------------------------------------

// requestedHeights runs one walk and returns the heights it would ask this peer
// for, which is what a deferral is visible in.
func (f *downloadFixture) requestedHeights(t *testing.T, peer *SyncPeer, activeTip HeaderNode, nowMicros int64) []int32 {
	t.Helper()

	blocks, _ := f.bd.FindNextBlocksToDownload(peer, activeTip, MaxBlocksInTransitPerPeer, nowMicros)

	heights := make([]int32, len(blocks))
	for i := range blocks {
		heights[i] = blocks[i].Height
	}

	return heights
}

// TestBlockParentMissing_DefersTheReRequest is the loop this task closes.
//
// A block whose parent is not in our chain yet is refused before admission
// (bridge PreAdmit) and goes back on offer. Nothing throttled that, and
// re-requests are tick-driven, so the walk asked for it again on the very next
// pass — and again, until the parent landed. Measured on the Task 7 distribution
// leg before this change: 476 pre-admit refusals against 36 real ingests, about
// twenty-four fetches of every block that arrived early.
//
// The bytes are what makes it matter. Each of those refusals is a whole block
// pulled across the wire and dropped, and on mainnet a block is gigabytes.
func TestBlockParentMissing_DefersTheReRequest(t *testing.T) {
	f := newDownloadFixture(t, 10)
	peer := f.peerAt(t, "1.2.3.4:8333", 10)
	activeTip := f.node(t, 0)

	// The walk asks for the window from our tip up.
	require.Equal(t, heightRange(1, 10), f.requestedHeights(t, peer, activeTip, testNow))

	// Block 3 arrives ahead of its parent and is refused.
	third := f.node(t, 3)
	require.True(t, f.bd.MarkBlockAsInFlight(peer, third, testNow))
	require.True(t, f.bd.BlockParentMissing(peer, third.Hash, testNow))

	// The next pass must not ask for it again: its parent is still missing and
	// the round-trip would fail exactly as it just did.
	next := testNow + micros(time.Second)

	got := f.requestedHeights(t, peer, activeTip, next)
	require.NotContains(t, got, int32(3), "a parent-missing block must not be re-requested immediately")
	require.Contains(t, got, int32(1), "the rest of the window is unaffected")
	require.Contains(t, got, int32(4), "including the blocks above the deferred one")
}

// TestBlockParentMissing_ReleasesAsSoonAsTheParentIsHeld pins the load-bearing
// half of the rule. The stamp's clock is only a safety valve; what actually
// makes a deferred block eligible again is holding its parent, because that is
// the condition whose absence refused it.
func TestBlockParentMissing_ReleasesAsSoonAsTheParentIsHeld(t *testing.T) {
	f := newDownloadFixture(t, 10)
	peer := f.peerAt(t, "1.2.3.4:8333", 10)
	activeTip := f.node(t, 0)

	third := f.node(t, 3)
	require.True(t, f.bd.MarkBlockAsInFlight(peer, third, testNow))
	require.True(t, f.bd.BlockParentMissing(peer, third.Hash, testNow))

	// Blocks 1 and 2 then arrive in order, so block 3's parent is now held.
	for h := 1; h <= 2; h++ {
		node := f.node(t, h)
		require.True(t, f.bd.MarkBlockAsInFlight(peer, node, testNow))
		require.True(t, f.bd.BlockReceived(peer, node.Hash, testNow))
	}

	// Well inside the stamp's window: the parent test, not the clock, is what
	// releases it.
	next := testNow + micros(time.Second)
	require.Less(t, next, testNow+micros(ParentMissingRetryDelay))

	require.Contains(t, f.requestedHeights(t, peer, activeTip, next), int32(3),
		"holding the parent must make the block eligible at once")
}

// TestBlockParentMissing_TheStampExpires covers the safety valve: a parent that
// never lands must not defer its child for ever. The child is retried once the
// stamp runs out, refused again if the parent is still missing, and re-stamped —
// one round-trip per delay instead of one per tick.
func TestBlockParentMissing_TheStampExpires(t *testing.T) {
	f := newDownloadFixture(t, 10)
	peer := f.peerAt(t, "1.2.3.4:8333", 10)
	activeTip := f.node(t, 0)

	third := f.node(t, 3)
	require.True(t, f.bd.MarkBlockAsInFlight(peer, third, testNow))
	require.True(t, f.bd.BlockParentMissing(peer, third.Hash, testNow))

	require.NotContains(t, f.requestedHeights(t, peer, activeTip, testNow+micros(ParentMissingRetryDelay)),
		int32(3), "still deferred at the boundary")

	require.Contains(t, f.requestedHeights(t, peer, activeTip, testNow+micros(ParentMissingRetryDelay)+1),
		int32(3), "past the boundary it is offered again, parent or no parent")
}

// TestBlockParentMissing_StampsArePrunedByHeight pins that the map cannot grow
// without bound. It is pruned by the same height watermark as haveData, on the
// same call, which is why the stamp carries the height the index knew for it.
func TestBlockParentMissing_StampsArePrunedByHeight(t *testing.T) {
	f := newDownloadFixture(t, 10)
	peer := f.peerAt(t, "1.2.3.4:8333", 10)

	for h := 1; h <= 5; h++ {
		node := f.node(t, h)
		require.True(t, f.bd.MarkBlockAsInFlight(peer, node, testNow))
		require.True(t, f.bd.BlockParentMissing(peer, node.Hash, testNow))
	}

	require.Len(t, f.bd.retryAfter, 5)

	// A walk whose active tip has moved past them prunes them, exactly as it
	// prunes the have-data records at the same heights.
	f.bd.FindNextBlocksToDownload(peer, f.node(t, 5), MaxBlocksInTransitPerPeer, testNow)

	require.Empty(t, f.bd.retryAfter, "stamps at or below the active tip are dead and must be dropped")
}

// TestBlockParentMissing_AnUnknownHashIsNotStamped pins the same rule haveData
// keeps: a hash the index cannot place has no height, so it could never be
// pruned, so it is never recorded.
func TestBlockParentMissing_AnUnknownHashIsNotStamped(t *testing.T) {
	f := newDownloadFixture(t, 3)
	peer := f.peerAt(t, "1.2.3.4:8333", 3)

	stranger := chainhash.Hash{0xAB}

	require.False(t, f.bd.BlockParentMissing(peer, stranger, testNow),
		"a hash this peer does not hold in flight releases nothing")
	require.Empty(t, f.bd.retryAfter)
}

// TestCheckStall_HonoursTheConfiguredRateFloor covers the rotation rate floor
// once it is an operator setting rather than the MinBlockDownloadBytesPerSec
// constant.
//
// The semantics are legacy's, not this port's invention. Legacy compares the
// tick's byte delta against the floor with UNSIGNED operands (services/legacy/
// netsync/manager.go:266 `recvDiff/... < minSyncPeerNetworkSpeed`), so a floor
// of 0 makes the comparison false on every tick and no violation is ever
// recorded: 0 disables the floor. Legacy also states the consequence it must
// then guard against — "regardless of how the speed threshold is configured (it
// may be 0, which would otherwise make any rate pass)", manager.go:321-326 —
// and answers it with a separate zero-bytes test, which is the last case below.
func TestCheckStall_HonoursTheConfiguredRateFloor(t *testing.T) {
	// One step past the rotation window, so the second CheckStall below is the
	// one that judges throughput. It stays far inside MaxBlockDownloadTime, so
	// the wall-clock cap on the suppression is not what decides these cases.
	const window = MaxLastBlockTime + time.Second

	// Half the stock floor: below the default, comfortably above zero.
	const belowDefault = uint64(MinBlockDownloadBytesPerSec) / 2

	tests := []struct {
		name   string
		floor  uint64
		perSec uint64
		want   StallAction
	}{
		{
			name:   "the stock floor rotates a peer delivering below it",
			floor:  MinBlockDownloadBytesPerSec,
			perSec: belowDefault,
			want:   StallActionRotateSyncPeer,
		},
		{
			name:   "the stock floor keeps a peer delivering exactly at it",
			floor:  MinBlockDownloadBytesPerSec,
			perSec: MinBlockDownloadBytesPerSec,
			want:   StallActionNone,
		},
		{
			name:   "a lowered floor keeps the same below-default peer",
			floor:  belowDefault / 2,
			perSec: belowDefault,
			want:   StallActionNone,
		},
		{
			name:   "a floor of zero keeps a peer at any rate at all",
			floor:  0,
			perSec: 1,
			want:   StallActionNone,
		},
		{
			name:   "a floor of zero still rotates a peer delivering nothing",
			floor:  0,
			perSec: 0,
			want:   StallActionRotateSyncPeer,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// A fresh fixture per case: a rotation clears the sync slot and the
			// peer's in-flight claims, so the cases cannot share peer state.
			f := newDownloadFixture(t, 3)
			peer := f.syncingPeer(t, "1.2.3.4:8333")

			f.bd.minDownloadBytesPerSec = tc.floor

			ingest := IngestSnapshot{Active: true, StartedMicros: testNow}

			// Seeds the sample every rate below is measured against.
			require.Equal(t, StallActionNone, f.bd.CheckStall(peer, ingest, testNow))

			ingest.BytesRead = tc.perSec * uint64(window/time.Second)

			require.Equal(t, tc.want, f.bd.CheckStall(peer, ingest, testNow+micros(window)),
				"floor %d B/s against a peer delivering %d B/s", tc.floor, tc.perSec)

			require.Equal(t, tc.want != StallActionRotateSyncPeer, peer.State.fSyncStarted,
				"the sync slot must follow the rotation decision")
		})
	}
}

// mixedInvMsg builds an inv message from entries the caller types
// individually, including nil entries, which is what the OnInv error-contract
// and skip-contract tests need and invMsg above cannot express.
func mixedInvMsg(t *testing.T, entries ...*wire.InvVect) *wire.MsgInv {
	t.Helper()

	msg := wire.NewMsgInv()

	for _, entry := range entries {
		if entry == nil {
			// Appended directly: AddInvVect would dereference it.
			msg.InvList = append(msg.InvList, nil)
			continue
		}

		require.NoError(t, msg.AddInvVect(entry))
	}

	return msg
}

func blockInv(hash chainhash.Hash) *wire.InvVect {
	return wire.NewInvVect(wire.InvTypeBlock, &hash)
}

func txInv(hash chainhash.Hash) *wire.InvVect {
	return wire.NewInvVect(wire.InvTypeTx, &hash)
}

// TestOnInv_DistinctUnknownBlocksEachDrawTheirOwnGetheaders records the Task 25
// decision on the phase-2 ledger line "M5 N-getheaders on distinct unknown
// invs", and pins the behavior that decision keeps.
//
// net_processing.cpp ProcessInvMessage answers each entry of the inv inside the
// per-entry loop (:2461-2462) and pushes one getheaders per new block hash
// (:2489-2493), whose hashStop is that entry's own hash (`inv.hash`, :2492).
// N distinct unknown hashes in one message therefore draw N getheaders from
// SVNode, and this port keeps that.
//
// The one-per-message rule OnInv already documents is NOT widened to distinct
// hashes, and the reason is in the message it would have to build: hashStop
// truncates the peer's reply, so one getheaders can only ever bound one of the
// announced branches. Collapsing three distinct announcements onto the first
// hash's hashStop would leave the other two unrequested, and nothing re-asks
// for them — OnHeaders only chains another getheaders off a full 2000-header
// batch. The duplicate-hash case is different in kind, and that is why it is
// the only one collapsed: the copies are byte-identical, so dropping them
// loses nothing.
//
// What this does leave standing is an amplification asymmetry (recorded as a
// residual, not fixed here): a peer spends 36 bytes per fabricated hash and
// draws a full locator back per hash. SVNode has the identical exposure at the
// identical site, so closing it here would be a divergence, not a port.
func TestOnInv_DistinctUnknownBlocksEachDrawTheirOwnGetheaders(t *testing.T) {
	f := newDownloadFixture(t, 3)
	peer := fullNodePeer("1.2.3.4:8333")

	unknown := []chainhash.Hash{{0xA1}, {0xA2}, {0xA3}}

	msgs, txHashes, err := f.bd.OnInv(peer, invMsg(t, wire.InvTypeBlock, unknown...))
	require.NoError(t, err)
	require.Empty(t, txHashes)
	require.Len(t, msgs, len(unknown), "one getheaders per distinct unknown hash, as net_processing.cpp:2489-2493 sends")

	locator := f.idx.Locator()

	for i, hash := range unknown {
		gh, ok := msgs[i].(*wire.MsgGetHeaders)
		require.True(t, ok, "message %d is %T", i, msgs[i])

		require.Equal(t, hash, gh.HashStop,
			"getheaders %d stops at the hash that entry announced, not at another entry's", i)

		require.Len(t, gh.BlockLocatorHashes, len(locator))

		for j := range locator {
			require.Equal(t, locator[j], *gh.BlockLocatorHashes[j],
				"every getheaders in the batch carries the same locator")
		}
	}
}

// TestOnInv_AnErrorDiscardsEverythingAlreadyQueued closes the phase-2 ledger's
// Task 6 minor M4 ("OnInv error contract") and the coverage gap the same entry
// names ("M4 nil-vs-partial msgs untested with multi-entry inv"). OnInv's own
// contract is "On error the message list is nil, never partial", and until now
// the only unsupported-type test sent a single-entry inv, where nil and
// partial are the same thing.
//
// Both directions are covered because they fail differently: an unsupported
// entry AFTER real work proves the queued messages and collected tx hashes are
// thrown away, and one BEFORE proves the return does not depend on having
// queued something first. net_processing.cpp:2514-2516 sets
// pfrom->fDisconnect and keeps looping; this port returns instead, because the
// caller drops the peer and messages for a dropped peer are not worth sending.
//
// The availability state each block entry set does stand — it is state, not
// output, and OnInv's doc comment says so.
func TestOnInv_AnErrorDiscardsEverythingAlreadyQueued(t *testing.T) {
	unknownA := chainhash.Hash{0xB1}
	unknownB := chainhash.Hash{0xB2}
	txA := chainhash.Hash{0xC1}
	bad := wire.NewInvVect(wire.InvTypeFilteredBlock, &chainhash.Hash{0xDD})

	tests := []struct {
		name            string
		entries         []*wire.InvVect
		wantLastUnknown chainhash.Hash
	}{
		{
			name:            "the unsupported entry lands after a block inv",
			entries:         []*wire.InvVect{blockInv(unknownA), bad},
			wantLastUnknown: unknownA,
		},
		{
			name:            "the unsupported entry lands after a tx inv",
			entries:         []*wire.InvVect{txInv(txA), bad},
			wantLastUnknown: chainhash.Hash{},
		},
		{
			name:            "the unsupported entry lands after both halves",
			entries:         []*wire.InvVect{blockInv(unknownA), txInv(txA), bad, blockInv(unknownB)},
			wantLastUnknown: unknownA,
		},
		{
			name:            "the unsupported entry lands first",
			entries:         []*wire.InvVect{bad, blockInv(unknownA), txInv(txA)},
			wantLastUnknown: chainhash.Hash{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newDownloadFixture(t, 3)
			peer := fullNodePeer("1.2.3.4:8333")

			msgs, txHashes, err := f.bd.OnInv(peer, mixedInvMsg(t, tc.entries...))
			require.Error(t, err)
			require.ErrorIs(t, err, ErrProtocolViolation)
			require.Nil(t, msgs, "the message list is nil, never partial")
			require.Nil(t, txHashes, "the collected tx hashes are discarded with the messages")

			require.Equal(t, tc.wantLastUnknown, peer.State.hashLastUnknownBlock,
				"availability updates made before the bad entry stand; nothing after it runs")
		})
	}
}

// TestOnInv_NilEntriesDoNotTruncateTheBatch is the other half of the M4 gap:
// OnInv skips a nil entry, and a multi-entry message is what proves the skip
// is a continue and not an early return. A nil entry cannot arrive off the
// wire — wire's own decoder never produces one — so this pins the guard's
// contract for every in-process caller instead (PeerManager.Inv and the
// scripted peers), in both orders.
func TestOnInv_NilEntriesDoNotTruncateTheBatch(t *testing.T) {
	unknownA := chainhash.Hash{0xE1}
	unknownB := chainhash.Hash{0xE2}
	txA := chainhash.Hash{0xF1}
	txB := chainhash.Hash{0xF2}

	tests := []struct {
		name     string
		entries  []*wire.InvVect
		wantMsgs int
		wantTxs  []chainhash.Hash
	}{
		{
			name:     "nil first, then a block inv",
			entries:  []*wire.InvVect{nil, blockInv(unknownA)},
			wantMsgs: 1,
		},
		{
			name:     "nil between two distinct block invs",
			entries:  []*wire.InvVect{blockInv(unknownA), nil, blockInv(unknownB)},
			wantMsgs: 2,
		},
		{
			name:     "nil last",
			entries:  []*wire.InvVect{blockInv(unknownA), nil},
			wantMsgs: 1,
		},
		{
			name:    "nil between two tx invs",
			entries: []*wire.InvVect{txInv(txA), nil, txInv(txB)},
			wantTxs: []chainhash.Hash{txA, txB},
		},
		{
			name:     "nil between the two halves",
			entries:  []*wire.InvVect{txInv(txA), nil, blockInv(unknownA)},
			wantMsgs: 1,
			wantTxs:  []chainhash.Hash{txA},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newDownloadFixture(t, 3)
			peer := fullNodePeer("1.2.3.4:8333")

			msgs, txHashes, err := f.bd.OnInv(peer, mixedInvMsg(t, tc.entries...))
			require.NoError(t, err)
			require.Len(t, msgs, tc.wantMsgs)

			if tc.wantTxs == nil {
				require.Empty(t, txHashes)
			} else {
				require.Equal(t, tc.wantTxs, txHashes)
			}
		})
	}
}

// TestNewGetHeaders_CopiesTheLocator guards the invariant the shared builder
// exists for. Phase-2 ledger, Task 6 minor M6: "locator-copy duplication" —
// HeaderSync.getHeaders and BlockDownloader.getHeadersFor carried the
// identical copy loop, and Task 25 extracted it into newGetHeaders.
//
// wire.MsgGetHeaders holds []*chainhash.Hash. A builder that appended
// &locator[i] would publish pointers INTO the caller's slice, so a later
// writer of that slice would rewrite an already-built message. Both original
// loops copied deliberately, and now one helper has to.
func TestNewGetHeaders_CopiesTheLocator(t *testing.T) {
	f := newDownloadFixture(t, 12)

	locator := f.idx.Locator()
	require.Greater(t, len(locator), 1, "the fixture must produce a multi-entry locator")

	original := append([]chainhash.Hash(nil), locator...)
	stop := chainhash.Hash{0x99}

	msg := newGetHeaders(locator, stop)
	require.Equal(t, stop, msg.HashStop)
	require.Equal(t, uint32(wire.ProtocolVersion), msg.ProtocolVersion)
	require.Len(t, msg.BlockLocatorHashes, len(original))

	for i := range original {
		require.NotSame(t, &locator[i], msg.BlockLocatorHashes[i],
			"entry %d must be a copy, not a pointer into the caller's slice", i)
	}

	// Overwrite the source slice: the built message must not move.
	for i := range locator {
		locator[i] = chainhash.Hash{byte(i + 1)}
	}

	for i := range original {
		require.Equal(t, original[i], *msg.BlockLocatorHashes[i],
			"entry %d changed after the source slice was overwritten", i)
	}

	require.Empty(t, newGetHeaders(nil, stop).BlockLocatorHashes,
		"an empty locator produces a message with no locator hashes, not a panic")
}

// TestGetHeaders_BothCallSitesKeepTheirOwnHashStop pins what the extraction in
// Task 25 had to preserve: the two callers share the locator copy but not the
// stop rule. HeaderSync sends the next checkpoint while headers-first mode
// runs (legacy netsync manager.go startSync); the inv answerer sends the hash
// the peer just announced (net_processing.cpp:2489-2493, `inv.hash`).
func TestGetHeaders_BothCallSitesKeepTheirOwnHashStop(t *testing.T) {
	f := newDownloadFixture(t, 12)

	cpHash := chainhash.Hash{0xC0}

	hs, err := NewHeaderSync(HeaderSyncConfig{
		Index:  f.idx,
		Params: syncTestParams([]chaincfg.Checkpoint{{Height: 1000, Hash: &cpHash}}),
	})
	require.NoError(t, err)

	// headersFirstMode is armed by the first sync peer, not by construction
	// (PeerEstablished, headersync.go:517-524), so the round has to be started
	// for the checkpoint hashStop rule to apply.
	require.NotEmpty(t, hs.PeerEstablished(fullNodePeer("5.6.7.8:8333")))
	require.True(t, hs.IsHeadersFirstMode(), "the checkpoint sits above the fixture tip")

	locator := f.idx.Locator()

	fromSync := hs.getHeaders(locator)
	require.Equal(t, cpHash, fromSync.HashStop, "headers-first sync stops at the next checkpoint")
	require.Equal(t, locator, locatorValues(fromSync))

	announced := chainhash.Hash{0x77}

	fromInv := f.bd.getHeadersFor(announced)
	require.Equal(t, announced, fromInv.HashStop, "the inv answerer stops at the announced hash")
	require.Equal(t, locator, locatorValues(fromInv))
}

// TestCheckStall_ReArmsTheStallClockForAPeerStillDelivering closes the phase-3
// ledger residual "Staller clause bandwidth re-arm ... parked for Task 25".
//
// net_processing.cpp DetectStalling (:5601-5626) does NOT disconnect every peer
// whose nStallingSince has run past the timeout. It asks
// IsBlockDownloadStallingFromPeer (:105-109) first, and takes the disconnect
// only when the peer's block bandwidth is under the floor:
//
//	if(IsBlockDownloadStallingFromPeer(config, pto, avgbw)) {
//	    ... pto->fDisconnect = true; return true;
//	}
//	else {
//	    LogPrint(... "Resetting stall (current speed %d) for peer=%d\n" ...);
//	    state->nStallingSince = GetTimeMicros();
//	}
//
// Phase 2 could only take the disconnect branch, because no per-peer meter
// existed; Task 6b built one (downloadStallingFrom), so the else branch is
// carried here. The re-arm is the load-bearing half: a peer that is genuinely
// delivering keeps its blocks AND gets a fresh full BlockStallingTimeout, so
// the same tick cannot condemn it again a microsecond later.
func TestCheckStall_ReArmsTheStallClockForAPeerStillDelivering(t *testing.T) {
	const rate = 4 * BlockStallingMinDownloadRate

	expired := testNow + micros(BlockStallingTimeout) + 1

	t.Run("a peer above the rate floor is re-armed, not disconnected", func(t *testing.T) {
		f := newDownloadFixture(t, 3)

		peer := f.deliveringPeer(t, "1.2.3.4:8333", rate, testNow)
		require.False(t, f.bd.downloadStallingFrom(peer, testNow), "the fixture must be above the floor")

		peer.State.nStallingSince = testNow

		// The ingest keeps running across the gap, which is what the C++
		// "current speed" reading describes.
		still := IngestSnapshot{Active: true, StartedMicros: testNow, BytesRead: rate * 12}

		require.Equal(t, StallActionNone, f.bd.CheckStall(peer, still, expired),
			"C++ takes the else branch for a peer still delivering")
		require.Equal(t, expired, peer.State.nStallingSince,
			"the else branch sets nStallingSince = now; without it the peer is condemned again next tick")
	})

	t.Run("the re-armed clock buys a full timeout", func(t *testing.T) {
		f := newDownloadFixture(t, 3)

		peer := f.deliveringPeer(t, "1.2.3.4:8333", rate, testNow)
		peer.State.nStallingSince = testNow

		still := IngestSnapshot{Active: true, StartedMicros: testNow, BytesRead: rate * 12}
		require.Equal(t, StallActionNone, f.bd.CheckStall(peer, still, expired))
		require.Equal(t, expired, peer.State.nStallingSince)

		// Delivery stops. The clock restarted at "expired", so the peer owes
		// another full BlockStallingTimeout before it can be condemned.
		require.Equal(t, StallActionNone, f.bd.CheckStall(peer, noIngest, expired+1))
		require.Equal(t, StallActionNone, f.bd.CheckStall(peer, noIngest, expired+micros(BlockStallingTimeout)))

		require.Equal(t, StallActionDisconnect,
			f.bd.CheckStall(peer, noIngest, expired+micros(BlockStallingTimeout)+1),
			"once the re-armed clock expires with no delivery, the disconnect branch runs")
	})

	t.Run("a peer below the rate floor is still disconnected", func(t *testing.T) {
		f := newDownloadFixture(t, 3)

		peer := f.deliveringPeer(t, "1.2.3.4:8333", BlockStallingMinDownloadRate-1, testNow)
		require.True(t, f.bd.downloadStallingFrom(peer, testNow))

		peer.State.nStallingSince = testNow

		dribble := IngestSnapshot{Active: true, StartedMicros: testNow, BytesRead: BlockStallingMinDownloadRate}

		require.Equal(t, StallActionDisconnect, f.bd.CheckStall(peer, dribble, expired),
			"IsBlockDownloadStallingFromPeer is true below the floor, so the disconnect branch runs")
		require.Equal(t, testNow, peer.State.nStallingSince,
			"the disconnect branch never re-arms the clock")
	})

	t.Run("no rate evidence at all is still a disconnect", func(t *testing.T) {
		f := newDownloadFixture(t, 3)

		peer := f.peerAt(t, "1.2.3.4:8333", 3)
		peer.State.nStallingSince = testNow

		// A peer nothing has measured reads as stalling, which is the state a
		// C++ association whose bandwidth average decayed to zero is in.
		require.Equal(t, StallActionDisconnect, f.bd.CheckStall(peer, noIngest, expired))
	})
}

// TestStallAction_DisconnectReasonNamesTheRule closes the phase-3 ledger
// residual "Distinguishing the two disconnect logs. The staller clause and the
// download timeout both log 'stalling block download', so an operator cannot
// tell which fired."
//
// SVNode logs the two rules with two different lines — "Peer=%d is stalling
// block download (current speed %d), disconnecting" (net_processing.cpp:5620)
// and "Timeout downloading block %s from peer=%d, disconnecting" (:5650) — and
// this is where the port's two texts are kept apart. The manager logs whatever
// this returns and hands the same string to Peer.Disconnect, so a collapse back
// to one text would be invisible without this test.
func TestStallAction_DisconnectReasonNamesTheRule(t *testing.T) {
	staller := StallActionDisconnect.DisconnectReason()
	timeout := StallActionDisconnectTimeout.DisconnectReason()

	require.NotEmpty(t, staller)
	require.NotEmpty(t, timeout)
	require.NotEqual(t, staller, timeout,
		"an operator must be able to tell the two disconnect rules apart in the logs")

	// Each names its own rule, not just the shared symptom.
	require.Contains(t, staller, "stall")
	require.Contains(t, timeout, "timeout")

	require.Empty(t, StallActionNone.DisconnectReason(), "no disconnect, no reason")
	require.Empty(t, StallActionRotateSyncPeer.DisconnectReason(),
		"a rotation keeps the peer connected, so it has no disconnect reason")

	// The two actions must also stay distinguishable as values, since that is
	// what carries the rule from CheckStall to the log.
	require.NotEqual(t, StallActionDisconnect, StallActionDisconnectTimeout)
	require.NotEqual(t, StallActionDisconnect.String(), StallActionDisconnectTimeout.String())
}

// TestOnInv_RoundOwnerDoesNotAnswerABlockInvWithGetHeaders pins the fix for a
// live testnet disconnect (Task 27, 2026-08-26): a block announced by inv while
// a headers-first round runs drew a second, overlapping getheaders to the peer
// driving the round. The peer answered both from the same locator with the
// same 2000 headers, and the second reply failed the no-progress terminator
// (acceptHeaders), disconnecting an honest sync peer. SVNode has no round and
// no terminator, so the duplicate is harmless there; here the round owner's own
// chain of requests reaches the announced block, so the inv answer is redundant
// for that one peer. Availability is still recorded.
func TestOnInv_RoundOwnerDoesNotAnswerABlockInvWithGetHeaders(t *testing.T) {
	f := newDownloadFixture(t, 3)

	owner := fullNodePeer("1.2.3.4:8333")
	require.Len(t, f.hs.PeerEstablished(owner), 1)
	require.True(t, owner.State.fSyncStarted)
	require.True(t, f.hs.IsHeadersFirstMode())

	unknown := chainhash.Hash{0xAB}

	msgs, txHashes, err := f.bd.OnInv(owner, invMsg(t, wire.InvTypeBlock, unknown))
	require.NoError(t, err)
	require.Empty(t, msgs, "the round owner's own getheaders chain will reach the block")
	require.Empty(t, txHashes)
	require.Equal(t, unknown, owner.State.hashLastUnknownBlock, "availability is still recorded")

	other := fullNodePeer("5.6.7.8:8333")

	msgs, _, err = f.bd.OnInv(other, invMsg(t, wire.InvTypeBlock, unknown))
	require.NoError(t, err)
	require.Len(t, msgs, 1, "a peer that does not own the round is answered as before")
}

// TestCheckStall_RotationDoesNotForgiveTheFrontBlockClock pins the fix for the
// second Task 27 finding: near the tip a block-withholding peer was re-admitted
// as a sync peer and rotated every 180 seconds for ever, because rotation's
// clearPeer zeroed nDownloadingSince and the next re-hand started the
// per-block timeout from scratch. SVNode never clears that clock short of a
// disconnect. A rotated peer that is handed blocks again therefore inherits the
// clock of the request it never answered, so the unconditional timeout
// accumulates across rotations and eventually disconnects it.
func TestCheckStall_RotationDoesNotForgiveTheFrontBlockClock(t *testing.T) {
	f := newDownloadFixture(t, 3)
	peer := f.syncingPeer(t, "1.2.3.4:8333")
	require.Equal(t, testNow, peer.State.nDownloadingSince)

	rotated := testNow + micros(MaxLastBlockTime) + 1
	require.Equal(t, StallActionNone, f.bd.CheckStall(peer, noIngest, testNow))
	require.Equal(t, StallActionRotateSyncPeer, f.bd.CheckStall(peer, noIngest, rotated))
	require.Empty(t, peer.State.vBlocksInFlight)

	rehanded := rotated + micros(time.Second)
	best := f.node(t, 3)
	require.True(t, f.bd.MarkBlockAsInFlight(peer, best, rehanded))
	require.Equal(t, testNow, peer.State.nDownloadingSince, "the re-hand inherits the unanswered request's clock")

	// Ten minutes after the ORIGINAL request the timeout fires, although only
	// seven minutes have passed since the re-hand.
	require.Equal(t, StallActionDisconnectTimeout, f.bd.CheckStall(peer, noIngest, testNow+micros(10*time.Minute)+1))
}

// TestBlockReceived_ClearsTheCarriedClock: a rotated peer that then DELIVERS
// has paid its debt; the next batch it is handed starts a fresh clock.
func TestBlockReceived_ClearsTheCarriedClock(t *testing.T) {
	f := newDownloadFixture(t, 3)
	peer := f.syncingPeer(t, "1.2.3.4:8333")

	rotated := testNow + micros(MaxLastBlockTime) + 1
	require.Equal(t, StallActionNone, f.bd.CheckStall(peer, noIngest, testNow))
	require.Equal(t, StallActionRotateSyncPeer, f.bd.CheckStall(peer, noIngest, rotated))

	rehanded := rotated + micros(time.Second)
	best := f.node(t, 3)
	require.True(t, f.bd.MarkBlockAsInFlight(peer, best, rehanded))
	require.Equal(t, testNow, peer.State.nDownloadingSince)

	delivered := rehanded + micros(time.Second)
	require.True(t, f.bd.BlockReceived(peer, best.Hash, delivered))

	again := delivered + micros(time.Second)
	require.True(t, f.bd.MarkBlockAsInFlight(peer, f.node(t, 2), again))
	require.Equal(t, again, peer.State.nDownloadingSince, "a delivery clears the carried clock")
}

// TestBlockDeferred_HoldsUntilTheStampEvenWhenTheParentIsHeld is the backoff
// twin of the parent-missing deferral. A block the ingest path refused for OUR
// fault (admission backoff after a transient store error) has its parent in
// the chain, so the parent-held release must not apply: only the stamp does.
// Measured live on regtest before this change: a 4.63 GB block was re-served
// and re-drained four extra times inside one 5 s backoff window.
func TestBlockDeferred_HoldsUntilTheStampEvenWhenTheParentIsHeld(t *testing.T) {
	f := newDownloadFixture(t, 10)
	peer := f.peerAt(t, "1.2.3.4:8333", 10)
	activeTip := f.node(t, 0)

	require.Equal(t, heightRange(1, 10), f.requestedHeights(t, peer, activeTip, testNow))

	first := f.node(t, 1)
	require.True(t, f.bd.MarkBlockAsInFlight(peer, first, testNow))
	require.True(t, f.bd.BlockDeferred(peer, first.Hash, testNow, testNow+micros(5*time.Second)))

	// Block 1's parent is the active tip itself, so the parent IS held.
	got := f.requestedHeights(t, peer, activeTip, testNow+micros(time.Second))
	require.NotContains(t, got, int32(1), "a backoff-deferred block must wait for its stamp even with its parent held")
	require.Contains(t, got, int32(2), "the rest of the window is unaffected")

	got = f.requestedHeights(t, peer, activeTip, testNow+micros(6*time.Second))
	require.Contains(t, got, int32(1), "the stamp's expiry puts the block back on offer")
}

// A parent-missing deferral keeps releasing on parent-held; the new flag must
// not change it.
func TestBlockParentMissing_StillReleasesOnParentHeld(t *testing.T) {
	f := newDownloadFixture(t, 10)
	peer := f.peerAt(t, "1.2.3.4:8333", 10)

	third := f.node(t, 3)
	require.True(t, f.bd.MarkBlockAsInFlight(peer, third, testNow))
	require.True(t, f.bd.BlockParentMissing(peer, third.Hash, testNow))

	require.False(t, f.bd.deferredForParent(third.Hash, true, testNow+micros(time.Second)),
		"holding the parent releases a parent-missing deferral immediately")
}
