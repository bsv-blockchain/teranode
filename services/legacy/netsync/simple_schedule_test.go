package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

const mb = 1_000_000

// deadlineAssigner has the chain at 10, with a recent mean block of 500 MB for the backstop.
func deadlineAssigner(peers, full []*assignerPeer) *downloadAssigner {
	remaining := 0
	for _, p := range peers {
		remaining += p.budget
	}

	return &downloadAssigner{peers: peers, full: full, remaining: remaining, tip: 10, mean: 500 * mb}
}

func measuredPeer(t *testing.T, sm *SyncManager, idx uint8, budget int, rate float64) *assignerPeer {
	t.Helper()

	p, _ := schedulerPeer(t, sm, idx, 5000)

	return &assignerPeer{peer: p, budget: budget, rate: rate, measured: true}
}

func wantedAt(height int32) wantedBlock {
	return wantedBlock{height: height, hash: chainhash.Hash{byte(height), byte(height >> 8), 0x7e}}
}

func placed(p *assignerPeer) []chainhash.Hash {
	if p.getData == nil {
		return nil
	}

	out := make([]chainhash.Hash, 0, len(p.getData.InvList))
	for _, iv := range p.getData.InvList {
		out = append(out, iv.Hash)
	}

	return out
}

// Block 755,254 (2026-10-08): 4 GB, second in the queue of a 7.3 MB/s peer behind another 4 GB
// block. A near block goes to the fast peer: the 7.3 MB/s peer cannot send 4 GB before the chain
// needs it.
func TestDeadlineANearBlockGoesToTheFastPeer(t *testing.T) {
	sm := schedulerManager(t)
	slow := measuredPeer(t, sm, 1, 2, 7.3*mb)
	fast := measuredPeer(t, sm, 2, 2, 40*mb)

	sm.requestBlocks(deadlineAssigner([]*assignerPeer{slow, fast}, nil), []wantedBlock{wantedAt(12)}, 0)

	require.Equal(t, []chainhash.Hash{wantedAt(12).hash}, placed(fast))
	require.Empty(t, placed(slow))
}

// Review focus 1: a node with one peer continues to download.
func TestDeadlineASinglePeerGetsBlocks(t *testing.T) {
	sm := schedulerManager(t)
	only := measuredPeer(t, sm, 1, 2, 2*mb)

	sm.requestBlocks(deadlineAssigner([]*assignerPeer{only}, nil), []wantedBlock{wantedAt(12), wantedAt(13)}, 0)

	require.Equal(t, []chainhash.Hash{wantedAt(12).hash, wantedAt(13).hash}, placed(only))
}

// Review focus 2 and the restart of 2026-10-08 06:33: with no measured and no remembered rates,
// each peer gets one block, near blocks first.
func TestDeadlineAColdStartGivesEachPeerOneNearBlock(t *testing.T) {
	sm := schedulerManager(t)
	a1, _ := schedulerPeer(t, sm, 1, 5000)
	a2, _ := schedulerPeer(t, sm, 2, 5000)

	p1 := &assignerPeer{peer: a1, budget: 1}
	p2 := &assignerPeer{peer: a2, budget: 1}

	sm.requestBlocks(deadlineAssigner([]*assignerPeer{p1, p2}, nil), []wantedBlock{wantedAt(11), wantedAt(12), wantedAt(13)}, 0)

	require.ElementsMatch(t, []chainhash.Hash{wantedAt(11).hash, wantedAt(12).hash}, append(placed(p1), placed(p2)...))
}

// A new peer with no rate, beside measured peers, gets one block from the far end of the pass, so
// its first block cannot stop the chain.
func TestDeadlineAnUnmeasuredPeerGetsTheFarBlock(t *testing.T) {
	sm := schedulerManager(t)
	fast := measuredPeer(t, sm, 1, 0, 50*mb)
	newcomer, _ := schedulerPeer(t, sm, 2, 5000)
	np := &assignerPeer{peer: newcomer, budget: 1}

	a := deadlineAssigner([]*assignerPeer{np}, []*assignerPeer{fast})
	a.far = []wantedBlock{wantedAt(900)}

	sm.requestBlocks(a, []wantedBlock{wantedAt(12)}, 0)

	require.Equal(t, []chainhash.Hash{wantedAt(900).hash}, placed(np))
}

// Equal peers share the blocks in turn: on the same arrival the peer with more room wins.
func TestDeadlineEqualPeersShareTheBlocks(t *testing.T) {
	sm := schedulerManager(t)
	a := measuredPeer(t, sm, 1, 2, 50*mb)
	b := measuredPeer(t, sm, 2, 2, 50*mb)

	sm.requestBlocks(deadlineAssigner([]*assignerPeer{a, b}, nil), []wantedBlock{wantedAt(12), wantedAt(13)}, 0)

	require.Equal(t, []chainhash.Hash{wantedAt(12).hash}, placed(a))
	require.Equal(t, []chainhash.Hash{wantedAt(13).hash}, placed(b))
}

// A new peer does not get a block the chain needs before a measured peer could land it: when the
// measured peers are full, the highest candidate of the pass can be the next block (review of
// 2026-10-08).
func TestDeadlineAnUnmeasuredPeerDoesNotGetANearBlock(t *testing.T) {
	sm := schedulerManager(t)
	fast := measuredPeer(t, sm, 1, 0, 50*mb)
	newcomer, _ := schedulerPeer(t, sm, 2, 5000)
	np := &assignerPeer{peer: newcomer, budget: 1}

	sm.requestBlocks(deadlineAssigner([]*assignerPeer{np}, []*assignerPeer{fast}), []wantedBlock{wantedAt(12)}, 0)

	require.Empty(t, placed(np), "with no far block of the window, a new peer gets no near block")
}

// The same restart: the six new peers got no block, because the candidates of a pass are the
// lowest unowned blocks. A new peer gets the highest unowned block of the full window.
func TestDeadlineAnUnmeasuredPeerGetsABlockFromTheTopOfTheWindow(t *testing.T) {
	sm := schedulerManager(t)
	slow := measuredPeer(t, sm, 1, 16, 3.9*mb)
	newcomer, _ := schedulerPeer(t, sm, 2, 5000)
	np := &assignerPeer{peer: newcomer, budget: 1}

	a := deadlineAssigner([]*assignerPeer{slow, np}, nil)
	a.far = []wantedBlock{wantedAt(900)}

	sm.requestBlocks(a, []wantedBlock{wantedAt(11), wantedAt(12)}, 0)

	require.Equal(t, []chainhash.Hash{wantedAt(900).hash}, placed(np))
}

// A slow measured peer does not keep new peers off far blocks: the test is a fixed lead of
// unmeasuredMinLead, not the arrival at the measured peers, which was 40 to 60 minutes.
func TestDeadlineAnUnmeasuredPeerGetsAFarBlockBesideASlowPeer(t *testing.T) {
	sm := schedulerManager(t)
	slow := measuredPeer(t, sm, 1, 0, 3.9*mb)
	newcomer, _ := schedulerPeer(t, sm, 2, 5000)
	np := &assignerPeer{peer: newcomer, budget: 1}

	a := deadlineAssigner([]*assignerPeer{np}, []*assignerPeer{slow})
	a.far = []wantedBlock{wantedAt(900)}

	sm.requestBlocks(a, []wantedBlock{wantedAt(12)}, 0)

	require.Equal(t, []chainhash.Hash{wantedAt(900).hash}, placed(np))
}

// Each block goes to the peer with the lowest (blocks owed + 1) / rate: a block size is not known
// before the download, so each block counts as one. A peer at 50 MB/s takes nine blocks before a
// peer at 5 MB/s takes one; at the tie, 10/50 against 1/5, the peer with more room takes block 20.
func TestScheduleTheFastPeerGetsMoreAndNearerBlocks(t *testing.T) {
	sm := schedulerManager(t)
	fast := measuredPeer(t, sm, 1, 16, 50*mb)
	slow := measuredPeer(t, sm, 2, 16, 5*mb)

	var blocks []wantedBlock
	for h := int32(11); h <= 21; h++ {
		blocks = append(blocks, wantedAt(h))
	}

	sm.requestBlocks(deadlineAssigner([]*assignerPeer{slow, fast}, nil), blocks, 0)

	require.Len(t, placed(fast), 10)
	require.Equal(t, wantedAt(11).hash, placed(fast)[0])
	require.Equal(t, []chainhash.Hash{wantedAt(20).hash}, placed(slow))
}

// No block waits while a peer has room. At 12:26 on 2026-10-08 the next block waited for a full
// fast peer, no peer was asked for it, and the chain stopped for 9 minutes.
func TestScheduleNoBlockWaitsWhileAPeerHasRoom(t *testing.T) {
	sm := schedulerManager(t)
	fast := measuredPeer(t, sm, 1, 0, 50*mb)
	slow := measuredPeer(t, sm, 2, 2, 2*mb)

	sm.requestBlocks(deadlineAssigner([]*assignerPeer{slow}, []*assignerPeer{fast}), []wantedBlock{wantedAt(11), wantedAt(12)}, 0)

	require.Equal(t, []chainhash.Hash{wantedAt(11).hash, wantedAt(12).hash}, placed(slow))
}

// The disk backstop counts the blocks a pass gives at the recent mean size, so one pass cannot
// ask for far more than the backstop. At 12:26 on 2026-10-08 the node held 136 GB against the
// 107 GB backstop, which was checked once for each pass.
func TestScheduleTheBackstopStopsAPassPartWay(t *testing.T) {
	sm := schedulerManager(t)
	p := measuredPeer(t, sm, 1, 16, 50*mb)

	a := deadlineAssigner([]*assignerPeer{p}, nil)
	a.held = parkBackstopBytes - 900*mb

	sm.requestBlocks(a, []wantedBlock{wantedAt(11), wantedAt(12), wantedAt(13)}, 0)

	require.Len(t, placed(p), 2, "two blocks at the 500 MB mean reach the backstop")
}

// A slow peer holds blocks in proportion to its rate against the fastest peer: 16 x 5 / 50 is 1.6,
// so 2. With 16 each, the slow peers' blocks sat near the tip behind 16 fast blocks, and on
// 2026-10-08 from 13:14 to 14:14 the chain waited for six blocks at peers of 2.8 to 7.9 MB/s whose
// first bytes came 3 to 8 minutes after the getdata.
func TestASlowPeerHoldsBlocksInProportionToItsRate(t *testing.T) {
	sm := schedulerManager(t)
	wireStreamingPath(sm)
	sm.settings.Legacy.MaxBlocksInTransitPerPeer = 16
	sm.blockSizeTracker.addBlockSize(10 * mb)

	fast, _ := schedulerPeer(t, sm, 1, 5000)
	slow, _ := schedulerPeer(t, sm, 2, 5000)
	sm.streams.rates[fast] = 50 * mb
	sm.streams.rates[slow] = 5 * mb

	a := sm.newDownloadAssigner()
	require.NotNil(t, a)

	budgets := map[*peerpkg.Peer]int{}
	for _, p := range a.peers {
		budgets[p.peer] = p.budget
	}

	require.Equal(t, 16, budgets[fast])
	require.Equal(t, 2, budgets[slow])
}

// The far block a peer with no rate is given is marked as a probe, so highestHeld does not count
// it, and the mark is dropped once the block leaves the window.
func TestAFarBlockGivenToAnUnmeasuredPeerIsMarkedAsAProbe(t *testing.T) {
	sm := schedulerManager(t)
	slow := measuredPeer(t, sm, 1, 16, 3.9*mb)
	newcomer, _ := schedulerPeer(t, sm, 2, 5000)
	np := &assignerPeer{peer: newcomer, budget: 1}

	a := deadlineAssigner([]*assignerPeer{slow, np}, nil)
	a.far = []wantedBlock{wantedAt(900)}

	sm.requestBlocks(a, []wantedBlock{wantedAt(11), wantedAt(12)}, 0)

	require.Equal(t, []chainhash.Hash{wantedAt(900).hash}, placed(np))
	require.Contains(t, sm.farProbes.within([]wantedBlock{wantedAt(11), wantedAt(900)}), wantedAt(900).hash)
	require.Empty(t, sm.farProbes.within([]wantedBlock{wantedAt(11)}), "the block left the window")
	require.Empty(t, sm.farProbes.within([]wantedBlock{wantedAt(900)}), "and its mark with it")
}
