package netsync

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/stretchr/testify/require"
)

// Above the last checkpoint there is no header cache, so the ledger is the only
// record of a block this node asked a peer for. These tests drive the download
// pass with headers-first mode off and a ledger seeded the way the inv path
// seeds it, against a real sqlitememory chain and a real park over a blob
// store, so the disk and chain checks the pass makes are the production ones
// and can be counted.

// headerSpyClient counts GetBlockHeader calls and delegates to the real client,
// as locatorSpyClient does for GetBlockLocator in inv_held_locally_test.go. It
// is how a test proves the pass spent no chain round trip on a block the
// ledger filter should have kept away from it.
type headerSpyClient struct {
	blockchain2.ClientI

	headerCalls atomic.Int32
}

func (c *headerSpyClient) GetBlockHeader(ctx context.Context, h *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	c.headerCalls.Add(1)

	return c.ClientI.GetBlockHeader(ctx, h)
}

// countingBlobStore counts every read the park makes of its store. holdsBlock
// reads a converted record through Get, so a Get per candidate is what the
// disk check costs and what the ledger filter must keep off the non-head blocks.
type countingBlobStore struct {
	blob.Store

	reads atomic.Int32
}

func (s *countingBlobStore) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	s.reads.Add(1)

	return s.Store.Get(ctx, key, fileType, opts...)
}

func (s *countingBlobStore) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
	s.reads.Add(1)

	return s.Store.Exists(ctx, key, fileType, opts...)
}

// newTipWalkManager is newPipelineParkManager (real sqlitememory LocalClient, real
// park over the counting blob store) with the fields the download pass
// dereferences, headers-first OFF and an empty header cache, which is the
// shape of every mainnet node above height 945000.
func newTipWalkManager(t *testing.T) (*SyncManager, *countingBlobStore, *headerSpyClient) {
	t.Helper()

	// adoptStranded's setGauges reaches the gauges the real constructor registers.
	initPrometheusMetrics()

	store := &countingBlobStore{Store: memory.New()}

	sm := newPipelineParkManager(t, store, 8)
	spy := &headerSpyClient{ClientI: sm.blockchainClient}
	sm.blockchainClient = spy
	sm.peerStates = txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState]()
	sm.blockDownloads = newBlockDownloadTracker(blockRequestAssignmentTTL)
	sm.streams = newStreamRegistry()
	sm.blockSizeTracker = newBlockSizeTracker(10)
	sm.headersFirstMode.Store(false)

	return sm, store, spy
}

// tipPeers connects n peers that record every getdata they are sent, registers
// each as a sync candidate and gives each the same measured rate, so
// peerQueueDepth's unmeasured-peer floor of two does not narrow any budget
// these tests reason about.
func tipPeers(t *testing.T, sm *SyncManager, n int) ([]*peerpkg.Peer, []*getDataRecorder) {
	t.Helper()

	peers := make([]*peerpkg.Peer, 0, n)
	recs := make([]*getDataRecorder, 0, n)

	for i := 0; i < n; i++ {
		p, rec := schedulerPeer(t, sm, uint8(40+i), 1000) //nolint:gosec // a small fixture index
		sm.streams.rates[p] = 1

		peers = append(peers, p)
		recs = append(recs, rec)
	}

	return peers, recs
}

// askedOf is every block hash the recorders between them have been sent.
func askedOf(recs ...*getDataRecorder) []chainhash.Hash {
	var out []chainhash.Hash
	for _, r := range recs {
		out = append(out, r.all()...)
	}

	return out
}

// countAsked is how many block hashes the recorders have been sent between them.
func countAsked(recs ...*getDataRecorder) int {
	return len(askedOf(recs...))
}

// oweBackdated records that owner was asked for each hash, in order, long enough
// ago that the retry window has passed. The tracker's clock is injectable, so
// the records age without sleeping a minute; the clock is restored afterwards.
func oweBackdated(t *testing.T, sm *SyncManager, owner *peerpkg.Peer, hashes ...chainhash.Hash) {
	t.Helper()

	sm.blockDownloads.now = func() time.Time { return time.Now().Add(-2 * blockRequestRetryInterval) }

	for _, h := range hashes {
		require.True(t, sm.blockDownloads.Add(owner, h))
	}

	sm.blockDownloads.now = time.Now
}

// TestAssignWantedBlocks_AtTheTipReAsksOnlyAQuietOwnersHeadOfQueue is the
// catch-up shape above the final checkpoint: one getblocks reply has a single
// peer owing many blocks, and that peer goes quiet. SV Node judges only the
// front of a peer's in-flight queue (FindNextBlocksToDownload's
// first-already-in-flight branch, net_processing.cpp:462-507); a peer sends its
// queue in order, so a later block from the same peer cannot be the stalled
// one. Re-asking the whole queue would download it twice when the peer was
// merely reading a multi-gigabyte block from disk, and checking the whole
// queue against disk and chain would cost a blob read and a round trip per
// owed block every thirty seconds.
//
// End state: exactly the owner's first block goes to another peer, the owner
// is let off that block and still owes the other nineteen, and the pass made
// one disk read and one chain round trip, both for the head.
func TestAssignWantedBlocks_AtTheTipReAsksOnlyAQuietOwnersHeadOfQueue(t *testing.T) {
	sm, store, spy := newTipWalkManager(t)

	peers, recs := tipPeers(t, sm, 3)
	owner, ownerRec := peers[0], recs[0]
	others := recs[1:]

	hashes := make([]chainhash.Hash, 20)
	for i := range hashes {
		hashes[i] = chainhash.Hash{0x90, byte(i)}
	}

	oweBackdated(t, sm, owner, hashes...)
	require.Equal(t, 20, sm.blockDownloads.CountForPeer(owner))

	sm.assignWantedBlocks()

	require.True(t, WaitUntil(func() bool { return countAsked(others...) >= 1 }, 5*time.Second),
		"the quiet owner's head-of-queue block must be asked of another peer")

	// The send is asynchronous; a negative assertion has to outwait it.
	time.Sleep(invQuietPeriod)

	require.Equal(t, 1, countAsked(others...), "one block per quiet owner per pass, not its whole queue")
	require.Equal(t, []chainhash.Hash{hashes[0]}, askedOf(others...),
		"and that one is the head of the quiet owner's queue")
	require.Zero(t, ownerRec.count(), "the owner is never asked for a block it already owes")

	require.Equal(t, 19, sm.blockDownloads.CountForPeer(owner), "the owner is let off the head and still owes the rest")
	require.True(t, sm.blockDownloads.HasOwner(owner, hashes[0]), "let off is not revoked: a late copy from the owner is still admitted")

	for _, h := range hashes[1:] {
		require.Len(t, sm.blockDownloads.OwnersOf(h), 1, "a block behind the head stays with its one owner")
	}

	require.Equal(t, int32(1), spy.headerCalls.Load(), "one chain round trip, for the head; the filter runs before the chain check")
	require.Equal(t, int32(1), store.reads.Load(), "one disk read, for the head; the filter runs before the disk check")
	require.Equal(t, int64(1), sm.waste.reAskedQuiet.Load())
}

// TestAssignWantedBlocks_AtTheTipWaitsOutAReAskBeforeMovingToTheNextHead pins
// the per-owner bound across passes. Once the head has been asked of another
// peer, the owner's next block is not re-asked until that re-ask has had its
// own retry window: SV Node's front stays the front until it arrives, and a
// second re-ask thirty seconds later for a peer that is merely slow to start
// would download one more block twice per sweep for as long as it stays quiet.
func TestAssignWantedBlocks_AtTheTipWaitsOutAReAskBeforeMovingToTheNextHead(t *testing.T) {
	sm, _, _ := newTipWalkManager(t)

	peers, recs := tipPeers(t, sm, 3)
	owner := peers[0]
	others := recs[1:]

	h0 := chainhash.Hash{0x91, 0}
	h1 := chainhash.Hash{0x91, 1}
	h2 := chainhash.Hash{0x91, 2}

	oweBackdated(t, sm, owner, h0, h1, h2)

	sm.assignWantedBlocks()

	require.True(t, WaitUntil(func() bool { return countAsked(others...) == 1 }, 5*time.Second))
	require.Equal(t, []chainhash.Hash{h0}, askedOf(others...))

	// A second pass inside the retry window of the head's re-ask: nothing more.
	sm.assignWantedBlocks()

	time.Sleep(invQuietPeriod)

	require.Equal(t, 1, countAsked(others...), "the owner's next block waits while the head's re-ask is fresh")
	require.Equal(t, 2, sm.blockDownloads.CountForPeer(owner))

	// Age every record past the retry window, the head's re-ask included. The
	// owner's next head is then re-asked, and so is the head itself, now owed
	// unforgiven by a second peer that has also sent nothing.
	sm.blockDownloads.now = func() time.Time { return time.Now().Add(2 * blockRequestRetryInterval) }

	sm.assignWantedBlocks()

	require.True(t, WaitUntil(func() bool { return countAsked(others...) == 3 }, 5*time.Second),
		"once the re-ask has had its window the owner's next head goes out")
	require.Contains(t, askedOf(others...), h1)
	require.NotContains(t, askedOf(others...), h2, "one head per owner per pass, still")
	require.Equal(t, 1, sm.blockDownloads.CountForPeer(owner))
}

// TestAssignWantedBlocks_AtTheTipReAsksABlockADemotedSyncPeerWasLetOff is the
// demotion path: handleCheckSyncPeer runs outside headers-first mode too, and
// demoteSyncPeer calls ForgetForRetryPeer, which leaves every block the peer
// owed with only a forgiven record. Nothing else names such a block at the
// tip, so the walk must, or the blocks are lost until the next inv.
func TestAssignWantedBlocks_AtTheTipReAsksABlockADemotedSyncPeerWasLetOff(t *testing.T) {
	sm, _, _ := newTipWalkManager(t)

	peers, recs := tipPeers(t, sm, 2)
	owner, other := peers[0], peers[1]
	otherRec := recs[1]

	hash := chainhash.Hash{0x92}

	require.True(t, sm.blockDownloads.Add(owner, hash))
	require.Len(t, sm.blockDownloads.ForgetForRetryPeer(owner, blockRequestRetryInterval), 1)
	require.Zero(t, sm.blockDownloads.CountForPeer(owner), "precondition: the owner has been let off, so Len does not count the block")

	sm.assignWantedBlocks()

	require.True(t, WaitUntil(func() bool { return otherRec.count() == 1 }, 5*time.Second),
		"a block whose every owner was let off is still wanted and must be asked of another peer")
	require.Equal(t, hash, otherRec.all()[0])
	require.True(t, sm.blockDownloads.HasOwner(other, hash))
}

// TestAssignWantedBlocks_AtTheTipLeavesABlockWhoseOwnerIsStillSendingBytes is
// the busy-owner rule carried to the tip: a peer delivering another block has
// not gone quiet, however long ago this one was asked for. The tip range must
// go through the same per-candidate checks as the header-cache range, not a
// shortcut from the ledger to the getdata.
func TestAssignWantedBlocks_AtTheTipLeavesABlockWhoseOwnerIsStillSendingBytes(t *testing.T) {
	sm, _, _ := newTipWalkManager(t)

	peers, recs := tipPeers(t, sm, 2)
	owner := peers[0]
	otherRec := recs[1]

	owed := chainhash.Hash{0x93}
	earlier := chainhash.Hash{0x94}

	oweBackdated(t, sm, owner, owed)

	// The owner is mid-way through an earlier block: bytes arrived a moment ago.
	s := sm.streams.start(earlier, 0, owner, 1<<30, time.Now().Add(-time.Minute))
	s.lastRead.Store(time.Now().UnixNano())

	sm.assignWantedBlocks()

	time.Sleep(invQuietPeriod)

	require.Zero(t, otherRec.count(), "an owner still sending block bytes is not quiet, so its block is not re-asked")
	require.Equal(t, 1, sm.blockDownloads.CountForPeer(owner))

	// The same stream, silent past the retry window: now the owner is quiet.
	s.lastRead.Store(time.Now().Add(-2 * blockRequestRetryInterval).UnixNano())

	sm.assignWantedBlocks()

	require.True(t, WaitUntil(func() bool { return otherRec.count() == 1 }, 5*time.Second))
	require.Equal(t, owed, otherRec.all()[0])
}

// TestAssignWantedBlocks_AtTheTipDoesNotReAskABlockItAlreadyHolds pins the
// step-9 seam: the walk asks blockHeldLocally before re-asking, so a block
// whose bytes are arriving from a peer other than its owner, or one the
// dispatcher has taken to validate, is not downloaded again. A second owed
// block in the same ledger is the positive control.
func TestAssignWantedBlocks_AtTheTipDoesNotReAskABlockItAlreadyHolds(t *testing.T) {
	sm, _, _ := newTipWalkManager(t)

	peers, recs := tipPeers(t, sm, 3)
	owner, racer := peers[0], peers[1]

	arriving := chainhash.Hash{0x95, 0}
	control := chainhash.Hash{0x95, 1}

	// Two owners, both quiet, so neither block is skipped on a busy owner;
	// arriving is streaming from a peer the ledger does not list for it. The
	// control's re-ask may land on either of the two peers that do not owe it,
	// so every recorder is read.
	oweBackdated(t, sm, owner, arriving)
	oweBackdated(t, sm, peers[2], control)

	sm.streams.start(arriving, 0, racer, 4<<30, time.Now())

	sm.assignWantedBlocks()

	require.True(t, WaitUntil(func() bool { return countAsked(recs...) == 1 }, 5*time.Second),
		"positive control: the owed block nobody is delivering must be asked of another peer")

	time.Sleep(invQuietPeriod)

	require.Equal(t, []chainhash.Hash{control}, askedOf(recs...), "a block whose bytes are arriving is held and is not re-asked")
	require.Zero(t, recs[2].count(), "and never of the peer that already owes it")
	require.Len(t, sm.blockDownloads.OwnersOf(arriving), 1)
}

// TestAssignWantedBlocks_AtTheCheckpointCrossingStillPlacesWhatTheCacheNames
// is the collateral the tip walk must not cause. When the committed tip passes
// the final checkpoint and headers-first mode switches off, the header cache
// can still name up to two thousand heights above it, and today's pass keeps
// requesting them. The ledger range is appended to that, never substituted
// for it.
func TestAssignWantedBlocks_AtTheCheckpointCrossingStillPlacesWhatTheCacheNames(t *testing.T) {
	sm, _, _ := newTipWalkManager(t)

	peers, recs := tipPeers(t, sm, 3)
	owner, ownerRec := peers[0], recs[0]

	// newHeaderCache carries no checkpoints, so every height it names is
	// wantable: the shape above the final checkpoint.
	parent := chainhash.Hash{0xaa}
	headers := chainOfHeaders(parent, 3)

	sm.headerCache = newHeaderCache()
	require.True(t, sm.headerCache.Fill(parent, 1, headers))

	fromCache := make([]chainhash.Hash, 0, len(headers))
	for _, h := range headers {
		fromCache = append(fromCache, h.BlockHash())
	}

	fromLedger := chainhash.Hash{0x96}
	oweBackdated(t, sm, owner, fromLedger)

	sm.assignWantedBlocks()

	// Cache-named blocks are dealt to any peer with room, the owner included;
	// only the ledger block avoids the owner. So every recorder is read.
	require.True(t, WaitUntil(func() bool { return countAsked(recs...) == 4 }, 5*time.Second),
		"the three cache-named blocks and the one ledger-named block are all placed")

	asked := askedOf(recs...)
	for _, h := range fromCache {
		require.Contains(t, asked, h)
	}

	require.Contains(t, asked, fromLedger)
	require.NotContains(t, ownerRec.all(), fromLedger, "the ledger block is never re-asked of the peer that owes it")

	// Cache entries come first in the range, so within the getdata that carries
	// the ledger block it is the last entry.
	for _, rec := range recs {
		got := rec.all()
		for i, h := range got {
			if h == fromLedger {
				require.Equal(t, len(got)-1, i, "the ledger-named block is appended after the cache-named range")
			}
		}
	}
}
