package netsync

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// recoveredParkHarness builds a SyncManager with one converted record parked
// exactly the way Recover leaves one after a restart: entries[hash].converted
// is true, and its prevBlock is a header the harness's own chain (a real
// sqlitememory-backed blockchain client, via newPipelineParkManager) already
// holds — regtest genesis, planted by pipelineHeaderFixture.
//
// The commit route below (outpoint-only, unified) and convertedRouteSpyValidation
// are the same combination TestCommitParkedBlock_RoutesAConvertedEntryWithoutReadingAWholeBlock
// and TestHandleConvertedBlock_CommitsWithoutTheBlock already prove reaches a
// real commit without standing up blockvalidation.Server's own dependencies
// (subtree fetch, UTXO create/spend, kafka, gRPC); this harness is built the
// same way so the "must leave the park" half of the test is a genuine commit,
// not a stand-in for one.
//
// proven says whether the header cache is given a genuine proof that the block
// is on the checkpointed chain. After a real restart it is not: the cache
// starts empty and reconcileRecoveredParents runs before any headers reply, so
// the unproven shape is the one production actually recovers into.
//
// recoveredParkFixture is what a recoveredParkHarness test needs besides the
// manager: the parked block's hash, its header (to prove it later through a
// real fill) and the spy that stands in for block validation.
type recoveredParkFixture struct {
	hash   chainhash.Hash
	header *wire.BlockHeader
	spy    *convertedRouteSpyValidation
}

func recoveredParkHarness(t *testing.T, proven bool) (*SyncManager, *recoveredParkFixture) {
	t.Helper()

	initPrometheusMetrics()

	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)
	sm.rejectedTxns = txmap.NewSyncedMap[chainhash.Hash, struct{}](100)

	sm.settings.BlockValidation.OutpointOnlyBelowCheckpoint = true
	sm.settings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	sm.utxoStore = &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}}

	// With a chain, so a commit stores the block where HandleConvertedBlock's
	// own GetBlockExists reads it back: the test that re-offers a block after a
	// proof needs the second attempt to be a real commit, not a repeat.
	spy := &convertedRouteSpyValidation{chain: sm.blockchainClient}
	sm.blockValidation = spy

	blk := wireBlockWithTxs(t, 6, false)
	// wireBlockWithTxs' Bits is mainnet genesis-era difficulty, a real target
	// no unmined test nonce clears; commitParkedBlock's PoW check is real code,
	// not mocked, so this needs regtest's PowLimitBits instead, same as the
	// tests this harness follows.
	blk.MsgBlock().Header.Bits = 0x207fffff
	pipelineHeaderFixture(t, sm, blk)
	mineRegtestPoW(t, blk)
	blk.SetHeight(1)

	if proven {
		proveBlockOrigin(t, sm, blk)
	} else {
		// The checkpoint sits above the block so the unified route applies, but
		// the cache has never seen a run that reaches it: unproven, as after a
		// restart.
		hash := *blk.Hash()
		sm.chainParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1, Hash: &hash}}
		sm.headerCache = newHeaderCache().WithCheckpoints(sm.chainParams.Checkpoints)
	}

	body := blockBodyBytes(t, blk)

	converted, err := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), sinkPayloadLen(body))
	require.NoError(t, err, "a well-formed block below the checkpoint must convert cleanly")
	require.True(t, converted, "sanity: this test needs an actual conversion, or it asserts nothing about the commit that follows")

	hash := *blk.Hash()

	// Seeded directly into the index rather than through Admit/Recover: this
	// harness needs one specific entry in place, not a restart scan over a
	// directory. Recover would build the same shape (a
	// pre-restart parkedAt, the record's height) from a real recovery pass;
	// block_park.go's own Recover is what
	// TestBlockPark_ARecoveredBlockKeepsTheAgeItHadBeforeTheRestart exercises
	// for that half. The sink's Admit leaves the entry out of children, so it
	// is added here the way Restore and Recover both do.
	sm.blockPark.entries[hash] = &parkedBlock{
		hash:      hash,
		prevBlock: blk.MsgBlock().Header.PrevBlock,
		height:    1,

		parkedAt: time.Now().Add(-parkStuckThreshold - time.Minute),
	}
	sm.blockPark.children[blk.MsgBlock().Header.PrevBlock] = append(sm.blockPark.children[blk.MsgBlock().Header.PrevBlock], hash)

	return sm, &recoveredParkFixture{hash: hash, header: &blk.MsgBlock().Header, spy: spy}
}

// recoveredParkHarnessNoParent is the other half: one entry parked behind a
// parent nothing has ever heard of. reconcileRecoveredParents must return
// without ever reaching Take for it, so nothing about a converted record or a
// real commit route is needed here — a bare entry is enough to prove the
// "still missing" branch leaves it alone.
func recoveredParkHarnessNoParent(t *testing.T) (*SyncManager, chainhash.Hash) {
	t.Helper()

	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)

	var hash, unresolvedParent chainhash.Hash
	hash[0] = 0xAA
	unresolvedParent[0] = 0xBB

	sm.blockPark.entries[hash] = &parkedBlock{
		hash:      hash,
		prevBlock: unresolvedParent,

		parkedAt: time.Now().Add(-parkStuckThreshold - time.Minute),
	}

	return sm, hash
}

// TestReconcileRecoveredParents_CommitsABlockWhoseParentIsAlreadyInTheChain is
// the case the sweep was standing in for. A node stops with a block parked
// behind a parent that then commits; the process ends; the next process recovers
// the block from disk and waits for a commit event that already happened.
//
// One pass after recovery settles it. A thirty-second ticker settles it too,
// eventually, and then keeps asking for the rest of the node's life.
func TestReconcileRecoveredParents_CommitsABlockWhoseParentIsAlreadyInTheChain(t *testing.T) {
	sm, f := recoveredParkHarness(t, true)

	n := sm.reconcileRecoveredParents(context.Background())

	require.Equal(t, 1, n, "the recovered block's parent is in the chain, so it must be handed on")
	require.False(t, sm.blockPark.Has(f.hash),
		"and it must leave the park, because no commit event will ever wake it")
	require.Equal(t, 1, f.spy.callCount(), "handed on means committed")
	require.True(t, f.spy.lastCall().headerProven, "a proven record carries its proof to block validation")
}

// TestReconcileRecoveredParents_AnUnprovenBlockWaitsForTheHeaderWalkAndCommitsWhenProven
// is the restart as it really happens. The header cache starts empty and
// reconcileRecoveredParents runs before any headers reply, so every recovered
// block on the unified route is unproven when it is first handed on.
// HandleConvertedBlock refuses it (a ServiceError, read as retry-later: blob
// kept, no mark, no blame) and the drain stamps it so the consumer does not
// spend every turn on it. Then a fill that reaches the pinned checkpoint proves
// it, and fillHeaderCache re-offers it at once: the second attempt is a real
// commit through the route that was refused the first time, carrying the proof.
//
// Nothing here forces the outcome: the proof is a genuine headerCache.Fill of
// the block's own header against the pinned hash, and the re-offer is the
// production hook on fillHeaderCache, driven through the production handler.
func TestReconcileRecoveredParents_AnUnprovenBlockWaitsForTheHeaderWalkAndCommitsWhenProven(t *testing.T) {
	sm, f := recoveredParkHarness(t, false)
	ctx := context.Background()

	n := sm.reconcileRecoveredParents(ctx)
	require.Equal(t, 1, n, "the parent is in the chain, so the block is handed on")

	// Refused, kept, and stamped.
	require.True(t, sm.blockPark.Has(f.hash), "an unproven block on the unified route stays parked")
	require.Zero(t, f.spy.callCount(), "the refusal happens before any RPC to block validation")

	entry, ok := sm.blockPark.entries[f.hash]
	require.True(t, ok)
	require.False(t, entry.awaitingProofAt.IsZero(), "the drain stamps the block so it is not re-dispatched every turn")

	_, committable := sm.blockPark.committableChildLocked(f.hash)
	require.False(t, committable, "within the floor the drain leaves it alone")

	exists, err := sm.blockchainClient.GetBlockExists(ctx, &f.hash)
	require.NoError(t, err)
	require.False(t, exists, "nothing was stored")

	// The header walk reaches the checkpoint: a one-header run from the
	// committed tip (genesis) ending at the pinned hash.
	p := peer.NewInboundPeer(ulogger.TestLogger{}, sm.settings, &peer.Config{})

	require.True(t, sm.fillHeaderCache(p, headersMsgOf(t, []*wire.BlockHeader{f.header})), "the run links to genesis and matches the checkpoint")
	require.True(t, sm.blockOrigin(f.hash).headerProven, "sanity: the fill must have proven the block")

	// Re-offered by the fill, committed through the same route, with the proof.
	require.False(t, sm.blockPark.Has(f.hash), "the proven block leaves the park")
	require.Equal(t, 1, f.spy.callCount(), "exactly one commit, the re-offer")
	require.True(t, f.spy.lastCall().headerProven, "the re-offer carries the proof the first attempt lacked")

	exists, err = sm.blockchainClient.GetBlockExists(ctx, &f.hash)
	require.NoError(t, err)
	require.True(t, exists, "the block is in the chain")
}

// TestReofferParkedBlocksProvenBy_AnUnprovenSiblingKeepsItsFloor is the
// re-offer with two blocks parked behind the tip: the one the fill proves, and
// a sibling at the same height the cache names a different hash for, a fork
// child below the last checkpoint that HandleConvertedBlock refused and the
// drain stamped. TakeChildrenForProof lifts both out of the park, because the
// proof could be for either; only the proven one may lose its floor. The
// sibling goes back exactly as it was, stamp and all, so the drain the re-offer
// triggers leaves it alone instead of dispatching it hot for HandleConvertedBlock
// to refuse again.
//
// The sibling is a bare entry with no record on disk, which is enough: the
// re-offer path for an unproven child is Restore, nothing reads it. If the
// stamp were lost the drain would take it (committableChildLocked no longer
// holds it) and the read would fail, so the stamp assertion is also the
// assertion that it was never dispatched.
func TestReofferParkedBlocksProvenBy_AnUnprovenSiblingKeepsItsFloor(t *testing.T) {
	sm, f := recoveredParkHarness(t, false)
	ctx := context.Background()

	var sibling chainhash.Hash
	sibling[0] = 0x5B

	stampedAt := time.Now()
	parent := f.header.PrevBlock

	sm.blockPark.entries[sibling] = &parkedBlock{
		hash:            sibling,
		prevBlock:       parent,
		height:          1,
		parkedAt:        stampedAt.Add(-time.Minute),
		awaitingProofAt: stampedAt,
	}
	// First among the children, so a copy that came back without its stamp is
	// in the index before the proven block's drain runs, whatever the map order.
	sm.blockPark.children[parent] = append([]chainhash.Hash{sibling}, sm.blockPark.children[parent]...)

	p := peer.NewInboundPeer(ulogger.TestLogger{}, sm.settings, &peer.Config{})

	require.True(t, sm.fillHeaderCache(p, headersMsgOf(t, []*wire.BlockHeader{f.header})), "the run links to genesis and matches the checkpoint")
	require.True(t, sm.blockOrigin(f.hash).headerProven, "sanity: the fill must have proven the block")
	require.False(t, sm.blockOrigin(sibling).headerProven, "sanity: the cache names a different hash at this height")

	// The proven block is re-offered and committed.
	require.False(t, sm.blockPark.Has(f.hash), "the proven block leaves the park")
	require.Equal(t, 1, f.spy.callCount(), "exactly one commit, the proven block's re-offer")

	exists, err := sm.blockchainClient.GetBlockExists(ctx, &f.hash)
	require.NoError(t, err)
	require.True(t, exists, "the proven block is in the chain")

	// The sibling is back as it was.
	entry, ok := sm.blockPark.entries[sibling]
	require.True(t, ok, "the unproven sibling stays parked")
	require.Equal(t, stampedAt, entry.awaitingProofAt, "the sibling keeps the floor it was stamped with; the proof was not for it")

	_, committable := sm.blockPark.committableChildLocked(sibling)
	require.False(t, committable, "so the drain the re-offer triggered left it alone")
}

// TestReconcileRecoveredParents_LeavesABlockWhoseParentIsGenuinelyMissing is the
// other half. A parent that has not arrived is not an error and must not be
// treated as one: the block waits for the ordinary commit event.
func TestReconcileRecoveredParents_LeavesABlockWhoseParentIsGenuinelyMissing(t *testing.T) {
	sm, parked := recoveredParkHarnessNoParent(t)

	n := sm.reconcileRecoveredParents(context.Background())

	require.Zero(t, n, "no parent in the chain means nothing to hand on")
	require.True(t, sm.blockPark.Has(parked),
		"and the block stays parked rather than being given up on")
}
