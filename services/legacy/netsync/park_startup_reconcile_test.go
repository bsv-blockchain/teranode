package netsync

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
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
func recoveredParkHarness(t *testing.T) (*SyncManager, chainhash.Hash) {
	t.Helper()

	initPrometheusMetrics()

	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)
	sm.rejectedTxns = txmap.NewSyncedMap[chainhash.Hash, struct{}](100)

	sm.settings.BlockValidation.OutpointOnlyBelowCheckpoint = true
	sm.settings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	sm.utxoStore = &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}}
	sm.blockValidation = &convertedRouteSpyValidation{}

	blk := wireBlockWithTxs(t, 6, false)
	// wireBlockWithTxs' Bits is mainnet genesis-era difficulty, a real target
	// no unmined test nonce clears; commitParkedBlock's PoW check is real code,
	// not mocked, so this needs regtest's PowLimitBits instead, same as the
	// tests this harness follows.
	blk.MsgBlock().Header.Bits = 0x207fffff
	pipelineHeaderFixture(t, sm, blk)
	mineRegtestPoW(t, blk)
	body := blockBodyBytes(t, blk)

	converted, err := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err, "a well-formed block below the checkpoint must convert cleanly")
	require.True(t, converted, "sanity: this test needs an actual conversion, or it asserts nothing about the commit that follows")

	hash := *blk.Hash()

	// Seeded directly into the index rather than through Admit/Recover: this
	// harness needs one specific entry in place, not a restart scan over a
	// directory. Recover would build the same shape (converted: true, a
	// pre-restart parkedAt) from a real recovery pass; block_park.go's own
	// Recover is what TestBlockPark_ARecoveredBlockKeepsTheAgeItHadBeforeTheRestart
	// exercises for that half.
	sm.blockPark.entries[hash] = &parkedBlock{
		hash:      hash,
		prevBlock: blk.MsgBlock().Header.PrevBlock,
		converted: true,
		parkedAt:  time.Now().Add(-parkStuckThreshold - time.Minute),
	}

	return sm, hash
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
		converted: true,
		parkedAt:  time.Now().Add(-parkStuckThreshold - time.Minute),
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
	sm, parked := recoveredParkHarness(t)

	n := sm.reconcileRecoveredParents(context.Background())

	require.Equal(t, 1, n, "the recovered block's parent is in the chain, so it must be handed on")
	require.False(t, sm.blockPark.Has(parked),
		"and it must leave the park, because nothing else will ever wake it")
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
