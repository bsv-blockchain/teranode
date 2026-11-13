//go:build bvgaps

package blockvalidation

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

const (
	// taggingParkedTimeout bounds how long the parked SetMinedMulti waits for the test to release it.
	taggingParkedTimeout = 20 * time.Second
	// taggingReachStoreTimeout bounds how long the test waits for the run to park in SetMinedMulti.
	taggingReachStoreTimeout = 10 * time.Second
	// taggingFinishTimeout bounds how long the test waits for the released run to finish.
	taggingFinishTimeout = 20 * time.Second
)

// taggingParkingUtxoStore wraps a real UTXO store and parks the first non-revert SetMinedMulti call
// (a set-mined tagging run) until released, so the test can invalidate the block mid-run.
type taggingParkingUtxoStore struct {
	utxostore.Store

	armed   atomic.Bool
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (s *taggingParkingUtxoStore) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, info utxostore.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	if s.armed.Load() && !info.UnsetMined {
		s.once.Do(func() { close(s.entered) })

		select {
		case <-s.release:
		case <-time.After(taggingParkedTimeout):
			return nil, errors.NewProcessingError("test never released the parked SetMinedMulti")
		}
	}

	return s.Store.SetMinedMulti(ctx, hashes, info)
}

// TestKnownDefect_InFlightMinedTagging_DoesNotOverwriteInvalidation checks that a tagging run in
// flight when its block is invalidated does not leave the block invalid, marked mined and still
// tagged.
//
// Spec: BV-COMMIT-005, BV-COMMIT-011 and BV-REVALIDATE-003 (tracked as GAP-BV-058).
//
// Scenario: Given a block at height 1 that is validated and stored but not yet tagged (mined_set
// false, valid). When a set-mined tagging run starts, reads invalid=false, and is parked inside
// SetMinedMulti while the block is invalidated (InvalidateBlock clears mined_set), and the run is
// then released. Then the block must not end up invalid AND mined_set AND its spend transaction
// still tagged: either the flag write is refused, or the tags are reverted.
//
// Today: stores/blockchain/sql/SetBlockMinedSet.go is an unconditional
// `UPDATE blocks SET mined_set = true WHERE hash = $1`, and BlockValidation.setTxMinedStatus
// (BlockValidation.go) calls it after tagging without re-checking the invalid flag it read at the
// start. The in-flight run overwrites the cleared mined_set; the BlockMinedUnset notification for a
// hash already being processed is skipped and the sweep no longer lists the block.
//
// The race is made deterministic with a UTXO-store decorator that parks the tagging run inside
// SetMinedMulti while InvalidateBlock runs on the real sqlitememory store.
func TestKnownDefect_InFlightMinedTagging_DoesNotOverwriteInvalidation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	harness := newMinedTagHarness(t, ctx, "inflight_mined_tagging_invalidation")
	defer harness.validation.StopCaches()

	// Given: the block is validated and stored (mined_set=false, subtrees_set=true); no tagging yet.
	require.NoError(t, harness.validation.ValidateBlock(ctx, harness.block, "test"))

	storedMeta := harness.blockMeta(t, ctx)
	require.False(t, storedMeta.MinedSet, "precondition: tagging has not run yet")
	require.False(t, storedMeta.Invalid, "precondition: block is valid when the run starts")

	parkingStore := &taggingParkingUtxoStore{
		Store:   harness.utxoStore,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	harness.validation.utxoStore = parkingStore
	parkingStore.armed.Store(true)

	// When: the tagging run starts (it reads invalid=false) and parks inside SetMinedMulti.
	runDone := make(chan error, 1)

	go func() {
		runDone <- harness.validation.setTxMinedStatus(ctx, harness.block.Hash())
	}()

	select {
	case <-parkingStore.entered:
	case <-time.After(taggingReachStoreTimeout):
		t.Fatal("precondition: the tagging run never reached SetMinedMulti")
	}

	// When: while the run is in flight the block is invalidated.
	harness.invalidateBlock(t, ctx)

	parkedMeta := harness.blockMeta(t, ctx)
	require.True(t, parkedMeta.Invalid, "precondition: block is invalid while the run is parked")
	require.False(t, parkedMeta.MinedSet, "precondition: InvalidateBlock cleared mined_set")

	// When: the parked run is released and finishes.
	close(parkingStore.release)

	select {
	case runErr := <-runDone:
		t.Logf("in-flight setTxMinedStatus returned: %v", runErr)
	case <-time.After(taggingFinishTimeout):
		t.Fatal("the tagging run did not finish after release")
	}

	// Then: the block is still invalid and is not left both mined_set and tagged.
	endMeta := harness.blockMeta(t, ctx)
	require.True(t, endMeta.Invalid, "the block is still invalid at the end")

	endTags, err := harness.utxoStore.Get(ctx, harness.spendTx.TxIDChainHash())
	require.NoError(t, err)

	tagsPresent := len(endTags.BlockIDs) > 0

	require.False(t, endMeta.MinedSet && tagsPresent,
		"block is invalid, yet the in-flight run left mined_set=true with the transaction still tagged (BlockIDs=%v); the unconditional SetBlockMinedSet overwrote the invalidation and the sweep will never revisit it", endTags.BlockIDs)
}
