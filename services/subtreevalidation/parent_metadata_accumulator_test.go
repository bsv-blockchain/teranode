package subtreevalidation

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/stretchr/testify/require"
)

// TestFilterParentMetadataForInputs covers the per-tx pre-filter step used by
// processTransactionsInLevels and processMissingTransactions to bound the
// per-call wire bandwidth in distributed-validator mode. The helper must
// return only the accumulator entries whose hashes appear in the tx's input
// prevouts — never the full block-scoped accumulator — and must return nil
// (not an empty map) when there are no matches so the downstream proto
// serialisation drops the field entirely.
func TestFilterParentMetadataForInputs(t *testing.T) {
	hashA := chainhash.Hash{0xaa}
	hashB := chainhash.Hash{0xbb}
	hashC := chainhash.Hash{0xcc} // referenced by an input but NOT in accumulator
	hashD := chainhash.Hash{0xdd} // in accumulator but NOT referenced by any input

	// Construct as single-map mode (delta=live, no snapshot) — semantically
	// identical to the pre-snapshot caller shape (CheckBlockSubtrees batch
	// loop / Phase 3 sequential).
	acc := &parentMetadataAccumulator{
		delta: map[chainhash.Hash]*validator.ParentTxMetadata{
			hashA: {BlockHeight: 100},
			hashB: {BlockHeight: 200},
			hashD: {BlockHeight: 400},
		},
	}

	t.Run("two matches one miss one ignored", func(t *testing.T) {
		// tx with three inputs: references A (match), C (miss), B (match).
		// hashD is in the accumulator but not in any input — must NOT appear
		// in the filtered output.
		tx := newTxWithInputs(t, hashA, hashC, hashB)

		got := filterParentMetadataForInputs(tx, acc)
		require.Len(t, got, 2, "exactly the 2 input hashes that match the accumulator must survive")
		require.NotNil(t, got[hashA])
		require.Equal(t, uint32(100), got[hashA].BlockHeight)
		require.NotNil(t, got[hashB])
		require.Equal(t, uint32(200), got[hashB].BlockHeight)
		require.Nil(t, got[hashC], "input C is not in accumulator — must not appear")
		require.Nil(t, got[hashD], "hashD is in accumulator but not in any input — must NOT leak into per-tx output")
	})

	t.Run("no matches returns nil", func(t *testing.T) {
		// tx with inputs that none match accumulator entries — nil result
		// so proto serialisation drops the field entirely.
		tx := newTxWithInputs(t, hashC, chainhash.Hash{0xee})
		got := filterParentMetadataForInputs(tx, acc)
		require.Nil(t, got, "no matches must produce nil (not empty map) so the wire form is field-absent")
	})

	t.Run("nil accumulator returns nil", func(t *testing.T) {
		tx := newTxWithInputs(t, hashA, hashB)
		require.Nil(t, filterParentMetadataForInputs(tx, nil))
	})

	t.Run("empty accumulator returns nil", func(t *testing.T) {
		tx := newTxWithInputs(t, hashA, hashB)
		require.Nil(t, filterParentMetadataForInputs(tx, &parentMetadataAccumulator{}))
		require.Nil(t, filterParentMetadataForInputs(tx, &parentMetadataAccumulator{
			delta: map[chainhash.Hash]*validator.ParentTxMetadata{},
		}))
	})

	t.Run("nil tx returns nil", func(t *testing.T) {
		require.Nil(t, filterParentMetadataForInputs(nil, acc))
	})

	t.Run("duplicate inputs to same parent collapse to one entry", func(t *testing.T) {
		// Two inputs both referencing the same parent — output map dedups by
		// hash key, so just one entry.
		tx := newTxWithInputs(t, hashA, hashA, hashA)
		got := filterParentMetadataForInputs(tx, acc)
		require.Len(t, got, 1)
		require.NotNil(t, got[hashA])
	})

	t.Run("snapshot+delta mode reads from both", func(t *testing.T) {
		// Pins the snapshot+delta view used by Phase 2: snapshot carries
		// prior-batch state, delta carries this subtree's contributions,
		// and a single filter call sees both.
		snapshot := map[chainhash.Hash]*validator.ParentTxMetadata{
			hashA: {BlockHeight: 100},
		}
		delta := map[chainhash.Hash]*validator.ParentTxMetadata{
			hashB: {BlockHeight: 200},
		}
		two := &parentMetadataAccumulator{snapshot: snapshot, delta: delta}

		tx := newTxWithInputs(t, hashA, hashB)
		got := filterParentMetadataForInputs(tx, two)
		require.Len(t, got, 2, "filter must see entries from BOTH snapshot and delta")
		require.NotNil(t, got[hashA])
		require.Equal(t, uint32(100), got[hashA].BlockHeight)
		require.NotNil(t, got[hashB])
		require.Equal(t, uint32(200), got[hashB].BlockHeight)
	})

	t.Run("delta wins over snapshot for same hash", func(t *testing.T) {
		// If a hash is in both maps, delta wins. This pins the read order
		// inside lookup() — delta is the more recent state.
		snapshot := map[chainhash.Hash]*validator.ParentTxMetadata{
			hashA: {BlockHeight: 100},
		}
		delta := map[chainhash.Hash]*validator.ParentTxMetadata{
			hashA: {BlockHeight: 999},
		}
		two := &parentMetadataAccumulator{snapshot: snapshot, delta: delta}

		tx := newTxWithInputs(t, hashA)
		got := filterParentMetadataForInputs(tx, two)
		require.Len(t, got, 1)
		require.Equal(t, uint32(999), got[hashA].BlockHeight,
			"delta must take precedence over snapshot when both contain the same hash")
	})
}

// TestParentMetadataAccumulatorAdd pins the first-writer-wins semantics of
// acc.add: subsequent adds for the same hash do NOT overwrite, regardless of
// whether the prior entry came from snapshot or delta. This invariant
// protects the consensus rule that an in-block parent's BlockHeight is
// recorded ONCE per block (the candidate block's height).
func TestParentMetadataAccumulatorAdd(t *testing.T) {
	hashA := chainhash.Hash{0xaa}
	hashB := chainhash.Hash{0xbb}

	t.Run("add into empty accumulator", func(t *testing.T) {
		acc := &parentMetadataAccumulator{}
		acc.add(hashA, &validator.ParentTxMetadata{BlockHeight: 100})
		require.NotNil(t, acc.delta)
		require.Equal(t, uint32(100), acc.delta[hashA].BlockHeight)
	})

	t.Run("add second hash extends delta", func(t *testing.T) {
		acc := &parentMetadataAccumulator{}
		acc.add(hashA, &validator.ParentTxMetadata{BlockHeight: 100})
		acc.add(hashB, &validator.ParentTxMetadata{BlockHeight: 200})
		require.Len(t, acc.delta, 2)
	})

	t.Run("repeat add to same hash is a no-op", func(t *testing.T) {
		acc := &parentMetadataAccumulator{}
		acc.add(hashA, &validator.ParentTxMetadata{BlockHeight: 100})
		acc.add(hashA, &validator.ParentTxMetadata{BlockHeight: 999})
		require.Equal(t, uint32(100), acc.delta[hashA].BlockHeight,
			"first-writer-wins: a second add for the same hash must NOT overwrite")
	})

	t.Run("add with hash already in snapshot is a no-op", func(t *testing.T) {
		acc := &parentMetadataAccumulator{
			snapshot: map[chainhash.Hash]*validator.ParentTxMetadata{
				hashA: {BlockHeight: 100},
			},
		}
		acc.add(hashA, &validator.ParentTxMetadata{BlockHeight: 999})
		require.Nil(t, acc.delta, "snapshot already covers this hash — delta must NOT be allocated")
	})

	t.Run("add to nil accumulator is a no-op", func(t *testing.T) {
		var acc *parentMetadataAccumulator
		acc.add(hashA, &validator.ParentTxMetadata{BlockHeight: 100})
		// Must not panic.
	})
}

// TestValidateMissingSubtreesWithOrderedRetryAccumulated_Phase2DeltasMergeAfterWait
// pins the Phase 2 / Phase 3 accumulator handling: SHARED snapshot for all
// Phase 2 goroutines (no per-subtree copy of the live accumulator), per-
// subtree fresh deltas, deltas merged into the live accumulator in block-
// subtree order after Phase 2 g.Wait(); Phase 3 sees the merged state.
func TestValidateMissingSubtreesWithOrderedRetryAccumulated_Phase2DeltasMergeAfterWait(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// 3 subtrees. The validateFn callback writes to the accumulator's delta
	// via acc.add — Phase 2 delta is per-subtree-fresh; Phase 3 delta is the
	// live accumulator.
	missing := []chainhash.Hash{{0x01}, {0x02}, {0x03}}

	// Track what the validateFn observed in the accumulator at each call.
	type observation struct {
		subtreeHash      chainhash.Hash
		deltaSizeOnEntry int
		sawSeededParent  bool
	}
	var observations []observation
	var observationsMu sync.Mutex

	// Each subtree contributes one tx-hash to its delta on validation.
	contribution := map[chainhash.Hash]chainhash.Hash{
		missing[0]: {0xa0},
		missing[1]: {0xb0},
		missing[2]: {0xc0},
	}

	// Pre-seed sentinel: lives in the live accumulator BEFORE Phase 2
	// spawns. Every Phase 2 goroutine MUST see it via acc.lookup — that
	// proves the snapshot view is shared (vs each goroutine getting an
	// empty private copy and missing prior-batch state).
	seededParent := chainhash.Hash{0xff, 0xee}
	seededHeight := uint32(7)

	validateFn := func(_ context.Context, h chainhash.Hash, acc *parentMetadataAccumulator) (*subtreepkg.Subtree, error) {
		require.NotNil(t, acc, "validateFn must receive a non-nil accumulator in Phase 2/3")

		// Lookup the pre-seeded parent — this exercises the shared snapshot
		// path. If the implementation copies the live map per subtree we'd
		// still find it (the copy would include the seed), so the harder
		// invariant to pin is the delta-isolation one below.
		seedView := acc.lookup(seededParent)

		observationsMu.Lock()
		observations = append(observations, observation{
			subtreeHash:      h,
			deltaSizeOnEntry: len(acc.delta),
			sawSeededParent:  seedView != nil && seedView.BlockHeight == seededHeight,
		})
		observationsMu.Unlock()

		// Contribute via acc.add — this writes to delta only.
		acc.add(contribution[h], &validator.ParentTxMetadata{BlockHeight: 42})
		return nil, nil
	}

	liveAccumulator := map[chainhash.Hash]*validator.ParentTxMetadata{
		seededParent: {BlockHeight: seededHeight},
	}
	err := server.validateMissingSubtreesWithOrderedRetryAccumulated(context.Background(), missing, liveAccumulator, validateFn)
	require.NoError(t, err)

	// All 3 subtrees succeeded in Phase 2; their deltas are now merged into
	// the live accumulator in block-subtree order. The live accumulator must
	// contain the original seed + all 3 contributions = 4 entries.
	require.Len(t, liveAccumulator, 4, "all Phase 2 successful contributions plus the pre-existing seed must be present after the merge")
	require.NotNil(t, liveAccumulator[seededParent], "pre-existing seed must survive")
	require.NotNil(t, liveAccumulator[contribution[missing[0]]])
	require.NotNil(t, liveAccumulator[contribution[missing[1]]])
	require.NotNil(t, liveAccumulator[contribution[missing[2]]])

	// CRITICAL Phase 2 invariants:
	//  - shared snapshot: every goroutine sees the pre-seeded parent via
	//    acc.lookup (not just the first one).
	//  - delta isolation: every goroutine enters with an empty delta — none
	//    of them see another subtree's in-flight contributions.
	require.Len(t, observations, 3)
	for _, obs := range observations {
		require.True(t, obs.sawSeededParent,
			"every Phase 2 goroutine must see the pre-seeded parent via the shared snapshot — proves snapshot is shared, not per-subtree-empty")
		require.Equal(t, 0, obs.deltaSizeOnEntry,
			"each Phase 2 subtree must enter validateFn with a FRESH (empty) delta — not the running state from other parallel subtree validations")
	}
}

// TestValidateMissingSubtreesWithOrderedRetryAccumulated_FailedPhase2DoesNotContribute
// pins the regression invariant: only fully-successful Phase 2 subtrees
// contribute to the live accumulator. Failed-Phase-2 subtree contributions
// are dropped from the merge; those subtrees retry in Phase 3 and their
// contributions enter the accumulator there instead.
func TestValidateMissingSubtreesWithOrderedRetryAccumulated_FailedPhase2DoesNotContribute(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	missing := []chainhash.Hash{{0x01}, {0x02}, {0x03}}
	// Track per-subtree whether we've validated it.
	validated := make(map[chainhash.Hash]bool)
	var validatedMu sync.Mutex

	// Index helpers — useful below.
	indexOf := map[chainhash.Hash]int{
		missing[0]: 0, missing[1]: 1, missing[2]: 2,
	}

	// Phase 2 contribution markers we'd write to the accumulator in any call.
	phase2Mark := chainhash.Hash{0xaa}
	phase3Mark := chainhash.Hash{0xbb}

	var phase2Count int
	var callMu sync.Mutex

	validateFn := func(_ context.Context, h chainhash.Hash, acc *parentMetadataAccumulator) (*subtreepkg.Subtree, error) {
		callMu.Lock()
		isPhase2 := phase2Count < len(missing)
		if isPhase2 {
			phase2Count++
		}
		callMu.Unlock()

		i := indexOf[h]

		if isPhase2 {
			// In Phase 2: subtree 1 fails BEFORE writing anything; subtree 0
			// fails AFTER writing a contribution; subtree 2 succeeds with a
			// contribution. Invariant: the failed subtrees' (0 and 1) Phase 2
			// contributions must NOT reach the live accumulator.
			switch i {
			case 0:
				// Subtree 0 writes a Phase 2 contribution then fails. The
				// contribution must NOT propagate — partial-success deltas
				// from failed subtrees are dropped.
				acc.add(phase2Mark, &validator.ParentTxMetadata{BlockHeight: 1})
				return nil, errors.NewTxMissingParentError("subtree 0 failed in phase 2")
			case 1:
				return nil, errors.NewTxMissingParentError("subtree 1 failed in phase 2")
			case 2:
				// Subtree 2 succeeds — its contribution IS merged.
				validatedMu.Lock()
				validated[h] = true
				validatedMu.Unlock()
				return nil, nil
			}
			return nil, nil
		}

		// Phase 3: write a different marker so the test can distinguish
		// Phase 2 vs Phase 3 contributions.
		acc.add(phase3Mark, &validator.ParentTxMetadata{BlockHeight: 99})
		validatedMu.Lock()
		validated[h] = true
		validatedMu.Unlock()
		return nil, nil
	}

	liveAccumulator := make(map[chainhash.Hash]*validator.ParentTxMetadata)
	err := server.validateMissingSubtreesWithOrderedRetryAccumulated(context.Background(), missing, liveAccumulator, validateFn)
	require.NoError(t, err)

	// Phase 2 partial-success contribution from the failed subtree must NOT
	// be in the live accumulator.
	require.Nil(t, liveAccumulator[phase2Mark],
		"Phase 2 contribution from a failed subtree must be dropped — only fully-successful subtrees contribute to the live accumulator")

	// Phase 3 retry contribution IS present.
	require.NotNil(t, liveAccumulator[phase3Mark],
		"Phase 3 sequential retry must mutate the live accumulator directly")
}

// newTxWithInputs builds a minimal extended tx with one input per supplied
// previous-tx hash. Used to drive filterParentMetadataForInputs without
// standing up a full signed-tx fixture.
func newTxWithInputs(t *testing.T, prevTxHashes ...chainhash.Hash) *bt.Tx {
	t.Helper()
	tx := bt.NewTx()
	for _, h := range prevTxHashes {
		// Build an input with PreviousTxID set to h. Use bt.NewTxFromString
		// path is heavy; instead construct directly so the test is isolated
		// from the full signing pipeline.
		input := &bt.Input{
			PreviousTxOutIndex: 0,
		}
		// Internal-order hash bytes copied into the input's PreviousTxID storage.
		hashCopy := h
		require.NoError(t, input.PreviousTxIDAdd(&hashCopy))
		tx.Inputs = append(tx.Inputs, input)
	}
	return tx
}
