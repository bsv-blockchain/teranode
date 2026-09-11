package subtreevalidation

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/require"
)

// recordingParentStore embeds MockUtxostore and records every key passed to
// BatchDecorate so a test can assert how many distinct parents were actually
// read from the store.
type recordingParentStore struct {
	*utxostore.MockUtxostore

	mu        sync.Mutex
	requested []chainhash.Hash
	fields    []fields.FieldName
	resolve   map[chainhash.Hash]*meta.Data
	// itemErrs models the Aerospike contract: BatchDecorate returns a nil
	// function error but records per-record failures on item.Err.
	itemErrs map[chainhash.Hash]error
}

func (s *recordingParentStore) BatchDecorate(_ context.Context, items []*utxostore.UnresolvedMetaData, f ...fields.FieldName) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.fields = f

	for _, it := range items {
		s.requested = append(s.requested, it.Hash)
		if err, ok := s.itemErrs[it.Hash]; ok {
			it.Err = err
			continue
		}
		if d, ok := s.resolve[it.Hash]; ok {
			it.Data = d
		}
	}

	return nil
}

func (s *recordingParentStore) hasField(want fields.FieldName) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, f := range s.fields {
		if f == want {
			return true
		}
	}

	return false
}

// Test_prefetchLevelParents_DedupsSharedParent proves the Phase-1 win: when a
// level has many transactions spending the same parent (the fan-out pattern that
// dominates the scaling test — one funding tx, many children), the bulk reader
// asks the store for that parent ONCE, not once per child. Without dedup the
// store sees N reads of the same record; with it, one.
func Test_prefetchLevelParents_DedupsSharedParent(t *testing.T) {
	ctx := context.Background()

	server, cleanup := setupTestServer(t)
	defer cleanup()

	lockingScript := "76a914000000000000000000000000000000000000000088ac"
	sharedParentHex := "0000000000000000000000000000000000000000000000000000000000000001"
	otherParentHex := "0000000000000000000000000000000000000000000000000000000000000002"

	mkTx := func(parentHex string, vout uint32) *bt.Tx {
		tx := bt.NewTx()
		require.NoError(t, tx.From(parentHex, vout, lockingScript, 1000))
		return tx
	}

	// 5 children of the shared parent + 1 tx spending a different parent.
	levelTxs := make([]missingTx, 0, 6)
	for i := 0; i < 5; i++ {
		levelTxs = append(levelTxs, missingTx{tx: mkTx(sharedParentHex, uint32(i)), idx: i})
	}
	levelTxs = append(levelTxs, missingTx{tx: mkTx(otherParentHex, 0), idx: 5})

	sharedParent := *levelTxs[0].tx.Inputs[0].PreviousTxIDChainHash()
	otherParent := *levelTxs[5].tx.Inputs[0].PreviousTxIDChainHash()

	store := &recordingParentStore{
		MockUtxostore: &utxostore.MockUtxostore{},
		resolve: map[chainhash.Hash]*meta.Data{
			sharedParent: {BlockHeights: []uint32{}},         // in-block / unconfirmed
			otherParent:  {BlockHeights: []uint32{100, 101}}, // earlier block
		},
	}
	server.utxoStore = store

	prefetched, err := server.prefetchLevelParents(ctx, levelTxs)
	require.NoError(t, err)

	// Both distinct parents resolved.
	require.Len(t, prefetched, 2)
	require.NotNil(t, prefetched[sharedParent])
	require.NotNil(t, prefetched[otherParent])

	// The dedup contract: each distinct parent read exactly once (2 total),
	// not once per child (which would be 6).
	require.Len(t, store.requested, 2,
		"shared parent must be read once per level, not once per child")
}

// mkExtendedTx builds a fully-extended single-input tx (From sets the input's
// PreviousTxScript, which is what IsExtended checks).
func mkExtendedTx(t *testing.T, parentHex string, vout uint32) *bt.Tx {
	t.Helper()

	const lockingScript = "76a914000000000000000000000000000000000000000088ac"

	tx := bt.NewTx()
	require.NoError(t, tx.From(parentHex, vout, lockingScript, 1000))

	return tx
}

// mkNonExtendedTx builds the same tx but clears the input's PreviousTxScript so
// IsExtended reports false — the validator would then need fields.Tx to extend it.
func mkNonExtendedTx(t *testing.T, parentHex string, vout uint32) *bt.Tx {
	t.Helper()

	tx := mkExtendedTx(t, parentHex, vout)
	tx.Inputs[0].PreviousTxScript = nil
	require.False(t, tx.IsExtended())

	return tx
}

// Test_prefetchLevelParents_FetchesOutputsWhenAllExtended proves the projection
// does not depend on the level already being extended: the validator re-extends
// every transaction from the store (GHSA-v76m-6vc7-g7c7), so parent outputs are
// fetched even when every tx in the level arrives extended.
//
// fields.Tx is never requested. That is a narrowing rather than a saved round
// trip — fields.Outputs is itself in the needsFullExternalTx trigger, so a large
// parent still costs an external read — but it avoids fetching the parent's
// inputs, which the extend path never reads.
func Test_prefetchLevelParents_FetchesOutputsWhenAllExtended(t *testing.T) {
	ctx := context.Background()

	server, cleanup := setupTestServer(t)
	defer cleanup()

	parentHex := "0000000000000000000000000000000000000000000000000000000000000001"

	levelTxs := []missingTx{
		{tx: mkExtendedTx(t, parentHex, 0), idx: 0},
		{tx: mkExtendedTx(t, parentHex, 1), idx: 1},
	}

	parent := *levelTxs[0].tx.Inputs[0].PreviousTxIDChainHash()

	store := &recordingParentStore{
		MockUtxostore: &utxostore.MockUtxostore{},
		resolve:       map[chainhash.Hash]*meta.Data{parent: {BlockHeights: []uint32{100}}},
	}
	server.utxoStore = store

	_, err := server.prefetchLevelParents(ctx, levelTxs)
	require.NoError(t, err)

	require.True(t, store.hasField(fields.BlockIDs), "BlockIDs is always needed")
	require.True(t, store.hasField(fields.BlockHeights), "BlockHeights is always needed")
	require.True(t, store.hasField(fields.Outputs),
		"parent outputs are needed even for a fully extended level: the validator re-extends every tx")
	require.False(t, store.hasField(fields.Tx),
		"fields.Tx is never needed — the extend path reads only Outputs[vout]")
}

// Test_prefetchLevelParents_FetchesOutputsWhenAnyNonExtended proves the other side
// side: the projection does not depend on whether the level happens to be
// extended. The validator re-extends every transaction from the store
// (GHSA-v76m-6vc7-g7c7), so parent outputs are fetched either way.
//
// The gate these two tests originally pinned — fields.Tx only when the level
// held a non-extended tx — mirrored the validator's old
// `extend := !tx.IsExtended()` decision and is gone with it. Keeping the pair
// preserves the level-shape polarity coverage they gave.
func Test_prefetchLevelParents_FetchesOutputsWhenAnyNonExtended(t *testing.T) {
	ctx := context.Background()

	server, cleanup := setupTestServer(t)
	defer cleanup()

	parentHex := "0000000000000000000000000000000000000000000000000000000000000001"

	// One extended, one non-extended: the projection must not depend on which.
	levelTxs := []missingTx{
		{tx: mkExtendedTx(t, parentHex, 0), idx: 0},
		{tx: mkNonExtendedTx(t, parentHex, 1), idx: 1},
	}

	parent := *levelTxs[0].tx.Inputs[0].PreviousTxIDChainHash()

	// The resolved parent carries outputs, as a real store asked for
	// fields.Outputs does. Without them the prefetched entry fails the
	// validator's Data.Tx guard and every parent falls back to a per-parent Get,
	// which is the regression the assertions below exist to catch.
	store := &recordingParentStore{
		MockUtxostore: &utxostore.MockUtxostore{},
		resolve: map[chainhash.Hash]*meta.Data{parent: {
			BlockHeights: []uint32{100},
			Tx: &bt.Tx{
				Outputs: []*bt.Output{
					{Satoshis: 1000, LockingScript: &bscript.Script{}},
					{Satoshis: 2000, LockingScript: &bscript.Script{}},
				},
			},
		}},
	}
	server.utxoStore = store

	prefetched, err := server.prefetchLevelParents(ctx, levelTxs)
	require.NoError(t, err)

	require.True(t, store.hasField(fields.BlockIDs), "BlockIDs is always needed")
	require.True(t, store.hasField(fields.BlockHeights), "BlockHeights is always needed")
	require.True(t, store.hasField(fields.Outputs),
		"parent outputs are needed for a level containing a non-extended tx")
	require.False(t, store.hasField(fields.Tx),
		"fields.Tx is never needed — the extend path reads only Outputs[vout]")

	// The prefetch is only useful if the validator's guard accepts it, and that
	// guard requires a non-nil Data.Tx, not merely a map entry. A prefetched
	// parent without one falls back to an individual per-parent Get.
	require.NotNil(t, prefetched[parent], "parent must be prefetched")
	require.NotNil(t, prefetched[parent].Tx,
		"prefetched parent must carry Data.Tx, or the validator's guard rejects it and re-reads per parent")
}

// Test_prefetchLevelParents_OmitsNotFoundParent proves a genuine not-found
// parent (the cross-subtree parent that lives in a later batch) is omitted from
// the result map, NOT treated as an error — the validator falls back to a
// per-parent Get and the existing missing-parent deferral handles it.
func Test_prefetchLevelParents_OmitsNotFoundParent(t *testing.T) {
	ctx := context.Background()

	server, cleanup := setupTestServer(t)
	defer cleanup()

	foundHex := "0000000000000000000000000000000000000000000000000000000000000001"
	missingHex := "0000000000000000000000000000000000000000000000000000000000000002"

	levelTxs := []missingTx{
		{tx: mkExtendedTx(t, foundHex, 0), idx: 0},
		{tx: mkExtendedTx(t, missingHex, 0), idx: 1},
	}

	found := *levelTxs[0].tx.Inputs[0].PreviousTxIDChainHash()
	missing := *levelTxs[1].tx.Inputs[0].PreviousTxIDChainHash()

	store := &recordingParentStore{
		MockUtxostore: &utxostore.MockUtxostore{},
		resolve:       map[chainhash.Hash]*meta.Data{found: {BlockHeights: []uint32{100}}},
		itemErrs:      map[chainhash.Hash]error{missing: errors.NewTxNotFoundError("%v not found", missing)},
	}
	server.utxoStore = store

	prefetched, err := server.prefetchLevelParents(ctx, levelTxs)
	require.NoError(t, err, "a not-found parent must not abort the bulk read")
	require.NotNil(t, prefetched[found])
	require.Nil(t, prefetched[missing], "not-found parent must be omitted so the validator falls back to Get")
}

// Test_prefetchLevelParents_AbortsOnRealItemError proves the halt-on-DB-error
// contract: a per-item error that is NOT a not-found (a store timeout,
// external-store read failure, decode error — all reported on item.Err while
// BatchDecorate returns nil) must abort the level, never be silently downgraded
// to a fallback Get that masks the partial DB failure.
func Test_prefetchLevelParents_AbortsOnRealItemError(t *testing.T) {
	ctx := context.Background()

	server, cleanup := setupTestServer(t)
	defer cleanup()

	failingHex := "0000000000000000000000000000000000000000000000000000000000000003"

	levelTxs := []missingTx{
		{tx: mkExtendedTx(t, failingHex, 0), idx: 0},
	}

	failing := *levelTxs[0].tx.Inputs[0].PreviousTxIDChainHash()

	store := &recordingParentStore{
		MockUtxostore: &utxostore.MockUtxostore{},
		itemErrs:      map[chainhash.Hash]error{failing: errors.NewStorageError("aerospike timeout")},
	}
	server.utxoStore = store

	_, err := server.prefetchLevelParents(ctx, levelTxs)
	require.Error(t, err, "a real per-item store error must abort the level, not be swallowed")
	require.True(t, errors.Is(err, errors.ErrStorageError), "abort must surface as a storage error")
}
