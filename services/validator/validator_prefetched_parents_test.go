package validator

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Test_getUtxoBlockHeightsAndExtendTx_Prefetched is the Phase-1 parity test:
// when the per-level bulk reader has already fetched a tx's parents, supplying
// them via PrefetchedParents must yield IDENTICAL block heights to the per-parent
// store path AND perform ZERO `Get` calls against the store. This is the core
// correctness contract — the bulk read is a pure read-source swap, not a change
// in what the validator computes (bitcoin-expert invariants #2/#4: the data and
// the unconfirmed-parent sentinel logic are unchanged; only the source differs).
func Test_getUtxoBlockHeightsAndExtendTx_Prefetched(t *testing.T) {
	ctx := context.Background()

	// Same extended 3-parent tx used by Test_getUtxoBlockHeights.
	tx, err := bt.NewTxFromString("010000000000000000ef03fe1a25c8774c1e827f9ebdae731fe609ff159d6f7c15094e1d467a99a01e03100000000002012affffffffa086010000000000018253a080075d834402e916390940782236b29d23db6f52dfc940a12b3eff99159c0000000000ffffffffa086010000000000100f5468616e6b7320456c69676975732161e4ed95239756bbb98d11dcf973146be0c17cc1cc94340deb8bc4d44cd88e92000000000a516352676a675168948cffffffff40548900000000000763516751676a680220aa4400000000001976a9149bc0bbdd3024da4d0c38ed1aecf5c68dd1d3fa1288ac20aa4400000000001976a914169ff4804fd6596deb974f360c21584aa1e19c9788ac00000000")
	require.NoError(t, err)

	parent0, err := chainhash.NewHashFromStr("10031ea0997a461d4e09157c6f9d15ff09e61f73aebd9e7f821e4c77c8251afe")
	require.NoError(t, err)
	parent1, err := chainhash.NewHashFromStr("9c1599ff3e2ba140c9df526fdb239db236227840093916e90244835d0780a053")
	require.NoError(t, err)
	parent2, err := chainhash.NewHashFromStr("928ed84cd4c48beb0d3494ccc17cc1e06b1473f9dc118db9bb56972395ede461")
	require.NoError(t, err)

	// Identical data to the "mined parent txs" subtest of Test_getUtxoBlockHeights:
	// parent1 has empty BlockHeights (unconfirmed → sentinel).
	prefetched := map[chainhash.Hash]*meta.Data{
		*parent0: {BlockHeights: []uint32{125, 126}},
		*parent1: {BlockHeights: []uint32{}},
		*parent2: {BlockHeights: []uint32{768, 769}},
	}

	mockUtxoStore := &utxostore.MockUtxostore{}
	v := &Validator{settings: settings.NewSettings(), utxoStore: mockUtxoStore}

	utxoHeights, err := v.getUtxoBlockHeightsAndExtendTx(ctx, tx, tx.TxID(), prefetched)
	require.NoError(t, err)

	require.Equal(t, []uint32{125, unconfirmedParentHeight, 768}, utxoHeights,
		"prefetched heights must match the per-parent store path exactly")

	mockUtxoStore.AssertNotCalled(t, "Get", mock.Anything, mock.Anything, mock.Anything)
}

// Test_getUtxoBlockHeightsAndExtendTx_PartialPrefetchFallsBackToStore proves the
// safety invariant: a parent absent from PrefetchedParents falls back to a store
// Get, so a partial prefetch never reduces correctness — the heights are identical
// to the all-store path, and only the missing parent is read.
func Test_getUtxoBlockHeightsAndExtendTx_PartialPrefetchFallsBackToStore(t *testing.T) {
	ctx := context.Background()

	tx, err := bt.NewTxFromString("010000000000000000ef03fe1a25c8774c1e827f9ebdae731fe609ff159d6f7c15094e1d467a99a01e03100000000002012affffffffa086010000000000018253a080075d834402e916390940782236b29d23db6f52dfc940a12b3eff99159c0000000000ffffffffa086010000000000100f5468616e6b7320456c69676975732161e4ed95239756bbb98d11dcf973146be0c17cc1cc94340deb8bc4d44cd88e92000000000a516352676a675168948cffffffff40548900000000000763516751676a680220aa4400000000001976a9149bc0bbdd3024da4d0c38ed1aecf5c68dd1d3fa1288ac20aa4400000000001976a914169ff4804fd6596deb974f360c21584aa1e19c9788ac00000000")
	require.NoError(t, err)

	parent0, err := chainhash.NewHashFromStr("10031ea0997a461d4e09157c6f9d15ff09e61f73aebd9e7f821e4c77c8251afe")
	require.NoError(t, err)
	parent1, err := chainhash.NewHashFromStr("9c1599ff3e2ba140c9df526fdb239db236227840093916e90244835d0780a053")
	require.NoError(t, err)
	parent2, err := chainhash.NewHashFromStr("928ed84cd4c48beb0d3494ccc17cc1e06b1473f9dc118db9bb56972395ede461")
	require.NoError(t, err)

	// Prefetch covers only parent0 and parent2; parent1 must fall back to the store.
	prefetched := map[chainhash.Hash]*meta.Data{
		*parent0: {BlockHeights: []uint32{125, 126}},
		*parent2: {BlockHeights: []uint32{768, 769}},
	}

	mockUtxoStore := &utxostore.MockUtxostore{}
	v := &Validator{settings: settings.NewSettings(), utxoStore: mockUtxoStore}

	// Only parent1 should ever be read from the store.
	mockUtxoStore.On("Get", mock.Anything, mock.MatchedBy(func(hash *chainhash.Hash) bool {
		return hash.IsEqual(parent1)
	}), mock.Anything).Return(&meta.Data{BlockHeights: []uint32{}}, nil).Once()

	utxoHeights, err := v.getUtxoBlockHeightsAndExtendTx(ctx, tx, tx.TxID(), prefetched)
	require.NoError(t, err)

	require.Equal(t, []uint32{125, unconfirmedParentHeight, 768}, utxoHeights)
	// parent0 and parent2 came from the prefetch; only parent1 hit the store.
	mockUtxoStore.AssertNumberOfCalls(t, "Get", 1)
}

// Test_getUtxoBlockHeightAndExtendForParentTx_InputIndexOutOfBounds guards the
// bounds check: an input index >= len(tx.Inputs) must return an out-of-bounds
// error rather than panic. The check is hoisted to the top of the function so
// it fires before the utxoHeights[idx] height-write loops (utxoHeights is sized
// to len(tx.Inputs) by the caller, so an out-of-range idx would otherwise panic
// there, before the extend path).
func Test_getUtxoBlockHeightAndExtendForParentTx_InputIndexOutOfBounds(t *testing.T) {
	ctx := context.Background()

	// Child tx with a single input, so len(tx.Inputs) == 1.
	childTx := &bt.Tx{Inputs: []*bt.Input{{}}}

	// utxoHeights sized exactly as the real caller does
	// (make([]uint32, len(tx.Inputs))), so the test exercises the true call
	// shape rather than an artificially oversized slice.
	utxoHeights := make([]uint32, len(childTx.Inputs))

	// Parent supplied via prefetch (Tx non-nil so the extend path would be
	// reached) with a recorded block height, so no store Get is needed.
	parentHash := chainhash.Hash{}
	prefetched := map[chainhash.Hash]*meta.Data{
		parentHash: {BlockHeights: []uint32{100}, Tx: &bt.Tx{Outputs: []*bt.Output{{}}}},
	}

	v := &Validator{}

	// idx == len(childTx.Inputs) would panic in the height-write loop
	// (utxoHeights[idx]) without the up-front guard.
	err := v.getUtxoBlockHeightAndExtendForParentTx(ctx, parentHash, []int{1}, utxoHeights, childTx, true, prefetched)
	require.Error(t, err)
	require.Contains(t, err.Error(), "out of bounds")
}

// Test_getUtxoBlockHeightAndExtendForParentTx_VoutOutOfRange guards the extend
// path against an out-of-range PreviousTxOutIndex. The vout comes from the
// (untrusted) child transaction; a raw tx that references a real parent but a
// vout beyond that parent's output count must be rejected with a clean error
// rather than panicking on txMeta.Tx.Outputs[vout] and crashing the validator.
func Test_getUtxoBlockHeightAndExtendForParentTx_VoutOutOfRange(t *testing.T) {
	ctx := context.Background()

	// Child tx with a single, validly-indexed input (idx 0) whose
	// PreviousTxOutIndex points past the parent's outputs.
	childTx := &bt.Tx{Inputs: []*bt.Input{{PreviousTxOutIndex: 99}}}
	utxoHeights := make([]uint32, len(childTx.Inputs))

	// Parent exists and is confirmed, but has only 2 outputs (vouts 0 and 1).
	parentHash := chainhash.Hash{}
	prefetched := map[chainhash.Hash]*meta.Data{
		parentHash: {BlockHeights: []uint32{100}, Tx: &bt.Tx{Outputs: []*bt.Output{{}, {}}}},
	}

	v := &Validator{}

	err := v.getUtxoBlockHeightAndExtendForParentTx(ctx, parentHash, []int{0}, utxoHeights, childTx, true, prefetched)
	require.Error(t, err)
	require.Contains(t, err.Error(), "has no output for index")
}
