package blockvalidation

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// Same UTXO commitment ambiguity as the validator-side regression test
// (services/validator/utxo_commitment_ambiguity_test.go), reached through the
// other ingress: subtree data served by a block-announcing peer arrives already
// in extended format, so the peer — not the local store — chooses the
// previous-output script and value used for script execution and value
// conservation. See GHSA-v76m-6vc7-g7c7.
const (
	ambiguousRealScriptHex = "\x51\xfe\xff\xff\xff" // OP_TRUE, then invalid opcode 0xfe: unspendable
	ambiguousRealSats      = uint64(252)
	ambiguousForgedSats    = uint64(4244635647)
)

// newAmbiguityBlockValidation returns a BlockValidation backed by a real
// sqlitememory UTXO store, plus that store, so tests can seed authoritative
// parents.
func newAmbiguityBlockValidation(t *testing.T, dbName string) (*BlockValidation, *sql.Store, context.Context) {
	t.Helper()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///" + dbName)
	require.NoError(t, err)

	store, err := sql.New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)
	require.NoError(t, store.SetBlockHeight(500))

	return &BlockValidation{
		logger:    logger,
		settings:  tSettings,
		utxoStore: store,
	}, store, ctx
}

// poisonedParent builds and stores a transaction whose output 0 holds the
// genuine, unspendable (script, satoshis) half of the colliding pair.
func poisonedParent(t *testing.T, ctx context.Context, store *sql.Store) *bt.Tx {
	t.Helper()

	parent := buildPoisonedParent(t, ctx, store)

	_, err := store.Create(ctx, parent, 499)
	require.NoError(t, err)

	return parent
}

// buildPoisonedParent builds the same transaction but leaves it out of the UTXO
// store, so a test can force resolution through the in-block parent map. Its own
// grandparent IS stored, so the parent itself still resolves.
func buildPoisonedParent(t *testing.T, ctx context.Context, store *sql.Store) *bt.Tx {
	t.Helper()

	payTo, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)

	// Grandparent exists in the store so the parent itself is resolvable when it
	// travels inside the block (the same-block-parent case below).
	// Seeded txs are extended so Create can compute their fees; this release
	// branch has no WithSkipExtendedInputs.
	grandparent := bt.NewTx()
	gpIn := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     0xffffffff,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x00}),
		PreviousTxScript:   payTo,
		PreviousTxSatoshis: 100_000,
	}
	require.NoError(t, gpIn.PreviousTxIDAdd(&chainhash.Hash{9}))
	grandparent.Inputs = append(grandparent.Inputs, gpIn)
	grandparent.Outputs = append(grandparent.Outputs, &bt.Output{Satoshis: 1000, LockingScript: payTo})
	_, err = store.Create(ctx, grandparent, 498)
	require.NoError(t, err)

	parent := bt.NewTx()
	in := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     0xffffffff,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x00}),
		PreviousTxScript:   payTo,
		PreviousTxSatoshis: 1000,
	}
	require.NoError(t, in.PreviousTxIDAdd(grandparent.TxIDChainHash()))
	parent.Inputs = append(parent.Inputs, in)

	realScript := bscript.Script([]byte(ambiguousRealScriptHex))
	parent.Outputs = append(parent.Outputs, &bt.Output{Satoshis: ambiguousRealSats, LockingScript: &realScript})

	return parent
}

// forgedSpendOf builds an extended transaction spending parent:0 but claiming a
// spendable OP_TRUE previous script and an inflated value — the pair that
// collides with the parent's real commitment.
func forgedSpendOf(t *testing.T, parent *bt.Tx) *bt.Tx {
	t.Helper()

	forgedScript := bscript.Script([]byte{0x51})
	tx := bt.NewTx()
	in := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     0xffffffff,
		UnlockingScript:    bscript.NewFromBytes([]byte{}),
		PreviousTxScript:   &forgedScript,
		PreviousTxSatoshis: ambiguousForgedSats,
	}
	require.NoError(t, in.PreviousTxIDAdd(parent.TxIDChainHash()))
	tx.Inputs = append(tx.Inputs, in)
	tx.SetExtended(true)

	payTo, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)
	tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: ambiguousForgedSats - 1, LockingScript: payTo})

	require.True(t, tx.IsExtended(), "fixture must arrive extended, as peer subtree data does")

	return tx
}

// TestExtendBatch_OverwritesPeerSuppliedPreviousOutputs is the GHSA-v76m-6vc7-g7c7
// regression test for the block-validation ingress leg.
//
// Fails before the fix: extendBatch skipped any tx reporting IsExtended(), so the
// peer's forged previous-output script and value survived into script validation
// and value conservation.
func TestExtendBatch_OverwritesPeerSuppliedPreviousOutputs(t *testing.T) {
	bv, store, ctx := newAmbiguityBlockValidation(t, "bv_ambiguity_store_parent")

	parent := poisonedParent(t, ctx, store)
	forged := forgedSpendOf(t, parent)

	block := testhelpers.CreateTestBlocks(t, 1)[0]
	batch := &SubtreeProcessingBatch{
		subtreeData: []*subtreepkg.Data{{Txs: []*bt.Tx{forged}}},
		batchStart:  0,
		batchEnd:    1,
		txRanges:    make([][2]int, 1),
	}

	require.NoError(t, bv.extendBatch(ctx, block, batch, map[chainhash.Hash]*bt.Tx{}))

	require.Equal(t, ambiguousRealSats, forged.Inputs[0].PreviousTxSatoshis,
		"previous-output value must come from the UTXO store, not the announcing peer")
	require.Equal(t, []byte(ambiguousRealScriptHex), []byte(*forged.Inputs[0].PreviousTxScript),
		"previous-output script must come from the UTXO store, not the announcing peer")
}

// TestExtendBatch_OverwritesPeerSuppliedSameBlockParent covers the same forgery
// where the parent is carried in the block itself rather than the UTXO store.
// The in-block parent's outputs are committed to by its txid, so they are
// authoritative and must win over the peer's claim.
func TestExtendBatch_OverwritesPeerSuppliedSameBlockParent(t *testing.T) {
	bv, store, ctx := newAmbiguityBlockValidation(t, "bv_ambiguity_inblock_parent")

	// Deliberately NOT stored: the in-block parent map must be the only way to
	// resolve this parent, or the test would pass on the store path and prove
	// nothing about the in-block one it is named for.
	parent := buildPoisonedParent(t, ctx, store)
	forged := forgedSpendOf(t, parent)

	block := testhelpers.CreateTestBlocks(t, 1)[0]
	// Parent precedes child in the subtree, as topological block ordering requires.
	batch := &SubtreeProcessingBatch{
		subtreeData: []*subtreepkg.Data{{Txs: []*bt.Tx{parent, forged}}},
		batchStart:  0,
		batchEnd:    1,
		txRanges:    make([][2]int, 1),
	}

	require.NoError(t, bv.extendBatch(ctx, block, batch, map[chainhash.Hash]*bt.Tx{}))

	require.Equal(t, ambiguousRealSats, forged.Inputs[0].PreviousTxSatoshis,
		"previous-output value must come from the in-block parent, not the announcing peer")
	require.Equal(t, []byte(ambiguousRealScriptHex), []byte(*forged.Inputs[0].PreviousTxScript),
		"previous-output script must come from the in-block parent, not the announcing peer")
}

// TestProcessSubtreeBatch_OverwritesPeerSuppliedPreviousOutputs protects the
// sequential path selected with SubtreeBatchPrefetchDepth=0. Serializing the
// forged transaction exercises the same extended bytes read from peer subtrees.
func TestProcessSubtreeBatch_OverwritesPeerSuppliedPreviousOutputs(t *testing.T) {
	for _, sameBlock := range []bool{false, true} {
		name := "stored_parent"
		if sameBlock {
			name = "same_block_parent"
		}
		t.Run(name, func(t *testing.T) {
			bv, store, ctx := newAmbiguityBlockValidation(t, "bv_sequential_"+name)
			t.Cleanup(func() { require.NoError(t, store.Close(ctx)) })
			blobs := memory.New()
			t.Cleanup(func() { require.NoError(t, blobs.Close(ctx)) })
			bv.subtreeStore = blobs
			parent := buildPoisonedParent(t, ctx, store)
			if !sameBlock {
				_, err := store.Create(ctx, parent, 499)
				require.NoError(t, err)
			}
			forged := forgedSpendOf(t, parent)
			txs := []*bt.Tx{forged}
			if sameBlock {
				txs = []*bt.Tx{parent, forged}
			}
			tree, err := subtreepkg.NewIncompleteTreeByLeafCount(len(txs) + 1)
			require.NoError(t, err)
			require.NoError(t, tree.AddCoinbaseNode())
			for _, tx := range txs {
				require.NoError(t, tree.AddNode(*tx.TxIDChainHash(), 1, 1))
			}
			data := subtreepkg.NewSubtreeData(tree)
			for i, tx := range txs {
				require.NoError(t, data.AddTx(tx, i+1))
			}
			treeBytes, err := tree.Serialize()
			require.NoError(t, err)
			dataBytes, err := data.Serialize()
			require.NoError(t, err)
			require.NoError(t, blobs.Set(ctx, tree.RootHash()[:], fileformat.FileTypeSubtreeToCheck, treeBytes))
			require.NoError(t, blobs.Set(ctx, tree.RootHash()[:], fileformat.FileTypeSubtreeData, dataBytes))
			block := testhelpers.CreateTestBlocks(t, 1)[0]
			block.Subtrees = []*chainhash.Hash{tree.RootHash()}
			batch, err := bv.processSubtreeBatch(ctx, block, 0, 1, map[chainhash.Hash]*bt.Tx{})
			require.NoError(t, err)
			defer batch.Close()
			require.Len(t, batch.batchTxs, len(txs))
			resolved := batch.batchTxs[len(txs)-1]
			require.Equal(t, ambiguousRealSats, resolved.Inputs[0].PreviousTxSatoshis)
			require.NotNil(t, resolved.Inputs[0].PreviousTxScript)
			require.Equal(t, []byte(ambiguousRealScriptHex), []byte(*resolved.Inputs[0].PreviousTxScript))
		})
	}
}

// A missing output entry must fail closed just like an out-of-range index.
func TestExtendTxFromSameBlockParents_NilOutput(t *testing.T) {
	parent := bt.NewTx()
	parent.Outputs = []*bt.Output{{Satoshis: 252, LockingScript: bscript.NewFromBytes([]byte{0x51})}}
	child := forgedSpendOf(t, parent)
	hash := *parent.TxIDChainHash()
	parent.Outputs[0] = nil
	require.NotPanics(t, func() {
		_, err := extendTxFromSameBlockParents(child, map[chainhash.Hash]*bt.Tx{hash: parent})
		require.Error(t, err)
		require.Contains(t, err.Error(), "non-existent output")
	})
}
