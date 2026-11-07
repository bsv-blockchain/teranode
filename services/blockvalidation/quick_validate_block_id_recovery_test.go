package blockvalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// These tests cover how quick validation recovers a block's id from the block ids
// recorded on its first non-coinbase transaction, when that transaction is also in a
// fork block that is already stored. The fork stamps its own id on the transaction,
// and StoreBlock refuses to write a block under an id another block is stored under,
// so taking the first recorded id blindly leaves the block unable to commit.
//
// Everything is real: a sqlitememory UTXO store and a sqlitememory blockchain store
// behind the ordinary LocalClient, via preBindHarness. Only the lookup-fault test
// wraps the blockchain client, to make GetBlockByID fail.

// storeForkHolding stores a fork block at preBindHeight, a sibling of whatever the
// test validates, and records tx in the UTXO store as mined in it. It returns the
// fork's block id.
func (h *preBindHarness) storeForkHolding(tx *bt.Tx, coinbaseNonce byte) uint32 {
	h.t.Helper()

	coinbase := preBindCoinbase(h.t, coinbaseNonce)
	fork := h.newPreBindBlock(coinbase, nil, coinbase.TxIDChainHash(), 1)

	require.NoError(h.t, h.chain.AddBlock(h.ctx, fork, "peer"))

	_, meta, err := h.chain.GetBlockHeader(h.ctx, fork.Hash())
	require.NoError(h.t, err)
	require.NotZero(h.t, meta.ID, "the fork must have a real block id for the test to mean anything")

	_, err = h.utxoStore.Create(h.ctx, tx, preBindHeight, utxo.WithSkipExtendedInputs(true),
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: meta.ID, BlockHeight: preBindHeight, SubtreeIdx: 0}))
	require.NoError(h.t, err)

	return meta.ID
}

// forkSharingBlock builds a block whose only non-coinbase transaction is child, and
// stores its body.
func (h *preBindHarness) forkSharingBlock(child *bt.Tx, coinbaseNonce byte) *model.Block {
	h.t.Helper()

	coinbase := preBindCoinbase(h.t, coinbaseNonce)
	st := h.oneSubtreeBody(coinbase, child)

	return h.newPreBindBlock(coinbase,
		[]*chainhash.Hash{st.RootHash()},
		composeBlockMerkleRoot(h.t, []chainhash.Hash{coinbaseSubstitutedRoot(h.t, st, coinbase)}),
		2)
}

// sameBlock returns a fresh value for the same block, with no id set, as a retry
// receives it.
func sameBlock(block *model.Block) *model.Block {
	return &model.Block{
		Header:           block.Header,
		CoinbaseTx:       block.CoinbaseTx,
		Height:           block.Height,
		Subtrees:         block.Subtrees,
		TransactionCount: block.TransactionCount,
	}
}

// requireStoredUnder asserts the blockchain store holds block under id.
func (h *preBindHarness) requireStoredUnder(block *model.Block, id uint32) {
	h.t.Helper()

	_, meta, err := h.chain.GetBlockHeader(h.ctx, block.Hash())
	require.NoError(h.t, err, "the block must be stored")
	require.Equal(h.t, id, meta.ID)
}

// TestQuickValidate_ForkHoldsFirstTx_FirstAttemptCommits is the case the shortcut
// broke on a first attempt, with no retry involved: the first non-coinbase
// transaction already carries a stored fork block's id, the block took that id, and
// StoreBlock refused it.
//
// Mutation target: taking recorded BlockIDs[0] without checking which block holds it.
// block.ID then equals the fork's id and commitBlock's AddBlock is refused.
func TestQuickValidate_ForkHoldsFirstTx_FirstAttemptCommits(t *testing.T) {
	h := newPreBindHarness(t, nil)

	parent := h.storeGenuineParent(0x41)
	child := preBindSpendOf(t, parent, 9_000)
	forkID := h.storeForkHolding(child, 0x40)

	block := h.forkSharingBlock(child, 0x42)

	require.NoError(t, h.bv.quickValidateBlock(h.ctx, block, "peer", ""),
		"the block must commit under an id the store accepts")

	require.NotEqual(t, forkID, block.ID, "the block must not take the id of the fork it shares a transaction with")
	require.Equal(t, 1, h.chain.assignCount(), "with only the fork's id recorded, the block takes its id from AssignBlockID")
	h.requireStoredUnder(block, block.ID)
}

// TestQuickValidate_ForkHoldsFirstTx_RetryKeepsOwnID covers the retry: a first
// attempt stamped the transaction and failed before it committed, so the transaction
// carries both the fork's id and this block's. The retry must pick this block's id,
// not the fork's and not a fresh one.
//
// Mutation target: falling back to AssignBlockID whenever the transaction carries
// more than one id. The retry would then reserve again; this pins that it does not.
func TestQuickValidate_ForkHoldsFirstTx_RetryKeepsOwnID(t *testing.T) {
	h := newPreBindHarness(t, nil)

	parent := h.storeGenuineParent(0x51)
	child := preBindSpendOf(t, parent, 9_000)
	forkID := h.storeForkHolding(child, 0x50)

	block := h.forkSharingBlock(child, 0x52)

	// The first attempt runs everything up to the commit, then stops, as an attempt
	// does when AddBlock fails.
	_, err := h.bv.processBlockSubtrees(h.ctx, block, false)
	require.NoError(t, err)

	firstID := block.ID
	require.NotEqual(t, forkID, firstID)

	recorded, err := h.utxoStore.Get(h.ctx, child.TxIDChainHash())
	require.NoError(t, err)
	require.ElementsMatch(t, []uint32{forkID, firstID}, recorded.BlockIDs,
		"precondition: the transaction must carry both ids, or the retry is not ambiguous")

	retry := sameBlock(block)

	require.NoError(t, h.bv.quickValidateBlock(h.ctx, retry, "peer", ""))

	require.Equal(t, firstID, retry.ID, "the retry must keep the id its transactions were stamped with")
	require.Equal(t, 1, h.chain.assignCount(), "the retry must recover the id, not reserve again")
	h.requireStoredUnder(retry, firstID)

	// Once committed, the block's own stored id is recognised by hash.
	again := sameBlock(block)

	recovered, err := h.bv.setQuickBlockID(h.ctx, again, child, "test")
	require.NoError(t, err)
	require.Equal(t, uint64(firstID), recovered)
	require.Equal(t, firstID, again.ID)
}

// failingBlockByIDClient makes GetBlockByID fail with a storage error, which is not
// the same as "no block has this id".
type failingBlockByIDClient struct {
	*countingBlockchainClient
}

func (c *failingBlockByIDClient) GetBlockByID(context.Context, uint64) (*model.Block, error) {
	return nil, errors.NewStorageError("injected GetBlockByID fault")
}

// TestSetQuickBlockID_LookupFault_ReturnsError pins that a failed lookup is not read
// as "no block holds this id". If it were, the fork's id would look free and the
// block would take it, which is the wedge again.
//
// Mutation target: treating every GetBlockByID error as not-found.
func TestSetQuickBlockID_LookupFault_ReturnsError(t *testing.T) {
	h := newPreBindHarness(t, nil)

	parent := h.storeGenuineParent(0x61)
	child := preBindSpendOf(t, parent, 9_000)
	h.storeForkHolding(child, 0x60)

	h.bv.blockchainClient = &failingBlockByIDClient{countingBlockchainClient: h.chain}

	block := h.forkSharingBlock(child, 0x62)

	_, err := h.bv.setQuickBlockID(h.ctx, block, child, "test")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrStorageError), "the lookup fault must surface, got %v", err)
	require.Zero(t, block.ID, "no id may be set when the lookup failed")
	require.Zero(t, h.chain.assignCount(), "AssignBlockID must not be reached")
}
