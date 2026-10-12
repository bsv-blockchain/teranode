package blockvalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// The quick-validation route is a hand-maintained mirror of model.Block.Valid.
// These cover three rules that exist on the Valid side but had no counterpart
// here, each reachable with a body a peer supplies.

// TestValidateSubtrees_DuplicateTransactionRejected — CVE-2012-2459.
//
// The merkle root CANNOT detect a duplicated trailing transaction: the
// duplicate-last-node-when-odd rule makes the mutated body produce the SAME root,
// and therefore the same block hash, as the honest block. So a body that binds to
// a checkpoint-certified header can still carry a repeated transaction, reusing the
// honest block's proof of work. model.CheckSubtreeSlicesForDuplicateTxs exists
// precisely for slice-holding paths that skip Valid's pooled check, and documents
// itself as mandatory before UTXOs are created — but nothing called it.
func TestValidateSubtrees_DuplicateTransactionRejected(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	dup := chainhash.HashH([]byte("duplicated tx"))

	st, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())
	require.NoError(t, st.AddNode(chainhash.HashH([]byte("tx a")), 1, 0))
	require.NoError(t, st.AddNode(dup, 1, 0))
	require.NoError(t, st.AddNode(dup, 1, 0)) // the CVE-2012-2459 mutation

	block := testhelpers.CreateTestBlocks(t, 1)[0]
	block.Subtrees = []*chainhash.Hash{st.RootHash()}
	block.SubtreeSlices = []*subtreepkg.Subtree{st}

	// Bind the body: the header commits to exactly these subtree roots, so the
	// merkle check cannot be what rejects it.
	root, err := st.RootHashWithReplaceRootNode(block.CoinbaseTx.TxIDChainHash(), 0, uint64(block.CoinbaseTx.Size()))
	require.NoError(t, err)
	block.Header.HashMerkleRoot = root

	_, err = suite.Server.blockValidation.validateSubtrees(context.Background(), block, 1)

	require.Error(t, err, "a duplicated transaction must be rejected even though the merkle root matches")
	require.True(t, errors.IsBlockCorrupt(err),
		"a peer-supplied body defect is corrupt, not invalid — the honest hash must not be condemned: got %v", err)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid))
}

// TestValidateSubtrees_NonCoinbaseBodyRejected — the subtree-carrying shape had no
// block.CoinbaseTx.IsCoinbase() check anywhere on this route. The shape check in
// readSubtree inspects subtreeData.Txs[0], a different object.
//
// Checked after CheckMerkleRoot, where the body is bound, so BlockInvalid is the
// correct class: the header commits to this transaction.
func TestValidateSubtrees_NonCoinbaseBodyRejected(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	spend := bt.NewTx()
	require.NoError(t, spend.From("6a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b", 0, "76a914000000000000000000000000000000000000000088ac", 1000))
	require.NoError(t, spend.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 900))
	spend.Inputs[0].UnlockingScript = bscript.NewFromBytes(make([]byte, 16))
	require.False(t, spend.IsCoinbase())

	st, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())
	require.NoError(t, st.AddNode(chainhash.HashH([]byte("tx a")), 1, 0))

	block := testhelpers.CreateTestBlocks(t, 1)[0]
	block.CoinbaseTx = spend
	block.Subtrees = []*chainhash.Hash{st.RootHash()}
	block.SubtreeSlices = []*subtreepkg.Subtree{st}

	root, err := st.RootHashWithReplaceRootNode(spend.TxIDChainHash(), 0, uint64(spend.Size()))
	require.NoError(t, err)
	block.Header.HashMerkleRoot = root

	_, err = suite.Server.blockValidation.validateSubtrees(context.Background(), block, 1)

	require.Error(t, err, "a subtree-carrying body whose coinbase is an ordinary spend must be rejected")
	require.True(t, errors.Is(err, errors.ErrBlockInvalid),
		"the body is bound, so this is genuine invalidity: got %v", err)
}

// TestQuickValidateBlock_LooseCoinbaseRejectedOnRoute drives the loose-but-not-strict
// shape through the route itself, not just the predicate. Both existing route tests use
// an ordinary spend, which go-bt's test and the strict one both reject — so swapping
// IsConsensusCoinbase back to CoinbaseTx.IsCoinbase() left the package green. This is the
// test that fails if the route stops using the consensus predicate.
func TestQuickValidateBlock_LooseCoinbaseRejectedOnRoute(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	suite.MockBlockchain.On("AssignBlockID", mock.Anything, mock.Anything).Return(uint64(1), nil).Maybe()
	suite.MockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	suite.MockBlockchain.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).Return(nil).Maybe()

	// Null prevout hash, index 0, sequence 0xFFFFFFFF: go-bt says coinbase, consensus does not.
	loose := bt.NewTx()
	require.NoError(t, loose.From("0000000000000000000000000000000000000000000000000000000000000000", 0, "", 0))
	loose.Inputs[0].SequenceNumber = 0xFFFFFFFF
	loose.Inputs[0].UnlockingScript = bscript.NewFromBytes(make([]byte, 16))
	require.NoError(t, loose.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1))
	require.True(t, loose.IsCoinbase(), "precondition: go-bt accepts this shape")

	block := testhelpers.CreateTestBlocks(t, 1)[0]
	block.CoinbaseTx = loose
	block.Header.HashMerkleRoot = loose.TxIDChainHash()
	block.Header.Nonce = 0
	testhelpers.MineHeader(block.Header)

	err := suite.Server.blockValidation.quickValidateBlock(suite.Ctx, block, "test", "")

	require.Error(t, err, "the route must apply the consensus predicate, not go-bt's looser one")
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "the body is bound: got %v", err)
	suite.MockBlockchain.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

// TestValidateSubtrees_LooseCoinbaseRejected pins the subtree-shape site to the
// consensus predicate. TestValidateSubtrees_NonCoinbaseBodyRejected cannot: its
// fixture is an ordinary spend, which go-bt's predicate and the strict one both
// reject, so that site could be swapped back to CoinbaseTx.IsCoinbase() with the
// suite still green. This fixture separates them.
func TestValidateSubtrees_LooseCoinbaseRejected(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	loose := bt.NewTx()
	require.NoError(t, loose.From("0000000000000000000000000000000000000000000000000000000000000000", 0, "", 0))
	loose.Inputs[0].SequenceNumber = 0xFFFFFFFF
	loose.Inputs[0].UnlockingScript = bscript.NewFromBytes(make([]byte, 16))
	require.NoError(t, loose.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1))
	require.True(t, loose.IsCoinbase(), "precondition: go-bt accepts this shape")

	st, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())
	require.NoError(t, st.AddNode(chainhash.HashH([]byte("tx a")), 1, 0))

	block := testhelpers.CreateTestBlocks(t, 1)[0]
	block.CoinbaseTx = loose
	block.Subtrees = []*chainhash.Hash{st.RootHash()}
	block.SubtreeSlices = []*subtreepkg.Subtree{st}

	root, err := st.RootHashWithReplaceRootNode(loose.TxIDChainHash(), 0, uint64(loose.Size()))
	require.NoError(t, err)
	block.Header.HashMerkleRoot = root

	_, err = suite.Server.blockValidation.validateSubtrees(context.Background(), block, 1)

	require.Error(t, err, "the subtree-shape site must apply the consensus predicate")
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "the body is bound: got %v", err)
}

// TestValidateSubtrees_FirstNodeMustBeCoinbasePlaceholder covers the precondition the
// duplicate scan depends on: it skips slot [0][0] only when that slot holds the
// placeholder. A first subtree whose node 0 is a real txid otherwise passes the merkle
// check with the body's true first transaction silently substituted. Valid enforces this
// as its step 7.
func TestValidateSubtrees_FirstNodeMustBeCoinbasePlaceholder(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	st, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, st.AddNode(chainhash.HashH([]byte("a real txid, not the placeholder")), 1, 0))
	require.NoError(t, st.AddNode(chainhash.HashH([]byte("tx b")), 1, 0))

	block := testhelpers.CreateTestBlocks(t, 1)[0]
	block.Subtrees = []*chainhash.Hash{st.RootHash()}
	block.SubtreeSlices = []*subtreepkg.Subtree{st}

	root, err := st.RootHashWithReplaceRootNode(block.CoinbaseTx.TxIDChainHash(), 0, uint64(block.CoinbaseTx.Size()))
	require.NoError(t, err)
	block.Header.HashMerkleRoot = root

	_, err = suite.Server.blockValidation.validateSubtrees(context.Background(), block, 1)

	require.Error(t, err, "a first subtree whose node 0 is not the coinbase placeholder must be rejected")
	require.True(t, errors.IsBlockCorrupt(err), "a peer-supplied body defect is corrupt, not invalid: got %v", err)
}

// TestIsConsensusCoinbase_RejectsNonNullPrevoutIndex — go-bt's IsCoinbase is a
// disjunction: a null prevout HASH plus EITHER a 0xFFFFFFFF prevout index OR a
// 0xFFFFFFFF sequence number. Consensus (COutPoint::IsNull) requires the null hash
// AND the 0xFFFFFFFF index; the sequence number says nothing about coinbase-ness.
//
// So a transaction with prevout (0x00..00, 0) and sequence 0xFFFFFFFF is accepted
// by go-bt and rejected by svnode — a chain-split shape if it decides a consensus
// verdict.
func TestIsConsensusCoinbase_RejectsNonNullPrevoutIndex(t *testing.T) {
	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000000", 0, "", 0))
	tx.Inputs[0].SequenceNumber = 0xFFFFFFFF
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes(make([]byte, 16))

	require.True(t, tx.IsCoinbase(), "precondition: go-bt accepts this shape")
	require.False(t, model.IsConsensusCoinbase(tx),
		"prevout index 0 is not a null outpoint; consensus requires 0xFFFFFFFF")

	// The genuine shape must still be accepted.
	real := bt.NewTx()
	require.NoError(t, real.From("0000000000000000000000000000000000000000000000000000000000000000", 0xFFFFFFFF, "", 0))
	real.Inputs[0].UnlockingScript = bscript.NewFromBytes(make([]byte, 16))
	require.True(t, model.IsConsensusCoinbase(real))
}

// TestQuickValidateBlock_SubtreeInvalidVerdictNotShadowed drives a subtree-carrying
// body all the way through quickValidateBlock so validateSubtrees' verdict passes
// through its caller.
//
// teranode's errors.Is walks the cause chain, so wrapping an invalid verdict in
// ErrProcessing does not hide it — it makes the error match BOTH, and routing then
// depends on which predicate a call site happens to test first. The corrupt verdict is
// returned unwrapped for exactly that reason; the invalid one must be too. Asserting
// only Is(err, ErrBlockInvalid) cannot see the difference, so this pins
// Is(err, ErrProcessing) being false.
func TestQuickValidateBlock_SubtreeInvalidVerdictNotShadowed(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	suite.MockBlockchain.On("AssignBlockID", mock.Anything, mock.Anything).Return(uint64(1), nil).Maybe()
	suite.MockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	suite.MockBlockchain.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).Return(nil).Maybe()

	txs := transactions.CreateTestTransactionChainWithCount(t, 4)
	regularTxs := txs[1:]

	// A coinbase go-bt accepts and consensus does not, so the body-derived coinbase-shape
	// check is what rejects the block. Since bitcoin-sv/teranode#4838 that check runs in the
	// whole-block binding pass, i.e. BEFORE the UTXO batches rather than after them, which is
	// what the no-mutation assertions at the end of this test now pin.
	loose := bt.NewTx()
	require.NoError(t, loose.From("0000000000000000000000000000000000000000000000000000000000000000", 0, "", 0))
	loose.Inputs[0].SequenceNumber = 0xFFFFFFFF
	loose.Inputs[0].UnlockingScript = bscript.NewFromBytes(make([]byte, 16))
	require.NoError(t, loose.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1))
	require.True(t, loose.IsCoinbase(), "precondition: go-bt accepts this shape")

	block := testhelpers.CreateTestBlocks(t, 1)[0]
	block.Height = 100
	block.CoinbaseTx = loose

	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(3)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*regularTxs[0].TxIDChainHash(), 1, 1))
	require.NoError(t, subtree.AddNode(*regularTxs[1].TxIDChainHash(), 2, 2))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, suite.Server.subtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes))

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	require.NoError(t, subtreeData.AddTx(loose, 0))
	require.NoError(t, subtreeData.AddTx(regularTxs[0], 1))
	require.NoError(t, subtreeData.AddTx(regularTxs[1], 2))

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)
	require.NoError(t, suite.Server.subtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes))

	block.Subtrees = []*chainhash.Hash{subtree.RootHash()}
	block.TransactionCount = 3

	block.Header.HashMerkleRoot, err = subtree.RootHashWithReplaceRootNode(loose.TxIDChainHash(), 0, 0)
	require.NoError(t, err)

	// All optional: the block is rejected before any of them, so a mandatory expectation would
	// assert the very calls the binding-before-mutation fix removes. Their absence is asserted
	// positively below instead.
	suite.MockUTXOStore.On("Get", mock.Anything, mock.Anything, mock.Anything).Return((*meta.Data)(nil), errors.NewNotFoundError("not found")).Maybe()
	suite.MockUTXOStore.On("SpendAndCreate", mock.Anything, mock.Anything, uint32(100), matchCreateOnly()).Return(&meta.Data{}, nil, nil).Maybe()
	suite.MockUTXOStore.On("SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, matchSpendOnly()).Return(nil, []*utxo.Spend{}, nil).Maybe()
	suite.MockUTXOStore.On("SetLocked", mock.Anything, mock.Anything, false).Return(nil).Maybe()
	suite.MockValidator.Errors = []error{nil, nil, nil}

	err = suite.Server.blockValidation.quickValidateBlock(suite.Ctx, block, "test", "")

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "got %v", err)
	require.False(t, errors.Is(err, errors.ErrProcessing),
		"the invalid verdict must reach the caller unwrapped, as the corrupt verdict does — a wrapped one matches both classes and routes by check order: got %v", err)
	suite.MockBlockchain.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	suite.MockUTXOStore.AssertNotCalled(t, "SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	suite.MockBlockchain.AssertNotCalled(t, "AssignBlockID", mock.Anything, mock.Anything)
}
