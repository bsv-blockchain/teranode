package blockassembly

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestReset_MoveForwardGetSubtreesFailure_PreservesMined drives the real
// BlockAssembler.reset path through a fork-shaped fixture (genesis → h1
// branching into main h2A→h3A and side h2B→h3B). After invalidating the
// fork tip the blockchain best becomes h3A and reset's getReorgBlocks
// yields moveBack=[h3B,h2B] / moveForward=[h2A,h3A].
//
// h2A's Subtrees references a hash that is intentionally NOT seeded in
// the blob store, so GetSubtrees inside the moveForwardTxMap loop fails.
// h2B's Subtrees references a real subtree (in the blob store) that
// contains tx_alone.
//
// Pre-fix, the loop silently `continue`d past h2A, leaving
// moveForwardTxMap incomplete. tx_alone (only in moveBack) was then
// classified as net-unmined and MarkTransactionsOnLongestChain wrote
// unmined_since > 0. BlockValidation's setTxMinedStatus background job
// would race to undo that, but in the meantime the tx was visible as
// unmined to mining and RPC consumers.
//
// Post-fix, reset detects moveForwardMapComplete == false and skips the
// moveBack marker entirely. tx_alone stays mined; the next reconcile
// cycle retries the GetSubtrees and the marker runs cleanly when the
// blob store is healthy again.
//
// To A/B verify: revert the moveForwardMapComplete-gated skip in
// BlockAssembler.reset and re-run this test - the final UnminedSince
// assertion fails.
func TestReset_MoveForwardGetSubtreesFailure_PreservesMined(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	ctx := t.Context()

	// Keep waitForBlockMinedSet snappy: failure is warn-and-continue, but the
	// default 45 retries × exponential backoff would add ~10 s for the
	// invalidated h3B branch. The reset path doesn't depend on the wait result
	// for correctness; it only blocks the test.
	items.blockAssembler.settings.BlockValidation.IsParentMinedRetryMaxRetry = 1

	// Mock subtreeProcessor so the dispatcher-side Reset returns immediately
	// without trying to load unmined transactions back into the assembler. The
	// behaviour under test runs BEFORE this call in reset(), so the mock's
	// fidelity is irrelevant to the assertion.
	mockStp := &subtreeprocessor.MockSubtreeProcessor{}
	mockStp.On("WaitForPendingBlocks", mock.Anything).Return(nil)
	mockStp.On("Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(subtreeprocessor.ResetResponse{})
	mockStp.On("GetCurrentBlockHeader").Return(model.GenesisBlockHeader)
	injectMockStp(t, items, mockStp)

	// Bump the UTXO store's "current block height" so MarkTransactionsOnLongestChain
	// writes a non-zero UnminedSince when called with longest=false. At default
	// height 0 the unmined marker is indistinguishable from "still mined" and the
	// test passes vacuously even with the production fix reverted.
	require.NoError(t, items.utxoStore.SetBlockHeight(150))

	// Create tx_alone in the UTXO store and mark it mined. The reset path is
	// the only thing that should be able to flip its UnminedSince here.
	txAlone := bt.NewTx()
	txAlone.LockTime = 0
	input := &bt.Input{
		PreviousTxOutIndex: 0,
		PreviousTxSatoshis: 5_000_000_000,
		SequenceNumber:     0xffffffff,
		UnlockingScript:    bscript.NewFromBytes([]byte{}),
	}
	_ = input.PreviousTxIDAdd(&chainhash.Hash{})
	txAlone.Inputs = []*bt.Input{input}
	txAlone.Outputs = []*bt.Output{{
		Satoshis:      100_000,
		LockingScript: bscript.NewFromBytes([]byte{0x76, 0xa9, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x88, 0xac}),
	}}
	_, err := items.utxoStore.Create(ctx, txAlone, 1)
	require.NoError(t, err)
	txAloneHash := txAlone.TxIDChainHash()
	require.NoError(t, items.utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*txAloneHash}, true))

	before, err := items.utxoStore.Get(ctx, txAloneHash)
	require.NoError(t, err)
	require.Equal(t, uint32(0), before.UnminedSince, "tx_alone must start mined")

	// Build the real subtree that h2B will reference. The subtree wraps
	// tx_alone and is seeded into the blob store, so GetSubtrees succeeds.
	realSubtree, err := subtree.NewTreeByLeafCount(64)
	require.NoError(t, err)
	require.NoError(t, realSubtree.AddCoinbaseNode())
	require.NoError(t, realSubtree.AddSubtreeNode(subtree.Node{Hash: *txAloneHash, Fee: 100, SizeInBytes: 250}))
	realSubtreeBytes, err := realSubtree.Serialize()
	require.NoError(t, err)
	realSubtreeHash := realSubtree.RootHash()
	require.NoError(t, items.blobStore.Set(ctx, realSubtreeHash[:], fileformat.FileTypeSubtree, realSubtreeBytes))
	require.NoError(t, items.blobStore.Set(ctx, realSubtreeHash[:], fileformat.FileTypeSubtreeMeta, []byte{}))

	// h2A references this hash; it is intentionally NOT seeded into the blob
	// store so GetSubtrees fails - the exact production failure mode the fix
	// guards against.
	missingSubtreeHash := chainhash.HashH([]byte("missing-subtree-for-h2A"))

	// Fork shape:
	//   genesis → h1 → h2A → h3A   (main, has missing subtree on h2A)
	//          ↘ h2B → h3B          (fork; BA is here at start)
	require.NoError(t, items.addBlock(ctx, blockHeader1))
	h1 := blockHeader1

	h2A := &model.BlockHeader{Version: 1, HashPrevBlock: h1.Hash(), HashMerkleRoot: &chainhash.Hash{}, Nonce: 22, Bits: *bits}
	h3A := &model.BlockHeader{Version: 1, HashPrevBlock: h2A.Hash(), HashMerkleRoot: &chainhash.Hash{}, Nonce: 23, Bits: *bits}
	h2B := &model.BlockHeader{Version: 1, HashPrevBlock: h1.Hash(), HashMerkleRoot: &chainhash.Hash{}, Nonce: 32, Bits: *bits}
	h3B := &model.BlockHeader{Version: 1, HashPrevBlock: h2B.Hash(), HashMerkleRoot: &chainhash.Hash{}, Nonce: 33, Bits: *bits}

	coinbaseTx, _ := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")

	require.NoError(t, items.blockchainClient.AddBlock(ctx, &model.Block{
		Header: h2A, CoinbaseTx: coinbaseTx, TransactionCount: 1,
		Subtrees: []*chainhash.Hash{&missingSubtreeHash},
	}, ""))
	require.NoError(t, items.blockchainClient.AddBlock(ctx, &model.Block{
		Header: h3A, CoinbaseTx: coinbaseTx, TransactionCount: 1,
		Subtrees: []*chainhash.Hash{},
	}, ""))
	require.NoError(t, items.blockchainClient.AddBlock(ctx, &model.Block{
		Header: h2B, CoinbaseTx: coinbaseTx, TransactionCount: 1,
		Subtrees: []*chainhash.Hash{realSubtreeHash},
	}, ""))
	require.NoError(t, items.blockchainClient.AddBlock(ctx, &model.Block{
		Header: h3B, CoinbaseTx: coinbaseTx, TransactionCount: 1,
		Subtrees: []*chainhash.Hash{},
	}, ""))

	// Park BA on the fork tip; the reset's getReorgBlocks will compute
	// moveBack=[h3B,h2B] / moveForward=[h2A,h3A] against the post-invalidate
	// canonical chain.
	items.blockAssembler.setBestBlockHeader(h3B, 3)

	_, err = items.blockchainClient.InvalidateBlock(ctx, h3B.Hash())
	require.NoError(t, err)

	// Drive the real reset path. The fix runs inside this call.
	require.NoError(t, items.blockAssembler.reset(ctx))

	// Critical assertion: tx_alone must still be mined. The fix forced the
	// reset to skip the moveBack marker because h2A's GetSubtrees failure
	// left moveForwardTxMap incomplete; without that skip the marker would
	// have written UnminedSince > 0 here (pre-fix observable failure).
	after, err := items.utxoStore.Get(ctx, txAloneHash)
	require.NoError(t, err)
	require.Equal(t, uint32(0), after.UnminedSince,
		"tx_alone must stay mined: h2A's GetSubtrees failed, so the moveForward map "+
			"was incomplete and reset must skip the moveBack unmined_since marker. "+
			"Pre-fix, the silently-incomplete map classified tx_alone as net-unmined and "+
			"wrote UnminedSince > 0 here.")
}
