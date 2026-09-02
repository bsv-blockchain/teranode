package blockvalidation

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	testutil "github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// newCommitBlockHarness builds the minimal in-memory harness for TestCommitBlock_AddsBlockAndSetsExists.
//
// It creates:
//   - a sqlitememory blockchain store (genesis auto-inserted)
//   - a LocalClient wrapping that store (provides real GetBlockExists/GetBlockIsMined)
//   - a BlockValidation wired to the LocalClient
//   - a mined block chained from genesis with a valid proof-of-work and block.ID pre-assigned
//
// The block has no subtrees so unlockSubtreeTransactionsIfNeeded is a no-op and
// updateSubtreesDAH only calls SetBlockSubtreesSet, which succeeds once AddBlock
// has committed the block.
func newCommitBlockHarness(t *testing.T) (*BlockValidation, *model.Block, context.Context) {
	t.Helper()

	ctx := context.Background()
	logger := ulogger.TestLogger{}

	tSettings := testutil.CreateBaseTestSettings(t)

	// Build a real sqlitememory blockchain store; genesis is auto-inserted on New.
	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	// Minimal stores — commitBlock does not use subtreeStore or utxoStore directly
	// (the subtree unlock is a no-op when block.SubtreeSlices is empty).
	subtreeStore := blobmemory.New()
	mockUTXO := &utxo.MockUtxostore{}

	bv := NewBlockValidation(ctx, logger, tSettings, blockchainClient, subtreeStore, nil, mockUTXO, nil, nil)

	// Build a coinbase paying block-1 subsidy; the private key / address are
	// throwaway since we never validate scripts here.
	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	coinbaseTx := bt.NewTx()
	require.NoError(t, coinbaseTx.From(
		"0000000000000000000000000000000000000000000000000000000000000000",
		0xffffffff, "", 0,
	))
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(
		[]byte{0x03, 0x01, 0x00, 0x00, '/', 'T', 'e', 's', 't'},
	)
	require.NoError(t, coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000))

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	// Block 1 chains from the genesis block already in the sqlitememory store.
	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: coinbaseTx.TxIDChainHash(), // single coinbase is its own merkle root
		Timestamp:      uint32(time.Now().Unix()),  //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}
	// Grind for a valid proof-of-work; the regression-net minimum difficulty
	// (207fffff) converges in at most a few thousand iterations.
	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}
		blockHeader.Nonce++
		if blockHeader.Nonce > 2_000_000 {
			t.Fatal("failed to find a valid nonce within iteration budget")
		}
	}

	const blockHeight = uint32(1)

	block, err := model.NewBlock(
		blockHeader,
		coinbaseTx,
		nil,                       // no subtrees — unlock is a no-op
		1,                         // transaction count (coinbase only)
		uint64(coinbaseTx.Size()), //nolint:gosec
		blockHeight,
		0, // ID will be assigned below
	)
	require.NoError(t, err)

	// Assign the block ID that processBlockSubtrees normally provides before
	// calling commitBlock.
	assignedID, err := blockchainClient.AssignBlockID(ctx, block.Hash())
	require.NoError(t, err)
	id32, err := blockIDToUint32(assignedID, block.Hash().String())
	require.NoError(t, err)
	block.ID = id32

	return bv, block, ctx
}

// TestCommitBlock_AddsBlockAndSetsExists verifies the extracted commit tail:
// AddBlock stores the block with MinedSet+SubtreesSet+ID, and SetBlockExists
// records it. Uses the same in-memory harness as the quick-validate tests.
func TestCommitBlock_AddsBlockAndSetsExists(t *testing.T) {
	// Build a *BlockValidation and a valid below-checkpoint block (no subtrees)
	// with block.ID already assigned, mirroring the state processBlockSubtrees
	// leaves before commitBlock runs.
	u, block, ctx := newCommitBlockHarness(t)

	require.NoError(t, u.commitBlock(ctx, block, "test-peer", "commitBlock"))

	// AddBlock committed it: GetBlockExists / blockchainClient reports it present + mined
	exists, err := u.blockchainClient.GetBlockExists(ctx, block.Hash())
	require.NoError(t, err)
	require.True(t, exists)

	mined, err := u.blockchainClient.GetBlockIsMined(ctx, block.Hash())
	require.NoError(t, err)
	require.True(t, mined, "commitBlock must AddBlock with MinedSet=true")
}
