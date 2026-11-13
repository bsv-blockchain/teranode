//go:build bvgaps

package blockvalidation

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestKnownDefect_BlockWithPreassignedID_TransactionsStillTaggedMined proves that a block arriving
// with a caller-supplied block ID skips mined-tagging even off the legacy route and above every
// checkpoint.
//
// Spec: BV-COMMIT-010, BV-COMMIT-005 and BV-INGRESS-013 (tracked as GAP-BV-039).
//
// Scenario: Given a regtest chain with no checkpoints and a block at height 1 whose single
// non-coinbase transaction is untagged, and the block already carries a reserved, non-zero block ID,
// when the block is validated on the non-legacy base URL "test", then its transaction must be tagged
// mined with that block ID. Equivalently, mined_set must not be true while the transaction is
// untagged; the flag may only be set once tagging has run.
//
// Today: Server.ProcessBlock copies any non-zero request.BlockId onto the block with no route or
// height check (Server.go ~1503), and BlockValidation.buildAddBlockOpts then adds EVERY block with a
// non-zero ID using WithMinedSet(true) (BlockValidation.go ~1681). The setMinedChan worker skips
// blocks whose mined_set is already true, so setTxMinedStatus never runs and the block's transaction
// is left without the block ID.
//
// This test drives the BlockValidation layer directly, from the state ProcessBlock leaves behind:
// block.ID set, base URL "test", regtest chain with no checkpoints.
func TestKnownDefect_BlockWithPreassignedID_TransactionsStillTaggedMined(t *testing.T) {
	const (
		blockHeight          = uint32(1)
		blockHeaderVersion   = uint32(4)
		fundingSatoshis      = 50 * 100000000
		childTxSatoshis      = 49 * 100000000
		subtreeLeafCount     = 2
		nonLegacyPeerBaseURL = "test" // the route on which no pre-assigned ID is licensed
	)

	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Precondition that makes this a non-checkpoint block.
	require.Empty(t, tSettings.ChainCfgParams.Checkpoints, "regtest has no checkpoints, so the block is above the highest checkpoint")

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	chain, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	utxoStoreURL, err := url.Parse("sqlitememory:///preassigned_block_id_mined_tagging")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	subtreeStore := blobmemory.New()
	txStore := blobmemory.New()

	subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
	subtreeValidationClient.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Given: a funding transaction already in the UTXO store as a genesis-mined tx, and a child
	// spending it that is the single non-coinbase transaction of the block.
	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	fundingTx := bt.NewTx()
	require.NoError(t, fundingTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	fundingTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'F', 'u', 'n', 'd'})
	require.NoError(t, fundingTx.AddP2PKHOutputFromAddress(address.AddressString, fundingSatoshis))

	childTx := bt.NewTx()
	require.NoError(t, childTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      fundingTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: fundingTx.Outputs[0].LockingScript,
		Satoshis:      fundingTx.Outputs[0].Satoshis,
	}))
	require.NoError(t, childTx.AddP2PKHOutputFromAddress(address.AddressString, childTxSatoshis))
	require.NoError(t, childTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey}))

	_, _, err = utxoStore.SpendAndCreate(ctx, fundingTx, 0, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 0, BlockHeight: 0}), utxostore.WithCreateOnly())
	require.NoError(t, err)

	_, _, err = utxoStore.SpendAndCreate(ctx, childTx, 0, utxostore.WithCreateOnly())
	require.NoError(t, err)

	// A stored subtree [coinbase, child] referenced by the block.
	subtree, err := subtreepkg.NewTreeByLeafCount(subtreeLeafCount)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*childTx.TxIDChainHash(), uint64(childTx.Size()), 0)) //nolint:gosec

	subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)
	require.NoError(t, subtreeMeta.SetTxInpointsFromTx(childTx))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeMetaBytes, err := subtreeMeta.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes))

	// The block at blockHeight on genesis containing [coinbase, child].
	coinbaseTx := coinbaseAtHeight(t, blockHeight)

	subtreeWithCoinbase := subtree.Duplicate()
	subtreeWithCoinbase.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec

	header := minedBIP34Header(t, blockHeaderVersion, tSettings.ChainCfgParams.GenesisHash, subtreeWithCoinbase.RootHash())

	block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{subtree.RootHash()},
		uint64(subtree.Length()), uint64(coinbaseTx.Size()+childTx.Size()), blockHeight, 0) //nolint:gosec
	require.NoError(t, err)

	// What ProcessBlock does for a non-zero request.BlockId: reserve the ID and stamp it on the block.
	reservedID, err := chain.AssignBlockID(ctx, block.Hash())
	require.NoError(t, err)
	preassignedBlockID, err := blockIDToUint32(reservedID, block.Hash().String())
	require.NoError(t, err)
	require.NotZero(t, preassignedBlockID)

	block.ID = preassignedBlockID

	validation := NewBlockValidation(ctx, logger, tSettings, chain, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)
	defer validation.StopCaches()

	childBeforeValidation, err := utxoStore.Get(ctx, childTx.TxIDChainHash())
	require.NoError(t, err)
	require.Empty(t, childBeforeValidation.BlockIDs, "child tx must not be mined before the block is validated")

	// When: the block with the pre-assigned ID is validated on a non-legacy route.
	require.NoError(t, validation.ValidateBlock(ctx, block, nonLegacyPeerBaseURL))

	// Then: if the block was stored with mined_set, its transaction must carry the block ID.
	_, storedBlockMeta, err := chain.GetBlockHeader(ctx, block.Hash())
	require.NoError(t, err)
	require.NotNil(t, storedBlockMeta)

	childAfterValidation, err := utxoStore.Get(ctx, childTx.TxIDChainHash())
	require.NoError(t, err)

	if storedBlockMeta.MinedSet {
		require.Contains(t, childAfterValidation.BlockIDs, block.ID,
			"the block was stored with mined_set=true (buildAddBlockOpts WithMinedSet) but its transaction was never tagged with the block ID; mined_set must not be true until tagging has run")
	}
}
