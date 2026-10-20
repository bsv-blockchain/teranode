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

// TestGapBV039_PreassignedBlockIDStillTagsTransactions is the gap-proof test for GAP-BV-039
// (BV-COMMIT-010, BV-COMMIT-005, BV-INGRESS-013).
//
// Spec-correct behaviour: a block that arrives with a caller-supplied (non-zero) block ID on a
// non-legacy route and above the highest hardcoded checkpoint must still have its transactions
// tagged mined with that ID. Equivalently, mined_set must not be true while the transactions are
// untagged; the flag may only be set once tagging has run.
//
// Why it is expected to fail today: Server.ProcessBlock copies any non-zero request.BlockId onto the
// block with no route or height check (Server.go ~1503), and BlockValidation.buildAddBlockOpts then
// adds EVERY block with a non-zero ID using WithMinedSet(true) (BlockValidation.go ~1681). The
// setMinedChan worker skips blocks whose mined_set is already true, so setTxMinedStatus never runs
// and the block's transaction is left without the block ID. This test drives the BlockValidation
// layer directly (the state ProcessBlock leaves behind: block.ID set, baseURL "test", regtest chain
// with no checkpoints, so the block is above the checkpoint on a non-legacy route).
func TestGapBV039_PreassignedBlockIDStillTagsTransactions(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Preconditions that make this a non-checkpoint, non-legacy block.
	require.Empty(t, tSettings.ChainCfgParams.Checkpoints, "regtest has no checkpoints, so the block is above the highest checkpoint")

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	utxoStoreURL, err := url.Parse("sqlitememory:///gap039")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	subtreeStore := blobmemory.New()
	txStore := blobmemory.New()

	subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
	subtreeValidationClient.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	// A parent funding transaction already in the UTXO store as a genesis-mined tx, and a child
	// spending it that is the single non-coinbase transaction of the block.
	fundingTx := bt.NewTx()
	require.NoError(t, fundingTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	fundingTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'F', 'u', 'n', 'd'})
	require.NoError(t, fundingTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000))

	childTx := bt.NewTx()
	require.NoError(t, childTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      fundingTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: fundingTx.Outputs[0].LockingScript,
		Satoshis:      fundingTx.Outputs[0].Satoshis,
	}))
	require.NoError(t, childTx.AddP2PKHOutputFromAddress(address.AddressString, 49*100000000))
	require.NoError(t, childTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey}))

	_, _, err = utxoStore.SpendAndCreate(ctx, fundingTx, 0, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 0, BlockHeight: 0}), utxostore.WithCreateOnly())
	require.NoError(t, err)

	_, _, err = utxoStore.SpendAndCreate(ctx, childTx, 0, utxostore.WithCreateOnly())
	require.NoError(t, err)

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
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

	const blockHeight = uint32(1)

	coinbaseTx := coinbaseAtHeight(t, blockHeight)

	replicated := subtree.Duplicate()
	replicated.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec

	hdr := minedBIP34Header(t, 4, tSettings.ChainCfgParams.GenesisHash, replicated.RootHash())

	block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{subtree.RootHash()},
		uint64(subtree.Length()), uint64(coinbaseTx.Size()+childTx.Size()), blockHeight, 0) //nolint:gosec
	require.NoError(t, err)

	// What ProcessBlock does for a non-zero request.BlockId: reserve the ID and stamp it on the block.
	reservedID, err := blockchainClient.AssignBlockID(ctx, block.Hash())
	require.NoError(t, err)
	id32, err := blockIDToUint32(reservedID, block.Hash().String())
	require.NoError(t, err)
	require.NotZero(t, id32)

	block.ID = id32

	bv := NewBlockValidation(ctx, logger, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)
	defer bv.StopCaches()

	// Precondition: the child is untagged before the block is validated.
	before, err := utxoStore.Get(ctx, childTx.TxIDChainHash())
	require.NoError(t, err)
	require.Empty(t, before.BlockIDs, "child tx must not be mined before the block is validated")

	// A non-legacy base URL: this is the route on which no pre-assigned ID is licensed.
	require.NoError(t, bv.ValidateBlock(ctx, block, "test"))

	_, hdrMeta, err := blockchainClient.GetBlockHeader(ctx, block.Hash())
	require.NoError(t, err)
	require.NotNil(t, hdrMeta)

	after, err := utxoStore.Get(ctx, childTx.TxIDChainHash())
	require.NoError(t, err)

	if hdrMeta.MinedSet {
		require.Contains(t, after.BlockIDs, block.ID,
			"GAP-BV-039: the block was stored with mined_set=true (buildAddBlockOpts WithMinedSet) but its transaction was never tagged with the block ID; mined_set must not be true until tagging has run")
	}
}
