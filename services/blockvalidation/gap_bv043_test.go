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
	"github.com/bsv-blockchain/teranode/errors"
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

// gap043CoinbaseOnlyBlock builds and mines a coinbase-only block on top of parent.
func gap043CoinbaseOnlyBlock(t *testing.T, parent *chainhash.Hash, height uint32) *model.Block {
	t.Helper()

	coinbaseTx := coinbaseAtHeight(t, height)
	hdr := minedBIP34Header(t, 4, parent, coinbaseTx.TxIDChainHash())

	block, err := model.NewBlock(hdr, coinbaseTx, nil, 1, uint64(coinbaseTx.Size()), height, 0) //nolint:gosec
	require.NoError(t, err)

	return block
}

// TestGapBV043_ReValidateInvalidAncestorVerdictInvalidatesBlock is the gap-proof test for GAP-BV-043
// (BV-OPTIMISTIC-005, BV-VERDICT-009).
//
// Spec-correct behaviour: when internal revalidation of an optimistically added block reaches the
// ancestor-membership check (checkOldBlockIDs) and that check returns an INVALID verdict (a parent
// transaction is mined only in a block that is not an ancestor of this block), the block must be
// invalidated in the blockchain store and an invalid-block notification published, as the other
// branches of the same function do.
//
// Why it is expected to fail today: BlockValidation.reValidateBlock ends with
// `return u.checkOldBlockIDs(ctx, oldBlockIDsMap, blockData.block)` (BlockValidation.go ~3470), which
// hands the ErrBlockInvalid verdict straight back to the revalidation worker. The worker only logs and
// retries ("retries exhausted"), so the optimistically added block stays valid on the chain and nothing
// is published. Fixture: a real sqlitememory blockchain store with main chain genesis-A and a fork
// block F; the funding transaction is stamped as mined in F only, and the child block C (on A) spends
// it, so Block.Valid passes but the ancestor check says the parent is not on C's chain.
func TestGapBV043_ReValidateInvalidAncestorVerdictInvalidatesBlock(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	utxoStoreURL, err := url.Parse("sqlitememory:///gap043")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	subtreeStore := blobmemory.New()
	txStore := blobmemory.New()

	subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
	subtreeValidationClient.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Main chain: genesis -> A. Fork: genesis -> F (stored second, equal work, so A stays the tip).
	blockA := gap043CoinbaseOnlyBlock(t, tSettings.ChainCfgParams.GenesisHash, 1)
	require.NoError(t, blockchainClient.AddBlock(ctx, blockA, "test"))

	blockF := gap043CoinbaseOnlyBlock(t, tSettings.ChainCfgParams.GenesisHash, 1)
	// make F a distinct block from A
	blockF.Header.Timestamp++
	blockF.Header.Nonce = 0
	for {
		if ok, _, _ := blockF.Header.HasMetTargetDifficulty(); ok {
			break
		}

		blockF.Header.Nonce++
	}

	require.NotEqual(t, blockA.Hash().String(), blockF.Hash().String())
	require.NoError(t, blockchainClient.AddBlock(ctx, blockF, "test"))

	bestHeader, _, err := blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, blockA.Hash().String(), bestHeader.Hash().String(), "precondition: A is the main-chain tip, F is a fork")

	_, fMeta, err := blockchainClient.GetBlockHeader(ctx, blockF.Hash())
	require.NoError(t, err)

	// The funding transaction is recorded as mined ONLY in the fork block F.
	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

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

	_, _, err = utxoStore.SpendAndCreate(ctx, fundingTx, 1, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: fMeta.ID, BlockHeight: 1}), utxostore.WithCreateOnly())
	require.NoError(t, err)

	_, _, err = utxoStore.SpendAndCreate(ctx, childTx, 2, utxostore.WithCreateOnly())
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

	const childHeight = uint32(2)

	coinbaseTx := coinbaseAtHeight(t, childHeight)

	replicated := subtree.Duplicate()
	replicated.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec

	hdr := minedBIP34Header(t, 4, blockA.Hash(), replicated.RootHash())

	blockC, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{subtree.RootHash()},
		uint64(subtree.Length()), uint64(coinbaseTx.Size()+childTx.Size()), childHeight, 0) //nolint:gosec
	require.NoError(t, err)

	// The block was added optimistically before validation completed.
	require.NoError(t, blockchainClient.AddBlock(ctx, blockC, "test"))

	kafkaProducer := &SafeMockKafkaProducer{}

	bv := NewBlockValidation(ctx, logger, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)
	defer bv.StopCaches()

	bv.invalidBlockKafkaProducer = kafkaProducer

	err = bv.reValidateBlock(revalidateBlockData{block: blockC, baseURL: "test", optimisticallyAdded: true})

	// Precondition: either reValidateBlock acted on the verdict and returned nil (the fix, as the
	// other branches do), or it returned the ancestor check's INVALID verdict (today). Anything else
	// is a setup error, not the gap.
	revalidateErr := err
	if revalidateErr != nil {
		require.True(t, errors.Is(revalidateErr, errors.ErrBlockInvalid), "precondition: expected the ancestor check's ErrBlockInvalid verdict, got: %v", revalidateErr)
	}

	_, cMeta, err := blockchainClient.GetBlockHeader(ctx, blockC.Hash())
	require.NoError(t, err)
	require.NotNil(t, cMeta)

	require.True(t, cMeta.Invalid,
		"GAP-BV-043: reValidateBlock returned the INVALID ancestor verdict without invalidating the optimistically added block")
	require.True(t, kafkaProducer.IsPublishCalled(),
		"GAP-BV-043: no invalid-block notification was published for the INVALID ancestor verdict")
}
