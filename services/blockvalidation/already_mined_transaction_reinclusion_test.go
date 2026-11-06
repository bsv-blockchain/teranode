//go:build bvgaps

package blockvalidation

import (
	"context"
	"net/url"
	"sync"
	"testing"
	"time"

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
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// reinclusionAddBlockRecorder is the real local blockchain client with every AddBlock call
// recorded by block hash, so the test can tell whether a block was stored before it was rejected.
type reinclusionAddBlockRecorder struct {
	blockchain.ClientI

	mu    sync.Mutex
	added map[chainhash.Hash]bool
}

func (c *reinclusionAddBlockRecorder) AddBlock(ctx context.Context, block *model.Block, peerID string, opts ...blockchainoptions.StoreBlockOption) error {
	c.mu.Lock()
	c.added[*block.Hash()] = true
	c.mu.Unlock()

	return c.ClientI.AddBlock(ctx, block, peerID, opts...)
}

// addBlockWasCalledFor reports whether AddBlock was ever called for the block with this hash.
func (c *reinclusionAddBlockRecorder) addBlockWasCalledFor(blockHash *chainhash.Hash) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.added[*blockHash]
}

// TestKnownDefect_BlockReincludingAncestorMinedTx_RejectedBeforeAddBlock proves that a block
// re-including a transaction already mined in one of its ancestors is stored before it is rejected.
//
// Spec: BV-BODY-017, BV-COMMIT-005 and AC-BV-BODY-017.1 (tracked as GAP-BV-011).
//
// Scenario: Given a chain genesis -> block 1 (contains transaction T, tagged mined in block 1)
// -> block 2 (coinbase only), when block 3 on top of block 2 includes T again, then ValidateBlock
// must return ErrBlockInvalid and AddBlock must never be called for block 3, so it can never become
// the tip.
//
// Today: Block.Valid only checks the PARENTS of each transaction against the chain
// (model/Block.go checkParentExistsOnChain); it never asks whether the transaction itself is already
// mined in an ancestor. That is only discovered after commit, in model.updateTxMinedStatus
// (update-tx-mined.go ~288 fast path, ~412 slow path), reached from BlockValidation.setTxMinedStatus
// once the block has already been added. So ValidateBlock returns nil and AddBlock has already run.
//
// Fixture: real sqlitememory blockchain and UTXO stores.
func TestKnownDefect_BlockReincludingAncestorMinedTx_RejectedBeforeAddBlock(t *testing.T) {
	const (
		minedInBlockHeight     = uint32(1) // block N: first mines T
		coinbaseOnlyHeight     = uint32(2) // block N+1: coinbase only
		reincludingBlockHeight = uint32(3) // block N+2: includes T again
		blockHeaderVersion     = uint32(4)
		fundingSatoshis        = 50 * 100000000
		reincludedTxSatoshis   = 49 * 100000000
		subtreeLeafCount       = 2
		peerBaseURL            = "test"
		minedTaggingTimeout    = 10 * time.Second
		minedTaggingPoll       = 20 * time.Millisecond
	)

	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	localClient, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	chain := &reinclusionAddBlockRecorder{ClientI: localClient, added: map[chainhash.Hash]bool{}}

	utxoStoreURL, err := url.Parse("sqlitememory:///already_mined_reinclusion")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	subtreeStore := blobmemory.New()
	txStore := blobmemory.New()

	subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
	subtreeValidationClient.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Given: a funding transaction already in the UTXO store as a genesis-mined tx, and
	// transaction T spending it.
	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	fundingTx := bt.NewTx()
	require.NoError(t, fundingTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	fundingTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'F', 'u', 'n', 'd'})
	require.NoError(t, fundingTx.AddP2PKHOutputFromAddress(address.AddressString, fundingSatoshis))

	reincludedTx := bt.NewTx()
	require.NoError(t, reincludedTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      fundingTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: fundingTx.Outputs[0].LockingScript,
		Satoshis:      fundingTx.Outputs[0].Satoshis,
	}))
	require.NoError(t, reincludedTx.AddP2PKHOutputFromAddress(address.AddressString, reincludedTxSatoshis))
	require.NoError(t, reincludedTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey}))

	_, _, err = utxoStore.SpendAndCreate(ctx, fundingTx, 0, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 0, BlockHeight: 0}), utxostore.WithCreateOnly())
	require.NoError(t, err)

	_, _, err = utxoStore.SpendAndCreate(ctx, reincludedTx, 0, utxostore.WithCreateOnly())
	require.NoError(t, err)

	// A stored subtree [coinbase, T] that both block N and block N+2 reference.
	subtree, err := subtreepkg.NewTreeByLeafCount(subtreeLeafCount)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*reincludedTx.TxIDChainHash(), uint64(reincludedTx.Size()), 0)) //nolint:gosec

	subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)
	require.NoError(t, subtreeMeta.SetTxInpointsFromTx(reincludedTx))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeMetaBytes, err := subtreeMeta.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes))

	// blockContainingReincludedTx builds and mines a block at the given height on parent that
	// includes the subtree [coinbase, T].
	blockContainingReincludedTx := func(parent *chainhash.Hash, height uint32) *model.Block {
		coinbaseTx := coinbaseAtHeight(t, height)

		subtreeWithCoinbase := subtree.Duplicate()
		subtreeWithCoinbase.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec

		header := minedBIP34Header(t, blockHeaderVersion, parent, subtreeWithCoinbase.RootHash())

		block, blockErr := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{subtree.RootHash()},
			uint64(subtree.Length()), uint64(coinbaseTx.Size()+reincludedTx.Size()), height, 0) //nolint:gosec
		require.NoError(t, blockErr)

		return block
	}

	validation := NewBlockValidation(ctx, logger, tSettings, chain, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)
	defer validation.StopCaches()

	// tagTransactionsAsMined drives mined-tagging for a stored block (what the setMined worker does)
	// and waits until the block's mined_set flag is true.
	tagTransactionsAsMined := func(block *model.Block) {
		_ = validation.setTxMinedStatus(ctx, block.Hash())

		require.Eventually(t, func() bool {
			mined, minedErr := chain.GetBlockIsMined(ctx, block.Hash())

			return minedErr == nil && mined
		}, minedTaggingTimeout, minedTaggingPoll, "block %s was not marked mined", block.Hash())
	}

	// Block N contains T: validate, store, and tag T as mined in N.
	blockN := blockContainingReincludedTx(tSettings.ChainCfgParams.GenesisHash, minedInBlockHeight)
	require.NoError(t, validation.ValidateBlock(ctx, blockN, peerBaseURL))
	tagTransactionsAsMined(blockN)

	_, blockNMeta, err := chain.GetBlockHeader(ctx, blockN.Hash())
	require.NoError(t, err)

	reincludedTxMeta, err := utxoStore.Get(ctx, reincludedTx.TxIDChainHash())
	require.NoError(t, err)
	require.Contains(t, reincludedTxMeta.BlockIDs, blockNMeta.ID, "precondition: T is mined in ancestor N")

	// Block N+1 is coinbase only.
	blockN1 := reinclusionCoinbaseOnlyBlock(t, blockN.Hash(), coinbaseOnlyHeight)
	require.NoError(t, validation.ValidateBlock(ctx, blockN1, peerBaseURL))
	tagTransactionsAsMined(blockN1)

	// When: block N+2 re-includes T, which is already mined in ancestor N.
	blockN2 := blockContainingReincludedTx(blockN1.Hash(), reincludingBlockHeight)

	validateErr := validation.ValidateBlock(ctx, blockN2, peerBaseURL)

	// Then: the block is rejected as invalid without ever being added to the chain.
	require.False(t, chain.addBlockWasCalledFor(blockN2.Hash()),
		"block N+2 re-includes a transaction already mined in ancestor N, so AddBlock must not be called for it; today the re-presentation is only detected post-commit (update-tx-mined.go)")
	require.Error(t, validateErr, "ValidateBlock must reject a block that re-includes a transaction mined in an ancestor")
	require.True(t, errors.Is(validateErr, errors.ErrBlockInvalid), "expected ErrBlockInvalid, got: %v", validateErr)
}

// reinclusionCoinbaseOnlyBlock builds a mined block on parent at height that contains only its
// coinbase transaction.
func reinclusionCoinbaseOnlyBlock(t *testing.T, parent *chainhash.Hash, height uint32) *model.Block {
	t.Helper()

	const blockHeaderVersion = uint32(4)

	coinbaseTx := coinbaseAtHeight(t, height)
	header := minedBIP34Header(t, blockHeaderVersion, parent, coinbaseTx.TxIDChainHash())

	block, err := model.NewBlock(header, coinbaseTx, nil, 1, uint64(coinbaseTx.Size()), height, 0) //nolint:gosec
	require.NoError(t, err)

	return block
}
