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

// gap011AddBlockRecorder is the real local client with AddBlock calls recorded by block hash.
type gap011AddBlockRecorder struct {
	blockchain.ClientI

	mu    sync.Mutex
	added map[chainhash.Hash]bool
}

func (c *gap011AddBlockRecorder) AddBlock(ctx context.Context, block *model.Block, peerID string, opts ...blockchainoptions.StoreBlockOption) error {
	c.mu.Lock()
	c.added[*block.Hash()] = true
	c.mu.Unlock()

	return c.ClientI.AddBlock(ctx, block, peerID, opts...)
}

func (c *gap011AddBlockRecorder) wasAdded(h *chainhash.Hash) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.added[*h]
}

// TestGapBV011_ReincludedTransactionRejectedBeforeAddBlock is the gap-proof test for GAP-BV-011
// (BV-BODY-017, BV-COMMIT-005).
//
// Spec-correct behaviour: a block that re-includes a transaction already mined in one of its
// ancestors must be rejected BEFORE it is stored: ValidateBlock must return ErrBlockInvalid and
// AddBlock must never be called for it, so the block can never become the tip.
//
// Why it is expected to fail today: Block.Valid only checks the PARENTS of each transaction against
// the chain (model/Block.go checkParentExistsOnChain); it never asks whether the transaction itself
// is already mined in an ancestor. That is only discovered after commit, in
// model.updateTxMinedStatus (update-tx-mined.go ~288 fast path, ~412 slow path), reached from
// BlockValidation.setTxMinedStatus once the block has already been added. So ValidateBlock returns
// nil and AddBlock has already run. Fixture: real sqlitememory blockchain and UTXO stores; chain
// genesis -> N (contains tx T, tagged mined) -> N+1 (coinbase only) -> N+2 (re-includes T).
func TestGapBV011_ReincludedTransactionRejectedBeforeAddBlock(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	localClient, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	blockchainClient := &gap011AddBlockRecorder{ClientI: localClient, added: map[chainhash.Hash]bool{}}

	utxoStoreURL, err := url.Parse("sqlitememory:///gap011")
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

	fundingTx := bt.NewTx()
	require.NoError(t, fundingTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	fundingTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'F', 'u', 'n', 'd'})
	require.NoError(t, fundingTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000))

	txT := bt.NewTx()
	require.NoError(t, txT.FromUTXOs(&bt.UTXO{
		TxIDHash:      fundingTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: fundingTx.Outputs[0].LockingScript,
		Satoshis:      fundingTx.Outputs[0].Satoshis,
	}))
	require.NoError(t, txT.AddP2PKHOutputFromAddress(address.AddressString, 49*100000000))
	require.NoError(t, txT.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey}))

	_, _, err = utxoStore.SpendAndCreate(ctx, fundingTx, 0, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 0, BlockHeight: 0}), utxostore.WithCreateOnly())
	require.NoError(t, err)

	_, _, err = utxoStore.SpendAndCreate(ctx, txT, 0, utxostore.WithCreateOnly())
	require.NoError(t, err)

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*txT.TxIDChainHash(), uint64(txT.Size()), 0)) //nolint:gosec

	subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)
	require.NoError(t, subtreeMeta.SetTxInpointsFromTx(txT))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeMetaBytes, err := subtreeMeta.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes))

	// blockWithT builds and mines a block at the given height on parent that includes subtree [coinbase, T].
	blockWithT := func(parent *chainhash.Hash, height uint32) *model.Block {
		coinbaseTx := coinbaseAtHeight(t, height)

		replicated := subtree.Duplicate()
		replicated.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec

		hdr := minedBIP34Header(t, 4, parent, replicated.RootHash())

		b, bErr := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{subtree.RootHash()},
			uint64(subtree.Length()), uint64(coinbaseTx.Size()+txT.Size()), height, 0) //nolint:gosec
		require.NoError(t, bErr)

		return b
	}

	bv := NewBlockValidation(ctx, logger, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)
	defer bv.StopCaches()

	// markMined drives mined-tagging for a stored block (what the setMined worker does) and waits for mined_set.
	markMined := func(b *model.Block) {
		_ = bv.setTxMinedStatus(ctx, b.Hash())

		require.Eventually(t, func() bool {
			mined, mErr := blockchainClient.GetBlockIsMined(ctx, b.Hash())

			return mErr == nil && mined
		}, 10*time.Second, 20*time.Millisecond, "block %s was not marked mined", b.Hash())
	}

	// Block N: contains T. Validate, store, tag T as mined in N.
	blockN := blockWithT(tSettings.ChainCfgParams.GenesisHash, 1)
	require.NoError(t, bv.ValidateBlock(ctx, blockN, "test"))
	markMined(blockN)

	_, nMeta, err := blockchainClient.GetBlockHeader(ctx, blockN.Hash())
	require.NoError(t, err)

	tMeta, err := utxoStore.Get(ctx, txT.TxIDChainHash())
	require.NoError(t, err)
	require.Contains(t, tMeta.BlockIDs, nMeta.ID, "precondition: T is mined in ancestor N")

	// Block N+1: coinbase only.
	blockN1 := gap011CoinbaseOnlyBlock(t, blockN.Hash(), 2)
	require.NoError(t, bv.ValidateBlock(ctx, blockN1, "test"))
	markMined(blockN1)

	// Block N+2 re-includes T, which is already mined in ancestor N.
	blockN2 := blockWithT(blockN1.Hash(), 3)

	validateErr := bv.ValidateBlock(ctx, blockN2, "test")

	require.False(t, blockchainClient.wasAdded(blockN2.Hash()),
		"GAP-BV-011: block N+2 re-includes a transaction already mined in ancestor N but AddBlock was called for it; the re-presentation is only detected post-commit (update-tx-mined.go)")
	require.Error(t, validateErr, "GAP-BV-011: ValidateBlock must reject the block")
	require.True(t, errors.Is(validateErr, errors.ErrBlockInvalid), "expected ErrBlockInvalid, got: %v", validateErr)
}

// gap011CoinbaseOnlyBlock builds a mined coinbase-only block (self-contained copy for this file).
func gap011CoinbaseOnlyBlock(t *testing.T, parent *chainhash.Hash, height uint32) *model.Block {
	t.Helper()

	coinbaseTx := coinbaseAtHeight(t, height)
	hdr := minedBIP34Header(t, 4, parent, coinbaseTx.TxIDChainHash())

	block, err := model.NewBlock(hdr, coinbaseTx, nil, 1, uint64(coinbaseTx.Size()), height, 0) //nolint:gosec
	require.NoError(t, err)

	return block
}
