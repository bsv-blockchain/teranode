package blockvalidation

import (
	"context"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
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
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// revertFailingUnsetStore wraps a real UTXO store and fails SetMinedMulti calls that unset mined tags,
// which is the revert of an invalid block's tags.
type revertFailingUnsetStore struct {
	utxostore.Store

	unsetCalls int
}

func (s *revertFailingUnsetStore) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, info utxostore.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	if info.UnsetMined {
		s.unsetCalls++

		return nil, errors.NewStorageError("injected SetMinedMulti failure during revert")
	}

	return s.Store.SetMinedMulti(ctx, hashes, info)
}

// TestSetTxMinedStatus_RevertFailureLeavesMinedSetFalse is the regression test for GAP-BV-057 (BV-COMMIT-011).
//
// When reverting an invalid block's mined tags, a failed SetMinedMulti must not be swallowed:
// setTxMinedStatus must return the error so mined_set stays false and the mined-not-set sweep
// retries. Before the fix, model.updateTxMinedStatus logged the failure on the unset path and
// returned success, and setTxMinedStatus went on to SetBlockMinedSet, so the block was recorded as
// processed while its transactions kept the stale tag. A UTXO-store decorator fails the unset
// SetMinedMulti call; this is not a real I/O fault.
func TestSetTxMinedStatus_RevertFailureLeavesMinedSetFalse(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	utxoStoreURL, err := url.Parse("sqlitememory:///gap057")
	require.NoError(t, err)

	realUtxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
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

	_, _, err = realUtxoStore.SpendAndCreate(ctx, fundingTx, 0, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 0, BlockHeight: 0}), utxostore.WithCreateOnly())
	require.NoError(t, err)

	_, _, err = realUtxoStore.SpendAndCreate(ctx, txT, 0, utxostore.WithCreateOnly())
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

	const blockHeight = uint32(1)

	coinbaseTx := coinbaseAtHeight(t, blockHeight)

	replicated := subtree.Duplicate()
	replicated.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec

	hdr := minedBIP34Header(t, 4, tSettings.ChainCfgParams.GenesisHash, replicated.RootHash())

	block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{subtree.RootHash()},
		uint64(subtree.Length()), uint64(coinbaseTx.Size()+txT.Size()), blockHeight, 0) //nolint:gosec
	require.NoError(t, err)

	// A bare BlockValidation (no background setMined worker or subscription) keeps the run
	// deterministic: the only setTxMinedStatus calls are the ones this test makes. The store writes
	// genesis unmined, and ValidateBlock waits for the parent to be mined.
	require.NoError(t, blockchainClient.SetBlockMinedSet(ctx, tSettings.ChainCfgParams.GenesisHash))

	bv := &BlockValidation{
		logger:                        logger,
		settings:                      tSettings,
		blockchainClient:              blockchainClient,
		subtreeStore:                  subtreeStore,
		txStore:                       txStore,
		utxoStore:                     realUtxoStore,
		subtreeValidationClient:       subtreeValidationClient,
		lastValidatedBlocks:           expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
		blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
		subtreeExistsCache:            expiringmap.New[chainhash.Hash, bool](10 * time.Minute),
		blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
		blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
		setMinedChan:                  make(chan *chainhash.Hash, 1),
		revalidateBlockChan:           make(chan revalidateBlockData, 1),
		stats:                         gocore.NewStat("blockvalidation"),
	}
	defer bv.StopCaches()

	// Validate, store and tag the block normally: T is mined in the block.
	require.NoError(t, bv.ValidateBlock(ctx, block, "test"))

	require.NoError(t, bv.setTxMinedStatus(ctx, block.Hash()))

	mined, err := blockchainClient.GetBlockIsMined(ctx, block.Hash())
	require.NoError(t, err)
	require.True(t, mined, "precondition: block is mined_set after normal tagging")

	_, blockMeta, err := blockchainClient.GetBlockHeader(ctx, block.Hash())
	require.NoError(t, err)

	tagged, err := realUtxoStore.Get(ctx, txT.TxIDChainHash())
	require.NoError(t, err)
	require.Contains(t, tagged.BlockIDs, blockMeta.ID, "precondition: T carries the block's tag")

	// The block is invalidated: InvalidateBlock clears mined_set so the revert is owed.
	_, err = blockchainClient.InvalidateBlock(ctx, block.Hash())
	require.NoError(t, err)

	_, blockMeta, err = blockchainClient.GetBlockHeader(ctx, block.Hash())
	require.NoError(t, err)
	require.True(t, blockMeta.Invalid, "precondition: block is invalid")
	require.False(t, blockMeta.MinedSet, "precondition: InvalidateBlock cleared mined_set, the revert is owed")

	// Now the revert runs against a UTXO store whose unset SetMinedMulti fails.
	failing := &revertFailingUnsetStore{Store: realUtxoStore}
	bv.utxoStore = failing

	// mmap-backed reloads make a leaked subtree observable: each one leaves a
	// backing file in this directory until it is closed.
	mmapDir := t.TempDir()
	bv.mmapDir = mmapDir

	revertErr := bv.setTxMinedStatus(ctx, block.Hash(), true)

	require.Positive(t, failing.unsetCalls, "precondition: the revert reached SetMinedMulti(unset)")

	entries, err := os.ReadDir(mmapDir)
	require.NoError(t, err)
	require.Empty(t, entries, "the subtrees reloaded for the failed revert must be released, removing their mmap backing files")

	_, blockMeta, err = blockchainClient.GetBlockHeader(ctx, block.Hash())
	require.NoError(t, err)

	require.False(t, blockMeta.MinedSet,
		"GAP-BV-057: the revert's SetMinedMulti failed (setTxMinedStatus returned err=%v) yet mined_set was set, so the sweep will never retry and T keeps a stale tag", revertErr)
	require.Error(t, revertErr, "GAP-BV-057: the revert failure must surface")

	// The retry runs against a healthy UTXO store but a subtree store that cannot
	// be read. If the reload's failure were dropped, the revert would go ahead
	// without the subtree, find nothing to unset, and set mined_set with T still
	// tagged. It must return the error and leave mined_set false instead.
	readErr := errors.NewStorageError("injected subtree store failure during revert")
	bv.utxoStore = realUtxoStore
	bv.subtreeStore = &failingGetStore{Store: subtreeStore, err: readErr}

	reloadErr := bv.setTxMinedStatus(ctx, block.Hash(), true)
	require.ErrorIs(t, reloadErr, readErr, "GAP-BV-057: a subtree read failure during the revert must surface")

	_, blockMeta, err = blockchainClient.GetBlockHeader(ctx, block.Hash())
	require.NoError(t, err)
	require.False(t, blockMeta.MinedSet, "GAP-BV-057: the revert could not read the block's subtree yet mined_set was set")

	stillTagged, err := realUtxoStore.Get(ctx, txT.TxIDChainHash())
	require.NoError(t, err)
	require.Contains(t, stillTagged.BlockIDs, blockMeta.ID, "precondition for the next sweep: T still carries the tag the revert owes")
}
