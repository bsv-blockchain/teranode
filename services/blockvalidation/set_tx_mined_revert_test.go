package blockvalidation

import (
	"bytes"
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

// minedFailingStore wraps a real UTXO store and fails SetMinedMulti on the MINED
// (not unset) path — how model.UpdateTxMinedStatus's store write fails on the
// normal valid-block path.
type minedFailingStore struct {
	utxostore.Store

	minedCalls int
}

func (s *minedFailingStore) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, info utxostore.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	if !info.UnsetMined {
		s.minedCalls++

		return nil, errors.NewStorageError("injected SetMinedMulti failure on the mined path")
	}

	return s.Store.SetMinedMulti(ctx, hashes, info)
}

// TestSetTxMinedStatus_ValidPathReleasesMmapSubtreesOnMinedUpdateError is one of
// two regressions for #2029 (the other is
// TestSetTxMinedStatus_CachedBlockReleasesMmapSubtreesWhenHeaderFetchFails).
//
// On the valid (non-unset) path setTxMinedStatus took a cached block that already
// holds mmap-backed subtrees, and before the fix released them only after a
// successful UpdateTxMinedStatus. The error returns (ErrBlockInvalid,
// ErrBlockParentNotMined, the generic update error, and the GetBlockHeaderIDs
// failure) skipped that release, so the one owned mmap mapping and its temp
// backing file leaked, and the setTxMined retry loop runs up to 10 times. Here a
// UTXO-store decorator fails the mined SetMinedMulti so the generic error return
// is taken; the block's mmap-backed subtree must still come back released, which
// is observable as its backing file being gone. A nil-entry check would miss it:
// the pinned release path nils entries without closing.
func TestSetTxMinedStatus_ValidPathReleasesMmapSubtreesOnMinedUpdateError(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	utxoStoreURL, err := url.Parse("sqlitememory:///gap2029")
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

	require.NoError(t, blockchainClient.SetBlockMinedSet(ctx, tSettings.ChainCfgParams.GenesisHash))

	// mmap-backed subtrees make a leaked (unreleased) subtree observable: each
	// leaves a backing file in this directory until it is closed.
	mmapDir := t.TempDir()

	bv := &BlockValidation{
		logger:                        logger,
		settings:                      tSettings,
		blockchainClient:              blockchainClient,
		subtreeStore:                  subtreeStore,
		txStore:                       txStore,
		utxoStore:                     realUtxoStore,
		subtreeValidationClient:       subtreeValidationClient,
		mmapDir:                       mmapDir,
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

	// Validate and store the block; this caches it in lastValidatedBlocks with its subtrees loaded.
	require.NoError(t, bv.ValidateBlock(ctx, block, "test"))

	// Hold the exact block object setTxMinedStatus will use and swap its loaded
	// subtree for an mmap-backed one built from the same bytes, reproducing a
	// block cached by the catchup quick-validate fallback with
	// blockvalidation_subtreeMmapDir set. setTxMinedStatus removes it from the
	// cache and takes ownership. getAndValidateSubtrees' "already loaded" check
	// reuses this mmap subtree (non-nil, Nodes present) rather than reloading it.
	cached, ok := bv.lastValidatedBlocks.Get(*block.Hash())
	require.True(t, ok, "precondition: the validated block is cached")
	require.True(t, bv.hasValidSubtrees(cached), "precondition: the cached block has its subtrees loaded")

	mmapSubtree, err := subtreepkg.NewSubtreeFromReaderMmap(bytes.NewReader(subtreeBytes), mmapDir)
	require.NoError(t, err)
	require.True(t, mmapSubtree.IsMmapBacked(), "precondition: the swapped-in subtree is mmap-backed")
	cached.ReplaceSubtreeSlices([]*subtreepkg.Subtree{mmapSubtree})

	entries, err := os.ReadDir(mmapDir)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "precondition: the cached block holds an mmap backing file to leak")

	// The mined update now fails, driving setTxMinedStatus down the generic error return.
	failing := &minedFailingStore{Store: realUtxoStore}
	bv.utxoStore = failing

	err = bv.setTxMinedStatus(ctx, block.Hash())
	require.Positive(t, failing.minedCalls, "precondition: the mined SetMinedMulti was reached")
	require.Error(t, err, "#2029: the injected mined-update failure must surface")

	// Despite the error, the owned block's mmap-backed subtree was released: its
	// backing file is gone. Before the fix the else branch's only release was on
	// the success path, so the generic error return skipped it and the mapping and
	// temp file leaked.
	entries, err = os.ReadDir(mmapDir)
	require.NoError(t, err)
	require.Empty(t, entries, "#2029: a failed valid-path update must still release the owned block's mmap subtrees")
}

// TestSetTxMinedStatus_CachedBlockReleasesMmapSubtreesWhenHeaderFetchFails covers
// the case #2029 leaves open today: a block taken from lastValidatedBlocks with
// mmap-backed subtrees already loaded, whose GetBlockHeader fetch fails before the
// valid-path subtree load. The release defer must be registered as soon as the
// block is owned (its cache entry deleted), not after the unset/valid branch, or
// that one mmap mapping and its temp backing file leak until process exit.
func TestSetTxMinedStatus_CachedBlockReleasesMmapSubtreesWhenHeaderFetchFails(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	var txHash chainhash.Hash
	txHash[0] = 0x11
	require.NoError(t, subtree.AddNode(txHash, 100, 0))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)

	mmapDir := t.TempDir()
	mmapSubtree, err := subtreepkg.NewSubtreeFromReaderMmap(bytes.NewReader(subtreeBytes), mmapDir)
	require.NoError(t, err)
	require.True(t, mmapSubtree.IsMmapBacked(), "precondition: the cached subtree is mmap-backed")

	coinbaseTx := coinbaseAtHeight(t, 1)
	hdr := minedBIP34Header(t, 4, tSettings.ChainCfgParams.GenesisHash, subtree.RootHash())

	block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{subtree.RootHash()},
		uint64(subtree.Length()), uint64(coinbaseTx.Size()), 1, 0) //nolint:gosec
	require.NoError(t, err)
	block.ReplaceSubtreeSlices([]*subtreepkg.Subtree{mmapSubtree})

	failing := &headerFailingClient{fail: true}

	bv := &BlockValidation{
		logger:              logger,
		settings:            tSettings,
		blockchainClient:    failing,
		mmapDir:             mmapDir,
		lastValidatedBlocks: expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
		blockExistsCache:    expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
		subtreeExistsCache:  expiringmap.New[chainhash.Hash, bool](10 * time.Minute),
		stats:               gocore.NewStat("blockvalidation"),
	}
	defer bv.StopCaches()

	bv.lastValidatedBlocks.Set(*block.Hash(), block)
	require.True(t, bv.hasValidSubtrees(block), "precondition: the cached block has its mmap subtree loaded")

	entries, err := os.ReadDir(mmapDir)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "precondition: the cached block holds an mmap backing file to leak")

	err = bv.setTxMinedStatus(ctx, block.Hash())
	require.Positive(t, failing.headerCalls, "precondition: GetBlockHeader was reached")
	require.Error(t, err, "#2029: the GetBlockHeader failure must surface")

	entries, err = os.ReadDir(mmapDir)
	require.NoError(t, err)
	require.Empty(t, entries,
		"#2029: a cached mmap-backed block must release its subtrees when GetBlockHeader fails before the valid-path load")
}

// headerFailingClient fails GetBlockHeader on demand. On the cached-block path
// only GetBlockHeader is reached before the failure, so the embedded nil ClientI
// is never called.
type headerFailingClient struct {
	blockchain.ClientI

	fail        bool
	headerCalls int
}

func (c *headerFailingClient) GetBlockHeader(_ context.Context, _ *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	if c.fail {
		c.headerCalls++
		return nil, nil, errors.NewServiceError("injected GetBlockHeader failure (blockchain service restart)")
	}

	return nil, nil, errors.NewServiceError("headerFailingClient: GetBlockHeader called with fail=false")
}
