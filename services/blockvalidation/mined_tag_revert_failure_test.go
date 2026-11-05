package blockvalidation

import (
	"context"
	"net/url"
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
	"github.com/bsv-blockchain/teranode/settings"
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

// Scenario numbers shared by the mined-tag tests.
const (
	minedTagBlockHeight      = uint32(1)
	minedTagBlockVersion     = uint32(4)
	minedTagSubtreeLeafCount = 2
	minedTagFundingSatoshis  = uint64(50 * 100000000)
	minedTagSpendSatoshis    = uint64(49 * 100000000)
)

// minedTagHarness is a bare BlockValidation over a real sqlitememory blockchain store and a real
// sqlitememory UTXO store, plus one block at height 1 whose single subtree holds spendTx. There is
// no background setMined worker or subscription, so the only setTxMinedStatus calls are the ones a
// test makes.
type minedTagHarness struct {
	settings   *settings.Settings
	chain      blockchain.ClientI
	utxoStore  utxostore.Store
	validation *BlockValidation
	block      *model.Block
	spendTx    *bt.Tx
}

// newMinedTagHarness builds the harness. The caller defers harness.validation.StopCaches().
func newMinedTagHarness(t *testing.T, ctx context.Context, utxoStoreName string) *minedTagHarness {
	t.Helper()

	initPrometheusMetrics()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	chain, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	utxoStoreURL, err := url.Parse("sqlitememory:///" + utxoStoreName)
	require.NoError(t, err)

	realUtxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	subtreeStore := blobmemory.New()
	txStore := blobmemory.New()

	subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
	subtreeValidationClient.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	fundingTx, spendTx := createMinedTagFundingAndSpend(t, ctx)

	_, _, err = realUtxoStore.SpendAndCreate(ctx, fundingTx, 0, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 0, BlockHeight: 0}), utxostore.WithCreateOnly())
	require.NoError(t, err)

	_, _, err = realUtxoStore.SpendAndCreate(ctx, spendTx, 0, utxostore.WithCreateOnly())
	require.NoError(t, err)

	subtree := storeMinedTagSubtreeWith(t, ctx, subtreeStore, spendTx)

	coinbaseTx := coinbaseAtHeight(t, minedTagBlockHeight)

	replicated := subtree.Duplicate()
	replicated.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec

	header := minedBIP34Header(t, minedTagBlockVersion, tSettings.ChainCfgParams.GenesisHash, replicated.RootHash())

	block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{subtree.RootHash()},
		uint64(subtree.Length()), uint64(coinbaseTx.Size()+spendTx.Size()), minedTagBlockHeight, 0) //nolint:gosec
	require.NoError(t, err)

	// The store writes genesis unmined, and ValidateBlock waits for the parent to be mined.
	require.NoError(t, chain.SetBlockMinedSet(ctx, tSettings.ChainCfgParams.GenesisHash))

	validation := &BlockValidation{
		logger:                        logger,
		settings:                      tSettings,
		blockchainClient:              chain,
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

	return &minedTagHarness{
		settings:   tSettings,
		chain:      chain,
		utxoStore:  realUtxoStore,
		validation: validation,
		block:      block,
		spendTx:    spendTx,
	}
}

// createMinedTagFundingAndSpend returns a coinbase-style funding transaction paying 50 BSV to a
// fresh key, and a transaction spending it for 49 BSV.
func createMinedTagFundingAndSpend(t *testing.T, ctx context.Context) (fundingTx, spendTx *bt.Tx) {
	t.Helper()

	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	fundingTx = bt.NewTx()
	require.NoError(t, fundingTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	fundingTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'F', 'u', 'n', 'd'})
	require.NoError(t, fundingTx.AddP2PKHOutputFromAddress(address.AddressString, minedTagFundingSatoshis))

	spendTx = bt.NewTx()
	require.NoError(t, spendTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      fundingTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: fundingTx.Outputs[0].LockingScript,
		Satoshis:      fundingTx.Outputs[0].Satoshis,
	}))
	require.NoError(t, spendTx.AddP2PKHOutputFromAddress(address.AddressString, minedTagSpendSatoshis))
	require.NoError(t, spendTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey}))

	return fundingTx, spendTx
}

// storeMinedTagSubtreeWith writes a two-leaf subtree (coinbase placeholder, then spendTx) and its
// meta to the subtree store, and returns the subtree.
func storeMinedTagSubtreeWith(t *testing.T, ctx context.Context, subtreeStore *blobmemory.Memory, spendTx *bt.Tx) *subtreepkg.Subtree {
	t.Helper()

	subtree, err := subtreepkg.NewTreeByLeafCount(minedTagSubtreeLeafCount)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*spendTx.TxIDChainHash(), uint64(spendTx.Size()), 0)) //nolint:gosec

	subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)
	require.NoError(t, subtreeMeta.SetTxInpointsFromTx(spendTx))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeMetaBytes, err := subtreeMeta.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes))

	return subtree
}

// blockMeta reads the block's header metadata (mined_set, invalid, ID) from the blockchain store.
func (harness *minedTagHarness) blockMeta(t *testing.T, ctx context.Context) *model.BlockHeaderMeta {
	t.Helper()

	_, meta, err := harness.chain.GetBlockHeader(ctx, harness.block.Hash())
	require.NoError(t, err)

	return meta
}

// invalidateBlock marks the harness block invalid, which clears its mined_set flag.
func (harness *minedTagHarness) invalidateBlock(t *testing.T, ctx context.Context) {
	t.Helper()

	_, err := harness.chain.InvalidateBlock(ctx, harness.block.Hash())
	require.NoError(t, err)
}

// revertFailingUtxoStore wraps a real UTXO store and fails SetMinedMulti calls that unset mined
// tags, which is the revert of an invalid block's tags.
type revertFailingUtxoStore struct {
	utxostore.Store

	unsetCalls int
}

func (s *revertFailingUtxoStore) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, info utxostore.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	if info.UnsetMined {
		s.unsetCalls++

		return nil, errors.NewStorageError("injected SetMinedMulti failure during revert")
	}

	return s.Store.SetMinedMulti(ctx, hashes, info)
}

// TestMinedTagRevertFailure_LeavesMinedSetUnsetAndSurfacesError checks that a failed revert of an
// invalid block's mined tags is reported and does not mark the block as processed.
//
// Spec: BV-COMMIT-011 (tracked as GAP-BV-057, fixed by bsv-blockchain/teranode#2016).
//
// Scenario: Given a block at height 1 that was validated and tagged (mined_set true, its spend
// transaction carries the block's ID), and the block is then invalidated (mined_set cleared, so the
// revert is owed). When the revert runs (setTxMinedStatus with unsetMined) against a UTXO store
// whose unset SetMinedMulti fails. Then setTxMinedStatus returns the error and mined_set stays
// false, so the sweep retries.
//
// The failure is injected with a UTXO-store decorator that fails the unset SetMinedMulti call, not
// with a real I/O fault.
func TestMinedTagRevertFailure_LeavesMinedSetUnsetAndSurfacesError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	harness := newMinedTagHarness(t, ctx, "mined_tag_revert_failure")
	defer harness.validation.StopCaches()

	// Given: the block is validated, stored and tagged normally, so the spend is mined in it.
	require.NoError(t, harness.validation.ValidateBlock(ctx, harness.block, "test"))

	require.NoError(t, harness.validation.setTxMinedStatus(ctx, harness.block.Hash()))

	mined, err := harness.chain.GetBlockIsMined(ctx, harness.block.Hash())
	require.NoError(t, err)
	require.True(t, mined, "precondition: block is mined_set after normal tagging")

	taggedMeta := harness.blockMeta(t, ctx)

	tagged, err := harness.utxoStore.Get(ctx, harness.spendTx.TxIDChainHash())
	require.NoError(t, err)
	require.Contains(t, tagged.BlockIDs, taggedMeta.ID, "precondition: the spend carries the block's tag")

	// Given: the block is invalidated; InvalidateBlock clears mined_set so the revert is owed.
	harness.invalidateBlock(t, ctx)

	invalidatedMeta := harness.blockMeta(t, ctx)
	require.True(t, invalidatedMeta.Invalid, "precondition: block is invalid")
	require.False(t, invalidatedMeta.MinedSet, "precondition: InvalidateBlock cleared mined_set, the revert is owed")

	// When: the revert runs against a UTXO store whose unset SetMinedMulti fails.
	failingStore := &revertFailingUtxoStore{Store: harness.utxoStore}
	harness.validation.utxoStore = failingStore

	const unsetMined = true

	revertErr := harness.validation.setTxMinedStatus(ctx, harness.block.Hash(), unsetMined)

	require.Positive(t, failingStore.unsetCalls, "precondition: the revert reached SetMinedMulti(unset)")

	// Then: mined_set stays false and the failure surfaces.
	revertedMeta := harness.blockMeta(t, ctx)

	require.False(t, revertedMeta.MinedSet,
		"the revert's SetMinedMulti failed (setTxMinedStatus returned err=%v) yet mined_set was set, so the sweep will never retry and the spend keeps a stale tag", revertErr)
	require.Error(t, revertErr, "the revert failure must surface")
}
