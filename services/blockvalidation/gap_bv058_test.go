package blockvalidation

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
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

// gap058BlockingStore wraps a real UTXO store and parks the first non-revert SetMinedMulti call
// (a set-mined tagging run) until released, so the test can invalidate the block mid-run.
type gap058BlockingStore struct {
	utxostore.Store

	armed   atomic.Bool
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (s *gap058BlockingStore) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, info utxostore.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	if s.armed.Load() && !info.UnsetMined {
		s.once.Do(func() { close(s.entered) })

		select {
		case <-s.release:
		case <-time.After(20 * time.Second):
			return nil, errors.NewProcessingError("gap058: test never released the blocked SetMinedMulti")
		}
	}

	return s.Store.SetMinedMulti(ctx, hashes, info)
}

// gap058Fixture is a validated, stored block whose single transaction has not been tagged yet,
// with a bare BlockValidation over real sqlitememory blockchain and UTXO stores.
type gap058Fixture struct {
	ctx              context.Context
	bv               *BlockValidation
	blockchainClient blockchain.ClientI
	utxoStore        utxostore.Store
	block            *model.Block
	tx               *bt.Tx
}

func newGap058Fixture(t *testing.T) *gap058Fixture {
	t.Helper()

	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	utxoStoreURL, err := url.Parse("sqlitememory:///gap058")
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
	require.NoError(t, blockchainClient.SetBlockMinedSet(ctx, tSettings.ChainCfgParams.GenesisHash, 0))

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
	t.Cleanup(bv.StopCaches)

	// Validate and store the block (mined_set=false, subtrees_set=true); no tagging has run yet.
	require.NoError(t, bv.ValidateBlock(ctx, block, "test"))

	_, preMeta, err := blockchainClient.GetBlockHeader(ctx, block.Hash())
	require.NoError(t, err)
	require.False(t, preMeta.MinedSet, "precondition: tagging has not run yet")
	require.False(t, preMeta.Invalid, "precondition: block is valid when the run starts")

	return &gap058Fixture{
		ctx:              ctx,
		bv:               bv,
		blockchainClient: blockchainClient,
		utxoStore:        realUtxoStore,
		block:            block,
		tx:               txT,
	}
}

// runParkedTagging starts a tagging run, parks it inside SetMinedMulti, calls interfere while it
// is parked, then releases it and returns the run's error.
func (f *gap058Fixture) runParkedTagging(t *testing.T, interfere func()) error {
	t.Helper()

	blocking := &gap058BlockingStore{
		Store:   f.utxoStore,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	f.bv.utxoStore = blocking
	blocking.armed.Store(true)

	runDone := make(chan error, 1)

	go func() {
		runDone <- f.bv.setTxMinedStatus(f.ctx, f.block.Hash())
	}()

	select {
	case <-blocking.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("precondition: the tagging run never reached SetMinedMulti")
	}

	interfere()

	close(blocking.release)

	select {
	case runErr := <-runDone:
		return runErr
	case <-time.After(20 * time.Second):
		t.Fatal("the tagging run did not finish after release")
	}

	return nil
}

// requireRequeued asserts the refused run asked the setMined worker to re-queue the block. The
// run itself must not put the block straight back on setMinedChan: the worker re-queues it
// through the backoff path, which TestSetMinedChanWorker_RequeueKeepsRetryCounterAndBacksOff covers.
func (f *gap058Fixture) requireRequeued(t *testing.T, runErr error) {
	t.Helper()

	var requeue *setMinedRequeueError
	require.ErrorAs(t, runErr, &requeue, "the refused run must return setMinedRequeueError, not nil and not a plain failure")
	require.Len(t, f.bv.setMinedChan, 0, "the run must leave the re-queue, and its backoff, to the worker")
}

// TestGapBV058_InFlightTaggingDoesNotOverwriteInvalidation is the regression test for GAP-BV-058
// (BV-COMMIT-005, BV-COMMIT-011, BV-REVALIDATE-003).
//
// A set-mined tagging run that is in flight when the block is invalidated must not leave the block
// invalid with mined_set=true and its transactions still tagged. InvalidateBlock clears mined_set so
// the block is processed again; an unconditional SetBlockMinedSet at the end of the run used to
// overwrite that, after which the setMined worker's MinedSet guard and the mined-not-set sweep both
// skipped the block for good. SetBlockMinedSet is now conditional on the mined_set generation the run
// started from, and setTxMinedStatus asks the worker to re-queue the block when the write is
// refused. The race is made
// deterministic with a UTXO-store decorator that parks the tagging run inside SetMinedMulti while
// InvalidateBlock runs on the real sqlitememory store.
func TestGapBV058_InFlightTaggingDoesNotOverwriteInvalidation(t *testing.T) {
	f := newGap058Fixture(t)
	ctx := f.ctx

	runErr := f.runParkedTagging(t, func() {
		_, err := f.blockchainClient.InvalidateBlock(ctx, f.block.Hash())
		require.NoError(t, err)

		_, midMeta, err := f.blockchainClient.GetBlockHeader(ctx, f.block.Hash())
		require.NoError(t, err)
		require.True(t, midMeta.Invalid, "precondition: block is invalid while the run is parked")
		require.False(t, midMeta.MinedSet, "precondition: InvalidateBlock cleared mined_set")
	})

	_, endMeta, err := f.blockchainClient.GetBlockHeader(ctx, f.block.Hash())
	require.NoError(t, err)
	require.True(t, endMeta.Invalid, "the block is still invalid at the end")

	endTags, err := f.utxoStore.Get(ctx, f.tx.TxIDChainHash())
	require.NoError(t, err)

	tagsPresent := len(endTags.BlockIDs) > 0

	require.False(t, endMeta.MinedSet, "the refused write must leave mined_set=false so the sweep still lists the block")

	// a refused write is not a failed run: the run returns the re-queue error, which the
	// worker handles without charging the retry counter
	f.requireRequeued(t, runErr)

	require.False(t, endMeta.MinedSet && tagsPresent,
		"GAP-BV-058: block is invalid, yet the in-flight run left mined_set=true with the transaction still tagged (BlockIDs=%v); the unconditional SetBlockMinedSet overwrote the invalidation and the sweep will never revisit it", endTags.BlockIDs)
}

// TestGapBV058_InFlightTaggingDoesNotOverwriteClearBlockMinedSet covers the reset that does not
// touch the invalid flag. Catchup calls ClearBlockMinedSet on a block that moved off the main chain.
// A run that started before, with onLongestChain=true, used to match a guard on invalid and write
// mined_set=true over the clear, after which the re-queued run skipped on MinedSet=true. The
// generation guard refuses that write, which is all this test proves.
//
// It does not prove the catchup fork case is closed. Catchup clears mined_set on the old-chain
// blocks before the reorg (catchup.go, step 5.5), so the re-queued run still finds the block on the
// main chain, tags it with onLongestChain=true and sets mined_set=true, and nothing clears it again
// after the real reorg. That ordering predates this change and needs a clear after the reorg.
func TestGapBV058_InFlightTaggingDoesNotOverwriteClearBlockMinedSet(t *testing.T) {
	f := newGap058Fixture(t)
	ctx := f.ctx

	runErr := f.runParkedTagging(t, func() {
		require.NoError(t, f.blockchainClient.ClearBlockMinedSet(ctx, f.block.Hash()))
	})

	_, endMeta, err := f.blockchainClient.GetBlockHeader(ctx, f.block.Hash())
	require.NoError(t, err)
	require.False(t, endMeta.Invalid, "precondition: the clear left invalid untouched")
	require.False(t, endMeta.MinedSet, "the run must not write mined_set=true over ClearBlockMinedSet")

	f.requireRequeued(t, runErr)
}

// TestGapBV058_InvalidFlagMovedBeforeRunStarts covers the window between the worker's read of
// the invalid flag and the run's own GetBlockHeader. The generation is captured at the second read,
// so it already counts an invalidation that landed in between and cannot catch it. The run must
// notice the caller's flag disagrees with the one it read, and re-queue without tagging.
func TestGapBV058_InvalidFlagMovedBeforeRunStarts(t *testing.T) {
	f := newGap058Fixture(t)
	ctx := f.ctx

	// the worker read invalid=false, then the block was invalidated before the run started
	_, err := f.blockchainClient.InvalidateBlock(ctx, f.block.Hash())
	require.NoError(t, err)

	runErr := f.bv.setTxMinedStatus(ctx, f.block.Hash(), false)

	_, endMeta, err := f.blockchainClient.GetBlockHeader(ctx, f.block.Hash())
	require.NoError(t, err)
	require.True(t, endMeta.Invalid)
	require.False(t, endMeta.MinedSet, "a run started on the stale flag must not set mined_set")

	endTags, err := f.utxoStore.Get(ctx, f.tx.TxIDChainHash())
	require.NoError(t, err)
	require.Empty(t, endTags.BlockIDs, "a run started on the stale flag must not tag the invalid block's transaction")

	f.requireRequeued(t, runErr)
}
