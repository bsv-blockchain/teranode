//go:build bvgaps

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

// TestGapBV058_InFlightTaggingDoesNotOverwriteInvalidation is the gap-proof test for GAP-BV-058
// (BV-COMMIT-005, BV-COMMIT-011, BV-REVALIDATE-003).
//
// Spec-correct behaviour: the mined_set / subtrees_set flag writes must be conditional on the block's
// state at the start of the run. A set-mined tagging run that is in flight when the block is
// invalidated must not leave the block invalid with mined_set=true and its transactions still
// tagged: either the flag write is refused, or the tags are reverted. The end state "invalid AND
// mined_set AND tags present" is the violation.
//
// Why it is expected to fail today: stores/blockchain/sql/SetBlockMinedSet.go is an unconditional
// `UPDATE blocks SET mined_set = true WHERE hash = $1`, and BlockValidation.setTxMinedStatus
// (BlockValidation.go ~1562) calls it after tagging without re-checking the invalid flag it read at
// the start. InvalidateBlock clears mined_set (InvalidateBlock.go ~110) but the in-flight run
// overwrites it; the BlockMinedUnset notification for a hash already being processed is skipped and
// the sweep no longer lists the block. The race is made deterministic with a UTXO-store decorator that
// parks the tagging run inside SetMinedMulti while InvalidateBlock runs on the real sqlitememory store.
func TestGapBV058_InFlightTaggingDoesNotOverwriteInvalidation(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	// Validate and store the block (mined_set=false, subtrees_set=true); no tagging has run yet.
	require.NoError(t, bv.ValidateBlock(ctx, block, "test"))

	_, preMeta, err := blockchainClient.GetBlockHeader(ctx, block.Hash())
	require.NoError(t, err)
	require.False(t, preMeta.MinedSet, "precondition: tagging has not run yet")
	require.False(t, preMeta.Invalid, "precondition: block is valid when the run starts")

	blocking := &gap058BlockingStore{
		Store:   realUtxoStore,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	bv.utxoStore = blocking
	blocking.armed.Store(true)

	// The tagging run starts (it read invalid=false) and parks inside SetMinedMulti.
	runDone := make(chan error, 1)

	go func() {
		runDone <- bv.setTxMinedStatus(ctx, block.Hash())
	}()

	select {
	case <-blocking.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("precondition: the tagging run never reached SetMinedMulti")
	}

	// While the run is in flight the block is invalidated.
	_, err = blockchainClient.InvalidateBlock(ctx, block.Hash())
	require.NoError(t, err)

	_, midMeta, err := blockchainClient.GetBlockHeader(ctx, block.Hash())
	require.NoError(t, err)
	require.True(t, midMeta.Invalid, "precondition: block is invalid while the run is parked")
	require.False(t, midMeta.MinedSet, "precondition: InvalidateBlock cleared mined_set")

	// Release the parked run and let it finish.
	close(blocking.release)

	select {
	case runErr := <-runDone:
		t.Logf("in-flight setTxMinedStatus returned: %v", runErr)
	case <-time.After(20 * time.Second):
		t.Fatal("the tagging run did not finish after release")
	}

	_, endMeta, err := blockchainClient.GetBlockHeader(ctx, block.Hash())
	require.NoError(t, err)
	require.True(t, endMeta.Invalid, "the block is still invalid at the end")

	endTags, err := realUtxoStore.Get(ctx, txT.TxIDChainHash())
	require.NoError(t, err)

	tagsPresent := len(endTags.BlockIDs) > 0

	require.False(t, endMeta.MinedSet && tagsPresent,
		"GAP-BV-058: block is invalid, yet the in-flight run left mined_set=true with the transaction still tagged (BlockIDs=%v); the unconditional SetBlockMinedSet overwrote the invalidation and the sweep will never revisit it", endTags.BlockIDs)
}
