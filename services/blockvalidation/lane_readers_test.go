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
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	blockchainsql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/lanereader"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// laneReaderAnswer is teranode_lane_reader_answers_total for one reader and kind, over every via.
func laneReaderAnswer(t *testing.T, reader, kind string) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	var sum float64

	for _, f := range families {
		if f.GetName() != "teranode_lane_reader_answers_total" {
			continue
		}

		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}

			if labels["reader"] == reader && labels["kind"] == kind {
				sum += m.GetCounter().GetValue()
			}
		}
	}

	return sum
}

// At off there is no lane index: the parent check's context is unchanged and setMined's verdict
// passes through with nothing compared.
func TestLaneReadersDoNothingAtOff(t *testing.T) {
	lanereader.Start(context.Background(), nil, test.CreateBaseTestSettings(t), nil)

	ctx := context.Background()
	blk := &model.Block{Header: &model.BlockHeader{HashPrevBlock: &chainhash.Hash{1}}}

	require.Equal(t, ctx, laneParentsContext(ctx, blk))

	verdict := errors.NewBlockInvalidError("duplicate")
	require.Equal(t, verdict, shadowLaneDuplicates(ctx, blk, 1, nil, verdict))
	require.NoError(t, shadowLaneDuplicates(ctx, blk, 1, nil, nil))
}

// At shadow the duplicate check's verdict is compared with the lane index in the background, and
// returned unchanged; an unset and a verdict that is neither a pass nor a duplicate are not
// compared.
func TestShadowLaneDuplicatesComparesSetMinedsVerdict(t *testing.T) {
	u, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.LaneMigration = settings.LaneMigrationShadow

	store, err := blockchainsql.New(ulogger.TestLogger{}, u, tSettings)
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close(context.Background()) })

	genesis, _, err := store.GetBestBlockHeader(context.Background())
	require.NoError(t, err)

	coinbase, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030100002f6d312d65752f29c267ffea1adb87f33b398fffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")
	require.NoError(t, err)

	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	blk := &model.Block{
		Header: &model.BlockHeader{
			Version: 1, Timestamp: 1729259800, Nonce: 77, HashPrevBlock: genesis.Hash(),
			HashMerkleRoot: &chainhash.Hash{}, Bits: *bits,
		},
		Height:           1,
		CoinbaseTx:       coinbase,
		TransactionCount: 2,
	}

	rows, err := lane.BuildMapRows(3, lanePositions(5))
	require.NoError(t, err)

	id, _, err := store.StoreBlock(context.Background(), blk, "", blockchainoptions.WithLaneMapRows(rows))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		lanereader.Start(context.Background(), nil, test.CreateBaseTestSettings(t), nil)
	})

	r := lanereader.Start(ctx, ulogger.TestLogger{}, tSettings, store)
	require.Eventually(t, func() bool { return r.Snapshot() != nil }, 10*time.Second, 10*time.Millisecond)

	agree := laneReaderAnswer(t, lanereader.ReaderBlockDuplicate, lanereader.KindAgree)
	notLoaded := laneReaderAnswer(t, lanereader.ReaderBlockDuplicate, lanereader.KindNotLoaded)

	require.NoError(t, shadowLaneDuplicates(ctx, blk, uint32(id), nil, nil)) //nolint:gosec // a test block ID
	require.Eventually(t, func() bool {
		return laneReaderAnswer(t, lanereader.ReaderBlockDuplicate, lanereader.KindAgree) == agree+1
	}, 10*time.Second, 10*time.Millisecond)

	verdict := errors.NewBlockInvalidError("contains a transaction already on our chain")
	require.Equal(t, verdict, shadowLaneDuplicates(ctx, blk, uint32(id), nil, verdict)) //nolint:gosec // a test block ID
	require.Eventually(t, func() bool {
		return laneReaderAnswer(t, lanereader.ReaderBlockDuplicate, lanereader.KindNotLoaded) == notLoaded+1
	}, 10*time.Second, 10*time.Millisecond)

	// Neither an unset nor a processing error is compared.
	other := errors.NewProcessingError("store down")
	require.Equal(t, other, shadowLaneDuplicates(ctx, blk, uint32(id), nil, other))   //nolint:gosec // a test block ID
	require.NoError(t, shadowLaneDuplicates(ctx, blk, uint32(id), []bool{true}, nil)) //nolint:gosec // a test block ID

	time.Sleep(100 * time.Millisecond)
	require.Equal(t, agree+1, laneReaderAnswer(t, lanereader.ReaderBlockDuplicate, lanereader.KindAgree))
	require.Equal(t, notLoaded+1, laneReaderAnswer(t, lanereader.ReaderBlockDuplicate, lanereader.KindNotLoaded))

	// The parent check's context carries the shadow at shadow.
	require.NotEqual(t, ctx, laneParentsContext(ctx, blk))
}

// startLaneReaderOn starts the process's lane reader at shadow on store and waits for its first
// snapshot; stop ends its follower.
func startLaneReaderOn(t *testing.T, tSettings *settings.Settings, store lanereader.Source) (r *lanereader.Reader, stop func()) {
	t.Helper()

	shadow := *tSettings
	shadow.UtxoStore.LaneMigration = settings.LaneMigrationShadow

	ctx, cancel := context.WithCancel(context.Background())

	r = lanereader.Start(ctx, ulogger.TestLogger{}, &shadow, store)
	require.Eventually(t, func() bool { return r.Snapshot() != nil }, 10*time.Second, 10*time.Millisecond)

	return r, cancel
}

// The real hooks count block validation's two checks: ValidateBlock's parent check under
// block_parent, and setTxMinedStatus's duplicate check, on the block's own rows, under
// block_duplicate.
func TestBlockValidationCountsItsLaneShadowsThroughTheRealHooks(t *testing.T) {
	for _, optimistic := range []bool{false, true} {
		name := "full validation"
		if optimistic {
			name = "optimistic mining, validated in the background"
		}

		t.Run(name, func(t *testing.T) { countsLaneShadowsThroughTheRealHooks(t, optimistic) })
	}
}

func countsLaneShadowsThroughTheRealHooks(t *testing.T, optimistic bool) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = optimistic
	tSettings.BlockValidation.OptimisticMiningPeerBlocks = optimistic

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	utxoStoreURL, err := url.Parse("sqlitememory:///laneshadow" + map[bool]string{false: "full", true: "optimistic"}[optimistic])
	require.NoError(t, err)

	realUtxoStore, err := utxosql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	subtreeStore := blobmemory.New()

	subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
	subtreeValidationClient.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	// The block's one transaction spends fundingTx, a parent outside the block.
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

	coinbaseTx := coinbaseAtHeight(t, 1)

	replicated := subtree.Duplicate()
	replicated.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec

	hdr := minedBIP34Header(t, 4, tSettings.ChainCfgParams.GenesisHash, replicated.RootHash())

	block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{subtree.RootHash()},
		uint64(subtree.Length()), uint64(coinbaseTx.Size()+txT.Size()), 1, 0) //nolint:gosec
	require.NoError(t, err)

	require.NoError(t, blockchainClient.SetBlockMinedSet(ctx, tSettings.ChainCfgParams.GenesisHash, 0))

	bv := &BlockValidation{
		logger:                        logger,
		settings:                      tSettings,
		blockchainClient:              blockchainClient,
		subtreeStore:                  subtreeStore,
		txStore:                       blobmemory.New(),
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

	t.Cleanup(func() { lanereader.Start(context.Background(), nil, test.CreateBaseTestSettings(t), nil) })

	// The parent check: fundingTx is outside the block, so ValidateBlock's parent read is told to
	// the shadow, which counts it under block_parent.
	_, stop := startLaneReaderOn(t, tSettings, blockChainStore)

	parentAnswers := laneReaderAnswerAll(t, lanereader.ReaderBlockParent)

	require.NoError(t, bv.ValidateBlock(ctx, block, "test"))
	require.Eventually(t, func() bool { return laneReaderAnswerAll(t, lanereader.ReaderBlockParent) == parentAnswers+1 }, 10*time.Second, 10*time.Millisecond)

	stop()

	// The duplicate check: give the stored block rows, then start a fresh index that loads them,
	// so the comparison on the block's own ID finds rows and agrees, where any other block's ID
	// (the parent, genesis) would count no_rows.
	_, blockMeta, err := blockchainClient.GetBlockHeader(ctx, block.Hash())
	require.NoError(t, err)

	rows, err := lane.BuildMapRows(3, lanePositions(1))
	require.NoError(t, err)
	require.NoError(t, blockChainStore.InsertLaneMapRows(ctx, blockMeta.ID, rows))

	r, stop := startLaneReaderOn(t, tSettings, blockChainStore)
	defer stop()

	require.Eventually(t, func() bool { return r.Snapshot().TipHeight() == 1 }, 10*time.Second, 10*time.Millisecond)

	agree := laneReaderAnswer(t, lanereader.ReaderBlockDuplicate, lanereader.KindAgree)

	require.NoError(t, bv.setTxMinedStatus(ctx, block.Hash()))
	require.Eventually(t, func() bool {
		return laneReaderAnswer(t, lanereader.ReaderBlockDuplicate, lanereader.KindAgree) == agree+1
	}, 10*time.Second, 10*time.Millisecond, "the comparison runs on the block's own rows")
}

// laneReaderAnswerAll is teranode_lane_reader_answers_total for one reader, over every kind.
func laneReaderAnswerAll(t *testing.T, reader string) float64 {
	t.Helper()

	var sum float64

	for _, kind := range []string{lanereader.KindAgree, lanereader.KindLanesOnly, lanereader.KindTodayOnly, lanereader.KindDifferentBlock,
		lanereader.KindNoPosition, lanereader.KindNoRows, lanereader.KindNotLoaded, lanereader.KindNotReady, lanereader.KindOffChain} {
		sum += laneReaderAnswer(t, reader, kind)
	}

	return sum
}
