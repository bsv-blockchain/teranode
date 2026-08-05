package blockvalidation

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestOptimisticCorrupt_InvalidateFailure_EscalatesNotSilentlyAccepted is the C2 guarantee
// (bitcoin-sv/teranode#4692): on the opt-in optimistic-background path a corrupt body was already
// AddBlock'd before block.Valid ran, so if the invalidate route's InvalidateBlock FAILS the block
// must NOT be left quietly on-chain — the guard escalates by re-queuing the block for revalidation
// (u.ReValidateBlock), which retries until the store recovers. Fault injection: InvalidateBlock is
// mocked to fail. Behaviour asserted: (1) InvalidateBlock is attempted, and (2) the block is
// re-queued on revalidateBlockChan (proving it is not silently accepted). A struct-literal
// BlockValidation is used (no background revalidate worker) so the re-queue can be observed on the
// channel directly — the same pattern existing revalidation tests use.
func TestOptimisticCorrupt_InvalidateFailure_EscalatesNotSilentlyAccepted(t *testing.T) {
	initPrometheusMetrics()
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	// Opt in to optimistic mining on peer paths so the add-before-validate path runs.
	tSettings.BlockValidation.OptimisticMining = true
	tSettings.BlockValidation.OptimisticMiningPeerBlocks = true

	// Build a corrupt-body block: a real coinbase + subtree, but a zeroed merkle root, so the
	// background block.Valid fails CheckMerkleRoot -> ERR_BLOCK_CORRUPT.
	privateKey, _ := bec.NewPrivateKey()
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	coinbaseTx := bt.NewTx()
	_ = coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})
	_ = coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000)

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)
	httpmock.RegisterResponder("GET", `=~^/subtree/[a-z0-9]+\z`, httpmock.NewBytesResponder(200, nodeBytes))

	subtreeStore := blobmemory.New()
	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	nBits, _ := model.NewNBitFromString("2000ffff")
	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: &chainhash.Hash{},         // zeroed -> corrupt in the background
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}
	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}
		blockHeader.Nonce++
	}

	block, err := model.NewBlock(blockHeader, coinbaseTx, []*chainhash.Hash{subtree.RootHash()}, uint64(subtree.Length()), uint64(coinbaseTx.Size()), 100, 0) //nolint:gosec
	require.NoError(t, err)

	// Fault injection: InvalidateBlock always fails, and signals that it was attempted.
	invalidateAttempted := make(chan struct{}, 1)
	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	mockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return([]*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil)
	mockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{1}, nil)
	mockBlockchain.On("GetNextWorkRequired", mock.Anything, mock.Anything, mock.Anything).Return(nBits, nil).Maybe()
	mockBlockchain.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil).Maybe()
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 99, MinedSet: true}, nil).Maybe()
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 100}, nil).Maybe()
	mockBlockchain.On("AddBlock", mock.Anything, block, mock.Anything, mock.Anything).Return(nil)
	mockBlockchain.On("InvalidateBlock", mock.Anything, block.Header.Hash()).Return([]chainhash.Hash{}, errors.NewError("invalidate store unavailable")).Run(func(mock.Arguments) {
		select {
		case invalidateAttempted <- struct{}{}:
		default:
		}
	})

	// The block carries a subtree, so validateBlockSubtrees runs before the optimistic AddBlock;
	// let it pass so the corrupt verdict comes from the background block.Valid (zeroed merkle).
	subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
	subtreeValidationClient.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)
	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	// Buffered so the escalation's ReValidateBlock enqueue never blocks and can be observed here.
	revalidateChan := make(chan revalidateBlockData, 2)

	bv := &BlockValidation{
		logger:                        logger,
		settings:                      tSettings,
		blockchainClient:              mockBlockchain,
		subtreeStore:                  subtreeStore,
		txStore:                       blobmemory.New(),
		utxoStore:                     utxoStore,
		subtreeValidationClient:       subtreeValidationClient,
		lastValidatedBlocks:           expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
		blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
		subtreeExistsCache:            expiringmap.New[chainhash.Hash, bool](10 * time.Minute),
		blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
		blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
		setMinedChan:                  make(chan *chainhash.Hash, 1),
		revalidateBlockChan:           revalidateChan,
		stats:                         gocore.NewStat("blockvalidation"),
	}
	defer bv.StopCaches()

	// Optimistic path: ValidateBlock returns nil after the optimistic AddBlock; the corrupt verdict
	// and the escalation happen in the background goroutine.
	err = bv.ValidateBlock(ctx, block, "test")
	require.NoError(t, err)

	select {
	case <-invalidateAttempted:
		// good: the invalidate route was taken (block not left silently accepted)
	case <-time.After(2 * time.Second):
		t.Fatal("InvalidateBlock was not attempted on the optimistic-background corrupt path")
	}

	// The invalidate FAILED, so the escalation must re-queue the block for revalidation rather than
	// leave the optimistically-added corrupt body quietly on-chain.
	select {
	case data := <-revalidateChan:
		require.Equal(t, block.Hash(), data.block.Hash(), "the corrupt block must be re-queued for revalidation after InvalidateBlock fails")
	case <-time.After(2 * time.Second):
		t.Fatal("InvalidateBlock failed but the block was NOT re-queued (C2): a corrupt tip would be left silently accepted")
	}
}

// TestValidateBlock_SubtreeCorrupt_StrikeGatedOnRevalidation covers the fourth corrupt-strike gate
// (bitcoin-sv/teranode#4692): when subtree validation returns a corrupt-body verdict, the serving
// peer is struck on a normal (serving) delivery but NOT on the revalidation path — RevalidateBlock
// carries the original announcing peer's stale ID, which neither served this read nor is
// necessarily connected. Fault injection: CheckBlockSubtrees returns corrupt. Behaviour: a strike
// on the non-revalidation run, none on the revalidation run.
func TestValidateBlock_SubtreeCorrupt_StrikeGatedOnRevalidation(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// A mined block carrying a subtree, so the flow reaches validateBlockSubtrees.
	coinbaseTx := bt.NewTx()
	_ = coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	nBits, _ := model.NewNBitFromString("2000ffff")
	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}
	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}
		blockHeader.Nonce++
	}

	block, err := model.NewBlock(blockHeader, coinbaseTx, []*chainhash.Hash{subtree.RootHash()}, uint64(subtree.Length()), uint64(coinbaseTx.Size()), 100, 0) //nolint:gosec
	require.NoError(t, err)

	// run drives ValidateBlockWithOptions with a corrupt subtree verdict and returns the strikes the
	// fake p2p client recorded.
	run := func(isRevalidation bool) []corruptBanScoreCall {
		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)
		utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
		require.NoError(t, err)

		mockBlockchain := &blockchain.Mock{}
		mockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil).Maybe()
		// Revalidation requires the block to currently be marked invalid.
		mockBlockchain.On("GetBlockHeader", mock.Anything, block.Header.Hash()).Return(block.Header, &model.BlockHeaderMeta{Invalid: true}, nil).Maybe()
		mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 99, MinedSet: true}, nil).Maybe()
		mockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return([]*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil).Maybe()
		mockBlockchain.On("GetNextWorkRequired", mock.Anything, mock.Anything, mock.Anything).Return(nBits, nil).Maybe()
		mockBlockchain.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil).Maybe()
		mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 100}, nil).Maybe()

		subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
		subtreeValidationClient.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(errors.NewBlockCorruptError("corrupt subtree body"))

		fake := &corruptStrikeP2PClient{}

		bv := &BlockValidation{
			logger:                        logger,
			settings:                      tSettings,
			blockchainClient:              mockBlockchain,
			subtreeStore:                  blobmemory.New(),
			txStore:                       blobmemory.New(),
			utxoStore:                     utxoStore,
			subtreeValidationClient:       subtreeValidationClient,
			p2pClient:                     fake,
			lastValidatedBlocks:           expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
			blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
			subtreeExistsCache:            expiringmap.New[chainhash.Hash, bool](10 * time.Minute),
			blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
			blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
			setMinedChan:                  make(chan *chainhash.Hash, 1),
			revalidateBlockChan:           make(chan revalidateBlockData, 2),
			stats:                         gocore.NewStat("blockvalidation"),
		}
		defer bv.StopCaches()

		err = bv.ValidateBlockWithOptions(ctx, block, "test", &ValidateBlockOptions{
			IsRevalidation:          isRevalidation,
			DisableOptimisticMining: true,
			PeerID:                  "announcing-peer",
		})
		require.Error(t, err)
		require.True(t, errors.IsBlockCorrupt(err), "a corrupt subtree body must surface as corrupt, got: %v", err)

		return fake.recorded()
	}

	// Normal serving delivery: the serving peer IS struck.
	serving := run(false)
	require.Len(t, serving, 1, "a non-revalidation corrupt subtree must strike the serving peer once")
	require.Equal(t, "announcing-peer", serving[0].peerID)

	// Revalidation: the stale announcing peer must NOT be struck.
	require.Empty(t, run(true), "revalidation must NOT strike the stale announcing peer (bitcoin-sv/teranode#4692)")
}
