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

// ancestorVerdictCoinbaseOnlyBlock builds and mines a coinbase-only block on top of parent.
func ancestorVerdictCoinbaseOnlyBlock(t *testing.T, parent *chainhash.Hash, height uint32) *model.Block {
	t.Helper()

	return ancestorVerdictCoinbaseOnlyBlockAt(t, parent, height, 0)
}

// ancestorVerdictCoinbaseOnlyBlockAt is ancestorVerdictCoinbaseOnlyBlock with an explicit header
// timestamp, re-mined after setting it. A timestamp of 0 keeps the helper's time.Now(). A chain of
// more than a few blocks needs rising timestamps, or the median-time-past check rejects the child.
func ancestorVerdictCoinbaseOnlyBlockAt(t *testing.T, parent *chainhash.Hash, height uint32, timestamp uint32) *model.Block {
	t.Helper()

	coinbaseTx := coinbaseAtHeight(t, height)
	hdr := minedBIP34Header(t, 4, parent, coinbaseTx.TxIDChainHash())
	remineAt(t, hdr, timestamp)

	block, err := model.NewBlock(hdr, coinbaseTx, nil, 1, uint64(coinbaseTx.Size()), height, 0) //nolint:gosec
	require.NoError(t, err)

	return block
}

// TestReValidateBlock_InvalidAncestorVerdictInvalidatesAndPublishes covers GAP-BV-043
// (BV-OPTIMISTIC-005, BV-VERDICT-009, bitcoin-sv/teranode#4980).
//
// When internal revalidation of an optimistically added block reaches the ancestor-membership
// check (checkOldBlockIDs) and that check returns an INVALID verdict, because a parent transaction
// is mined only in a block that is not an ancestor of this one, reValidateBlock must invalidate the
// block in the blockchain store and publish an invalid-block notification. Before the fix it handed
// the verdict back to the revalidation worker unacted, so the block stayed on the chain once the
// worker's retries ran out and nothing was published.
//
// Fixture: a real sqlitememory blockchain store with main chain genesis-A and a fork block F. The
// funding transaction is stamped as mined in F only, and the child block C (on A) spends it, so
// Block.Valid passes but the ancestor check says the parent is not on C's chain.
func TestReValidateBlock_InvalidAncestorVerdictInvalidatesAndPublishes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bv, blockchainClient, blockC, kafkaProducer := newAncestorVerdictFixture(ctx, t, true, false)

	err := bv.reValidateBlock(revalidateBlockData{block: blockC, baseURL: "test", optimisticallyAdded: true})

	// The verdict is still returned after acting on it, like every other branch of reValidateBlock.
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "expected the ancestor check's ErrBlockInvalid verdict, got: %v", err)

	_, cMeta, err := blockchainClient.GetBlockHeader(ctx, blockC.Hash())
	require.NoError(t, err)
	require.NotNil(t, cMeta)

	require.True(t, cMeta.Invalid,
		"reValidateBlock returned the INVALID ancestor verdict without invalidating the optimistically added block")
	require.True(t, kafkaProducer.IsPublishCalled(),
		"no invalid-block notification was published for the INVALID ancestor verdict")
}

// TestReValidateBlock_InvalidAncestorVerdictNeverStoredBlockStillPublishes covers the block that
// reaches reValidateBlock without ever being stored: the non-optimistic path enqueues it when
// fetching its parent headers fails. The SQL store's InvalidateBlock treats an unknown hash as a
// no-op success, so nothing is written, and the invalid-block notification still reaches p2p.
func TestReValidateBlock_InvalidAncestorVerdictNeverStoredBlockStillPublishes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bv, blockchainClient, blockC, kafkaProducer := newAncestorVerdictFixture(ctx, t, false, false)

	err := bv.reValidateBlock(revalidateBlockData{block: blockC, baseURL: "test"})
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "expected the ancestor check's ErrBlockInvalid verdict, got: %v", err)

	exists, err := blockchainClient.GetBlockExists(ctx, blockC.Hash())
	require.NoError(t, err)
	require.False(t, exists, "a never-stored block must not be written by the revalidation path")

	require.True(t, kafkaProducer.IsPublishCalled(),
		"no invalid-block notification was published for a never-stored block with an INVALID ancestor verdict")
}

// TestReValidateBlock_InvalidAncestorVerdictOnForkChainIsNotActedOn covers the false INVALID that
// the revalidation path must not act on. checkOldBlockIDs asks whether a parent's block is on the
// MAIN chain, not whether it is an ancestor of this block. The two agree only while this block's
// own parent is on the main chain. On the in-memory chain-check route every parent block deeper
// than blockvalidation_previous_block_header_count goes straight to that main-chain lookup. So an
// honest block whose chain a reorg has moved off the main chain reads as spending an off-chain
// parent.
//
// Fixture: fork chain genesis-F1..F12-C, with the funding tx mined in F1 and C spending it, and a
// longer main chain genesis-A1..A14. C is honest, because F1 is its own ancestor. F1 is 12 blocks
// below C, past the 11-header window, so the ancestor check resolves it against the main chain and
// says INVALID. reValidateBlock must hand that verdict back unacted, as it did before the INVALID
// ancestor branch existed, because C's parent F12 is not on the main chain.
func TestReValidateBlock_InvalidAncestorVerdictOnForkChainIsNotActedOn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bv, blockchainClient, blockC, kafkaProducer := newAncestorVerdictFixture(ctx, t, true, true)

	err := bv.reValidateBlock(revalidateBlockData{block: blockC, baseURL: "test"})

	// The verdict itself is still wrong on this route. What this test pins is that it is not acted on.
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "expected the main-chain ancestor check to return ErrBlockInvalid for a block on a fork chain, got: %v", err)

	_, cMeta, err := blockchainClient.GetBlockHeader(ctx, blockC.Hash())
	require.NoError(t, err)
	require.NotNil(t, cMeta)

	require.False(t, cMeta.Invalid,
		"reValidateBlock invalidated an honest block whose parent is off the main chain, on a main-chain verdict")
	require.False(t, kafkaProducer.IsPublishCalled(),
		"reValidateBlock published an invalid-block notification for an honest block whose parent is off the main chain")
}

// newAncestorVerdictFixture builds the GAP-BV-043 fixture and returns a BlockValidation wired to
// real sqlitememory stores, the blockchain client, the child block C and the Kafka producer. When
// storeC is false, C is never added to the blockchain store.
//
// When deepFork is true it builds the fixture TestReValidateBlock_InvalidAncestorVerdictOnForkChainIsNotActedOn
// describes instead: C is honest, on a fork chain F1..F12 that funds it from F1, and a longer main
// chain A1..A14 has moved that fork off the main chain. It turns on the in-memory chain check and
// sets the parent-header window to its floor of 11, so F1 falls outside the window.
func newAncestorVerdictFixture(ctx context.Context, t *testing.T, storeC bool, deepFork bool) (*BlockValidation, blockchain.ClientI, *model.Block, *SafeMockKafkaProducer) {
	t.Helper()

	initPrometheusMetrics()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	const (
		deepForkLength      = 12
		deepForkMainLength  = deepForkLength + 2
		deepForkHeaderCount = 11 // the settings floor, MedianTimeSpan
	)

	if deepFork {
		tSettings.BlockChain.UseInMemoryChainCheck = true
		tSettings.BlockValidation.PreviousBlockHeaderCount = deepForkHeaderCount
	}

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	utxoStoreURL, err := url.Parse("sqlitememory:///revalidateancestorverdict")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	subtreeStore := blobmemory.New()
	txStore := blobmemory.New()

	subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
	subtreeValidationClient.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	var (
		blockA         *model.Block // main-chain tip
		blockF         *model.Block // the block the funding tx is mined in
		childParent    *model.Block // C's parent
		childHeight    = uint32(2)
		childTimestamp = uint32(0) // 0 keeps time.Now()
	)

	if deepFork {
		// Rising timestamps ending in the past, so every block passes median-time-past.
		baseTime := uint32(time.Now().Add(-time.Hour).Unix()) //nolint:gosec

		// The fork first, the chain C was validated on, then a longer main chain that moves it off.
		parent := tSettings.ChainCfgParams.GenesisHash

		for h := uint32(1); h <= deepForkLength; h++ {
			b := ancestorVerdictCoinbaseOnlyBlockAt(t, parent, h, baseTime+h)
			require.NoError(t, blockchainClient.AddBlock(ctx, b, "test"))

			if h == 1 {
				blockF = b
			}

			childParent = b
			parent = b.Hash()
		}

		parent = tSettings.ChainCfgParams.GenesisHash

		for h := uint32(1); h <= deepForkMainLength; h++ {
			blockA = ancestorVerdictCoinbaseOnlyBlockAt(t, parent, h, baseTime+h)
			require.NoError(t, blockchainClient.AddBlock(ctx, blockA, "test"))
			parent = blockA.Hash()
		}

		childHeight = deepForkLength + 1
		childTimestamp = baseTime + childHeight
	} else {
		// Main chain: genesis -> A. Fork: genesis -> F (stored second, equal work, so A stays the tip).
		blockA = ancestorVerdictCoinbaseOnlyBlock(t, tSettings.ChainCfgParams.GenesisHash, 1)
		require.NoError(t, blockchainClient.AddBlock(ctx, blockA, "test"))

		// coinbaseAtHeight draws a fresh key per call, so F's merkle root, and so its hash, differs from A's.
		blockF = ancestorVerdictCoinbaseOnlyBlock(t, tSettings.ChainCfgParams.GenesisHash, 1)
		require.NotEqual(t, blockA.Hash().String(), blockF.Hash().String())
		require.NoError(t, blockchainClient.AddBlock(ctx, blockF, "test"))

		childParent = blockA
	}

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

	coinbaseTx := coinbaseAtHeight(t, childHeight)

	replicated := subtree.Duplicate()
	replicated.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec

	hdr := minedBIP34Header(t, 4, childParent.Hash(), replicated.RootHash())
	remineAt(t, hdr, childTimestamp)

	blockC, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{subtree.RootHash()},
		uint64(subtree.Length()), uint64(coinbaseTx.Size()+childTx.Size()), childHeight, 0) //nolint:gosec
	require.NoError(t, err)

	if storeC {
		// The block was added optimistically before validation completed.
		require.NoError(t, blockchainClient.AddBlock(ctx, blockC, "test"))
	}

	if deepFork {
		onMain, err := blockchainClient.CheckBlockIsInCurrentChain(ctx, []uint32{fMeta.ID})
		require.NoError(t, err)
		require.False(t, onMain, "precondition: F1, which funds C, is off the main chain")
	}

	kafkaProducer := &SafeMockKafkaProducer{}

	bv := NewBlockValidation(ctx, logger, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)
	t.Cleanup(bv.StopCaches)

	bv.invalidBlockKafkaProducer = kafkaProducer

	return bv, blockchainClient, blockC, kafkaProducer
}

// remineAt sets hdr's timestamp and mines it again. A timestamp of 0 leaves hdr unchanged.
func remineAt(t *testing.T, hdr *model.BlockHeader, timestamp uint32) {
	t.Helper()

	if timestamp == 0 {
		return
	}

	hdr.Timestamp = timestamp
	hdr.Nonce = 0

	for {
		if ok, _, _ := hdr.HasMetTargetDifficulty(); ok {
			return
		}

		hdr.Nonce++
	}
}
