package blockvalidation

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/blockvalidation_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jellydator/ttlcache/v3"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestValidateBlock_UnusableHeaderRunIsRebuiltByHashWalk pins the recovery the re-queue could
// never deliver.
//
// GetBlockHeaders memoizes its answer per (parentHash, count) key for up to chainWalkCacheTTL —
// ten minutes. The re-queue's four attempts all fall inside ~600ms of each other, so every one
// of them replayed the identical unusable run and the block was dropped regardless. The fix is
// the fallback netsync, subtreevalidation and blockassembly already use for the same problem:
// re-derive the run by walking HashPrevBlock through GetBlockHeader, which is keyed by hash and
// therefore neither range-memoized nor sensitive to which block is canonical.
//
// So the store here NEVER returns a usable run — not on the first call, not on any later one.
// Only the hash walk can produce the genesis-rooted chain, and the block must be validated and
// stored on the first attempt. IsRequeuedRetry is set to forbid the re-queue outright: this is
// an assertion that recovery happens inline, not that a retry eventually stumbles into it.
func TestValidateBlock_UnusableHeaderRunIsRebuiltByHashWalk(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, cleanup := setup(t)
	defer cleanup()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = true

	coinbaseTx, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	require.NoError(t, subtreeData.AddTx(coinbaseTx, 0))

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	now := uint32(time.Now().Add(-time.Hour).Unix())

	// The real chain, rooted at genesis: genesis <- greatGrandparent <- grandparent <- parent.
	// Three deep on purpose, so the truncated run below can be anchored AND linked and still be
	// wrong — see the stale run's comment.
	genesisHash := &chainhash.Hash{}

	greatGrandparent := &model.BlockHeader{
		Version: 1, HashPrevBlock: genesisHash, HashMerkleRoot: &chainhash.Hash{},
		Timestamp: now, Bits: *nBits,
	}
	mineHeader(t, greatGrandparent)

	grandparent := &model.BlockHeader{
		Version: 1, HashPrevBlock: greatGrandparent.Hash(), HashMerkleRoot: &chainhash.Hash{},
		Timestamp: now + 1, Bits: *nBits,
	}
	mineHeader(t, grandparent)

	parent := &model.BlockHeader{
		Version: 1, HashPrevBlock: grandparent.Hash(), HashMerkleRoot: &chainhash.Hash{},
		Timestamp: now + 2, Bits: *nBits,
	}
	mineHeader(t, parent)

	// What the range fetch keeps handing back: the true parent chain truncated at its OLDEST
	// end. It is anchored at the block's parent and it is correctly linked, so neither of those
	// guards can see anything wrong with it — only its length gives it away, and the median it
	// would yield comes from a narrower window and is biased high. This is the case the hash
	// walk exists for.
	staleRun := []*model.BlockHeader{parent, grandparent}
	require.False(t, staleRun[len(staleRun)-1].HashPrevBlock.IsEqual(genesisHash),
		"the stale run must not reach genesis, or the carve-out would legitimately accept it")

	blockHeader := &model.BlockHeader{
		Version: 1, HashPrevBlock: parent.Hash(), HashMerkleRoot: subtree.RootHash(),
		Timestamp: now + 100, Bits: *nBits,
	}
	mineHeader(t, blockHeader)

	block := &model.Block{
		Header:           blockHeader,
		Subtrees:         []*chainhash.Hash{subtree.RootHash()},
		Height:           3,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: uint64(subtree.Length()),
		SizeInBytes:      123123,
	}

	mockBlockchain := new(blockchain.Mock)
	mockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil).Maybe()

	// The hash-keyed reads, registered per hash ahead of the catch-all so the walk can follow
	// parent -> grandparent -> genesis. This is the ONLY route to a usable run in this test.
	var walkHops atomic.Int32

	mockBlockchain.On("GetBlockHeader", mock.Anything, parent.Hash()).
		Return(parent, &model.BlockHeaderMeta{ID: 2, Height: 2}, nil).
		Run(func(mock.Arguments) { walkHops.Add(1) }).Maybe()
	mockBlockchain.On("GetBlockHeader", mock.Anything, grandparent.Hash()).
		Return(grandparent, &model.BlockHeaderMeta{ID: 1, Height: 1}, nil).
		Run(func(mock.Arguments) { walkHops.Add(1) }).Maybe()
	mockBlockchain.On("GetBlockHeader", mock.Anything, greatGrandparent.Hash()).
		Return(greatGrandparent, &model.BlockHeaderMeta{ID: 0, Height: 0}, nil).
		Run(func(mock.Arguments) { walkHops.Add(1) }).Maybe()
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(parent, &model.BlockHeaderMeta{ID: 2, Height: 2}, nil).Maybe()

	// Every range fetch is unusable, for the whole life of the test.
	mockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
		Return(staleRun,
			[]*model.BlockHeaderMeta{{ID: 2, Height: 2}, {ID: 1, Height: 1}}, nil).Maybe()

	mockBlockchain.On("GetNextWorkRequired", mock.Anything, mock.Anything, mock.Anything).Return(nBits, nil).Maybe()
	mockBlockchain.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil).Maybe()
	mockBlockchain.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil).Maybe()
	mockBlockchain.On("GetBlocksSubtreesNotSet", mock.Anything).Return([]*model.Block{}, nil).Maybe()
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(parent, &model.BlockHeaderMeta{Height: 2}, nil).Maybe()
	mockBlockchain.On("GetBlock", mock.Anything, mock.Anything).Return(&model.Block{
		Header: parent, CoinbaseTx: coinbaseTx, Subtrees: []*chainhash.Hash{}, Height: 2,
	}, nil).Maybe()
	mockBlockchain.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).
		Return([]uint32{2, 1, 0}, nil).Maybe()
	mockBlockchain.On("SetBlockMinedSet", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockBlockchain.On("InvalidateBlock", mock.Anything, mock.Anything).Return([]chainhash.Hash{}, nil).Maybe()
	mockBlockchain.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(true, nil).Maybe()

	notificationChan := make(chan *blockchain_api.Notification, 1)
	mockBlockchain.On("Subscribe", mock.Anything, mock.Anything).Return(notificationChan, nil).Maybe()

	var added atomic.Int32

	mockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Run(func(mock.Arguments) { added.Add(1) }).Maybe()

	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, mockBlockchain, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	subtreeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes))

	// IsRequeuedRetry forbids the re-queue, so nothing but an inline repair can rescue this.
	err = bv.ValidateBlockWithOptions(ctx, block, "", &ValidateBlockOptions{
		IsRequeuedRetry: true,
	})

	require.NoError(t, err, "the hash walk must rebuild a usable run when the range fetch cannot")
	require.Positive(t, walkHops.Load(), "recovery must go through the hash-keyed walk, not the range fetch")
	require.Positive(t, added.Load(), "a repaired header run must end with the block stored")
}

// TestServerValidateBlock_UnusableHeaderRunIsRebuiltByHashWalk is the gRPC counterpart of the
// test above.
//
// Server.ValidateBlock was the one block.Valid call site still fetching its parent run with a
// bare GetBlockHeaders, so it alone got no hash-walk repair. That matters more here than
// anywhere else, not less: this entry point has no re-queue behind it — cmd/checkblock and any
// RPC caller get exactly one attempt — and GetBlockHeaders' answer stays memoized per
// (parentHash, count) for chainWalkCacheTTL, so a caller who retried by hand within ten minutes
// would be handed the identical unusable run.
//
// The fixture is deliberately the same shape as its sibling: the range fetch never returns a
// usable run, so the only route to a NoError verdict is the walk. Before the wiring this
// returned an ErrBlockHeaderContext instead.
func TestServerValidateBlock_UnusableHeaderRunIsRebuiltByHashWalk(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, cleanup := setup(t)
	defer cleanup()

	tSettings := test.CreateBaseTestSettings(t)

	coinbaseTx, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	require.NoError(t, subtreeData.AddTx(coinbaseTx, 0))

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	now := uint32(time.Now().Add(-time.Hour).Unix())

	genesisHash := &chainhash.Hash{}

	greatGrandparent := &model.BlockHeader{
		Version: 1, HashPrevBlock: genesisHash, HashMerkleRoot: &chainhash.Hash{},
		Timestamp: now, Bits: *nBits,
	}
	mineHeader(t, greatGrandparent)

	grandparent := &model.BlockHeader{
		Version: 1, HashPrevBlock: greatGrandparent.Hash(), HashMerkleRoot: &chainhash.Hash{},
		Timestamp: now + 1, Bits: *nBits,
	}
	mineHeader(t, grandparent)

	parent := &model.BlockHeader{
		Version: 1, HashPrevBlock: grandparent.Hash(), HashMerkleRoot: &chainhash.Hash{},
		Timestamp: now + 2, Bits: *nBits,
	}
	mineHeader(t, parent)

	// Anchored and correctly linked, but truncated at its oldest end and short of genesis —
	// invisible to the anchor and linkage guards, and the median it yields is biased high.
	staleRun := []*model.BlockHeader{parent, grandparent}

	// This path runs the full block.Valid, which recomputes the merkle root with the coinbase
	// substituted into leaf 0 — so the header must carry that root, not the bare subtree root the
	// optimistic sibling gets away with.
	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size()))

	blockHeader := &model.BlockHeader{
		Version: 1, HashPrevBlock: parent.Hash(), HashMerkleRoot: replicatedSubtree.RootHash(),
		Timestamp: now + 100, Bits: *nBits,
	}
	mineHeader(t, blockHeader)

	block := &model.Block{
		Header:           blockHeader,
		Subtrees:         []*chainhash.Hash{subtree.RootHash()},
		Height:           3,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: uint64(subtree.Length()),
		SizeInBytes:      123123,
	}

	mockBlockchain := new(blockchain.Mock)

	var walkHops atomic.Int32

	mockBlockchain.On("GetBlockHeader", mock.Anything, parent.Hash()).
		Return(parent, &model.BlockHeaderMeta{ID: 2, Height: 2}, nil).
		Run(func(mock.Arguments) { walkHops.Add(1) }).Maybe()
	mockBlockchain.On("GetBlockHeader", mock.Anything, grandparent.Hash()).
		Return(grandparent, &model.BlockHeaderMeta{ID: 1, Height: 1}, nil).
		Run(func(mock.Arguments) { walkHops.Add(1) }).Maybe()
	mockBlockchain.On("GetBlockHeader", mock.Anything, greatGrandparent.Hash()).
		Return(greatGrandparent, &model.BlockHeaderMeta{ID: 0, Height: 0}, nil).
		Run(func(mock.Arguments) { walkHops.Add(1) }).Maybe()

	// Every range fetch is unusable, for the whole life of the test.
	mockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
		Return(staleRun,
			[]*model.BlockHeaderMeta{{ID: 2, Height: 2}, {ID: 1, Height: 1}}, nil).Maybe()

	mockBlockchain.On("GetNextWorkRequired", mock.Anything, mock.Anything, mock.Anything).Return(nBits, nil).Maybe()
	mockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil).Maybe()

	// checkOldBlockIDs picks its route from BlockChain.UseInMemoryChainCheck, which comes from
	// whatever settings_local.conf the machine happens to carry — the in-memory route calls
	// CheckBlockIsInCurrentChain, the prefetch route calls GetBlockHeaderIDs. Mock both, so the
	// test asserts the same thing on a developer box as it does in CI.
	mockBlockchain.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(true, nil).Maybe()
	mockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).
		Return([]uint32{2, 1, 0}, nil).Maybe()

	bv := &BlockValidation{
		blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
		blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
		logger:                        ulogger.TestLogger{},
		settings:                      tSettings,
		blockchainClient:              mockBlockchain,
		subtreeStore:                  subtreeStore,
		txStore:                       txStore,
		utxoStore:                     utxoStore,
		subtreeValidationClient:       subtreeValidationClient,
		blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
		stats:                         gocore.NewStat("test"),
	}
	defer bv.blockExistsCache.Stop()

	// blockAssemblyClient is left nil so WaitForBlockAssemblyReady is a no-op.
	server := &Server{
		logger:              ulogger.TestLogger{},
		settings:            tSettings,
		blockValidation:     bv,
		blockchainClient:    mockBlockchain,
		subtreeStore:        subtreeStore,
		txStore:             txStore,
		utxoStore:           utxoStore,
		stats:               gocore.NewStat("test"),
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
	}

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes))

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	resp, err := server.ValidateBlock(ctx, &blockvalidation_api.ValidateBlockRequest{
		Block:  blockBytes,
		Height: 3,
	})

	require.NoError(t, err, "the gRPC path must rebuild a usable run by hash walk, not report a header-context failure")
	require.NotNil(t, resp)
	require.True(t, resp.Ok)
	require.Positive(t, walkHops.Load(), "recovery must go through the hash-keyed walk, not the range fetch")
}

// TestServerValidateBlock_UnrepairableRunIsNotAVerdictOnTheBlock pins the other half of the gRPC
// path's contract: what happens when the walk cannot repair the run either.
//
// The block-validation service persists invalid verdicts, and an operator running cmd/checkblock
// reads "block is not valid" as a consensus judgement. A parent-header run our own store could not
// produce correctly says nothing about the block, so it must reach the caller as
// ErrBlockHeaderContext — retryable, and explicitly not ErrBlockInvalid. That distinction is the
// whole reason this error kind exists rather than reusing ErrBlockInvalid.
//
// Same fixture as the test above with one change: the hash walk fails too, so parentHeaderRun
// keeps the unusable batched run and block.Valid reaches its median-time-past check with it.
func TestServerValidateBlock_UnrepairableRunIsNotAVerdictOnTheBlock(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, cleanup := setup(t)
	defer cleanup()

	tSettings := test.CreateBaseTestSettings(t)

	coinbaseTx, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	require.NoError(t, subtreeData.AddTx(coinbaseTx, 0))

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	now := uint32(time.Now().Add(-time.Hour).Unix())

	genesisHash := &chainhash.Hash{}

	grandparent := &model.BlockHeader{
		Version: 1, HashPrevBlock: genesisHash, HashMerkleRoot: &chainhash.Hash{},
		Timestamp: now, Bits: *nBits,
	}
	mineHeader(t, grandparent)

	parent := &model.BlockHeader{
		Version: 1, HashPrevBlock: grandparent.Hash(), HashMerkleRoot: &chainhash.Hash{},
		Timestamp: now + 1, Bits: *nBits,
	}
	mineHeader(t, parent)

	// Anchored and linked, but short of genesis — unusable, and invisible to those two guards.
	staleRun := []*model.BlockHeader{parent}

	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size()))

	blockHeader := &model.BlockHeader{
		Version: 1, HashPrevBlock: parent.Hash(), HashMerkleRoot: replicatedSubtree.RootHash(),
		Timestamp: now + 100, Bits: *nBits,
	}
	mineHeader(t, blockHeader)

	block := &model.Block{
		Header:           blockHeader,
		Subtrees:         []*chainhash.Hash{subtree.RootHash()},
		Height:           2,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: uint64(subtree.Length()),
		SizeInBytes:      123123,
	}

	mockBlockchain := new(blockchain.Mock)

	mockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
		Return(staleRun, []*model.BlockHeaderMeta{{ID: 1, Height: 1}}, nil).Maybe()

	// The walk cannot help either: no header is retrievable by hash.
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()

	mockBlockchain.On("GetNextWorkRequired", mock.Anything, mock.Anything, mock.Anything).Return(nBits, nil).Maybe()
	mockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil).Maybe()
	mockBlockchain.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(true, nil).Maybe()
	mockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).
		Return([]uint32{1, 0}, nil).Maybe()

	bv := &BlockValidation{
		blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
		blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
		logger:                        ulogger.TestLogger{},
		settings:                      tSettings,
		blockchainClient:              mockBlockchain,
		subtreeStore:                  subtreeStore,
		txStore:                       txStore,
		utxoStore:                     utxoStore,
		subtreeValidationClient:       subtreeValidationClient,
		blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
		stats:                         gocore.NewStat("test"),
	}
	defer bv.blockExistsCache.Stop()

	server := &Server{
		logger:              ulogger.TestLogger{},
		settings:            tSettings,
		blockValidation:     bv,
		blockchainClient:    mockBlockchain,
		subtreeStore:        subtreeStore,
		txStore:             txStore,
		utxoStore:           utxoStore,
		stats:               gocore.NewStat("test"),
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
	}

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes))

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	_, err = server.ValidateBlock(ctx, &blockvalidation_api.ValidateBlockRequest{
		Block:  blockBytes,
		Height: 2,
	})

	require.Error(t, err)

	unwrapped := errors.UnwrapGRPC(err)
	require.True(t, errors.Is(unwrapped, errors.ErrBlockHeaderContext),
		"a run our own store could not produce must reach the caller as a header-context failure")
	require.False(t, errors.Is(unwrapped, errors.ErrBlockInvalid),
		"and must never be relabelled a consensus verdict on the block")
	require.NotContains(t, err.Error(), "block is not valid")
}

// TestParentHeaderRun_WalkedRunIsNewestFirstAndPairedWithItsMetadata pins the walk's output
// SHAPE, which the end-to-end tests above cannot see.
//
// walkParentChain documents that it returns headers newest-first, matching GetBlockHeaders so the
// two are interchangeable to callers. Nothing enforced it. Reversing the append leaves every
// existing test green — they assert only that validation succeeded and that the walk ran — while
// in production parentBlockHeadersMeta[0] becomes the metadata of a header up to
// PreviousBlockHeaderCount blocks below the parent. checkParentInvalid then reads the wrong
// block, and a block whose parent is flagged invalid is validated and added.
//
// Asserting the slices directly pins order, length, identity and header/metadata pairing at once,
// which is the only place the newest-first contract is enforced.
func TestParentHeaderRun_WalkedRunIsNewestFirstAndPairedWithItsMetadata(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	now := uint32(time.Now().Add(-time.Hour).Unix())
	genesisHash := &chainhash.Hash{}

	greatGrandparent := &model.BlockHeader{
		Version: 1, HashPrevBlock: genesisHash, HashMerkleRoot: &chainhash.Hash{},
		Timestamp: now, Bits: *nBits,
	}
	mineHeader(t, greatGrandparent)

	grandparent := &model.BlockHeader{
		Version: 1, HashPrevBlock: greatGrandparent.Hash(), HashMerkleRoot: &chainhash.Hash{},
		Timestamp: now + 1, Bits: *nBits,
	}
	mineHeader(t, grandparent)

	parent := &model.BlockHeader{
		Version: 1, HashPrevBlock: grandparent.Hash(), HashMerkleRoot: &chainhash.Hash{},
		Timestamp: now + 2, Bits: *nBits,
	}
	mineHeader(t, parent)

	blockHeader := &model.BlockHeader{
		Version: 1, HashPrevBlock: parent.Hash(), HashMerkleRoot: &chainhash.Hash{},
		Timestamp: now + 100, Bits: *nBits,
	}
	mineHeader(t, blockHeader)

	block := &model.Block{Header: blockHeader, Height: 3}

	mockBlockchain := new(blockchain.Mock)

	// Unusable batched run: anchored and linked, but short of genesis. Forces the walk.
	mockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.BlockHeader{parent, grandparent},
			[]*model.BlockHeaderMeta{{ID: 2, Height: 2}, {ID: 1, Height: 1}}, nil).Maybe()

	mockBlockchain.On("GetBlockHeader", mock.Anything, parent.Hash()).
		Return(parent, &model.BlockHeaderMeta{ID: 2, Height: 2}, nil).Maybe()
	mockBlockchain.On("GetBlockHeader", mock.Anything, grandparent.Hash()).
		Return(grandparent, &model.BlockHeaderMeta{ID: 1, Height: 1}, nil).Maybe()
	mockBlockchain.On("GetBlockHeader", mock.Anything, greatGrandparent.Hash()).
		Return(greatGrandparent, &model.BlockHeaderMeta{ID: 0, Height: 0}, nil).Maybe()

	bv := &BlockValidation{
		logger:           ulogger.TestLogger{},
		settings:         tSettings,
		blockchainClient: mockBlockchain,
		stats:            gocore.NewStat("test"),
	}

	headers, metas, err := bv.parentHeaderRun(ctx, block, tSettings.BlockValidation.PreviousBlockHeaderCount)
	require.NoError(t, err)

	// Newest-first, walking back from the block's parent to genesis.
	require.Equal(t, []*model.BlockHeader{parent, grandparent, greatGrandparent}, headers,
		"the walked run must be newest-first, matching GetBlockHeaders' order")

	require.Len(t, metas, len(headers), "every header must carry its own metadata")

	ids := make([]uint32, len(metas))
	for i, m := range metas {
		ids[i] = m.ID
	}

	require.Equal(t, []uint32{2, 1, 0}, ids,
		"metadata must stay paired with its header — blockHeaderIDs and checkParentInvalid index into this")
}
