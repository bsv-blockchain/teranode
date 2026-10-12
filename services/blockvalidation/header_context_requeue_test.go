package blockvalidation

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestShouldRequeueForHeaderContext pins the one decision both re-queue sites make, so they
// cannot answer it differently.
//
// They used to. The optimistic site re-queued ANY error out of CheckHeaderContextual that was not
// ErrBlockInvalid, while the non-optimistic site narrowed to ErrBlockHeaderContext with a
// documented reason: the revalidation worker is a single goroutine off a two-slot channel that
// retries three times, so re-queuing a permanent failure buys four full validation passes and
// nothing else. Both sites were right about the narrow case and only one enforced it.
//
// Nothing separates them today — every other error CheckHeaderContextual can return is
// unreachable (CalculateMedianTimestamp errors only on an empty window, which MedianTimeWindow
// already refuses, and the safeconversion calls cannot overflow a value derived from a uint32
// timestamp). That is exactly why this is worth pinning rather than leaving to two copies of a
// condition to keep in step: the day one becomes reachable is not the day anyone rechecks both.
func TestShouldRequeueForHeaderContext(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		opts *ValidateBlockOptions
		want bool
	}{
		{
			name: "header-context failure on a first attempt is re-queued",
			err:  errors.NewBlockHeaderContextError("unanchored run"),
			opts: &ValidateBlockOptions{},
			want: true,
		},
		{
			name: "header-context failure on a retry is not re-queued again",
			err:  errors.NewBlockHeaderContextError("unanchored run"),
			opts: &ValidateBlockOptions{IsRequeuedRetry: true},
			want: false,
		},
		{
			// The asymmetry this closes. A bare processing error fails identically every time —
			// model.Block.Valid reports the target-difficulty consensus check as one — so
			// re-queuing it is four guaranteed-futile validation passes ahead of real work.
			name: "a processing failure that is not a header-context failure is not re-queued",
			err:  errors.NewProcessingError("something else entirely"),
			opts: &ValidateBlockOptions{},
			want: false,
		},
		{
			name: "a storage failure is not re-queued",
			err:  errors.NewStorageError("the store is down"),
			opts: &ValidateBlockOptions{},
			want: false,
		},
		{
			name: "no failure is not re-queued",
			err:  nil,
			opts: &ValidateBlockOptions{},
			want: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, shouldRequeueForHeaderContext(tc.err, tc.opts))
		})
	}
}

// TestShouldRequeueForHeaderContext_MatchesWrappedCause guards the property the narrow match
// depends on: ErrBlockHeaderContext wraps ErrProcessing so existing retryable handling keeps
// working, and Error.Is walks the chain by code. A wrapped header-context failure must therefore
// still be recognised, or the re-queue would silently stop firing the moment a caller added
// context to the error.
func TestShouldRequeueForHeaderContext_MatchesWrappedCause(t *testing.T) {
	wrapped := errors.NewServiceError("outer context",
		errors.NewBlockHeaderContextError("median-time-past window is not a linked chain"))

	require.True(t, errors.Is(wrapped, errors.ErrBlockHeaderContext), "precondition")
	require.True(t, shouldRequeueForHeaderContext(wrapped, &ValidateBlockOptions{}))
}

// TestValidateBlock_HeaderContextRetryBudgetIsBounded pins the site that SUPPLIES the
// no-more-re-queues flag, which TestShouldRequeueForHeaderContext cannot see.
//
// The drain worker re-enters ValidateBlockWithOptions with IsRequeuedRetry: true, so a retry that
// fails again cannot enqueue itself and the worker's own counter stays the only retry budget.
// Drop that flag and the predicate above still answers correctly every time it is asked — it is
// simply asked with the wrong options, and a block whose parent-header run never heals is
// re-validated every revalidateFromScratchDelay forever, each pass a full validation occupying
// one of only two channel slots ahead of real work.
//
// So the assertion is not on a count but on the count STOPPING: the budget must be exhausted,
// not renewed.
func TestValidateBlock_HeaderContextRetryBudgetIsBounded(t *testing.T) {
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

	// Offset from the other fixtures in this package: an identical chain mines an identical block
	// hash, which collides in the caches these tests share.
	now := uint32(time.Now().Add(-3 * time.Hour).Unix())

	// Three deep, so the two-header run below is anchored AND linked and still short of genesis —
	// invisible to both of those guards, which is the shape the median-window check must catch.
	greatGrandparent := &model.BlockHeader{
		Version: 1, HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{},
		Timestamp: now, Bits: *nBits,
	}
	mineHeader(t, greatGrandparent)

	grandparent := &model.BlockHeader{
		Version: 1, HashPrevBlock: greatGrandparent.Hash(), HashMerkleRoot: &chainhash.Hash{},
		Timestamp: now, Bits: *nBits,
	}
	mineHeader(t, grandparent)

	parent := &model.BlockHeader{
		Version: 1, HashPrevBlock: grandparent.Hash(), HashMerkleRoot: &chainhash.Hash{},
		Timestamp: now + 1, Bits: *nBits,
	}
	mineHeader(t, parent)

	// Full block.Valid runs here, so the header must carry the merkle root taken with the coinbase
	// substituted into leaf 0, not the bare subtree root.
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

	// The run NEVER heals: anchored and linked, but two headers short of genesis, on every call
	// for the whole life of the test. This is the permanent failure the budget exists to bound.
	shortRun := []*model.BlockHeader{parent, grandparent}

	var attempts atomic.Int32

	mockBlockchain := new(blockchain.Mock)
	mockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
		Return(shortRun, []*model.BlockHeaderMeta{{ID: 1, Height: 2}, {ID: 0, Height: 1}}, nil).
		Run(func(mock.Arguments) { attempts.Add(1) }).Maybe()

	// The hash walk cannot repair it either, and fails on the first hop so each attempt costs one
	// call rather than a hundred.
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("not found")).Maybe()

	mockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil).Maybe()
	mockBlockchain.On("GetNextWorkRequired", mock.Anything, mock.Anything, mock.Anything).Return(nBits, nil).Maybe()
	mockBlockchain.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil).Maybe()
	mockBlockchain.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil).Maybe()
	mockBlockchain.On("GetBlocksSubtreesNotSet", mock.Anything).Return([]*model.Block{}, nil).Maybe()
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(parent, &model.BlockHeaderMeta{Height: 2}, nil).Maybe()
	mockBlockchain.On("GetBlock", mock.Anything, mock.Anything).Return(&model.Block{
		Header: parent, CoinbaseTx: coinbaseTx, Subtrees: []*chainhash.Hash{}, Height: 2,
	}, nil).Maybe()
	mockBlockchain.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(true, nil).Maybe()
	mockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{1, 0}, nil).Maybe()
	mockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockBlockchain.On("InvalidateBlock", mock.Anything, mock.Anything).Return([]chainhash.Hash{}, nil).Maybe()
	mockBlockchain.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockBlockchain.On("SetBlockMinedSet", mock.Anything, mock.Anything).Return(nil).Maybe()

	notificationChan := make(chan *blockchain_api.Notification, 1)
	mockBlockchain.On("Subscribe", mock.Anything, mock.Anything).Return(notificationChan, nil).Maybe()

	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, mockBlockchain, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	// Serialize(), not SerializeNodes(): block.Valid deserializes the whole subtree.
	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes))

	// No IsRequeuedRetry: the re-queue must fire, exactly as it does in production.
	// DisableOptimisticMining so the failure is returned synchronously rather than happening in a
	// background Valid — this is also the branch every catchup block takes.
	err = bv.ValidateBlockWithOptions(ctx, block, "", &ValidateBlockOptions{
		DisableOptimisticMining: true,
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockHeaderContext))

	// Let the re-queue and every retry it is entitled to run to completion.
	time.Sleep(3 * time.Second)
	settled := attempts.Load()

	require.Greater(t, settled, int32(1), "the re-queue must actually have fired")

	// Then give it as long again. A renewed budget keeps going; an exhausted one does not.
	time.Sleep(3 * time.Second)

	require.Equal(t, settled, attempts.Load(),
		"validation attempts must stop once the retry budget is spent, not renew themselves every %s",
		revalidateFromScratchDelay)
}
