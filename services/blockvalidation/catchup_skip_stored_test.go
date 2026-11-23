package blockvalidation

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// The scale-3 incident of 2026-10-26: a child went to catchup while its parent was being stored.
// By the time catchup reached its fetch phase the parent was stored, yet catchup moved the FSM to
// CATCHINGBLOCKS and downloaded the parent's 274 GB of subtree data for six minutes before
// ValidateBlock reported "tried to validate existing valid block".

// TestFetchAndValidateBlocks_AllHeadersStored_SkipsCatchingBlocks: when every block catchup was
// about to fetch is already stored as valid, it returns without moving the FSM to CATCHINGBLOCKS.
func TestFetchAndValidateBlocks_AllHeadersStored_SkipsCatchingBlocks(t *testing.T) {
	server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	blocks := testhelpers.CreateTestBlockChain(t, 3)
	mockBlockchainClient.On("Run", mock.Anything, mock.Anything).Return(nil)
	mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(blocks[1].Header, &model.BlockHeaderMeta{Height: 1}, nil)

	catchupCtx := &CatchupContext{
		blockUpTo:    blocks[2],
		blockHeaders: []*model.BlockHeader{blocks[1].Header, blocks[2].Header},
	}

	require.NoError(t, server.fetchAndValidateBlocks(context.Background(), catchupCtx))
	mockBlockchainClient.AssertNotCalled(t, "CatchUpBlocks", mock.Anything)
	mockBlockchainClient.AssertCalled(t, "Run", mock.Anything, mock.Anything)
	require.Empty(t, catchupCtx.blockHeaders)
}

// expectFSMRunning serves RUNNING to the state read a catchup run makes before CATCHINGBLOCKS,
// where it decides the block assembly hand-off (issue 2067).
func expectFSMRunning(m *blockchain.Mock) {
	running := blockchain.FSMStateRUNNING
	m.On("GetFSMCurrentState", mock.Anything).Return(&running, nil).Maybe()
}

// TestFetchAndValidateBlocks_DropsStoredPrefixOnly: stored blocks at the front are dropped, the
// rest are still caught up.
func TestFetchAndValidateBlocks_DropsStoredPrefixOnly(t *testing.T) {
	server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	blocks := testhelpers.CreateTestBlockChain(t, 4)
	mockBlockchainClient.On("GetBlockHeader", mock.Anything, blocks[1].Header.Hash()).Return(blocks[1].Header, &model.BlockHeaderMeta{Height: 1}, nil)
	mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(nil, nil, errors.NewBlockNotFoundError("not stored"))

	// Stop at the FSM transition: the test only needs to see what catchup would fetch.
	mockBlockchainClient.ExpectedCalls = filterMockCalls(mockBlockchainClient.ExpectedCalls, "CatchUpBlocks")
	mockBlockchainClient.On("CatchUpBlocks", mock.Anything).Return(errors.NewStateError("stop here"))

	expectFSMRunning(mockBlockchainClient)

	catchupCtx := &CatchupContext{
		blockUpTo:    blocks[3],
		blockHeaders: []*model.BlockHeader{blocks[1].Header, blocks[2].Header, blocks[3].Header},
	}

	require.Error(t, server.fetchAndValidateBlocks(context.Background(), catchupCtx))
	require.Equal(t, []*model.BlockHeader{blocks[2].Header, blocks[3].Header}, catchupCtx.blockHeaders)
}

// TestFetchAndValidateBlocks_StoredInvalidBlockIsKept: a block stored as invalid is not skipped,
// so validation still returns the invalid verdict for it, as before.
func TestFetchAndValidateBlocks_StoredInvalidBlockIsKept(t *testing.T) {
	server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	blocks := testhelpers.CreateTestBlockChain(t, 3)
	mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(blocks[1].Header, &model.BlockHeaderMeta{Height: 1, Invalid: true}, nil)
	mockBlockchainClient.ExpectedCalls = filterMockCalls(mockBlockchainClient.ExpectedCalls, "CatchUpBlocks")
	mockBlockchainClient.On("CatchUpBlocks", mock.Anything).Return(errors.NewStateError("stop here"))

	expectFSMRunning(mockBlockchainClient)

	catchupCtx := &CatchupContext{
		blockUpTo:    blocks[2],
		blockHeaders: []*model.BlockHeader{blocks[1].Header, blocks[2].Header},
	}

	require.Error(t, server.fetchAndValidateBlocks(context.Background(), catchupCtx))
	require.Len(t, catchupCtx.blockHeaders, 2)
	mockBlockchainClient.AssertCalled(t, "CatchUpBlocks", mock.Anything)
}

// TestFetchAndValidateBlocks_ExistsCheckErrorHalts: a store error while checking for stored blocks
// aborts catchup instead of guessing.
func TestFetchAndValidateBlocks_ExistsCheckErrorHalts(t *testing.T) {
	server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	blocks := testhelpers.CreateTestBlockChain(t, 3)
	mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(nil, nil, errors.NewServiceError("blockchain service unavailable"))

	catchupCtx := &CatchupContext{
		blockUpTo:    blocks[2],
		blockHeaders: []*model.BlockHeader{blocks[1].Header, blocks[2].Header},
	}

	err := server.fetchAndValidateBlocks(context.Background(), catchupCtx)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrStorageError), "a local lookup failure is a storage fault, never the peer's")
	mockBlockchainClient.AssertNotCalled(t, "CatchUpBlocks", mock.Anything)
}

// TestBlockWorker_BlockStoredSinceHeaders_SkipsSubtreeFetch: a block stored after the header phase
// (by the normal path, while catchup was starting) is not prewarmed. It is still delivered, marked
// as stored, so ordered delivery does not stall.
func TestBlockWorker_BlockStoredSinceHeaders_SkipsSubtreeFetch(t *testing.T) {
	server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	blocks := testhelpers.CreateTestBlockChain(t, 2)
	block := blocks[1]
	mockBlockchainClient.On("GetBlockExists", mock.Anything, block.Header.Hash()).Return(true, nil)

	var fetchCalls atomic.Int32
	server.fetchSubtreeDataForBlockFn = func(context.Context, *model.Block, string, string) (map[string]struct{}, map[chainhash.Hash]map[fileformat.FileType]struct{}, error) {
		fetchCalls.Add(1)
		return nil, nil, nil
	}

	workQueue := make(chan workItem, 1)
	resultQueue := make(chan resultItem, 1)
	workQueue <- workItem{block: block, index: 0}
	close(workQueue)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, server.blockWorker(ctx, 0, workQueue, resultQueue, "peer", "http://peer/", block))
	require.Zero(t, fetchCalls.Load(), "a stored block must not be prewarmed")

	result := <-resultQueue
	require.NoError(t, result.err)
	require.True(t, result.alreadyStored)
}

// TestBlockWorker_StoredWhileParkedOnBudget_SkipsSubtreeFetch: a worker parked on the prefetch
// budget (behind another block's prewarm) checks for the stored block only once it holds the
// reservation. A block the normal path stored during the wait is not prewarmed, and the reservation
// is handed back.
func TestBlockWorker_StoredWhileParkedOnBudget_SkipsSubtreeFetch(t *testing.T) {
	server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	const budget = int64(64 << 20)
	server.catchupPrefetchBudget = semaphore.NewWeighted(budget)
	server.catchupPrefetchBudgetBytes = budget
	require.True(t, server.catchupPrefetchBudget.TryAcquire(budget), "another block's prewarm holds the whole budget")

	blocks := testhelpers.CreateTestBlockChain(t, 2)
	block := blocks[1]
	mockBlockchainClient.On("GetBlockExists", mock.Anything, block.Header.Hash()).Return(false, nil)

	var fetchCalls atomic.Int32
	server.fetchSubtreeDataForBlockFn = func(context.Context, *model.Block, string, string) (map[string]struct{}, map[chainhash.Hash]map[fileformat.FileType]struct{}, error) {
		fetchCalls.Add(1)
		return nil, nil, nil
	}

	workQueue := make(chan workItem, 1)
	resultQueue := make(chan resultItem, 1)
	workQueue <- workItem{block: block, index: 0}
	close(workQueue)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	workerErr := make(chan error, 1)
	go func() { workerErr <- server.blockWorker(ctx, 0, workQueue, resultQueue, "peer", "http://peer/", block) }()

	// Let the worker reach the budget wait, then store the block and free the budget.
	time.Sleep(100 * time.Millisecond)
	server.blockValidation.blockExistsCache.Set(*block.Header.Hash(), true)
	server.releaseCatchupPrefetch(budget)

	require.NoError(t, <-workerErr)
	require.Zero(t, fetchCalls.Load(), "a block stored while the worker was parked must not be prewarmed")

	result := <-resultQueue
	require.NoError(t, result.err)
	require.True(t, result.alreadyStored)
	require.True(t, server.catchupPrefetchBudget.TryAcquire(budget), "the reservation must be released when the block is found stored")
}

// TestBlockWorker_ExistsCheckError_FailsBlock: a store error on the exists check fails the block
// instead of fetching on a guess.
func TestBlockWorker_ExistsCheckError_FailsBlock(t *testing.T) {
	server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	blocks := testhelpers.CreateTestBlockChain(t, 2)
	block := blocks[1]
	mockBlockchainClient.On("GetBlockExists", mock.Anything, block.Header.Hash()).Return(false, errors.NewServiceError("blockchain service unavailable"))

	var fetchCalls atomic.Int32
	server.fetchSubtreeDataForBlockFn = func(context.Context, *model.Block, string, string) (map[string]struct{}, map[chainhash.Hash]map[fileformat.FileType]struct{}, error) {
		fetchCalls.Add(1)
		return nil, nil, nil
	}

	workQueue := make(chan workItem, 1)
	resultQueue := make(chan resultItem, 1)
	workQueue <- workItem{block: block, index: 0}
	close(workQueue)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, server.blockWorker(ctx, 0, workQueue, resultQueue, "peer", "http://peer/", block))
	require.Zero(t, fetchCalls.Load())

	result := <-resultQueue
	require.Error(t, result.err)
	require.True(t, errors.Is(result.err, errors.ErrStorageError), "a local lookup failure is a storage fault, never the peer's")
}

// TestOrderedDelivery_CarriesAlreadyStored: the stored mark reaches validation, which skips quick
// validation for it.
func TestOrderedDelivery_CarriesAlreadyStored(t *testing.T) {
	server, _, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	blocks := testhelpers.CreateTestBlockChain(t, 2)
	resultQueue := make(chan resultItem, 1)
	validateBlocksChan := make(chan blockForValidation, 1)
	resultQueue <- resultItem{block: blocks[1], index: 0, alreadyStored: true}

	var size atomic.Int64
	size.Store(1)

	require.NoError(t, server.orderedDelivery(context.Background(), resultQueue, validateBlocksChan, 1, blocks[1], &size))

	item := <-validateBlocksChan
	require.True(t, item.alreadyStored)
}

// TestFetchAndValidateBlocks_TrimmedPrefixKeepsHeights: blocks are numbered from the common
// ancestor by their position in the header list, so trimming the stored front must move the
// starting height with it. Without that, in the incident case the child gets its parent's height,
// fails the BIP34 coinbase-height check and is stored invalid.
func TestFetchAndValidateBlocks_TrimmedPrefixKeepsHeights(t *testing.T) {
	server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	blocks := testhelpers.CreateTestBlockChain(t, 4)
	mockBlockchainClient.On("GetBlockHeader", mock.Anything, blocks[1].Header.Hash()).Return(blocks[1].Header, &model.BlockHeaderMeta{Height: 101}, nil)
	mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(nil, nil, errors.NewBlockNotFoundError("not stored"))
	mockBlockchainClient.ExpectedCalls = filterMockCalls(mockBlockchainClient.ExpectedCalls, "CatchUpBlocks")
	mockBlockchainClient.On("CatchUpBlocks", mock.Anything).Return(errors.NewStateError("stop here"))
	expectFSMRunning(mockBlockchainClient)

	catchupCtx := &CatchupContext{
		blockUpTo:          blocks[3],
		blockHeaders:       []*model.BlockHeader{blocks[1].Header, blocks[2].Header, blocks[3].Header},
		commonAncestorMeta: &model.BlockHeaderMeta{Height: 100},
	}

	require.Error(t, server.fetchAndValidateBlocks(context.Background(), catchupCtx))
	require.Equal(t, uint32(102), catchupFirstBlockHeight(catchupCtx), "the first remaining block sits one above the last stored one")
	require.Equal(t, uint32(100), catchupCtx.commonAncestorMeta.Height, "the common ancestor itself is reported unchanged")
}

// TestCatchupFirstBlockHeight_DefaultsToCommonAncestor: an untrimmed list starts one above the
// common ancestor, as before.
func TestCatchupFirstBlockHeight_DefaultsToCommonAncestor(t *testing.T) {
	catchupCtx := &CatchupContext{commonAncestorMeta: &model.BlockHeaderMeta{Height: 99}}
	require.Equal(t, uint32(100), catchupFirstBlockHeight(catchupCtx))
}

// TestValidateBlocksOnChannel_AlreadyStoredSkipsQuickValidation: a block the worker found stored
// goes to normal validation even when quick validation is on, and normal validation stops at the
// stored block.
func TestValidateBlocksOnChannel_AlreadyStoredSkipsQuickValidation(t *testing.T) {
	server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	blocks := testhelpers.CreateTestBlockChain(t, 2)
	block := blocks[1]
	block.Height = 1
	mockBlockchainClient.On("GetBlockExists", mock.Anything, block.Header.Hash()).Return(true, nil)
	mockBlockchainClient.On("GetBlockHeader", mock.Anything, block.Header.Hash()).Return(block.Header, &model.BlockHeaderMeta{Height: 1}, nil)

	catchupCtx := &CatchupContext{
		blockUpTo:               block,
		useQuickValidation:      true,
		highestCheckpointHeight: 1000,
	}

	validateBlocksChan := make(chan blockForValidation, 1)
	validateBlocksChan <- blockForValidation{block: block, alreadyStored: true}
	close(validateBlocksChan)

	var size atomic.Int64
	size.Store(1)

	quickWrites := make(chan *SubtreeWriteJob, 1)
	require.NoError(t, server.validateBlocksOnChannel(validateBlocksChan, context.Background(), catchupCtx, &size, quickWrites))
	require.Zero(t, size.Load())
	require.Empty(t, quickWrites, "a stored block must not go through quick validation")
}

// TestValidateBlocksOnChannel_StoredAfterWorkerCheckSkipsQuickValidation: a block stored after the
// worker checked it (while the worker was parked on the prefetch budget, or during its prewarm)
// arrives unmarked. Validation checks again, so it still never reaches quick validation, whose
// commit would collide with the stored block.
func TestValidateBlocksOnChannel_StoredAfterWorkerCheckSkipsQuickValidation(t *testing.T) {
	server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	blocks := testhelpers.CreateTestBlockChain(t, 2)
	block := blocks[1]
	block.Height = 1
	mockBlockchainClient.On("GetBlockExists", mock.Anything, block.Header.Hash()).Return(true, nil)
	mockBlockchainClient.On("GetBlockHeader", mock.Anything, block.Header.Hash()).Return(block.Header, &model.BlockHeaderMeta{Height: 1}, nil)

	catchupCtx := &CatchupContext{
		blockUpTo:               block,
		useQuickValidation:      true,
		highestCheckpointHeight: 1000,
	}

	validateBlocksChan := make(chan blockForValidation, 1)
	validateBlocksChan <- blockForValidation{block: block}
	close(validateBlocksChan)

	var size atomic.Int64
	size.Store(1)

	quickWrites := make(chan *SubtreeWriteJob, 1)
	require.NoError(t, server.validateBlocksOnChannel(validateBlocksChan, context.Background(), catchupCtx, &size, quickWrites))
	require.Zero(t, size.Load())
	require.Empty(t, quickWrites, "a block stored after the worker check must not go through quick validation")
}

// TestValidateBlocksOnChannel_ExistsCheckErrorHalts: a store error on validation's own exists check
// halts the catchup as a storage fault instead of guessing.
func TestValidateBlocksOnChannel_ExistsCheckErrorHalts(t *testing.T) {
	server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	blocks := testhelpers.CreateTestBlockChain(t, 2)
	block := blocks[1]
	block.Height = 1
	mockBlockchainClient.On("GetBlockExists", mock.Anything, block.Header.Hash()).Return(false, errors.NewServiceError("blockchain service unavailable"))

	catchupCtx := &CatchupContext{
		blockUpTo:               block,
		useQuickValidation:      true,
		highestCheckpointHeight: 1000,
	}

	validateBlocksChan := make(chan blockForValidation, 1)
	validateBlocksChan <- blockForValidation{block: block}
	close(validateBlocksChan)

	var size atomic.Int64
	size.Store(1)

	err := server.validateBlocksOnChannel(validateBlocksChan, context.Background(), catchupCtx, &size, make(chan *SubtreeWriteJob, 1))
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrStorageError), "a local lookup failure is a storage fault, never the peer's")
}

// TestFetchBlocksConcurrently_NumbersFromFirstBlockHeight: once the stored front was trimmed,
// delivered blocks are numbered from firstBlockHeight, not from the common ancestor.
func TestFetchBlocksConcurrently_NumbersFromFirstBlockHeight(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	suite.MockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil).Maybe()
	defer suite.Cleanup()

	blocks := testhelpers.CreateTestBlockChain(t, 4)

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	// blocks[1] was stored and trimmed: the list starts at blocks[2].
	batch := bytes.Buffer{}
	for i := 3; i >= 2; i-- {
		blockBytes, err := blocks[i].Bytes()
		require.NoError(t, err)
		batch.Write(blockBytes)
	}

	httpmock.RegisterResponder("GET", fmt.Sprintf("http://test-peer/blocks/%s?n=%d", blocks[3].Header.Hash().String(), 2), httpmock.NewBytesResponder(200, batch.Bytes()))

	var size atomic.Int64
	size.Store(2)
	validateBlocksChan := make(chan blockForValidation, 2)

	catchupCtx := &CatchupContext{
		blockUpTo:          blocks[3],
		baseURL:            "http://test-peer",
		blockHeaders:       []*model.BlockHeader{blocks[2].Header, blocks[3].Header},
		commonAncestorMeta: &model.BlockHeaderMeta{Height: 99},
		firstBlockHeight:   101,
	}

	require.NoError(t, suite.Server.fetchBlocksConcurrently(suite.Ctx, catchupCtx, validateBlocksChan, &size))

	for i, want := range []uint32{101, 102} {
		select {
		case item := <-validateBlocksChan:
			require.Equal(t, want, item.block.Height, "block %d", i)
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for block %d", i)
		}
	}
}

// allStoredPromotionSetup stores the only block catchup was about to fetch, on a real FSM authority
// that starts in CATCHINGBLOCKS.
func allStoredPromotionSetup(t *testing.T) (*Server, *promotionAuthorityClient, *promotionFaultStore, *CatchupContext) {
	t.Helper()
	server, client, store, catchupCtx := newPromotionAuthority(t)
	cache := expiringmap.New[chainhash.Hash, bool](time.Minute)
	t.Cleanup(cache.Stop)
	server.blockValidation = &BlockValidation{blockchainClient: client, blockExistsCache: cache}

	block := testhelpers.CreateTestBlocksWithPrev(t, 1, server.settings.ChainCfgParams.GenesisHash)[0]
	_, _, err := store.StoreBlock(context.Background(), block, "test")
	require.NoError(t, err)

	catchupCtx.blockUpTo = block
	catchupCtx.blockHeaders = []*model.BlockHeader{block.Header}

	return server, client, store, catchupCtx
}

// TestFetchAndValidateBlocks_AllHeadersStored_PromotesFromCatchingBlocks: a node left in
// CATCHINGBLOCKS must still be promoted to RUNNING when there is nothing left to fetch.
func TestFetchAndValidateBlocks_AllHeadersStored_PromotesFromCatchingBlocks(t *testing.T) {
	server, client, store, catchupCtx := allStoredPromotionSetup(t)

	require.NoError(t, server.fetchAndValidateBlocks(context.Background(), catchupCtx))
	require.Empty(t, catchupCtx.blockHeaders)
	requirePromotionState(t, client, store, blockchain.FSMStateRUNNING)
	require.Equal(t, 1, client.runCalls)
}

// TestFetchAndValidateBlocks_AllHeadersStored_PreservesOperatorIdle: the promotion request on the
// all-stored path must not undo an operator STOP.
func TestFetchAndValidateBlocks_AllHeadersStored_PreservesOperatorIdle(t *testing.T) {
	server, client, store, catchupCtx := allStoredPromotionSetup(t)
	_, err := client.authority.Idle(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)

	require.NoError(t, server.fetchAndValidateBlocks(context.Background(), catchupCtx))
	requirePromotionState(t, client, store, blockchain.FSMStateIDLE)
	require.Equal(t, 1, client.runCalls)
}

// TestStoredHeaderPrefix_RealStore: on a sqlitememory store, the prefix stops at the first block
// that is stored invalid or not stored at all, and reports the height of the last stored one.
func TestStoredHeaderPrefix_RealStore(t *testing.T) {
	server, _, store, _ := newPromotionAuthority(t)

	chain := testhelpers.CreateTestBlocksWithPrev(t, 3, server.settings.ChainCfgParams.GenesisHash)
	for _, block := range chain[:2] {
		_, _, err := store.StoreBlock(context.Background(), block, "test")
		require.NoError(t, err)
	}

	_, err := store.InvalidateBlock(context.Background(), chain[1].Header.Hash())
	require.NoError(t, err)

	headers := []*model.BlockHeader{chain[0].Header, chain[1].Header, chain[2].Header}

	stored, lastStoredHeight, err := server.storedHeaderPrefix(context.Background(), headers)
	require.NoError(t, err)
	require.Equal(t, 1, stored, "a block stored invalid ends the prefix")
	require.Equal(t, uint32(1), lastStoredHeight)

	stored, _, err = server.storedHeaderPrefix(context.Background(), headers[2:])
	require.NoError(t, err)
	require.Zero(t, stored, "a block that is not stored ends the prefix")
}

// TestStoredLookupError_KeepsLocalLabels: a failed stored-block lookup is always local, but
// shutdown and an unreachable blockchain service keep their own local labels instead of being
// reported as a storage fault. Deadlines have no label of their own, so they stay storage faults.
func TestStoredLookupError_KeepsLocalLabels(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	for _, tc := range []struct {
		name string
		ctx  context.Context
		err  error
		want error
		not  error
	}{
		{"cancelled", context.Background(), context.Canceled, errors.ErrContextCanceled, errors.ErrStorageError},
		// The gRPC client hands back UnwrapGRPC's ERR_ERROR, which has lost the Canceled status, so
		// shutdown is recognised by the caller's own context.
		{"grpc cancelled", cancelled, errors.UnwrapGRPC(status.Error(codes.Canceled, "context canceled")), errors.ErrContextCanceled, errors.ErrStorageError},
		{"unavailable", context.Background(), errors.NewServiceUnavailableError("blockchain down"), errors.ErrServiceUnavailable, errors.ErrStorageError},
		{"deadline", context.Background(), context.DeadlineExceeded, errors.ErrStorageError, nil},
		{"other", context.Background(), errors.NewServiceError("boom"), errors.ErrStorageError, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := storedLookupError(tc.ctx, tc.err, "lookup of %s failed", "block")
			require.True(t, errors.Is(err, tc.want), "got %v", err)
			if tc.not != nil {
				require.False(t, errors.Is(err, tc.not), "got %v", err)
			}
		})
	}
}
