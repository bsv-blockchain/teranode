package blockvalidation

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/blockvalidation_api"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jarcoal/httpmock"
	"github.com/jellydator/ttlcache/v3"
	"github.com/stretchr/testify/require"
)

// Most of these tests drive BlockFound with WaitToComplete through the real pipeline: a
// blockFoundCh worker, the priority queue and a block-processing worker, against the sqlitememory
// UTXO store of newProcessBlockFoundHarness. The rest call one step of that pipeline directly with
// a reply, to reach an answer the full pipeline cannot reach on its own. The caller must be
// answered with the block's outcome:
// nil only once the block is stored (BV-INGRESS-005), and an error when it was handed to catchup
// instead (BV-INGRESS-017).

const waitTestPeer = "http://test-peer"

// startBlockFoundPipeline runs one blockFoundCh worker and the block-processing workers until ctx
// ends.
func startBlockFoundPipeline(ctx context.Context, s *Server) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case bf := <-s.blockFoundCh:
				s.processBlockFoundChItem(ctx, 0, bf)
			}
		}
	}()

	s.startBlockProcessingSystem(ctx)
}

// serveBlock answers the peer's /block/<hash> fetches with block. Fetch number holdFetch (counted
// from 1) waits until release is closed; 0 holds none. It returns the fetch counter.
func serveBlock(t *testing.T, block *model.Block, holdFetch int64, release <-chan struct{}) *atomic.Int64 {
	t.Helper()

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	var fetches atomic.Int64

	httpmock.ActivateNonDefault(util.HTTPClient())
	t.Cleanup(httpmock.DeactivateAndReset)

	httpmock.RegisterResponder(http.MethodGet, waitTestPeer+"/block/"+block.Hash().String(),
		func(*http.Request) (*http.Response, error) {
			if fetches.Add(1) == holdFetch {
				<-release
			}

			return httpmock.NewBytesResponse(http.StatusOK, blockBytes), nil
		})

	return &fetches
}

func blockFoundAndWait(ctx context.Context, s *Server, block *model.Block) <-chan error {
	done := make(chan error, 1)

	go func() {
		_, err := s.BlockFound(ctx, &blockvalidation_api.BlockFoundRequest{
			Hash:           block.Hash().CloneBytes(),
			BaseUrl:        waitTestPeer,
			WaitToComplete: true,
		})
		if err != nil {
			// BlockFound wraps its error for gRPC; unwrap it so the tests can check the code.
			err = errors.UnwrapGRPC(err)
		}
		done <- err
	}()

	return done
}

// TestBlockFoundWait_ReturnsOnlyOnceTheBlockIsStored holds the worker's fetch of the block, the
// first step of validation, after the block has been queued. Before the fix, the caller was
// answered nil as soon as the block was queued, while the worker had not yet fetched it.
func TestBlockFoundWait_ReturnsOnlyOnceTheBlockIsStored(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, store, block := newProcessBlockFoundHarness(ctx, t)

	// Fetch 1 classifies the block before it is queued; fetch 2 is the worker's validation fetch.
	release := make(chan struct{})
	fetches := serveBlock(t, block, 2, release)

	startBlockFoundPipeline(ctx, s)

	done := blockFoundAndWait(ctx, s, block)

	require.Eventually(t, func() bool { return fetches.Load() == 2 }, 10*time.Second, 10*time.Millisecond,
		"setup: the worker never started validating the queued block")

	select {
	case err := <-done:
		t.Fatalf("BlockFound returned before the block was validated (err=%v)", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(release)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("BlockFound did not return after the block was validated")
	}

	exists, err := store.GetBlockExists(ctx, block.Hash())
	require.NoError(t, err)
	require.True(t, exists, "a nil answer must mean the block is stored")
}

// TestBlockFoundWait_ParentMissingReturnsServiceError: a block whose parent is not stored goes to
// catchup, not validation. The caller must get a retry-later ERR_SERVICE_ERROR, not nil.
func TestBlockFoundWait_ParentMissingReturnsServiceError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, store, block := newProcessBlockFoundHarness(ctx, t)
	delete(store.BlockExists, *block.Header.HashPrevBlock)
	delete(store.Blocks, *block.Header.HashPrevBlock)

	serveBlock(t, block, 0, nil)
	startBlockFoundPipeline(ctx, s)

	select {
	case err := <-blockFoundAndWait(ctx, s, block):
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrServiceError), "want ERR_SERVICE_ERROR, got %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("BlockFound did not return for a block handed to catchup")
	}

	exists, err := store.GetBlockExists(ctx, block.Hash())
	require.NoError(t, err)
	require.False(t, exists)
}

// TestBlockFoundWait_ValidationFailureReachesCaller: the worker's verdict must reach the caller.
// The served block's header timestamp is before its parent's, which fails the median-time-past
// rule, so validation rejects it.
func TestBlockFoundWait_ValidationFailureReachesCaller(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, store, block := newProcessBlockFoundHarness(ctx, t)

	parent := store.Blocks[*block.Header.HashPrevBlock]
	header := *block.Header
	header.Timestamp = parent.Header.Timestamp - 1
	header.Nonce = 0

	for {
		if ok, _, _ := header.HasMetTargetDifficulty(); ok {
			break
		}

		require.Less(t, header.Nonce, uint32(10_000_000), "could not grind a nonce meeting the easy target")
		header.Nonce++
	}

	bad, err := model.NewBlock(&header, block.CoinbaseTx, block.Subtrees, block.TransactionCount, block.SizeInBytes, block.Height, 0)
	require.NoError(t, err)

	serveBlock(t, bad, 0, nil)
	startBlockFoundPipeline(ctx, s)

	select {
	case err := <-blockFoundAndWait(ctx, s, bad):
		require.Error(t, err, "a block that fails validation must not be answered nil")
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "want the validation verdict, got %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("BlockFound did not return for a block that failed validation")
	}
}

// TestBlockFoundWait_CallerContextEndsTheWait: a caller that gives up gets its context error, and
// a late answer finds room in the reply, so the worker that sends it never blocks.
func TestBlockFoundWait_CallerContextEndsTheWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	callerCtx, callerCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer callerCancel()

	// No worker reads blockFoundCh, so nothing answers.
	select {
	case err := <-blockFoundAndWait(callerCtx, s, block):
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrContextCanceled), "want a context error, got %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("BlockFound ignored its caller's context")
	}

	bf := <-s.blockFoundCh
	require.NotNil(t, bf.reply)
	bf.reply.answer(nil) // must not block though the caller has gone
}

// TestBlockFoundWait_FetchFailureAnswersAtOnceAndRequeues: the worker's fetch fails with HTTP 500,
// a failure that is not a verdict on the block, so the block goes back on the queue for a retry.
// The caller must get that failure at once, not after the retries, because it can call BlockFound
// again itself (see answerQueuedBlock). The block must still be requeued, for the node's own sake.
func TestBlockFoundWait_FetchFailureAnswersAtOnceAndRequeues(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, store, block := newProcessBlockFoundHarness(ctx, t)

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	httpmock.ActivateNonDefault(util.HTTPClient())
	t.Cleanup(httpmock.DeactivateAndReset)

	// Fetch 1 classifies the block before it is queued and succeeds; every later fetch fails.
	var fetches atomic.Int64

	httpmock.RegisterResponder(http.MethodGet, waitTestPeer+"/block/"+block.Hash().String(),
		func(*http.Request) (*http.Response, error) {
			if fetches.Add(1) == 1 {
				return httpmock.NewBytesResponse(http.StatusOK, blockBytes), nil
			}

			return httpmock.NewStringResponse(http.StatusInternalServerError, "peer failed"), nil
		})

	startBlockFoundPipeline(ctx, s)

	// Well under blockFetchRetryDelay, so an answer after the first retry would miss it.
	select {
	case err := <-blockFoundAndWait(ctx, s, block):
		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrBlockInvalid), "a failed fetch is not a verdict on the block, got %v", err)
	case <-time.After(blockFetchRetryDelay / 2):
		t.Fatal("BlockFound waited for the retries instead of answering the first failure")
	}

	require.Eventually(t, func() bool { return s.blockPriorityQueue.Contains(*block.Hash()) }, 5*time.Second, 10*time.Millisecond,
		"the block must still be requeued for a retry after the caller was answered")

	exists, err := store.GetBlockExists(ctx, block.Hash())
	require.NoError(t, err)
	require.False(t, exists)
}

// invalidStoredChain reports every block as stored, and stored invalid.
type invalidStoredChain struct {
	blockchain.ClientI
}

func (c *invalidStoredChain) GetBlockExists(context.Context, *chainhash.Hash) (bool, error) {
	return true, nil
}

func (c *invalidStoredChain) GetBlockHeader(context.Context, *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	return &model.BlockHeader{}, &model.BlockHeaderMeta{Invalid: true}, nil
}

// TestBlockFoundWait_StoredInvalidBlockIsNotSuccess: a block already stored, but marked invalid,
// "exists" for every existence check. A waiting caller must get ERR_BLOCK_INVALID, not nil.
func TestBlockFoundWait_StoredInvalidBlockIsNotSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)
	s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings, &invalidStoredChain{ClientI: s.blockchainClient}, blobmemory.New(), s.txStore, s.utxoStore, nil, nil)

	select {
	case err := <-blockFoundAndWait(ctx, s, block):
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "want ERR_BLOCK_INVALID, got %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("BlockFound did not return for a stored block")
	}
}

// awaitReply reads reply's answer, failing the test if none comes.
func awaitReply(t *testing.T, reply *blockFoundReply) error {
	t.Helper()

	select {
	case err := <-reply.ch:
		return err
	case <-time.After(30 * time.Second):
		t.Fatal("the reply was never answered")
		return nil
	}
}

// useInvalidStoredChain makes every block read as stored invalid by s's validation.
func useInvalidStoredChain(ctx context.Context, s *Server) {
	s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings, &invalidStoredChain{ClientI: s.blockchainClient}, blobmemory.New(), s.txStore, s.utxoStore, nil, nil)
}

// TestBlockFoundWait_WorkerNilWithoutStoreIsNotSuccess: the worker's processing returns nil for a
// queued block whose parent has gone missing by the time it runs, because processBlockFound hands
// the block to catchup. The block is not stored, so the reply must be a retry-later
// ERR_SERVICE_ERROR, not the nil the processing returned.
func TestBlockFoundWait_WorkerNilWithoutStoreIsNotSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, store, block := newProcessBlockFoundHarness(ctx, t)
	delete(store.BlockExists, *block.Header.HashPrevBlock)
	delete(store.Blocks, *block.Header.HashPrevBlock)

	serveBlock(t, block, 0, nil)

	reply := newBlockFoundReply()
	s.blockPriorityQueue.Add(processBlockFound{hash: block.Hash(), baseURL: waitTestPeer, reply: reply}, PriorityChainExtending, block.Height)
	s.startBlockProcessingSystem(ctx)

	err := awaitReply(t, reply)
	require.Error(t, err, "a block the worker did not store must not be answered nil")
	require.True(t, errors.Is(err, errors.ErrServiceError), "want ERR_SERVICE_ERROR, got %v", err)

	exists, err := store.GetBlockExists(ctx, block.Hash())
	require.NoError(t, err)
	require.False(t, exists)
}

// TestBlockFoundWait_WorkerNilForStoredInvalidIsNotSuccess: the worker's processing returns nil for
// a queued block it finds already stored, here stored invalid. The reply must be ERR_BLOCK_INVALID.
func TestBlockFoundWait_WorkerNilForStoredInvalidIsNotSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)
	useInvalidStoredChain(ctx, s)

	reply := newBlockFoundReply()
	s.blockPriorityQueue.Add(processBlockFound{hash: block.Hash(), baseURL: waitTestPeer, reply: reply}, PriorityChainExtending, block.Height)
	s.startBlockProcessingSystem(ctx)

	err := awaitReply(t, reply)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "want ERR_BLOCK_INVALID, got %v", err)
}

// TestBlockFoundWait_StoredInvalidBeforeQueueIsNotSuccess covers the two "already stored" answers
// before the queue, which a block stored invalid also reaches: processBlockFoundChannel's exists
// check and addBlockToPriorityQueue's. Both must answer ERR_BLOCK_INVALID, not nil.
func TestBlockFoundWait_StoredInvalidBeforeQueueIsNotSuccess(t *testing.T) {
	steps := map[string]func(ctx context.Context, t *testing.T, s *Server, bf processBlockFound){
		"processBlockFoundChannel": func(ctx context.Context, t *testing.T, s *Server, bf processBlockFound) {
			require.NoError(t, s.processBlockFoundChannel(ctx, bf))
		},
		"addBlockToPriorityQueue": func(ctx context.Context, _ *testing.T, s *Server, bf processBlockFound) {
			s.addBlockToPriorityQueue(ctx, bf)
		},
	}

	for name, step := range steps {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			s, _, block := newProcessBlockFoundHarness(ctx, t)
			useInvalidStoredChain(ctx, s)

			reply := newBlockFoundReply()
			step(ctx, t, s, processBlockFound{hash: block.Hash(), baseURL: waitTestPeer, reply: reply})

			err := awaitReply(t, reply)
			require.Error(t, err)
			require.True(t, errors.Is(err, errors.ErrBlockInvalid), "want ERR_BLOCK_INVALID, got %v", err)
			require.False(t, s.blockPriorityQueue.Contains(*block.Hash()), "a stored block must not be queued")
		})
	}
}

// requireNotStoredAnswer checks a catchup divert's answer: a retry-later ERR_SERVICE_ERROR, with
// the block neither stored nor queued.
func requireNotStoredAnswer(ctx context.Context, t *testing.T, s *Server, store *blockchain_store.MockStore, block *model.Block, reply *blockFoundReply) {
	t.Helper()

	err := awaitReply(t, reply)
	require.Error(t, err, "a block diverted to catchup must not be answered nil")
	require.True(t, errors.Is(err, errors.ErrServiceError), "want ERR_SERVICE_ERROR, got %v", err)
	require.False(t, s.blockPriorityQueue.Contains(*block.Hash()), "a diverted block must not be queued")

	exists, err := store.GetBlockExists(ctx, block.Hash())
	require.NoError(t, err)
	require.False(t, exists)
}

// TestBlockFoundWait_CatchupAttemptsSpentIsNotSuccess: a block with a missing parent whose catchup
// attempts are spent is dropped until the cooldown lapses. The caller must not get nil.
func TestBlockFoundWait_CatchupAttemptsSpentIsNotSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, store, block := newProcessBlockFoundHarness(ctx, t)
	delete(store.BlockExists, *block.Header.HashPrevBlock)
	delete(store.Blocks, *block.Header.HashPrevBlock)

	s.settings.BlockValidation.CatchupMaxAttemptsPerBlock = 1
	s.recordCatchupAttempt(block.Hash())
	require.True(t, s.catchupAttemptsExhausted(block.Hash()), "setup: the block's catchup attempts must be spent")

	reply := newBlockFoundReply()
	s.queueFetchedBlock(ctx, processBlockFound{hash: block.Hash(), baseURL: waitTestPeer, reply: reply}, block)

	requireNotStoredAnswer(ctx, t, s, store, block, reply)
	require.Nil(t, s.processBlockNotify.Get(*block.Hash()), "a block with spent attempts must not be sent to catchup")
}

// TestBlockFoundWait_AlreadyInCatchupIsNotSuccess: a block with a missing parent that catchup is
// already working on is added as an alternative source. The caller must not get nil.
func TestBlockFoundWait_AlreadyInCatchupIsNotSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, store, block := newProcessBlockFoundHarness(ctx, t)
	delete(store.BlockExists, *block.Header.HashPrevBlock)
	delete(store.Blocks, *block.Header.HashPrevBlock)

	s.processBlockNotify.Set(*block.Hash(), true, ttlcache.DefaultTTL)

	reply := newBlockFoundReply()
	s.queueFetchedBlock(ctx, processBlockFound{hash: block.Hash(), baseURL: waitTestPeer, reply: reply}, block)

	requireNotStoredAnswer(ctx, t, s, store, block, reply)

	alternatives := s.catchupAlternatives.Get(*block.Hash())
	require.NotNil(t, alternatives, "the announcement must be kept as a catchup alternative")
	require.Len(t, alternatives.Value(), 1)
}

// movedTipChain reports tip as the best block and passes every other call through.
type movedTipChain struct {
	blockchain.ClientI
	tip    *model.BlockHeader
	height uint32
}

func (c *movedTipChain) GetBestBlockHeader(context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	return c.tip, &model.BlockHeaderMeta{Height: c.height}, nil
}

// TestBlockFoundWait_BacklogDivertIsNotSuccess: with UseCatchupWhenBehind and a backed-up queue, a
// block that does not extend the best chain is sent to catchup though its parent is stored. The
// caller must not get nil.
func TestBlockFoundWait_BacklogDivertIsNotSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, store, block := newProcessBlockFoundHarness(ctx, t)
	s.settings.BlockValidation.UseCatchupWhenBehind = true

	// The classifier sees a best block other than the parent, so the block classifies as a fork.
	// Only the classifier sees it: the store's best block is read by a background goroutine.
	s.blockClassifier = NewBlockClassifier(ulogger.TestLogger{}, 0, &movedTipChain{
		ClientI: s.blockchainClient,
		tip:     &model.BlockHeader{Version: 1, HashPrevBlock: &chainhash.Hash{1}, HashMerkleRoot: &chainhash.Hash{}, Bits: block.Header.Bits},
		height:  block.Height + 100,
	})

	// More than 10 queued blocks is a backlog.
	for i := 0; i < 11; i++ {
		h := chainhash.Hash{0xbb, byte(i)}
		s.blockPriorityQueue.Add(processBlockFound{hash: &h, baseURL: waitTestPeer}, PriorityDeepFork, 1)
	}

	serveBlock(t, block, 0, nil)

	reply := newBlockFoundReply()
	require.NoError(t, s.processBlockFoundChannel(ctx, processBlockFound{hash: block.Hash(), baseURL: waitTestPeer, reply: reply}))

	requireNotStoredAnswer(ctx, t, s, store, block, reply)
}

const waitTestSecondPeer = "http://second-peer"

// startBlockFoundIntake runs workers blockFoundCh workers, with no block-processing worker, so a
// queued block stays on the queue until the test starts one.
func startBlockFoundIntake(ctx context.Context, s *Server, workers int) {
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			for {
				select {
				case <-ctx.Done():
					return
				case bf := <-s.blockFoundCh:
					s.processBlockFoundChItem(ctx, workerID, bf)
				}
			}
		}(i)
	}
}

// failingSource makes the second peer's fetch of block fail with HTTP 500. Each fetch waits for
// release when it is not nil. It returns the fetch counter.
func failingSource(block *model.Block, release <-chan struct{}) *atomic.Int64 {
	var fetches atomic.Int64

	httpmock.RegisterResponder(http.MethodGet, waitTestSecondPeer+"/block/"+block.Hash().String(),
		func(*http.Request) (*http.Response, error) {
			fetches.Add(1)

			if release != nil {
				<-release
			}

			return httpmock.NewStringResponse(http.StatusInternalServerError, "peer failed"), nil
		})

	return &fetches
}

func blockFoundFromSecondPeer(ctx context.Context, s *Server, block *model.Block) <-chan error {
	done := make(chan error, 1)

	go func() {
		_, err := s.BlockFound(ctx, &blockvalidation_api.BlockFoundRequest{
			Hash:           block.Hash().CloneBytes(),
			BaseUrl:        waitTestSecondPeer,
			WaitToComplete: true,
		})
		if err != nil {
			err = errors.UnwrapGRPC(err)
		}
		done <- err
	}()

	return done
}

// requireStillWaiting fails if done has an answer: the block has not been processed yet.
func requireStillWaiting(t *testing.T, done <-chan error) {
	t.Helper()

	select {
	case err := <-done:
		t.Fatalf("the waiter was answered before the queued block was processed (err=%v)", err)
	case <-time.After(300 * time.Millisecond):
	}
}

// requireStoredAnswers starts the block-processing workers and checks both waiters get nil for a
// block that is then stored.
func requireStoredAnswers(ctx context.Context, t *testing.T, s *Server, store *blockchain_store.MockStore, block *model.Block, waiters ...<-chan error) {
	t.Helper()

	s.startBlockProcessingSystem(ctx)

	for _, done := range waiters {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(30 * time.Second):
			t.Fatal("a waiter was not answered after the block was processed")
		}
	}

	exists, err := store.GetBlockExists(ctx, block.Hash())
	require.NoError(t, err)
	require.True(t, exists)
}

// TestBlockFoundWait_SecondWaiterFetchFailureDoesNotSettleIt: a second waiter announces a hash that
// is already queued, from a source that cannot serve it. Its own failed fetch must not answer it:
// it gets the queued block's outcome. Both the plain path and the backed-up-queue path fetch before
// queueing, so both are covered.
func TestBlockFoundWait_SecondWaiterFetchFailureDoesNotSettleIt(t *testing.T) {
	for name, backlog := range map[string]bool{"addBlockToPriorityQueue": false, "backlog branch": true} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			s, store, block := newProcessBlockFoundHarness(ctx, t)

			serveBlock(t, block, 0, nil)
			secondFetches := failingSource(block, nil)
			startBlockFoundIntake(ctx, s, 1)

			first := blockFoundAndWait(ctx, s, block)
			require.Eventually(t, func() bool { return s.blockPriorityQueue.Contains(*block.Hash()) }, 10*time.Second, 10*time.Millisecond,
				"setup: the first announcement was never queued")

			if backlog {
				s.settings.BlockValidation.UseCatchupWhenBehind = true

				// More than 10 queued blocks is a backlog.
				for i := 0; i < 11; i++ {
					h := chainhash.Hash{0xcc, byte(i)}
					s.blockPriorityQueue.Add(processBlockFound{hash: &h, baseURL: waitTestPeer}, PriorityDeepFork, 1)
				}
			}

			second := blockFoundFromSecondPeer(ctx, s, block)
			require.Eventually(t, func() bool { return secondFetches.Load() >= 1 }, 10*time.Second, 10*time.Millisecond,
				"setup: the second announcement's source was never fetched")

			requireStillWaiting(t, second)
			requireStoredAnswers(ctx, t, s, store, block, first, second)
		})
	}
}

// TestBlockFoundWait_BlockQueuedDuringSecondWaitersFetch: the hash is not queued when the second
// waiter's fetch starts, and is queued by the time that fetch fails. The failure must not answer
// the waiter either.
func TestBlockFoundWait_BlockQueuedDuringSecondWaitersFetch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, store, block := newProcessBlockFoundHarness(ctx, t)

	release := make(chan struct{})

	serveBlock(t, block, 0, nil)
	secondFetches := failingSource(block, release)
	startBlockFoundIntake(ctx, s, 2)

	second := blockFoundFromSecondPeer(ctx, s, block)
	require.Eventually(t, func() bool { return secondFetches.Load() == 1 }, 10*time.Second, 10*time.Millisecond,
		"setup: the second announcement's fetch never started")

	first := blockFoundAndWait(ctx, s, block)
	require.Eventually(t, func() bool { return s.blockPriorityQueue.Contains(*block.Hash()) }, 10*time.Second, 10*time.Millisecond,
		"setup: the first announcement was never queued")

	close(release)

	requireStillWaiting(t, second)
	requireStoredAnswers(ctx, t, s, store, block, first, second)
}

// TestBlockFoundWait_FailedFetchWithNothingQueuedAnswersAtOnce: joining must not swallow the answer
// of a waiter with nothing to join.
func TestBlockFoundWait_FailedFetchWithNothingQueuedAnswersAtOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	httpmock.ActivateNonDefault(util.HTTPClient())
	t.Cleanup(httpmock.DeactivateAndReset)

	failingSource(block, nil)
	startBlockFoundIntake(ctx, s, 1)

	select {
	case err := <-blockFoundFromSecondPeer(ctx, s, block):
		require.Error(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("a waiter whose fetch failed, with nothing queued, was never answered")
	}
}

// hangingLookupChain ends the chosen lookup of hash only when its caller's context ends, as a real
// client does when the deadline passes during the call, and returns that context's error.
type hangingLookupChain struct {
	blockchain.ClientI
	hash       *chainhash.Hash
	hangExists bool
}

func (c *hangingLookupChain) GetBlockExists(ctx context.Context, hash *chainhash.Hash) (bool, error) {
	if !hash.IsEqual(c.hash) {
		return c.ClientI.GetBlockExists(ctx, hash)
	}

	if c.hangExists {
		<-ctx.Done()
		return false, ctx.Err()
	}

	return true, nil
}

func (c *hangingLookupChain) GetBlockHeader(ctx context.Context, hash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	if !hash.IsEqual(c.hash) {
		return c.ClientI.GetBlockHeader(ctx, hash)
	}

	<-ctx.Done()

	return nil, nil, ctx.Err()
}

// TestBlockFoundWait_ContextEndsDuringLookup: the caller's context ends during one of BlockFound's
// store reads. The caller must get ERR_CONTEXT_CANCELED, not an ERR_SERVICE_ERROR that blames the
// store.
func TestBlockFoundWait_ContextEndsDuringLookup(t *testing.T) {
	for name, hangExists := range map[string]bool{"existence check": true, "stored header read": false} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			s, _, block := newProcessBlockFoundHarness(ctx, t)
			s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings,
				&hangingLookupChain{ClientI: s.blockchainClient, hash: block.Hash(), hangExists: hangExists},
				blobmemory.New(), s.txStore, s.utxoStore, nil, nil)

			callerCtx, callerCancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer callerCancel()

			select {
			case err := <-blockFoundAndWait(callerCtx, s, block):
				require.Error(t, err)
				require.True(t, errors.Is(err, errors.ErrContextCanceled), "want ERR_CONTEXT_CANCELED, got %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("BlockFound ignored its caller's context")
			}
		})
	}
}

// failingExistsChain fails every existence check of hash while the caller's context is live.
type failingExistsChain struct {
	blockchain.ClientI
	hash *chainhash.Hash
}

func (c *failingExistsChain) GetBlockExists(ctx context.Context, hash *chainhash.Hash) (bool, error) {
	if !hash.IsEqual(c.hash) {
		return c.ClientI.GetBlockExists(ctx, hash)
	}

	return false, errors.NewStorageError("store unavailable")
}

// TestBlockFoundWait_LookupFailureWithLiveContextIsServiceError: a store failure while the caller
// is still waiting is the store's failure, so it stays a retry-later ERR_SERVICE_ERROR.
func TestBlockFoundWait_LookupFailureWithLiveContextIsServiceError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)
	s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings,
		&failingExistsChain{ClientI: s.blockchainClient, hash: block.Hash()},
		blobmemory.New(), s.txStore, s.utxoStore, nil, nil)

	select {
	case err := <-blockFoundAndWait(ctx, s, block):
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrServiceError), "want ERR_SERVICE_ERROR, got %v", err)
		require.False(t, errors.Is(err, errors.ErrContextCanceled), "the caller's context did not end, got %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("BlockFound did not return for a failed lookup")
	}
}

// TestBlockFoundWait_QueuedBlockFinishesDuringSecondWaitersFetch: the hash is queued when the second
// waiter's fetch starts, and a worker takes and stores it before that fetch fails. By then there is
// no entry left to join, so only a join before the fetch gives the waiter the stored outcome.
func TestBlockFoundWait_QueuedBlockFinishesDuringSecondWaitersFetch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, store, block := newProcessBlockFoundHarness(ctx, t)

	release := make(chan struct{})

	serveBlock(t, block, 0, nil)
	secondFetches := failingSource(block, release)
	startBlockFoundIntake(ctx, s, 2)

	first := blockFoundAndWait(ctx, s, block)
	require.Eventually(t, func() bool { return s.blockPriorityQueue.Contains(*block.Hash()) }, 10*time.Second, 10*time.Millisecond,
		"setup: the first announcement was never queued")

	second := blockFoundFromSecondPeer(ctx, s, block)
	require.Eventually(t, func() bool { return secondFetches.Load() == 1 }, 10*time.Second, 10*time.Millisecond,
		"setup: the second announcement's fetch never started")

	s.startBlockProcessingSystem(ctx)

	select {
	case err := <-first:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("the first waiter was not answered after the block was processed")
	}

	close(release)

	select {
	case err := <-second:
		require.NoError(t, err, "the second waiter's failed fetch must not override the queued block's outcome")
	case <-time.After(30 * time.Second):
		t.Fatal("the second waiter was not answered after the block was processed")
	}

	exists, err := store.GetBlockExists(ctx, block.Hash())
	require.NoError(t, err)
	require.True(t, exists)
}
