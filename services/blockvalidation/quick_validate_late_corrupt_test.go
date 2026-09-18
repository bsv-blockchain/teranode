package blockvalidation

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// A corrupt verdict is still reachable AFTER the whole-block binding pass, and these
// tests are what keeps the corrupt branch covered now that an unbound body is rejected
// before the pipeline (bitcoin-sv/teranode#4838).
//
// The route is CVE-2012-2459's own arithmetic. A subtree of three nodes and one of
// four whose tail is the third node repeated produce the SAME merkle root, because
// BuildMerkleTreeStoreFromBytes duplicates the last node at every odd level. So a
// structure blob replaced between the whole-block read and the batch read with the
// duplicate-tail representation of itself:
//
//   - satisfies the per-batch anchor, since the recomputed root still equals the key;
//   - satisfies the tail CheckMerkleRoot, since the rebuilt slice has that same root;
//   - is built, queued for writing, and mutated on; and then
//   - fails the tail duplicate-transaction scan as ERR_BLOCK_CORRUPT.
//
// That is a late corrupt verdict with a non-empty freshly-written set, which is what
// the corrupt branch's write wait and its cleanup exist for.

// lateCorruptFixture is one block whose structure blob turns into its own
// duplicate-tail representation from the second read onwards.
type lateCorruptFixture struct {
	block       *model.Block
	subtreeHash *chainhash.Hash
	store       *replacingSubtreeStore
}

// newLateCorruptFixture installs the replacing blob store on the suite and returns the
// block. The subtree_data carries the duplicated tail from the start: the whole-block
// pass never opens subtree_data, so only the structure has to change between reads.
func newLateCorruptFixture(t *testing.T, suite *CatchupTestSuite, height uint32) *lateCorruptFixture {
	t.Helper()

	store := newReplacingSubtreeStore(suite.Server.subtreeStore)
	suite.Server.subtreeStore = store
	suite.Server.blockValidation.subtreeStore = store

	txs := transactions.CreateTestTransactionChainWithCount(t, 4)
	coinbaseTx := txs[0]
	regularTxs := txs[1:]

	honest, err := subtreepkg.NewIncompleteTreeByLeafCount(3)
	require.NoError(t, err)
	require.NoError(t, honest.AddCoinbaseNode())
	require.NoError(t, honest.AddNode(*regularTxs[0].TxIDChainHash(), 1, 1))
	require.NoError(t, honest.AddNode(*regularTxs[1].TxIDChainHash(), 2, 2))

	duplicated, err := subtreepkg.NewIncompleteTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, duplicated.AddCoinbaseNode())
	require.NoError(t, duplicated.AddNode(*regularTxs[0].TxIDChainHash(), 1, 1))
	require.NoError(t, duplicated.AddNode(*regularTxs[1].TxIDChainHash(), 2, 2))
	require.NoError(t, duplicated.AddNode(*regularTxs[1].TxIDChainHash(), 2, 2))

	// The whole point of the fixture. Asserted rather than assumed so it cannot drift
	// into passing the anchor for the wrong reason.
	require.Equal(t, honest.RootHash().String(), duplicated.RootHash().String(),
		"the duplicate-tail representation must recompute to the same root")
	require.Equal(t, honest.Size(), duplicated.Size())

	subtreeHash := honest.RootHash()

	honestBytes, err := honest.Serialize()
	require.NoError(t, err)
	require.NoError(t, store.Set(t.Context(), subtreeHash[:], fileformat.FileTypeSubtreeToCheck, honestBytes))

	duplicatedBytes, err := duplicated.Serialize()
	require.NoError(t, err)
	store.replaceAfterFirstRead(subtreeHash, fileformat.FileTypeSubtreeToCheck, duplicatedBytes)

	subtreeData := subtreepkg.NewSubtreeData(duplicated)
	require.NoError(t, subtreeData.AddTx(coinbaseTx, 0))
	require.NoError(t, subtreeData.AddTx(regularTxs[0], 1))
	require.NoError(t, subtreeData.AddTx(regularTxs[1], 2))
	require.NoError(t, subtreeData.AddTx(regularTxs[1], 3))

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)
	require.NoError(t, store.Set(t.Context(), subtreeHash[:], fileformat.FileTypeSubtreeData, subtreeDataBytes))

	block := testhelpers.CreateTestBlocks(t, 1)[0]
	block.Height = height
	block.CoinbaseTx = coinbaseTx
	block.Subtrees = []*chainhash.Hash{subtreeHash}
	block.TransactionCount = 4

	block.Header.HashMerkleRoot, err = honest.RootHashWithReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, 0)
	require.NoError(t, err)

	return &lateCorruptFixture{block: block, subtreeHash: subtreeHash, store: store}
}

// TestQuickValidateBlock_LateCorruptVerdictUnwrapped pins the corrupt guard at
// quickValidateBlock's processBlockSubtrees call site: a corrupt verdict produced by
// the TAIL, after the pipeline has run, must be returned UNWRAPPED and must not be
// shadowed by the outer ErrProcessing wrap, which would mis-route it as a transient
// local failure and retry the same corrupt body.
//
// Mutation proof: deleting the `if errors.IsBlockCorrupt(err) { return err }` guard
// after processBlockSubtrees makes the error fall through to NewProcessingError, so
// IsBlockCorrupt goes false and errors.Is(err, ErrProcessing) goes true.
func TestQuickValidateBlock_LateCorruptVerdictUnwrapped(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()
	setupQuickValidateMocks(suite)

	fixture := newLateCorruptFixture(t, suite, 100)

	err := suite.Server.blockValidation.quickValidateBlock(suite.Ctx, fixture.block, "test", "")
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err),
		"a late corrupt verdict must be returned unwrapped, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrProcessing),
		"a corrupt verdict must NOT be shadowed by an outer ErrProcessing (bitcoin-sv/teranode#4692)")
}

// TestQuickValidateBlockAsync_LateCorruptVerdictUnwrapped is the async twin: the
// corrupt verdict from processBlockSubtreesPipelineAsync's tail must likewise be
// returned unwrapped, with the write job for this block already queued.
func TestQuickValidateBlockAsync_LateCorruptVerdictUnwrapped(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()
	setupQuickValidateMocks(suite)

	fixture := newLateCorruptFixture(t, suite, 100)

	writeJobsChan := make(chan *SubtreeWriteJob, 16)

	g, gCtx := errgroup.WithContext(suite.Ctx)
	g.Go(func() error { return suite.Server.blockValidation.subtreeWriteWorker(gCtx, writeJobsChan) })

	_, freshlyWritten, err := suite.Server.blockValidation.quickValidateBlockAsync(suite.Ctx, fixture.block, "test", "", writeJobsChan)

	close(writeJobsChan)
	require.NoError(t, g.Wait())

	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err),
		"a late corrupt verdict must be returned unwrapped on the async path, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrProcessing),
		"a corrupt verdict must NOT be shadowed by an outer ErrProcessing (bitcoin-sv/teranode#4692)")
	require.Contains(t, freshlyWritten, *fixture.subtreeHash,
		"the build phase ran, so this attempt owns a freshly-written blob for cleanup to delete")
}

// TestTryQuickValidation_LateCorruptPath_WaitsForDelayedWriteBeforeCleanup restores the
// corrupt-branch ordering coverage (bitcoin-sv/teranode#4692) against a body that
// reaches the build phase: tryQuickValidation's corrupt branch must not run
// removeCatchupSubtreeFiles until every write job this block queued has actually been
// written (or skipped) by a worker, or the worker's later Set would silently resurrect
// a blob cleanup had just deleted.
//
// Mutation proof: replacing the context-aware select{} in tryQuickValidation with a
// bare removeCatchupSubtreeFiles call would let this test observe the corrupt branch
// return, and the FileTypeSubtree blob be absent, WHILE the write is still gated.
func TestTryQuickValidation_LateCorruptPath_WaitsForDelayedWriteBeforeCleanup(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()
	setupQuickValidateMocks(suite)

	rec := &banScoreRecorder{}
	suite.Server.blockValidation.p2pClient = rec

	fixture := newLateCorruptFixture(t, suite, 100)

	// The job this attempt queues goes into relayChan; gatedWriteJobRelay holds it there
	// until release is closed, then forwards it to workerChan where the real
	// subtreeWriteWorker performs the write.
	relayChan := make(chan *SubtreeWriteJob, 16)
	workerChan := make(chan *SubtreeWriteJob, 16)
	release := make(chan struct{})

	go gatedWriteJobRelay(relayChan, workerChan, release)
	go func() { _ = suite.Server.blockValidation.subtreeWriteWorker(suite.Ctx, workerChan) }()

	catchupCtx := &CatchupContext{
		blockUpTo:               fixture.block,
		baseURL:                 "http://peer",
		peerID:                  "peer-corrupt",
		startTime:               time.Now(),
		useQuickValidation:      true,
		highestCheckpointHeight: 1000,
	}

	type result struct {
		tryNormal bool
		err       error
	}

	resultCh := make(chan result, 1)

	go func() {
		tryNormal, err := suite.Server.tryQuickValidation(suite.Ctx, fixture.block, catchupCtx, "peer-corrupt", "http://peer", relayChan, nil)
		resultCh <- result{tryNormal, err}
	}()

	// The write is still gated behind release: tryQuickValidation must not have
	// returned, and the blob must not exist yet.
	select {
	case <-resultCh:
		t.Fatal("tryQuickValidation returned before the delayed write was released")
	case <-time.After(200 * time.Millisecond):
	}

	exists, err := suite.Server.subtreeStore.Exists(suite.Ctx, fixture.subtreeHash[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.False(t, exists, "the write is still gated behind release")

	close(release)

	var res result

	select {
	case res = <-resultCh:
	case <-time.After(5 * time.Second):
		t.Fatal("tryQuickValidation did not return after the delayed write was released")
	}

	require.Error(t, res.err)
	require.True(t, errors.IsBlockCorrupt(res.err), "the corrupt verdict must propagate, got: %v", res.err)
	require.False(t, res.tryNormal, "a corrupt quick-path verdict must NOT fall through to normal validation of the same body")
	require.Equal(t, []string{"peer-corrupt"}, rec.struck(), "the serving peer must be struck for the corrupt body")

	// Cleanup ran AFTER the write landed (the <-waitDone case, not <-ctx.Done()): the
	// freshly-written FileTypeSubtree blob must be gone, not resurrected by a race.
	exists, err = suite.Server.subtreeStore.Exists(suite.Ctx, fixture.subtreeHash[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.False(t, exists, "cleanup must have run after the delayed write landed, deleting it")
}

// TestTryQuickValidation_LateCorruptPath_SiblingWorkerErrorDoesNotHang restores the
// second half of that ordering coverage: tryQuickValidation shares its context with
// the write-worker pool's errgroup, so a SIBLING worker's failure cancels it too. A
// bare wg.Wait() would hang forever on this block's stranded job; the context-aware
// select must return promptly via <-ctx.Done(), skip removeCatchupSubtreeFiles
// entirely, and still surface the original corrupt error unchanged.
//
// No consumer is ever attached to writeJobsChan, modelling every real worker having
// already exited via its own ctx.Done() branch before draining this block's job.
func TestTryQuickValidation_LateCorruptPath_SiblingWorkerErrorDoesNotHang(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()
	setupQuickValidateMocks(suite)

	fixture := newLateCorruptFixture(t, suite, 100)

	counting := &delCountingStore{Store: suite.Server.subtreeStore}
	suite.Server.subtreeStore = counting
	suite.Server.blockValidation.subtreeStore = counting

	rec := &banScoreRecorder{}
	suite.Server.blockValidation.p2pClient = rec

	catchupCtx := &CatchupContext{
		blockUpTo:               fixture.block,
		baseURL:                 "http://peer",
		peerID:                  "peer-corrupt",
		startTime:               time.Now(),
		useQuickValidation:      true,
		highestCheckpointHeight: 1000,
	}

	// Shared errgroup context, mirroring fetchAndValidateBlocks' real wiring. Delayed so
	// THIS block's own pipeline completes and returns its corrupt verdict — with its job
	// still sitting unconsumed in writeJobsChan — before the sibling fails.
	errGroup, gCtx := errgroup.WithContext(suite.Ctx)
	errGroup.Go(func() error {
		time.Sleep(200 * time.Millisecond)
		return errors.NewProcessingError("sibling worker: simulated write failure elsewhere in the pool")
	})

	writeJobsChan := make(chan *SubtreeWriteJob, 16)

	type result struct {
		tryNormal bool
		err       error
	}

	resultCh := make(chan result, 1)

	go func() {
		tryNormal, err := suite.Server.tryQuickValidation(gCtx, fixture.block, catchupCtx, "peer-corrupt", "http://peer", writeJobsChan, nil)
		resultCh <- result{tryNormal, err}
	}()

	select {
	case res := <-resultCh:
		require.Error(t, res.err)
		require.True(t, errors.IsBlockCorrupt(res.err), "the original corrupt error must be returned unchanged, got: %v", res.err)
		require.False(t, res.tryNormal)
	case <-time.After(5 * time.Second):
		t.Fatal("tryQuickValidation hung waiting on a stranded write job after a sibling worker's own error cancelled the shared context")
	}

	require.Equal(t, 0, counting.count(), "removeCatchupSubtreeFiles must be skipped entirely — no Del call — when the shared context is cancelled")
	require.Equal(t, []string{"peer-corrupt"}, rec.struck(), "the serving peer must still be struck even when cleanup is skipped")

	_ = errGroup.Wait() // drain the sibling goroutine so it doesn't leak past the test
}
