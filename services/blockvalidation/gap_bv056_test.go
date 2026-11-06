package blockvalidation

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// gap056FailingUnlockStore is the real sqlitememory UTXO store except that the first failUnlocks
// post-commit unlocks (SetLocked(..., false)) fail. Later unlocks go through to the real store.
//
// The failure is a bare driver-style error, not a teranode StorageError, as the SQL store gives
// from db.Begin and txn.Commit. A StorageError would read as transient through any wrap, so it
// could not show that commitBlock's own ServiceError wrap is what the legacy park keys on.
type gap056FailingUnlockStore struct {
	utxo.Store

	failUnlocks int32
	unlockCalls atomic.Int32

	// onFail, when set, runs before each injected failure. A test uses it to cancel the catch-up
	// context, as a sibling failure in the errgroup does.
	onFail func()

	// beforeUnlock, when set, runs at the start of every unlock, before any injected failure. A
	// test uses it to hold the post-commit unlock in flight.
	beforeUnlock func()

	// panicOnFail makes each injected failure a panic instead of an error.
	panicOnFail bool
}

func (s *gap056FailingUnlockStore) SetLocked(ctx context.Context, txHashes []chainhash.Hash, value bool) error {
	if !value && s.beforeUnlock != nil {
		s.beforeUnlock()
	}

	if !value && s.unlockCalls.Add(1) <= s.failUnlocks {
		if s.onFail != nil {
			s.onFail()
		}

		if s.panicOnFail {
			panic("gap056: simulated panic in the post-commit unlock")
		}

		return gap056DriverError{}
	}

	return s.Store.SetLocked(ctx, txHashes, value)
}

// gap056DriverError stands in for a raw database driver error. A local type, since depguard forbids
// the stdlib errors package and forbidigo forbids fmt.Errorf in this package.
type gap056DriverError struct{}

func (gap056DriverError) Error() string { return "gap056: simulated unlock failure after AddBlock" }

// shrinkPostCommitUnlockRetryBackoff makes the post-commit unlock retries, and the existence probe's
// retries and recheck, near-immediate for one test.
func shrinkPostCommitUnlockRetryBackoff(t *testing.T) {
	t.Helper()

	savedBackoff, savedRecheck := postCommitUnlockRetryBackoff, postCommitProbeRecheckDelay
	postCommitUnlockRetryBackoff = time.Millisecond
	postCommitProbeRecheckDelay = time.Millisecond

	t.Cleanup(func() {
		postCommitUnlockRetryBackoff = savedBackoff
		postCommitProbeRecheckDelay = savedRecheck
	})
}

// gap056QuickValidate runs one honest single-subtree block through tryQuickValidation with a real
// write worker, with the UTXO store failing the first failUnlocks unlocks. It returns the block,
// its served subtree, the spending child, the store double and tryQuickValidation's results.
func gap056QuickValidate(t *testing.T, failUnlocks int32) (*preBindHarness, *model.Block, *chainhash.Hash, *chainhash.Hash, *gap056FailingUnlockStore, bool, error) {
	t.Helper()

	return gap056QuickValidateWith(t, gap056Config{failUnlocks: failUnlocks})
}

// gap056Config shapes one gap056QuickValidateWith run.
type gap056Config struct {
	// failUnlocks is how many post-commit unlocks the UTXO store fails.
	failUnlocks int32
	// panicOnUnlockFail makes each injected unlock failure a panic.
	panicOnUnlockFail bool
	// cancelOnUnlockFail cancels the context given to tryQuickValidation at each injected unlock
	// failure, as a sibling failure in the catch-up errgroup does.
	cancelOnUnlockFail bool
	// wrapChain, when set, wraps the blockchain client that BlockValidation commits through. It
	// gets the cancel func of the context given to tryQuickValidation, so a wrapper can end that
	// context mid-commit.
	wrapChain func(blockchain.ClientI, context.CancelFunc) blockchain.ClientI
	// notCommitted skips the precondition that AddBlock stored the block.
	notCommitted bool
	// beforeUnlock, when set, runs at the start of every post-commit unlock with the
	// BlockValidation that is committing, while that unlock is in flight.
	beforeUnlock func(bv *BlockValidation)
	// beforeRun, when set, gets the spending child's hash before tryQuickValidation runs.
	beforeRun func(child *chainhash.Hash)
}

// gap056QuickValidateWith is gap056QuickValidate with the failure shape given by cfg.
func gap056QuickValidateWith(t *testing.T, cfg gap056Config) (*preBindHarness, *model.Block, *chainhash.Hash, *chainhash.Hash, *gap056FailingUnlockStore, bool, error) {
	t.Helper()

	shrinkPostCommitUnlockRetryBackoff(t)

	h := newPreBindHarness(t, nil)

	quickCtx, cancelQuick := context.WithCancel(h.ctx)
	t.Cleanup(cancelQuick)

	failing := &gap056FailingUnlockStore{Store: h.utxoStore, failUnlocks: cfg.failUnlocks, panicOnFail: cfg.panicOnUnlockFail}
	if cfg.cancelOnUnlockFail {
		failing.onFail = cancelQuick
	}

	if cfg.beforeUnlock != nil {
		failing.beforeUnlock = func() { cfg.beforeUnlock(h.bv) }
	}

	h.bv.utxoStore = failing

	if cfg.wrapChain != nil {
		h.bv.blockchainClient = cfg.wrapChain(h.chain, cancelQuick)
	}

	coinbase := preBindCoinbase(t, 0x56)
	parent := h.storeGenuineParent(0x56)
	child := preBindSpendOf(t, parent, 9_000)

	served := h.oneSubtreeBody(coinbase, child)

	block := h.newPreBindBlock(coinbase,
		[]*chainhash.Hash{served.RootHash()},
		composeBlockMerkleRoot(t, []chainhash.Hash{coinbaseSubstitutedRoot(t, served, coinbase)}),
		2)

	server, catchupCtx := h.newAbortServer()
	catchupCtx.blockUpTo = block

	writeJobsChan := make(chan *SubtreeWriteJob, 16)

	g, gCtx := errgroup.WithContext(h.ctx)
	g.Go(func() error { return h.bv.subtreeWriteWorker(gCtx, writeJobsChan) })

	if cfg.beforeRun != nil {
		cfg.beforeRun(child.TxIDChainHash())
	}

	tryNormal, quickErr := server.tryQuickValidation(quickCtx, block, catchupCtx, "peer", "http://peer", writeJobsChan, nil)

	close(writeJobsChan)
	require.NoError(t, g.Wait())

	if cfg.notCommitted {
		return h, block, served.RootHash(), child.TxIDChainHash(), failing, tryNormal, quickErr
	}

	// Preconditions: the unlock was attempted (so the failure was injected), and AddBlock committed
	// the block with subtrees_set.
	require.Positive(t, failing.unlockCalls.Load(), "precondition: the post-commit unlock must have been attempted")

	_, meta, err := h.chain.GetBlockHeader(h.ctx, block.Hash())
	require.NoError(t, err, "precondition: the block must have been committed by AddBlock")
	require.True(t, meta.SubtreesSet, "precondition: the quick route commits with subtrees_set=true")

	return h, block, served.RootHash(), child.TxIDChainHash(), failing, tryNormal, quickErr
}

// TestGapBV056_PostCommitUnlockFailureKeepsCommittedBlocksSubtreeFiles covers GAP-BV-056
// (BV-QUICK-015, BV-QUICK-021, BV-COMMIT-002), bitcoin-sv/teranode#4987.
//
// Once AddBlock has committed a block with subtrees_set and mined_set, a failing post-commit unlock
// is retried, and the block's .subtree files are never deleted. Here only the first unlock fails,
// so the retry clears the lock and quick validation succeeds.
//
// Mutation proof: calling unlockSubtreeTransactionsIfNeeded once instead of
// unlockSubtreeTransactionsWithRetry in commitBlock fails the first assertion, because the single
// unlock attempt fails and quick validation returns its error.
func TestGapBV056_PostCommitUnlockFailureKeepsCommittedBlocksSubtreeFiles(t *testing.T) {
	h, _, subtreeRoot, childHash, failing, tryNormal, quickErr := gap056QuickValidate(t, 1)

	require.NoError(t, quickErr, "a retried unlock that succeeds must leave quick validation successful")
	require.False(t, tryNormal, "a committed block must not fall through to normal validation")
	require.GreaterOrEqual(t, failing.unlockCalls.Load(), int32(2), "the failed unlock must have been retried")

	exists, err := h.subtreeStore.Exists(h.ctx, subtreeRoot[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, exists, "committed block has subtrees_set=true but its .subtree file was deleted")

	created, err := h.utxoStore.Get(h.ctx, childHash, fields.Locked)
	require.NoError(t, err)
	require.False(t, created.Locked, "committed block's outputs must not stay locked after the unlock failure")
}

// TestGapBV056_PostCommitUnlockExhaustedAbortsWithoutRemovingFiles covers the case the retries
// cannot fix: every unlock fails. tryQuickValidation must abort with the block-committed marker,
// keep the committed block's .subtree file, and return a transient error class, so the legacy park
// keeps the block for a retry instead of rejecting one that is already stored.
//
// The outputs stay locked: that is the known residual of bitcoin-sv/teranode#4987, and the last
// assertion records it so a future recovery path has to change this test on purpose.
//
// Mutation proof: removing the isBlockCommitted branch in tryQuickValidation sends the error to the
// local-fault branch, which deletes the .subtree file and returns tryNormal=true. Wrapping the
// exhausted unlock in a ProcessingError instead of a ServiceError fails the transient assertion.
func TestGapBV056_PostCommitUnlockExhaustedAbortsWithoutRemovingFiles(t *testing.T) {
	h, _, subtreeRoot, childHash, failing, tryNormal, quickErr := gap056QuickValidate(t, 1_000)

	require.Error(t, quickErr)
	require.True(t, isBlockCommitted(quickErr), "the post-commit failure must carry the block-committed marker: %v", quickErr)
	require.True(t, errors.IsTransientLocalError(quickErr), "the post-commit failure must read as transient to the legacy park: %v", quickErr)
	require.False(t, tryNormal, "a committed block must not fall through to normal validation")
	require.Equal(t, int32(postCommitUnlockRetries+1), failing.unlockCalls.Load(), "every retry must have been attempted")

	exists, err := h.subtreeStore.Exists(h.ctx, subtreeRoot[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, exists, "committed block has subtrees_set=true but its .subtree file was deleted")

	created, err := h.utxoStore.Get(h.ctx, childHash, fields.Locked)
	require.NoError(t, err)
	require.True(t, created.Locked, "no recovery path unlocks a committed block's outputs after the retries are exhausted")
}

// TestGapBV056_PostCommitUnlockPanicAbortsWithoutRemovingFiles covers a panic in the post-commit
// unlock. recoverChildPanic turns it into a local-panic error, and commitBlock wraps that in the
// block-committed error, so the error tryQuickValidation sees carries both markers. The committed
// block's .subtree file must stay, and the panic marker must survive for releaseCatchupLock.
//
// Mutation proof: letting the isLocalPanic branch in tryQuickValidation take an error that carries
// both markers (adding && !isLocalPanic(err) to the isBlockCommitted check) sends it to the
// local-panic sweep, which deletes the committed block's .subtree file.
func TestGapBV056_PostCommitUnlockPanicAbortsWithoutRemovingFiles(t *testing.T) {
	h, _, subtreeRoot, _, _, tryNormal, quickErr := gap056QuickValidateWith(t, gap056Config{failUnlocks: 1_000, panicOnUnlockFail: true})

	require.Error(t, quickErr)
	require.True(t, isBlockCommitted(quickErr), "the post-commit failure must carry the block-committed marker: %v", quickErr)
	require.True(t, isLocalPanic(quickErr), "the panic marker must survive the block-committed wrap: %v", quickErr)
	require.False(t, tryNormal, "a committed block must not fall through to normal validation")

	exists, err := h.subtreeStore.Exists(h.ctx, subtreeRoot[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, exists, "committed block has subtrees_set=true but its .subtree file was deleted")
}

// TestGapBV056_PostCommitUnlockRetriesSurviveCancelledCatchupContext covers a cancelled catch-up
// context during the post-commit unlock. In catch-up, ctx is the errgroup context, which a failure
// elsewhere in the run cancels. Here the first unlock fails and cancels ctx in the same step. The
// retries must still run, and the block's outputs must end unlocked.
//
// Mutation proof: running the retries on ctx instead of context.WithoutCancel(ctx) in
// unlockSubtreeTransactionsWithRetry stops after one unlock, with only "context canceled", and
// leaves the child output locked.
func TestGapBV056_PostCommitUnlockRetriesSurviveCancelledCatchupContext(t *testing.T) {
	h, _, subtreeRoot, childHash, failing, tryNormal, quickErr := gap056QuickValidateWith(t, gap056Config{
		failUnlocks:        1,
		cancelOnUnlockFail: true,
	})

	require.NoError(t, quickErr, "a cancelled catch-up context must not stop the post-commit unlock retries")
	require.False(t, tryNormal, "a committed block must not fall through to normal validation")
	require.GreaterOrEqual(t, failing.unlockCalls.Load(), int32(2), "the failed unlock must have been retried")

	exists, err := h.subtreeStore.Exists(h.ctx, subtreeRoot[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, exists, "committed block has subtrees_set=true but its .subtree file was deleted")

	created, err := h.utxoStore.Get(h.ctx, childHash, fields.Locked)
	require.NoError(t, err)
	require.False(t, created.Locked, "committed block's outputs must not stay locked when the catch-up context is cancelled")
}

// TestGapBV056_PostCommitUnlockTimeoutKeepsStoreError covers retries stopped by postCommitTimeout.
// The returned error must carry the last store error, not only the context error.
func TestGapBV056_PostCommitUnlockTimeoutKeepsStoreError(t *testing.T) {
	saved := postCommitTimeout
	postCommitTimeout = 0

	t.Cleanup(func() { postCommitTimeout = saved })

	_, _, _, _, _, tryNormal, quickErr := gap056QuickValidateWith(t, gap056Config{failUnlocks: 1_000}) //nolint:dogsled // only the results matter here

	require.Error(t, quickErr)
	require.True(t, isBlockCommitted(quickErr), "the post-commit failure must carry the block-committed marker: %v", quickErr)
	require.False(t, tryNormal)
	require.Contains(t, quickErr.Error(), gap056DriverError{}.Error(), "the store error must stay in the returned error")
}

// gap056AmbiguousAddBlockChain stands in for a blockchain client whose AddBlock reply is lost. When
// store is true it calls the real AddBlock first, so the block is stored, then returns an error, as
// a deadline that ends after the server committed does.
//
// The existence probe (GetBlockExists) can be shaped too. With probeErr set it fails: every call
// when probeFails is 0, otherwise only the first probeFails calls. The first staleFalse calls that
// do not fail answer false, as a "not stored" answer cached before the commit does.
type gap056AmbiguousAddBlockChain struct {
	blockchain.ClientI

	store      bool
	probeErr   error
	probeFails int32
	staleFalse int32

	// cancelBeforeErr, when set, runs before AddBlock returns its error, so the caller's context
	// has ended by the time commitBlock sees the error.
	cancelBeforeErr context.CancelFunc

	probeCalls atomic.Int32
}

func (c *gap056AmbiguousAddBlockChain) AddBlock(ctx context.Context, block *model.Block, peerID string, opts ...options.StoreBlockOption) error {
	if c.store {
		if err := c.ClientI.AddBlock(ctx, block, peerID, opts...); err != nil {
			return err
		}
	}

	if c.cancelBeforeErr != nil {
		c.cancelBeforeErr()

		return context.Canceled
	}

	return context.DeadlineExceeded
}

func (c *gap056AmbiguousAddBlockChain) GetBlockExists(ctx context.Context, hash *chainhash.Hash) (bool, error) {
	call := c.probeCalls.Add(1)

	if c.probeErr != nil && (c.probeFails == 0 || call <= c.probeFails) {
		return false, c.probeErr
	}

	if c.probeErr != nil {
		call -= c.probeFails
	}

	if call <= c.staleFalse {
		return false, nil
	}

	return c.ClientI.GetBlockExists(ctx, hash)
}

// TestGapBV056_AddBlockErrorAfterCommitFinishesTheCommit covers an AddBlock that stores the block
// but returns an error to the client. commitBlock must find the block stored and finish the commit:
// no sweep of the .subtree file, no fall-through, and the outputs unlocked.
//
// Mutation proof: returning the AddBlock error as a plain ProcessingError, without the existence
// probe, sends it to the local-fault branch, which deletes the .subtree file of the stored block
// and returns tryNormal=true.
func TestGapBV056_AddBlockErrorAfterCommitFinishesTheCommit(t *testing.T) {
	h, block, subtreeRoot, childHash, _, tryNormal, quickErr := gap056QuickValidateWith(t, gap056Config{
		wrapChain: func(c blockchain.ClientI, _ context.CancelFunc) blockchain.ClientI {
			return &gap056AmbiguousAddBlockChain{ClientI: c, store: true}
		},
	})

	require.NoError(t, quickErr, "a stored block must finish its commit after a lost AddBlock reply")
	require.False(t, tryNormal, "a stored block must not fall through to normal validation")

	_, meta, err := h.chain.GetBlockHeader(h.ctx, block.Hash())
	require.NoError(t, err)
	require.True(t, meta.SubtreesSet)

	exists, err := h.subtreeStore.Exists(h.ctx, subtreeRoot[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, exists, "stored block has subtrees_set=true but its .subtree file was deleted")

	created, err := h.utxoStore.Get(h.ctx, childHash, fields.Locked)
	require.NoError(t, err)
	require.False(t, created.Locked, "the commit tail must unlock the stored block's outputs")
}

// TestGapBV056_AddBlockErrorWithFailedProbeAbortsWithoutRemovingFiles covers an AddBlock error when
// every existence probe attempt fails, so commitBlock cannot tell if the block is stored. It must
// abort with the block-committed marker and keep the .subtree file.
//
// The block is stored here, and its outputs stay locked: the residual tracked in
// bsv-blockchain/teranode#2015, reached only once the probe retries run out. The last assertion
// records it so a future recovery path has to change this test on purpose.
func TestGapBV056_AddBlockErrorWithFailedProbeAbortsWithoutRemovingFiles(t *testing.T) {
	var chain *gap056AmbiguousAddBlockChain

	h, _, subtreeRoot, childHash, _, tryNormal, quickErr := gap056QuickValidateWith(t, gap056Config{
		wrapChain: func(c blockchain.ClientI, _ context.CancelFunc) blockchain.ClientI {
			chain = &gap056AmbiguousAddBlockChain{ClientI: c, store: true, probeErr: gap056DriverError{}}
			return chain
		},
		notCommitted: true,
	})

	require.Error(t, quickErr)
	require.True(t, isBlockCommitted(quickErr), "an AddBlock error with an unknown outcome must carry the block-committed marker: %v", quickErr)
	require.True(t, errors.IsTransientLocalError(quickErr), "an unknown outcome must read as transient to the legacy park: %v", quickErr)
	require.False(t, tryNormal, "a block that may be stored must not fall through to normal validation")

	exists, err := h.subtreeStore.Exists(h.ctx, subtreeRoot[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, exists, "a block that may be stored must keep its .subtree file")

	require.Equal(t, int32(postCommitProbeRetries+1), chain.probeCalls.Load(), "every probe retry must have been attempted")

	created, err := h.utxoStore.Get(h.ctx, childHash, fields.Locked)
	require.NoError(t, err)
	require.True(t, created.Locked, "no recovery path unlocks a stored block's outputs once the probe retries are exhausted")
}

// TestGapBV056_AddBlockErrorNotStoredKeepsLocalFaultRoute pins the unchanged route: AddBlock fails,
// the probe and its recheck both say the block is not stored and ctx is live, so the error stays a
// local fault. The attempt's .subtree file is swept and catch-up falls through to normal validation.
func TestGapBV056_AddBlockErrorNotStoredKeepsLocalFaultRoute(t *testing.T) {
	var chain *gap056AmbiguousAddBlockChain

	h, block, subtreeRoot, _, _, tryNormal, quickErr := gap056QuickValidateWith(t, gap056Config{
		wrapChain: func(c blockchain.ClientI, _ context.CancelFunc) blockchain.ClientI {
			chain = &gap056AmbiguousAddBlockChain{ClientI: c}
			return chain
		},
		notCommitted: true,
	})

	require.NoError(t, quickErr)
	require.True(t, tryNormal, "a block proven not stored takes the local-fault route to normal validation")
	// Two probes from commitBlock (the answer and its recheck), then the local-fault branch's own
	// check, right before the sweep, that no other route stored the block meanwhile.
	require.Equal(t, int32(3), chain.probeCalls.Load(), "a not-stored answer must be rechecked once before the sweep")

	stored, err := h.chain.GetBlockExists(h.ctx, block.Hash())
	require.NoError(t, err)
	require.False(t, stored)

	exists, err := h.subtreeStore.Exists(h.ctx, subtreeRoot[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.False(t, exists, "the local-fault route sweeps the attempt's .subtree file")
}

// TestGapBV056_AddBlockErrorWithTransientProbeFailureFinishesTheCommit covers an AddBlock that
// stores the block but loses its reply, followed by one failed existence probe. The probe must be
// retried, find the block stored and finish the commit: files kept, outputs unlocked. Before the
// probe was retried, the single failure aborted the commit and left the outputs locked for good.
//
// Mutation proof: running the probe once instead of through retry.Retry fails the first assertion,
// and the child output stays locked.
func TestGapBV056_AddBlockErrorWithTransientProbeFailureFinishesTheCommit(t *testing.T) {
	var chain *gap056AmbiguousAddBlockChain

	h, _, subtreeRoot, childHash, _, tryNormal, quickErr := gap056QuickValidateWith(t, gap056Config{
		wrapChain: func(c blockchain.ClientI, _ context.CancelFunc) blockchain.ClientI {
			chain = &gap056AmbiguousAddBlockChain{ClientI: c, store: true, probeErr: gap056DriverError{}, probeFails: 1}
			return chain
		},
	})

	require.NoError(t, quickErr, "one failed probe must be retried, and the stored block must finish its commit")
	require.False(t, tryNormal, "a stored block must not fall through to normal validation")
	require.GreaterOrEqual(t, chain.probeCalls.Load(), int32(2), "the failed probe must have been retried")

	exists, err := h.subtreeStore.Exists(h.ctx, subtreeRoot[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, exists, "stored block has subtrees_set=true but its .subtree file was deleted")

	created, err := h.utxoStore.Get(h.ctx, childHash, fields.Locked)
	require.NoError(t, err)
	require.False(t, created.Locked, "the commit tail must unlock the stored block's outputs after a retried probe")
}

// TestGapBV056_AddBlockErrorWithStaleNotStoredProbeFinishesTheCommit covers a first probe that
// answers "not stored" for a block that is stored, as a false cached in the blockchain store's
// response cache can between StoreBlock's commit and its cache reset. The recheck after the delay
// must find the block stored and finish the commit, not sweep the files of a stored block.
//
// Mutation proof: returning the first probe's answer without the recheck sends the error to the
// local-fault route, which deletes the .subtree file and returns tryNormal=true.
func TestGapBV056_AddBlockErrorWithStaleNotStoredProbeFinishesTheCommit(t *testing.T) {
	var chain *gap056AmbiguousAddBlockChain

	h, _, subtreeRoot, childHash, _, tryNormal, quickErr := gap056QuickValidateWith(t, gap056Config{
		wrapChain: func(c blockchain.ClientI, _ context.CancelFunc) blockchain.ClientI {
			chain = &gap056AmbiguousAddBlockChain{ClientI: c, store: true, staleFalse: 1}
			return chain
		},
	})

	require.NoError(t, quickErr, "a stale not-stored answer must be rechecked, and the stored block must finish its commit")
	require.False(t, tryNormal, "a stored block must not fall through to normal validation")
	require.Equal(t, int32(2), chain.probeCalls.Load(), "the not-stored answer must have been rechecked once")

	exists, err := h.subtreeStore.Exists(h.ctx, subtreeRoot[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, exists, "stored block has subtrees_set=true but its .subtree file was deleted")

	created, err := h.utxoStore.Get(h.ctx, childHash, fields.Locked)
	require.NoError(t, err)
	require.False(t, created.Locked, "the commit tail must unlock the stored block's outputs")
}

// TestGapBV056_AddBlockErrorAfterCtxEndedAbortsWithoutRemovingFiles covers the ctx arm: the caller's
// context ends before AddBlock returns its error, and the block is not stored yet. The server can
// still be committing it, so commitBlock must abort with the block-committed marker and keep the
// .subtree file rather than take the local-fault route.
//
// Mutation proof: dropping the ctx.Err() check from the switch in commitBlock sends the error to the
// local-fault route, which returns tryNormal=true without the block-committed marker.
func TestGapBV056_AddBlockErrorAfterCtxEndedAbortsWithoutRemovingFiles(t *testing.T) {
	h, block, subtreeRoot, _, _, tryNormal, quickErr := gap056QuickValidateWith(t, gap056Config{
		wrapChain: func(c blockchain.ClientI, cancel context.CancelFunc) blockchain.ClientI {
			return &gap056AmbiguousAddBlockChain{ClientI: c, cancelBeforeErr: cancel}
		},
		notCommitted: true,
	})

	require.Error(t, quickErr)
	require.True(t, isBlockCommitted(quickErr), "an AddBlock error after ctx ended must carry the block-committed marker: %v", quickErr)
	require.True(t, errors.IsTransientLocalError(quickErr), "an unknown outcome must read as transient to the legacy park: %v", quickErr)
	require.False(t, tryNormal, "a block that may still be committing must not fall through to normal validation")

	stored, err := h.chain.GetBlockExists(h.ctx, block.Hash())
	require.NoError(t, err)
	require.False(t, stored, "precondition: the block is not stored")

	exists, err := h.subtreeStore.Exists(h.ctx, subtreeRoot[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, exists, "a block that may still be committing must keep its .subtree file")
}
