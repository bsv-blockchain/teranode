package blockvalidation

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/blockvalidation_api"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// banScoreRecorder is a P2PClientI that records AddBanScore calls, so a test can assert the serving
// peer was struck for a corrupt body.
type banScoreRecorder struct {
	P2PClientI

	mu      sync.Mutex
	strikes []string
}

func (b *banScoreRecorder) AddBanScore(_ context.Context, peerID, _ string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.strikes = append(b.strikes, peerID)

	return nil
}

func (b *banScoreRecorder) struck() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, len(b.strikes))
	copy(out, b.strikes)

	return out
}

// TestValidateBlock_CorruptBody_NotWrappedInvalidOverRPC pins the gRPC ValidateBlock corrupt wrap
// (bitcoin-sv/teranode#4692): a corrupt body must NOT be surfaced as ERR_BLOCK_INVALID across the
// stateless RPC boundary. The handler returns errors.WrapGRPC(NewBlockCorruptError(...)); after
// UnwrapGRPC the recovered error must be IsBlockCorrupt true AND errors.Is(_, ErrBlockInvalid)
// false, so a caller (checkblock CLI) cannot mistake a re-downloadable corrupt body for a
// consensus-invalid one.
//
// Mutation proof: replacing the corrupt branch's NewBlockCorruptError with NewBlockInvalidError
// (i.e. deleting the `if errors.IsBlockCorrupt(err)` guard so it falls through to the invalid wrap)
// makes errors.Is(unwrapped, ErrBlockInvalid) true and IsBlockCorrupt false — reddening this test.
func TestValidateBlock_CorruptBody_NotWrappedInvalidOverRPC(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	// Corrupt the BODY: a 1-byte coinbase scriptSig fails the outer coinbase-length check (< 2
	// bytes) and, because it is caught before the merkle binding, yields an UNBOUND corrupt verdict
	// from block.Valid rather than a consensus-invalid one.
	block.CoinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00})

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	_, rpcErr := s.ValidateBlock(ctx, &blockvalidation_api.ValidateBlockRequest{
		Block:  blockBytes,
		Height: block.Height,
	})
	require.Error(t, rpcErr)

	unwrapped := errors.UnwrapGRPC(rpcErr)
	require.NotNil(t, unwrapped)
	require.True(t, errors.IsBlockCorrupt(unwrapped),
		"a corrupt body must survive the RPC boundary as corrupt, got: %v", unwrapped)
	require.False(t, errors.Is(unwrapped, errors.ErrBlockInvalid),
		"a corrupt body must NOT be surfaced as ERR_BLOCK_INVALID across the RPC boundary (bitcoin-sv/teranode#4692)")
}

// TestQuickValidateBlock_CorruptSubtreeVerdictUnwrapped pins the quickValidateBlock corrupt guard
// (bitcoin-sv/teranode#4692): a corrupt-body verdict from processBlockSubtrees (here a merkle-root
// mismatch surfaced by validateSubtrees) must be returned UNWRAPPED — IsBlockCorrupt true — and must
// NOT be shadowed by the outer ErrProcessing wrap, which would mis-route it as a transient local
// failure and retry the same corrupt body instead of re-downloading a fresh one.
//
// Mutation proof: deleting the `if errors.IsBlockCorrupt(err) { return err }` guard makes the error
// fall through to NewProcessingError, so IsBlockCorrupt goes false and errors.Is(err, ErrProcessing)
// goes true — reddening both assertions below.
func TestQuickValidateBlock_CorruptSubtreeVerdictUnwrapped(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()
	setupQuickValidateMocks(suite)

	block := buildOneSubtreeBlock(t, suite, 100)
	// Zero the header merkle root so the final validateSubtrees CheckMerkleRoot cannot match the
	// computed root — an unbound body-derived defect classified ERR_BLOCK_CORRUPT.
	block.Header.HashMerkleRoot = &chainhash.Hash{}

	err := suite.Server.blockValidation.quickValidateBlock(suite.Ctx, block, "test", "")
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err),
		"a corrupt subtree verdict must be returned unwrapped, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrProcessing),
		"a corrupt verdict must NOT be shadowed by an outer ErrProcessing (bitcoin-sv/teranode#4692)")
}

// TestQuickValidateBlockAsync_CorruptSubtreeVerdictUnwrapped is the async twin of the guard above:
// the corrupt-body verdict from processBlockSubtreesPipelineAsync must likewise be returned unwrapped
// and not shadowed by ErrProcessing (bitcoin-sv/teranode#4692).
//
// Mutation proof: same as the sync test — deleting the `if errors.IsBlockCorrupt(err) { return err }`
// guard in quickValidateBlockAsync reddens both assertions.
func TestQuickValidateBlockAsync_CorruptSubtreeVerdictUnwrapped(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()
	setupQuickValidateMocks(suite)

	block := buildOneSubtreeBlock(t, suite, 100)
	block.Header.HashMerkleRoot = &chainhash.Hash{}

	// Buffered large enough that the async path never blocks queuing write jobs (one per subtree;
	// this block has a single subtree), so no consumer goroutine is needed.
	writeJobsChan := make(chan *SubtreeWriteJob, 16)

	_, _, err := suite.Server.blockValidation.quickValidateBlockAsync(suite.Ctx, block, "test", "", writeJobsChan)
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err),
		"a corrupt subtree verdict must be returned unwrapped on the async path, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrProcessing),
		"a corrupt verdict must NOT be shadowed by an outer ErrProcessing (bitcoin-sv/teranode#4692)")
}

// TestTryQuickValidation_CorruptPath pins the catchup quick-path corrupt branch
// (bitcoin-sv/teranode#4692): when quickValidateBlockAsync returns a corrupt-body verdict,
// tryQuickValidation must (1) strike the serving peer, (2) delete only the (hash, fileType) pair
// THIS attempt itself freshly wrote — FileTypeSubtree, built and queued by buildSubtreeJobsForBatch
// before the merkle check fails — so the failed content is not re-applied on retry, (3) leave
// FileTypeSubtreeToCheck/FileTypeSubtreeData untouched here, since this test passes no fetch-phase
// freshness (nil) — the case where the fetch phase DID mark them fresh, and cleanup must delete
// them too, is covered separately by
// TestTryQuickValidation_CorruptPath_MergesFetchAndQuickFreshness — and (4) return (false,
// corruptErr) — aborting for a fresh re-download rather than falling through to normal validation
// of the SAME corrupt body (which would return (true, nil)).
//
// A real subtreeWriteWorker drains writeJobsChan so the wg.Wait() barrier before cleanup has
// something to wait for; without a consumer the queued job would never be marked done and cleanup
// would never run.
//
// Mutation proof: deleting the `u.removeCatchupSubtreeFiles` call in this branch leaves the
// freshly-written FileTypeSubtree blob present after the call, reddening the "blob removed"
// assertion; reverting to the old wide per-hash delete would also delete
// FileTypeSubtreeToCheck/FileTypeSubtreeData, reddening the "untouched" assertions.
func TestTryQuickValidation_CorruptPath(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()
	setupQuickValidateMocks(suite)

	rec := &banScoreRecorder{}
	suite.Server.blockValidation.p2pClient = rec

	block := buildOneSubtreeBlock(t, suite, 100)
	// Zero the header merkle root so quickValidateBlockAsync's final merkle check fails corrupt.
	block.Header.HashMerkleRoot = &chainhash.Hash{}
	subtreeHash := block.Subtrees[0]

	// The peer-supplied blobs (fetched by an earlier, different producer, before quick validation
	// ever runs) are present before the corrupt drop. FileTypeSubtree does not exist yet — it is
	// only written during this attempt's own build+queue phase.
	for _, ft := range []fileformat.FileType{fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtreeData} {
		present, err := suite.Server.subtreeStore.Exists(suite.Ctx, subtreeHash[:], ft)
		require.NoError(t, err)
		require.True(t, present, "%s must exist before the corrupt drop", ft)
	}

	catchupCtx := &CatchupContext{
		blockUpTo:               block,
		baseURL:                 "http://peer",
		peerID:                  "peer-corrupt",
		startTime:               time.Now(),
		useQuickValidation:      true,
		highestCheckpointHeight: 1000, // >= block.Height so the quick path is taken
	}

	writeJobsChan := make(chan *SubtreeWriteJob, 16)
	// Drain the channel with a real worker so the job this attempt queues is actually written
	// (and Done()'d), letting tryQuickValidation's context-aware wait resolve promptly rather than
	// only on suite.Ctx's 30s timeout.
	go func() { _ = suite.Server.blockValidation.subtreeWriteWorker(suite.Ctx, writeJobsChan) }()

	tryNormal, err := suite.Server.tryQuickValidation(suite.Ctx, block, catchupCtx, "peer-corrupt", "http://peer", writeJobsChan, nil)
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "the corrupt verdict must propagate, got: %v", err)
	require.False(t, tryNormal, "a corrupt quick-path verdict must NOT fall through to normal validation of the same body")

	require.Equal(t, []string{"peer-corrupt"}, rec.struck(), "the serving peer must be struck for the corrupt body")

	// removeCatchupSubtreeFiles ran and deleted exactly the freshly-written FileTypeSubtree blob.
	stillThereSubtree, err := suite.Server.subtreeStore.Exists(suite.Ctx, subtreeHash[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.False(t, stillThereSubtree, "the freshly-written FileTypeSubtree blob must be removed after the corrupt drop")

	// The pre-existing, non-freshly-written blobs must survive: provenance is per-(hash,
	// fileType), so cleanup must not sweep siblings this attempt never touched.
	for _, ft := range []fileformat.FileType{fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtreeData} {
		stillThere, existsErr := suite.Server.subtreeStore.Exists(suite.Ctx, subtreeHash[:], ft)
		require.NoError(t, existsErr)
		require.True(t, stillThere, "%s was never freshly written by this attempt and must survive cleanup", ft)
	}
}

// TestTryQuickValidation_CorruptPath_MergesFetchAndQuickFreshness drives the real full-pipeline
// shape (bitcoin-sv/teranode#4692): a blockForValidation carrying fetch-phase freshness for
// FileTypeSubtreeToCheck and FileTypeSubtreeData (exactly what fetchSubtreeDataForBlock records
// and orderedDelivery attaches to the item in production) enters quick validation via
// validateBlocksOnChannel, quick validation builds and queues FileTypeSubtree before its final
// merkle check fails corrupt, and cleanup must delete ALL THREE same-attempt fresh types —
// merging the fetch phase's freshness with quick validation's own — while leaving a second,
// unrelated hash (present in the store but never marked fresh by anything this attempt did)
// completely untouched. Without threading item.freshlyWritten into tryQuickValidation and merging
// it with quickValidateBlockAsync's own snapshot, FileTypeSubtreeToCheck/FileTypeSubtreeData would
// survive the corrupt drop and findLocalSubtreeFile would reuse them on the very next retry —
// reopening the reuse bug this whole cleanup exists to close, just for the fetched types instead
// of the built one.
//
// Mutation proof: dropping the mergeFreshlyWritten call in tryQuickValidation's corrupt branch (or
// passing nil instead of item.freshlyWritten at its call site) leaves FileTypeSubtreeToCheck and
// FileTypeSubtreeData present after cleanup, reddening the "same-attempt fresh types deleted"
// assertions; widening removeCatchupSubtreeFiles back to a per-hash delete would instead delete
// the untouched sibling hash's blobs, reddening the "unrelated hash untouched" assertions.
func TestTryQuickValidation_CorruptPath_MergesFetchAndQuickFreshness(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()
	setupQuickValidateMocks(suite)

	rec := &banScoreRecorder{}
	suite.Server.blockValidation.p2pClient = rec

	block := buildOneSubtreeBlock(t, suite, 100)
	// Zero the header merkle root so quickValidateBlockAsync's final merkle check fails corrupt,
	// AFTER this block's own FileTypeSubtree has already been built and queued.
	block.Header.HashMerkleRoot = &chainhash.Hash{}
	subtreeHash := block.Subtrees[0]

	// A second, unrelated hash this attempt never touched at all — e.g. an already-persisted,
	// permanently-promoted subtree a doctored body could name. Present in the store, absent from
	// every freshness set, so it must survive regardless of what happens to subtreeHash.
	untouchedHash := chainhash.HashH([]byte("untouched-sibling"))
	for _, ft := range []fileformat.FileType{fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtreeData, fileformat.FileTypeSubtree} {
		require.NoError(t, suite.Server.subtreeStore.Set(suite.Ctx, untouchedHash[:], ft, []byte{0x01}))
	}

	// The fetch phase's own freshness for THIS block's hash — what fetchSubtreeDataForBlock would
	// have recorded and orderedDelivery would have attached to blockForValidation.freshlyWritten
	// in the real pipeline.
	fetchFreshlyWritten := map[chainhash.Hash]map[fileformat.FileType]struct{}{
		*subtreeHash: {
			fileformat.FileTypeSubtreeToCheck: {},
			fileformat.FileTypeSubtreeData:    {},
		},
	}

	catchupCtx := &CatchupContext{
		blockUpTo:               block,
		baseURL:                 "http://peer",
		peerID:                  "peer-corrupt",
		startTime:               time.Now(),
		useQuickValidation:      true,
		highestCheckpointHeight: 1000,
	}

	writeJobsChan := make(chan *SubtreeWriteJob, 16)
	go func() { _ = suite.Server.blockValidation.subtreeWriteWorker(suite.Ctx, writeJobsChan) }()

	validateBlocksChan := make(chan blockForValidation, 1)
	validateBlocksChan <- blockForValidation{block: block, freshlyWritten: fetchFreshlyWritten}
	close(validateBlocksChan)

	var size atomic.Int64
	size.Store(1)

	err := suite.Server.validateBlocksOnChannel(validateBlocksChan, suite.Ctx, catchupCtx, &size, writeJobsChan)
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "the corrupt verdict must propagate, got: %v", err)
	require.Equal(t, []string{"peer-corrupt"}, rec.struck(), "the serving peer must be struck for the corrupt body")

	// All three same-attempt fresh types for THIS hash are gone: quick validation's own
	// FileTypeSubtree, and the fetch phase's FileTypeSubtreeToCheck/FileTypeSubtreeData.
	for _, ft := range []fileformat.FileType{fileformat.FileTypeSubtree, fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtreeData} {
		exists, existsErr := suite.Server.subtreeStore.Exists(suite.Ctx, subtreeHash[:], ft)
		require.NoError(t, existsErr)
		require.False(t, exists, "%s was freshly written by this attempt (fetch phase or quick validation) and must be removed", ft)
	}

	// The unrelated sibling hash was never marked fresh by anything this attempt did, and must
	// survive untouched.
	for _, ft := range []fileformat.FileType{fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtreeData, fileformat.FileTypeSubtree} {
		exists, existsErr := suite.Server.subtreeStore.Exists(suite.Ctx, untouchedHash[:], ft)
		require.NoError(t, existsErr)
		require.True(t, exists, "%s belongs to an unrelated hash this attempt never wrote and must survive", ft)
	}
}

// TestValidateBlocksOnChannel_CorruptBody_CleansUpAndPreservesClassification pins the cleanup on
// the FULL-validation catchup branch (bitcoin-sv/teranode#4692): when validateBlocksOnChannel's
// ValidateBlockWithOptions returns a corrupt-body verdict (here a merkle-root mismatch), the branch
// must (1) delete exactly the (hash, fileType) pairs this attempt's fetch phase marked freshly
// written — simulated here via item.freshlyWritten, since this test drives validateBlocksOnChannel
// directly rather than through the full fetchBlocksConcurrently pipeline that normally populates it
// — so the failed content cannot be re-applied on retry, and (2) preserve the ORIGINAL corrupt
// classification out of the loop (never downgraded to a local storage/processing error, never
// poisoned invalid).
//
// Mutation proof: deleting the new u.removeCatchupSubtreeFiles call in the corrupt branch leaves both
// blobs present after the call, reddening the removal assertions.
func TestValidateBlocksOnChannel_CorruptBody_CleansUpAndPreservesClassification(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	suite.MockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil).Maybe()
	suite.MockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil).Maybe()
	suite.MockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 99, MinedSet: true}, nil).Maybe()
	suite.MockBlockchain.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil).Maybe()
	easyNBits, _ := model.NewNBitFromString("207fffff")
	suite.MockBlockchain.On("GetNextWorkRequired", mock.Anything, mock.Anything, mock.Anything).Return(easyNBits, nil).Maybe()

	// Drive the corrupt verdict from subtree validation — a body-derived defect surfaced on the full
	// path (e.g. a CVE-2012-2459 duplicate in the received subtree). This keeps the header PoW/merkle
	// valid so the flow reaches validateBlockSubtrees, where CheckBlockSubtrees returns corrupt.
	subtreeVal := &subtreevalidation.MockSubtreeValidation{}
	subtreeVal.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.NewBlockCorruptError("[CheckBlockSubtrees] duplicate transaction in received subtree (CVE-2012-2459)"))
	suite.Server.blockValidation.subtreeValidationClient = subtreeVal

	block := buildOneSubtreeBlock(t, suite, 100)
	// buildOneSubtreeBlock sets the header merkle root AFTER mining, leaving PoW stale. The full
	// validation path checks PoW before subtree validation, so re-mine the nonce to the (easy 207fffff)
	// target while keeping the correct merkle root.
	for {
		if ok, _, _ := block.Header.HasMetTargetDifficulty(); ok {
			break
		}
		block.Header.Nonce++
	}
	subtreeHash := block.Subtrees[0]

	// The peer-supplied blobs exist before the corrupt verdict.
	for _, ft := range []fileformat.FileType{fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtreeData} {
		present, err := suite.Server.subtreeStore.Exists(suite.Ctx, subtreeHash[:], ft)
		require.NoError(t, err)
		require.True(t, present, "%s must exist before the corrupt verdict", ft)
	}

	catchupCtx := &CatchupContext{
		blockUpTo:          block,
		baseURL:            "http://peer",
		peerID:             "peer-corrupt",
		startTime:          time.Now(),
		useQuickValidation: false, // force the normal (full) validation path, not the quick path
	}

	// Simulate the fetch phase's output: this is what fetchSubtreeDataForBlock would have
	// returned had this test driven the full pipeline, rather than validateBlocksOnChannel
	// directly (bitcoin-sv/teranode#4692).
	freshlyWritten := map[chainhash.Hash]map[fileformat.FileType]struct{}{
		*subtreeHash: {
			fileformat.FileTypeSubtreeToCheck: {},
			fileformat.FileTypeSubtreeData:    {},
		},
	}

	validateBlocksChan := make(chan blockForValidation, 1)
	validateBlocksChan <- blockForValidation{block: block, freshlyWritten: freshlyWritten}
	close(validateBlocksChan)

	var size atomic.Int64
	size.Store(1)

	err := suite.Server.validateBlocksOnChannel(validateBlocksChan, context.Background(), catchupCtx, &size, nil)
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "the corrupt classification must survive the catchup loop, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "a corrupt body must never be poisoned invalid")

	// The cleanup ran: both peer-supplied, freshly-written blobs are gone.
	for _, ft := range []fileformat.FileType{fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtreeData} {
		stillThere, existsErr := suite.Server.subtreeStore.Exists(suite.Ctx, subtreeHash[:], ft)
		require.NoError(t, existsErr)
		require.False(t, stillThere, "%s must be removed after a full-validation corrupt verdict", ft)
	}
}

// TestValidateBlocksOnChannel_CommittedSubtreeSurvivesLaterCorruptCleanup pins the run-scoped
// committed-dependency guard end to end (bitcoin-sv/teranode#4692). Two blocks arrive in one catchup
// run naming the SAME subtree hash — the shape a doctored body gets for free, since it only has to
// name another in-run block's subtree hashes against a real header from the primary's own chain.
// The first block validates and commits; the second is corrupt, and its attempt-scoped freshness
// names both the shared hash and a hash of its own. Cleanup must skip the shared hash, because the
// chain now depends on it and nothing in this package would ever regenerate those blobs for a block
// that is already committed, while still deleting the corrupt attempt's own.
//
// Mutation proof: removing the catchupCtx.markSubtreesCommitted call at validateBlocksOnChannel's
// success tail (or the subtreeCommitted skip inside removeCatchupSubtreeFiles) deletes the shared
// hash's blobs and reddens the survival assertions.
func TestValidateBlocksOnChannel_CommittedSubtreeSurvivesLaterCorruptCleanup(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()
	setupQuickValidateMocks(suite)

	suite.MockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil).Maybe()
	suite.MockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil).Maybe()
	suite.MockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 100, MinedSet: true}, nil).Maybe()
	suite.MockBlockchain.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil).Maybe()
	easyNBits, _ := model.NewNBitFromString("207fffff")
	suite.MockBlockchain.On("GetNextWorkRequired", mock.Anything, mock.Anything, mock.Anything).Return(easyNBits, nil).Maybe()

	// The second block's corrupt verdict comes from subtree validation on the FULL path. The first
	// block is below the run's highest verified checkpoint, so it never reaches this client.
	subtreeVal := &subtreevalidation.MockSubtreeValidation{}
	subtreeVal.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.NewBlockCorruptError("[CheckBlockSubtrees] duplicate transaction in received subtree (CVE-2012-2459)"))
	suite.Server.blockValidation.subtreeValidationClient = subtreeVal

	committedBlock := buildOneSubtreeBlock(t, suite, 100)
	sharedHash := *committedBlock.Subtrees[0]

	// The second block NAMES the same subtree hash — which is the whole point here, and is what a
	// doctored body gets for free. Built as a distinct block over the first block's header (moved
	// off it by the nonce, then re-mined to the easy target the full path checks before subtree
	// validation) rather than through the builder again, since the blobs for that hash are already
	// in the store.
	//
	// It names TWO subtrees: the shared one, and one of its own. The second is the control that
	// makes this test pin "registered only AFTER success" on its own — registering at the wrong
	// point (above the error handling, or inside tryQuickValidation) would protect the corrupt
	// block's own subtrees too and silently disable the cleanup entirely.
	ownHash := chainhash.HashH([]byte("corrupt-attempt-own-subtree"))
	corruptHeader := *committedBlock.Header
	corruptBlock := &model.Block{
		Header:           &corruptHeader,
		CoinbaseTx:       committedBlock.CoinbaseTx,
		Subtrees:         []*chainhash.Hash{&sharedHash, &ownHash},
		TransactionCount: committedBlock.TransactionCount,
		Height:           101,
	}

	corruptBlock.Header.Nonce += 1_000_000
	for {
		if ok, _, _ := corruptBlock.Header.HasMetTargetDifficulty(); ok {
			break
		}

		corruptBlock.Header.Nonce++
	}

	require.NotEqual(t, committedBlock.Hash().String(), corruptBlock.Hash().String())

	// The corrupt attempt's own subtree blobs: freshly written by it, depended on by nothing that
	// committed, so they must still be deleted — the guard narrows deletion, it does not disable it.
	peerSuppliedTypes := []fileformat.FileType{fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtreeData}
	for _, ft := range peerSuppliedTypes {
		require.NoError(t, suite.Server.subtreeStore.Set(suite.Ctx, ownHash[:], ft, []byte{0x01}))
	}

	catchupCtx := &CatchupContext{
		blockUpTo:               corruptBlock,
		baseURL:                 "http://peer",
		peerID:                  "peer-corrupt",
		startTime:               time.Now(),
		useQuickValidation:      true,
		highestCheckpointHeight: 100, // block 100 takes the quick path, block 101 the full one
	}

	writeJobsChan := make(chan *SubtreeWriteJob, 16)
	go func() { _ = suite.Server.blockValidation.subtreeWriteWorker(suite.Ctx, writeJobsChan) }()

	// The corrupt attempt's own fetch-phase freshness names both hashes, exactly as it would if the
	// doctored body listed a subtree an earlier in-run block also used.
	corruptFreshlyWritten := map[chainhash.Hash]map[fileformat.FileType]struct{}{
		sharedHash: {fileformat.FileTypeSubtreeToCheck: {}, fileformat.FileTypeSubtreeData: {}},
		ownHash:    {fileformat.FileTypeSubtreeToCheck: {}, fileformat.FileTypeSubtreeData: {}},
	}

	validateBlocksChan := make(chan blockForValidation, 2)
	validateBlocksChan <- blockForValidation{block: committedBlock}
	validateBlocksChan <- blockForValidation{block: corruptBlock, freshlyWritten: corruptFreshlyWritten}
	close(validateBlocksChan)

	var size atomic.Int64
	size.Store(2)

	err := suite.Server.validateBlocksOnChannel(validateBlocksChan, suite.Ctx, catchupCtx, &size, writeJobsChan)
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "the corrupt verdict must propagate, got: %v", err)
	require.True(t, catchupCtx.subtreeCommitted(sharedHash), "the first block committed, so its subtree must be registered")
	require.False(t, catchupCtx.subtreeCommitted(ownHash),
		"the corrupt block never committed, so the subtrees it names must NOT be registered — registration belongs after the error handling, not before it")

	for _, ft := range peerSuppliedTypes {
		exists, existsErr := suite.Server.subtreeStore.Exists(suite.Ctx, sharedHash[:], ft)
		require.NoError(t, existsErr)
		require.True(t, exists, "%s must survive: a block already committed in this run depends on this subtree", ft)

		exists, existsErr = suite.Server.subtreeStore.Exists(suite.Ctx, ownHash[:], ft)
		require.NoError(t, existsErr)
		require.False(t, exists, "%s belongs only to the corrupt attempt and must still be deleted", ft)
	}
}

// warnCaptureLogger records Warnf messages so a test can assert what was logged, delegating every
// other method to an embedded real test logger.
type warnCaptureLogger struct {
	ulogger.Logger

	mu       sync.Mutex
	warnings []string
}

func (l *warnCaptureLogger) Warnf(format string, args ...interface{}) {
	l.mu.Lock()
	l.warnings = append(l.warnings, fmt.Sprintf(format, args...))
	l.mu.Unlock()
	l.Logger.Warnf(format, args...)
}

func (l *warnCaptureLogger) warned() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.warnings))
	copy(out, l.warnings)

	return out
}

// TestNewBlockValidation_OptimisticMiningPeerDisabled_WarnsAtStartup pins the F5 startup warning
// (bitcoin-sv/teranode#4692): when optimistic mining is globally enabled but disabled on peer-served
// and catch-up blocks (OptimisticMining && !OptimisticMiningPeerBlocks), NewBlockValidation must emit
// a single warning naming the restore knob. The warning is logged synchronously at construction, so a
// cancelled context tears down the background workers immediately.
//
// Mutation proof: deleting the Warnf guard in NewBlockValidation drops the message, reddening the
// positive case; the negative controls confirm it is not emitted otherwise.
func TestNewBlockValidation_OptimisticMiningPeerDisabled_WarnsAtStartup(t *testing.T) {
	const wantPhrase = "optimistic mining is enabled but disabled on peer-served"

	newWith := func(t *testing.T, optimistic, peerBlocks bool) *warnCaptureLogger {
		t.Helper()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = optimistic
		tSettings.BlockValidation.OptimisticMiningPeerBlocks = peerBlocks

		logger := &warnCaptureLogger{Logger: ulogger.TestLogger{}}
		// nil blockchainClient keeps the subscribe goroutine from starting; the warning is logged
		// synchronously before any goroutine anyway.
		bv := NewBlockValidation(ctx, logger, tSettings, nil, nil, nil, nil, nil, nil)
		require.NotNil(t, bv)

		return logger
	}

	warnedFor := func(logger *warnCaptureLogger) bool {
		for _, w := range logger.warned() {
			if strings.Contains(w, wantPhrase) {
				return true
			}
		}

		return false
	}

	t.Run("optimistic on, peer-blocks off: warns", func(t *testing.T) {
		require.True(t, warnedFor(newWith(t, true, false)),
			"must warn that optimistic mining is disabled on peer-served/catch-up blocks")
	})

	t.Run("optimistic on, peer-blocks on: no warning", func(t *testing.T) {
		require.False(t, warnedFor(newWith(t, true, true)), "peer-blocks enabled: no warning")
	})

	t.Run("optimistic off: no warning", func(t *testing.T) {
		require.False(t, warnedFor(newWith(t, false, false)), "optimistic mining off: no warning")
	})
}
