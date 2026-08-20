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

	err := suite.Server.blockValidation.quickValidateBlockAsync(suite.Ctx, block, "test", "", writeJobsChan)
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err),
		"a corrupt subtree verdict must be returned unwrapped on the async path, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrProcessing),
		"a corrupt verdict must NOT be shadowed by an outer ErrProcessing (bitcoin-sv/teranode#4692)")
}

// TestTryQuickValidation_CorruptPath pins the catchup quick-path corrupt branch
// (bitcoin-sv/teranode#4692): when quickValidateBlockAsync returns a corrupt-body verdict,
// tryQuickValidation must (1) strike the serving peer, (2) delete the peer-supplied .subtree blobs
// so the failed content is not re-applied on retry, and (3) return (false, corruptErr) — aborting
// for a fresh re-download rather than falling through to normal validation of the SAME corrupt body
// (which would return (true, nil)).
//
// Mutation proof: deleting the `u.removeCatchupSubtreeFiles` call in this branch leaves the
// FileTypeSubtreeToCheck blob present after the call, reddening the "blob removed" assertion.
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

	// The peer-supplied blob is present before the corrupt drop.
	present, err := suite.Server.subtreeStore.Exists(suite.Ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
	require.NoError(t, err)
	require.True(t, present, "the .subtree-to-check blob must exist before the corrupt drop")

	catchupCtx := &CatchupContext{
		blockUpTo:               block,
		baseURL:                 "http://peer",
		peerID:                  "peer-corrupt",
		startTime:               time.Now(),
		useQuickValidation:      true,
		highestCheckpointHeight: 1000, // >= block.Height so the quick path is taken
	}

	writeJobsChan := make(chan *SubtreeWriteJob, 16)

	tryNormal, err := suite.Server.tryQuickValidation(suite.Ctx, block, catchupCtx, "peer-corrupt", "http://peer", writeJobsChan)
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "the corrupt verdict must propagate, got: %v", err)
	require.False(t, tryNormal, "a corrupt quick-path verdict must NOT fall through to normal validation of the same body")

	require.Equal(t, []string{"peer-corrupt"}, rec.struck(), "the serving peer must be struck for the corrupt body")

	// removeCatchupSubtreeFiles ran: the peer-supplied blob is gone, so it cannot be re-applied.
	stillThere, err := suite.Server.subtreeStore.Exists(suite.Ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
	require.NoError(t, err)
	require.False(t, stillThere, "the corrupt .subtree-to-check blob must be removed after the corrupt drop")
}

// TestValidateBlocksOnChannel_CorruptBody_CleansUpAndPreservesClassification pins the F2 cleanup on
// the FULL-validation catchup branch (bitcoin-sv/teranode#4692): when validateBlocksOnChannel's
// ValidateBlockWithOptions returns a corrupt-body verdict (here a merkle-root mismatch), the branch
// must (1) delete the peer-supplied FileTypeSubtreeToCheck and FileTypeSubtreeData blobs so the
// failed content cannot be re-applied on retry, and (2) preserve the ORIGINAL corrupt classification
// out of the loop (never downgraded to a local storage/processing error, never poisoned invalid).
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

	validateBlocksChan := make(chan blockForValidation, 1)
	validateBlocksChan <- blockForValidation{block: block}
	close(validateBlocksChan)

	var size atomic.Int64
	size.Store(1)

	err := suite.Server.validateBlocksOnChannel(validateBlocksChan, context.Background(), catchupCtx, &size, nil)
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "the corrupt classification must survive the catchup loop, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "a corrupt body must never be poisoned invalid")

	// The F2 cleanup ran: both peer-supplied blobs are gone.
	for _, ft := range []fileformat.FileType{fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtreeData} {
		stillThere, existsErr := suite.Server.subtreeStore.Exists(suite.Ctx, subtreeHash[:], ft)
		require.NoError(t, existsErr)
		require.False(t, stillThere, "%s must be removed after a full-validation corrupt verdict", ft)
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
