package netsync

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/stretchr/testify/require"
)

// parkReadFaultStore wraps a real blob store and, when armed, fails every
// GetIoReader with an error of the test's choosing without touching the blob.
// That is exactly the shape of the two failures that must never destroy a
// parked block: the store's own permit wait running out, and the read being
// cancelled on shutdown. Both are raised by file.acquireReadPermit
// (stores/blob/file/file.go), which returns a ServiceUnavailable error when the
// deadline passes and a ContextCanceled error when the caller's context is
// cancelled — neither of which says anything at all about the blob.
type parkReadFaultStore struct {
	blob.Store

	mu  sync.Mutex
	err error

	// existsKey and existsErr arm a fault on Exists for ONE key only. The
	// commit path's completeness check (hasCompleteRecord) stats a record's
	// subtree files, and a test about a stat that cannot run must fault that
	// stat without faulting the existence pre-check every sink write makes
	// for every other block's files (filestorer.NewFileStorer).
	existsKey []byte
	existsErr error
}

func (s *parkReadFaultStore) failReadsWith(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.err = err
}

// failExistsFor makes every Exists for key fail with err, whatever the file
// type. A nil key disarms it.
func (s *parkReadFaultStore) failExistsFor(key []byte, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.existsKey = key
	s.existsErr = err
}

func (s *parkReadFaultStore) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
	s.mu.Lock()
	faultKey, err := s.existsKey, s.existsErr
	s.mu.Unlock()

	if faultKey != nil && err != nil && bytes.Equal(faultKey, key) {
		return false, err
	}

	return s.Store.Exists(ctx, key, fileType, opts...)
}

func (s *parkReadFaultStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	s.mu.Lock()
	err := s.err
	s.mu.Unlock()

	if err != nil {
		return nil, err
	}

	return s.Store.GetIoReader(ctx, key, fileType, opts...)
}

// Get is the converted-record read path (blockPark.ReadConverted), unlike the
// raw whole-block path (blockPark.Read), which streams through GetIoReader
// above. Every block parks as a converted record now that the pipeline sink is
// the only sink, so a test arming only GetIoReader would silently stop
// faulting anything: this must fail the same way for the same reason.
func (s *parkReadFaultStore) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	s.mu.Lock()
	err := s.err
	s.mu.Unlock()

	if err != nil {
		return nil, err
	}

	return s.Store.Get(ctx, key, fileType, opts...)
}

// requireStillParked asserts the end state a block that was NOT judged must
// reach: still in the index, still on disk, still charged, not re-requested and
// the peer unblamed.
func (h *parkWiringHarness) requireStillParked(t *testing.T, hash chainhash.Hash, bytesBefore int64) {
	t.Helper()

	require.Equal(t, 1, h.sm.blockPark.Len(), "the block must still be parked")
	require.Equal(t, bytesBefore, h.sm.blockPark.Bytes(), "putting a block back must not lose or double its budget")
	require.Contains(t, parkDirEntries(t, h.parkDir), hash.String()+".block",
		"the downloaded block must still be on disk")

	before := h.rec.getDataCount()

	h.sm.fetchHeaderBlocks()

	require.False(t, WaitUntil(func() bool { return h.rec.askedForSince(before, hash) }, time.Second),
		"a block that was not judged must not be asked for again while it is still on disk — holdsBlock excludes it")

	require.False(t, h.rec.wasRejected(hash), "a local fault is not the peer's fault")

	_, failed := h.sm.recentlyFailedBlocks.Get(hash)
	require.False(t, failed, "a block nobody could judge must not be written off as a failure")
}

// TestSyncManager_AParkedBlockSurvivesAReadThatSaysNothingAboutTheBlock is the
// data-loss case. Reading a parked block back can fail for two reasons that are
// not about the block at all: the blob store had no read permit free inside the
// park's deadline, and the read was cancelled because the node is shutting down.
// Treating either as "the blob is corrupt" throws away a block that is already
// fully downloaded, validated and on disk — and both fire in bursts, so it is
// many blocks at once, each of which is then downloaded a second time.
func TestSyncManager_AParkedBlockSurvivesAReadThatSaysNothingAboutTheBlock(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{
			// file.acquireReadPermit's deadline branch, verbatim.
			name: "no read permit came free in time",
			err:  errors.NewServiceUnavailableError("[File] read operation timed out waiting for semaphore permit"),
		},
		{
			// file.acquireReadPermit's cancellation branch, verbatim.
			name: "the read was cancelled by shutdown",
			err:  errors.NewContextCanceledError("[File] read operation canceled while waiting for semaphore permit", context.Canceled),
		},
		{
			// file.acquireReadPermit's third branch, and the whole point of the
			// classifier defaulting to "keep": an error nobody anticipated is
			// not evidence that the blob is bad, and reading it as such is how a
			// good block gets destroyed by a condition nobody thought about.
			name: "the read failed for a reason nobody classified",
			err:  errors.NewProcessingError("[File] failed to acquire read permit"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newParkWiringHarness(t, true)

			child := h.blocks[1].MsgBlock().BlockHash()
			parent := h.blocks[0].MsgBlock().BlockHash()

			require.NoError(t, h.deliver(t, 1))
			require.Equal(t, 1, h.sm.blockPark.Len())

			parkedBytes := h.sm.blockPark.Bytes()

			h.store.failReadsWith(tc.err)

			// The parent is in the chain and the drain its commit would have
			// scheduled runs, so the drain reaches for the child and the read
			// fails for a reason that is nothing to do with the child. The
			// parent is stored directly rather than delivered, because the
			// faulted store would fail the parent's own record read first and
			// leave two blocks parked instead of one.
			h.chainHolds(t, parent)
			h.sm.drainParkedDescendants(parent)

			h.requireStillParked(t, child, parkedBytes)
			require.Zero(t, h.validation.callsFor(child), "a block whose record would not read back never reached block validation")
		})
	}
}

// TestSyncManager_AParkedBlockSurvivesATransientCommitFailure. A commit can fail
// because this node's own storage is briefly unwell — the UTXO store answering
// ErrServiceUnavailable when a batch does not complete in time is the common
// one. That is not a judgement on the block, and the block is already on disk,
// so throwing it away buys a re-download of something we already have. It stays
// parked and the sweep tries again.
func TestSyncManager_AParkedBlockSurvivesATransientCommitFailure(t *testing.T) {
	h := newParkWiringHarness(t, true)

	child := h.blocks[1].MsgBlock().BlockHash()
	parent := h.blocks[0].MsgBlock().BlockHash()

	// Block validation's verdict on the child, once: the store was not
	// answering. sm.ProcessBlock wraps it in a ProcessingError and
	// IsTransientLocalError walks the chain, so the row is still RetryLater.
	h.validation.failOnce(child, errors.NewStorageError("the store is not answering"))

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	parkedBytes := h.sm.blockPark.Bytes()

	// The parent arrives, commits for real, and the drain behind it offers the
	// child to block validation.
	require.NoError(t, h.deliver(t, 0))

	h.requireCommitted(t, parent)
	require.Equal(t, 1, h.validation.callsFor(child), "the child reached block validation, which is where the store failed")

	h.requireStillParked(t, child, parkedBytes)
}

// TestParkCommitFailure_AParentNotYetMarkedMinedKeepsTheBlock pins the row for a
// parent that is in the chain and valid but whose setTxMined has not finished.
// waitForPreviousBlockMined gives up with ErrBlockParentNotMined after about
// 80 s with the default retry settings, and that says nothing about the child:
// the same block commits on the next attempt once the parent is marked. Reading
// it as a rejection threw away a downloaded block, wrote it off in
// recentlyFailedBlocks and downloaded it again, 20 times on mainnet since
// 2026-09-28.
func TestParkCommitFailure_AParentNotYetMarkedMinedKeepsTheBlock(t *testing.T) {
	parent := chainhash.HashH([]byte("parent"))

	for _, tc := range []struct {
		name string
		err  error
	}{
		{
			name: "plain, as waitForPreviousBlockMined returns it",
			err:  errors.NewBlockParentNotMinedError("[waitForPreviousBlockMined][height:%d] parent %s not mined yet", 2, parent.String()),
		},
		{
			name: "wrapped by a caller further up",
			err:  errors.NewProcessingError("failed to process block", errors.NewBlockParentNotMinedError("parent %s not mined yet", parent.String())),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := parkCommitFailure(tc.err)

			require.Equal(t, parkDispositionParentNotMinedYet, d)
			require.Equal(t, parkBlobKeep, d.blob, "the downloaded block must stay parked")
			require.False(t, d.markFailed, "a block whose parent is merely slow must not be written off")
			require.False(t, d.blamePeer, "the peer sent a good block; the delay is ours")
			require.NotEqual(t, parkDispositionRetryLater.reason, d.reason, "an operator must be able to tell this apart from a busy store")
			require.NotEqual(t, parkDispositionParentGone.reason, d.reason, "an operator must be able to tell this apart from a reorg")
		})
	}
}

// TestSyncManager_AParkedBlockSurvivesAParentThatIsSlowToBeMarkedMined is the
// mainnet incident end to end through the park. The parent is in the chain but
// GetBlockIsMined keeps answering false for longer than the retry budget, so the
// first commit attempt gives up. The block must stay parked (not deleted, not
// written off, no peer blamed, not asked for again) and the sweep's next pass
// must commit it from disk once the parent is marked, without a fetch.
//
// The FSM is RUNNING so the blame suppression that applies while catching blocks
// cannot hide a reject.
func TestSyncManager_AParkedBlockSurvivesAParentThatIsSlowToBeMarkedMined(t *testing.T) {
	h := newParkWiringHarnessInState(t, true, blockchain2.FSMStateRUNNING)

	child := h.blocks[1].MsgBlock().BlockHash()
	parent := h.blocks[0].MsgBlock().BlockHash()

	// The wait must run: it is skipped only on the below-checkpoint outpoint-only
	// path. The harness's one retry at 1 ms makes the wait give up after two
	// lookups.
	h.sm.settings.BlockValidation.OutpointOnlyBelowCheckpoint = false

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	parkedBytes := h.sm.blockPark.Bytes()

	// The parent is in the chain and valid, but its mined flag is not set yet.
	h.chainHoldsUnmined(t, parent)

	h.sm.sweepParkedBlocks(time.Now().Add(parkStuckThreshold + time.Second))

	require.Zero(t, h.validation.callCount(), "sanity: the first attempt must have given up on the parent's mined flag")
	require.Equal(t, int32(2), h.chain.minedPolls.Load(), "one lookup and one retry before the wait gave up")
	h.requireStillParked(t, child, parkedBytes)

	getDataBefore := h.rec.getDataCount()

	// setTxMined finishes for the parent, and the next sweep pass resubmits the
	// kept block.
	require.NoError(t, h.chain.SetBlockMinedSet(h.sm.ctx, &parent))

	h.sm.sweepParkedBlocks(time.Now().Add(2*parkStuckThreshold + time.Second))

	h.requireCommitted(t, child)
	require.Zero(t, h.sm.blockPark.Len(), "the committed block must leave the park")
	require.Zero(t, h.sm.blockPark.Bytes(), "committing must give the park budget back")
	require.False(t, h.rec.askedForSince(getDataBefore, child), "the block was on disk, so it must not be fetched again")
	require.False(t, h.rec.wasRejected(child), "the peer must never have been blamed")
}

// parkCommitChain is the shape a block-validation verdict has by the time
// parkCommitFailure sees it: quickValidateBlock's ProcessingError around the
// subtree pipeline's, the gRPC hop (WrapGRPC in the server, UnwrapGRPC in the
// client), and netsync ProcessBlock's own ProcessingError on top. inner is what
// the pipeline raised. The gRPC round trip is included rather than stylised
// because it rebuilds the chain from details, and a code dropped there would
// make a classifier arm dead in production while a bare-error test stayed
// green.
func parkCommitChain(inner error) error {
	server := errors.NewProcessingError("[quickValidateBlock][hash] failed to process block subtrees",
		errors.NewProcessingError("[processBlockSubtrees][hash] subtree 0 failed", inner))

	return errors.NewProcessingError("failed to process block", errors.UnwrapGRPC(errors.WrapGRPC(server)))
}

// TestParkCommitFailure_ACorruptVerdictIsNotHeldAgainstTheBlockOrThePeer pins
// the RecordCorrupt row. On the converted route the body was verified against
// the header at the sink, so a corrupt verdict later is about this node's own
// files: the record is dropped and downloaded again, the hash is not written
// off and the peer is not told anything. Ported from the deleted
// TestHandleBlockMsg_CorruptBody_NotMarkedFailed, whose property this is.
func TestParkCommitFailure_ACorruptVerdictIsNotHeldAgainstTheBlockOrThePeer(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{
			name: "bare, as processBlockFound returns a corrupt verdict on the unified route",
			err:  errors.NewBlockCorruptError("[quickValidateBlock][hash] merkle root does not match"),
		},
		{
			name: "through the production chain",
			err:  parkCommitChain(errors.NewBlockCorruptError("[bindSubtreeBodyToHeader][hash] subtree does not hash to its key")),
		},
		{
			name: "corrupt wrapped around a key-mismatch processing error, as the legacy unified branch returns it",
			err:  parkCommitChain(errors.NewBlockCorruptError("[processBlockFound][hash] a subtree file this node wrote does not hash to its key", errors.NewProcessingError("subtree key mismatch"))),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := parkCommitFailure(tc.err)

			require.Equal(t, parkDispositionRecordCorrupt, d)
			require.Equal(t, parkBlobDrop, d.blob, "the record is dropped so the block is downloaded again")
			require.False(t, d.markFailed, "a corrupt verdict on this node's own record must not write the block off")
			require.False(t, d.blamePeer, "the peer's bytes were verified at the sink; the fault is local")
			require.NotEqual(t, parkDispositionBlobUnusable.reason, d.reason, "an operator must be able to tell a corrupt verdict from an unreadable blob")
		})
	}
}

// TestParkCommitFailure_AMissingSubtreeFileDropsTheRecordWithoutJudgingIt pins
// the FilesGone row and the two arms that must win over it: a Service or
// Storage wrap keeps the blob, and a judgement that happens to wrap a not-found
// stays a judgement.
func TestParkCommitFailure_AMissingSubtreeFileDropsTheRecordWithoutJudgingIt(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want parkDisposition
	}{
		{
			name: "the blob store's bare not-found under readSubtree's wrap",
			err:  parkCommitChain(errors.NewNotFoundError("[readSubtree/hash] failed to get subtree data", errors.ErrNotFound)),
			want: parkDispositionFilesGone,
		},
		{
			name: "a blob-not-found inside",
			err:  parkCommitChain(errors.NewNotFoundError("[readSubtreeStructure/hash] failed to get subtree", errors.NewBlobNotFoundError("no such blob"))),
			want: parkDispositionFilesGone,
		},
		{
			name: "a not-found around a storage error, readSubtreeStructure's shape for an open failure that is not ENOENT",
			err:  parkCommitChain(errors.NewNotFoundError("[readSubtreeStructure/hash] failed to get subtree", errors.NewStorageError("open: permission denied"))),
			want: parkDispositionRetryLater,
		},
		{
			name: "a service error around a not-found, the full route's wrap",
			err:  errors.NewProcessingError("failed to process block", errors.NewServiceError("failed block validation BlockFound", errors.NewNotFoundError("subtree not found"))),
			want: parkDispositionRetryLater,
		},
		{
			name: "a judgement around a not-found stays a judgement",
			err:  parkCommitChain(errors.NewBlockInvalidError("[ValidateBlock][hash] block is invalid", errors.NewNotFoundError("subtree not found"))),
			want: parkDispositionBlockInvalid,
		},
		{
			// The shape upstream's batch path (SpendAndCreateMulti, PR 1875) returns for a parent
			// read that failed on the store, wrapped as block validation and netsync's ProcessBlock
			// wrap it: a local fault, so the block is kept and nobody is blamed.
			name: "a store fault reading a parent output under two processing wraps",
			err: parkCommitChain(errors.NewProcessingError("failed to process block",
				errors.NewProcessingError("failed to read parent output", errors.NewStorageError("aerospike timeout")))),
			want: parkDispositionRetryLater,
		},
		{
			name: "a missing parent block carries code 3 inside code 10 and is the parent's row, not this one",
			err:  errors.NewProcessingError("failed to get block header for previous block", errors.NewBlockNotFoundError("block not found", errors.ErrNotFound)),
			want: parkDispositionParentGone,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := parkCommitFailure(tc.err)

			require.Equal(t, tc.want, d)

			if tc.want == parkDispositionFilesGone {
				require.Equal(t, parkBlobDrop, d.blob, "a record whose files are gone is dropped and downloaded again")
				require.False(t, d.markFailed, "a file going missing on this node must not write the block off")
				require.False(t, d.blamePeer, "the files going missing on this node is not the peer's fault")
			}
		})
	}
}

// TestParkCommitFailure_AMissingParentOutputKeepsTheBlockAndBlamesNobody pins
// the LocalUtxoFault row: a code-30 miss from the UTXO store, in the shape the
// SQL store's batched spend raises it (a UtxoError whose Join chains the
// per-spend TxNotFound), keeps the blob with its own operator-facing reason.
// A code-3 not-found must not land here: 3 is a file, 30 is an output.
func TestParkCommitFailure_AMissingParentOutputKeepsTheBlockAndBlamesNobody(t *testing.T) {
	err := parkCommitChain(errors.NewUtxoError("error in sql spend (batched mode) - errors",
		errors.NewTxNotFoundError("output %d of %s not found", 0, "abcd")))

	d := parkCommitFailure(err)

	require.Equal(t, parkDispositionLocalUtxoFault, d)
	require.Equal(t, parkBlobKeep, d.blob, "a re-download cannot repair a UTXO set, so the block stays parked")
	require.False(t, d.markFailed, "the block is canonical below the checkpoint; the UTXO set is what is wrong")
	require.False(t, d.blamePeer, "the peer sent a good block")
	require.NotEqual(t, parkDispositionRetryLater.reason, d.reason, "an operator must be able to tell this from a busy store")
	require.NotEqual(t, parkDispositionParentGone.reason, d.reason, "and from a reorg")
	require.NotEqual(t, parkDispositionParentNotMinedYet.reason, d.reason, "and from a late mined flag")

	fileMiss := parkCommitChain(errors.NewNotFoundError("[readSubtree/hash] failed to get subtree data", errors.ErrNotFound))
	require.NotEqual(t, parkDispositionLocalUtxoFault, parkCommitFailure(fileMiss), "a missing file is not a missing output")
}

// TestParkCommitFailure_TheFullRoutesInvalidTransactionVerdictIsAJudgement pins
// the shape block validation's full route returns for a legacy block whose fault
// is a consensus-invalid transaction: ErrBlockInvalid around subtree validation's
// processing wrap around ErrTxInvalid, with no corrupt code anywhere in the chain.
// It lands on BlockRejected. The corrupt-coded shape the p2p route returns for the
// same fault is pinned alongside so the two are visibly different rows: on the
// legacy route that shape would be a loop (drop, re-ask, drop), which is why the
// producer never raises it there.
func TestParkCommitFailure_TheFullRoutesInvalidTransactionVerdictIsAJudgement(t *testing.T) {
	legacy := parkCommitChain(errors.NewBlockInvalidError("[ValidateBlock][hash] block contains invalid transactions",
		errors.NewProcessingError("[CheckBlockSubtrees] failed to process transactions",
			errors.NewTxInvalidError("transaction in subtree is invalid"))))
	require.False(t, errors.IsBlockCorrupt(legacy), "fixture: the legacy route's verdict carries no corrupt code")
	require.True(t, errors.Is(legacy, errors.ErrTxInvalid), "fixture: the cause is still in the chain")

	d := parkCommitFailure(legacy)
	require.Equal(t, parkDispositionBlockInvalid, d)
	require.True(t, d.markFailed, "a judgement is remembered so the block is not asked for again")
	require.True(t, d.blamePeer, "the sink bound the body to the header, so the block the peer sent is the one judged")
	require.True(t, d.dropPeer, "and a verdict that says invalid drops the association that delivered it")

	p2p := parkCommitChain(errors.NewBlockCorruptError("[ValidateBlock][hash] block contains invalid transactions",
		errors.NewProcessingError("[CheckBlockSubtrees] failed to process transactions",
			errors.NewTxInvalidError("transaction in subtree is invalid"))))
	require.Equal(t, parkDispositionRecordCorrupt, parkCommitFailure(p2p),
		"the p2p route's corrupt-coded shape is the RecordCorrupt row; the producer must not raise it on the legacy route")
}

// TestSyncManager_ARejectedParkedBlockDropsItsPeerInEveryState pins the drain's
// half of peer punishment. Block validation judges a parked block invalid, in
// so many words, at the drain's real commit attempt over the real chain. End
// state in both FSM states: the park is empty, the hash is written off, and the
// peer that delivered the block is disconnected. The reject is sent only when
// RUNNING (the existing suppression while catching blocks, pinned alongside);
// the drop is not suppressed, because a block judged invalid is invalid during
// replay too, and the base disconnected for one in every FSM state.
//
// Before this step a block that failed commit cost the peer a reject only, and
// only in RUNNING: the peer stayed connected and stayed sync peer.
//
// The third subtest is the sub-peer shape: the body arrived on a DATA1 sub-peer
// of an association, so the entry's peer is the sub-peer, and the drop must
// resolve to the primary, the only peer netsync's bookkeeping knows. The fourth
// pins the two rows that must NOT drop: a policy decline, which is this node's
// configuration, and the default arm, where an error nobody has classified
// lands.
func TestSyncManager_ARejectedParkedBlockDropsItsPeerInEveryState(t *testing.T) {
	for _, tc := range []struct {
		name         string
		fsmState     blockchain2.FSMStateType
		expectReject bool
	}{
		{name: "catching blocks: no reject, but the peer is dropped", fsmState: blockchain2.FSMStateCATCHINGBLOCKS, expectReject: false},
		{name: "running: the reject is sent and the peer is dropped", fsmState: blockchain2.FSMStateRUNNING, expectReject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newParkWiringHarnessInState(t, true, tc.fsmState)

			child := h.blocks[1].MsgBlock().BlockHash()
			parent := h.blocks[0].MsgBlock().BlockHash()

			h.validation.failOnce(child, errors.NewBlockInvalidError("[ValidateBlock][%s] block is invalid", child.String()))

			require.NoError(t, h.deliver(t, 1))
			require.Equal(t, 1, h.sm.blockPark.Len())
			require.True(t, h.peer.Connected(), "sanity: the delivering peer is connected before the verdict")

			h.sm.parkCommits = make(chan parkCommit, parkSweepRPCBudget)

			require.NoError(t, h.deliver(t, 0))
			h.drainOneParkCommit(t)

			h.requireCommitted(t, parent)
			require.Equal(t, 1, h.validation.callsFor(child), "the child reached block validation, which is what judged it")

			require.Zero(t, h.sm.blockPark.Len(), "a judged block does not stay parked")

			_, failed := h.sm.recentlyFailedBlocks.Get(child)
			require.True(t, failed, "a judged block is written off")

			require.True(t, disconnectsWithin(h.peer, 2*time.Second),
				"the peer that delivered a block judged invalid must be dropped, in every FSM state")

			if tc.expectReject {
				require.True(t, WaitUntil(func() bool { return h.rec.wasRejected(child) }, 5*time.Second),
					"when RUNNING the peer is told the block was rejected, before it is dropped")
			} else {
				require.False(t, h.rec.wasRejected(child), "while catching blocks no reject is sent; only the drop stands")
			}
		})
	}

	t.Run("a body that arrived on a DATA1 sub-peer drops the association's primary", func(t *testing.T) {
		h := newParkWiringHarnessInState(t, true, blockchain2.FSMStateRUNNING)

		// A second connected peer as the DATA1 sub-peer of an association whose
		// primary is the harness's own sync peer.
		data1, _, _ := connectRecordingPeer(t, 72, 1000)
		assoc := peerpkg.NewAssociation([]byte{0x04, 0x05, 0x06}, h.peer)
		h.peer.SetAssociation(assoc)
		require.True(t, assoc.AddStream(wire.StreamTypeData1, data1))
		data1.SetAssociation(assoc)
		data1.SetStreamType(wire.StreamTypeData1)

		entry := parkedBlock{hash: h.blocks[1].MsgBlock().BlockHash(), peer: data1}

		h.sm.applyParkDisposition(entry, parkDispositionBlockInvalid)

		require.True(t, disconnectsWithin(h.peer, 2*time.Second), "the primary is what netsync knows; the drop must resolve to it")
		require.True(t, disconnectsWithin(data1, 2*time.Second), "and the sub-peer goes with it")
	})

	t.Run("a policy decline and the default arm drop nobody", func(t *testing.T) {
		declined := parkCommitFailure(parkCommitChain(errors.NewBlockPolicyDeclinedError("block size exceeds excessiveblocksize")))
		require.Equal(t, parkDispositionPolicyDeclined, declined)
		require.Equal(t, parkBlobDrop, declined.blob)
		require.True(t, declined.markFailed, "written off so it is not asked for again")
		require.False(t, declined.blamePeer, "this node's configuration is not the peer's conduct")
		require.False(t, declined.dropPeer)

		unclassified := parkCommitFailure(parkCommitChain(errors.NewBlockError("something nobody classified")))
		require.Equal(t, parkDispositionBlockRejected, unclassified)
		require.True(t, unclassified.blamePeer, "the default arm still sends the reject, as it always did")
		require.False(t, unclassified.dropPeer, "an error nobody has classified must not rotate the sync peer")
	})
}
