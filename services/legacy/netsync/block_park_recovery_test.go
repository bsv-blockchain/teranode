package netsync

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// peerMsgRecorder collects what a peer's remote end is actually sent. The park
// paths are all defined by what reaches the peer — a getblocks that keeps it
// sending, a getdata that asks for a block again, a reject that tells it a block
// was bad — so every assertion here is made on the far side of the wire rather
// than on manager state.
type peerMsgRecorder struct {
	mu        sync.Mutex
	getData   []chainhash.Hash
	getBlocks int
	rejects   []chainhash.Hash
}

func (r *peerMsgRecorder) recordGetData(msg *wire.MsgGetData) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, iv := range msg.InvList {
		if iv.Type == wire.InvTypeBlock {
			r.getData = append(r.getData, iv.Hash)
		}
	}
}

func (r *peerMsgRecorder) recordGetBlocks() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.getBlocks++
}

func (r *peerMsgRecorder) recordReject(msg *wire.MsgReject) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.rejects = append(r.rejects, msg.Hash)
}

func (r *peerMsgRecorder) getBlocksCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.getBlocks
}

func (r *peerMsgRecorder) getDataCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.getData)
}

// askedForSince reports whether hash appears in a getdata recorded after the
// first from getdata block hashes already seen.
func (r *peerMsgRecorder) askedForSince(from int, hash chainhash.Hash) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if from > len(r.getData) {
		return false
	}

	for _, got := range r.getData[from:] {
		if got.IsEqual(&hash) {
			return true
		}
	}

	return false
}

func (r *peerMsgRecorder) wasRejected(hash chainhash.Hash) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, got := range r.rejects {
		if got.IsEqual(&hash) {
			return true
		}
	}

	return false
}

// connectRecordingPeer returns a live peer whose remote end records the three
// message kinds the park paths are supposed to send.
func connectRecordingPeer(t *testing.T, idx uint8, lastBlock int32) (*peerpkg.Peer, *peerpkg.Peer, *peerMsgRecorder) {
	t.Helper()

	rec := &peerMsgRecorder{}
	chainParams := &chaincfg.MainNetParams

	remoteCfg := peerpkg.Config{
		Listeners: peerpkg.MessageListeners{
			OnGetData:   func(_ *peerpkg.Peer, msg *wire.MsgGetData) { rec.recordGetData(msg) },
			OnGetBlocks: func(_ *peerpkg.Peer, _ *wire.MsgGetBlocks) { rec.recordGetBlocks() },
			OnReject:    func(_ *peerpkg.Peer, msg *wire.MsgReject) { rec.recordReject(msg) },
		},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      chainParams,
	}
	localCfg := peerpkg.Config{
		Listeners:        peerpkg.MessageListeners{},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      chainParams,
	}

	remote, local, err := MakeConnectedPeers(t, remoteCfg, localCfg, idx)
	require.NoError(t, err)

	local.UpdateLastBlockHeight(lastBlock)

	t.Cleanup(func() {
		local.DisconnectWithInfo("test over")
		remote.DisconnectWithInfo("test over")
	})

	return local, remote, rec
}

// TestSyncManager_AParkedOrphanIsStillAnsweredWithAGetblocks is the one the
// park broke. In the legacy sync protocol an orphan is not only a block out of
// order: the peer pushes its tip after delivering a batch and then waits for the
// next getblocks before it sends anything else. Keeping the block instead of
// throwing it away must not swallow that answer — the park keeps the download,
// the getblocks fetches the gap, and they are not alternatives.
//
// Out of headers-first mode, which is every node past the final checkpoint,
// nothing else sends anything: fetchMoreHeaderBlocks returns immediately. The
// peer would sit silent until the stall detector rotated it.
func TestSyncManager_AParkedOrphanIsStillAnsweredWithAGetblocks(t *testing.T) {
	for _, tc := range []struct {
		name         string
		headersFirst bool
	}{
		{name: "past the final checkpoint", headersFirst: false},
		{name: "in headers-first mode", headersFirst: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newParkWiringHarness(t, true)
			h.sm.headersFirstMode.Store(tc.headersFirst)

			child := h.blocks[1].MsgBlock().BlockHash()

			require.NoError(t, h.deliver(t, 1))

			require.Equal(t, 1, h.sm.blockPark.Len(), "the block must be kept, not thrown away")
			require.Contains(t, parkDirEntries(t, h.parkDir), child.String()+".block")

			require.True(t, WaitUntil(func() bool { return h.rec.getBlocksCount() > 0 }, 5*time.Second),
				"an orphan must be answered with a getblocks or the peer sends nothing more")
		})
	}
}

// TestSyncManager_AParkedFrontBlockIsAskedForAgainWhenItIsGivenUp pins the
// give-up-then-re-request path for the block sync is most exposed on: the very
// next one it wants. The wanted-range pass recomputes what it wants and who
// owes it from the committed tip on every call, with no position or index of
// its own to lose, which is what makes this safe with no header list behind it
// at all.
//
// The front is the block one above the committed tip. On a real chain the
// harness's blocks[0] commits on arrival (its parent is genesis), so the front
// here is blocks[1], parked above a blocks[0] that is committed after it.
func TestSyncManager_AParkedFrontBlockIsAskedForAgainWhenItIsGivenUp(t *testing.T) {
	h := newParkWiringHarness(t, true)

	front := h.blocks[1].MsgBlock().BlockHash()

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len(), "the front block parks like any other orphan")

	// The block is given up on. The trigger used to be a thirty-minute timer;
	// there is no timer any more, and the rules that replaced it deliberately do
	// not rewind, because a block the chain has gone past should not be asked
	// for again. So this drives the give-up through a path that does rewind and
	// that a node meets in earnest: the parent turns up, the sweep goes to
	// commit the block, and its blob will not read back.
	h.chainHolds(t, h.blocks[0].MsgBlock().BlockHash())
	h.store.failReadsWith(errors.ErrBlobNotFound)

	h.sm.sweepParkedBlocks(time.Now().Add(parkStuckThreshold + time.Second))

	require.Zero(t, h.sm.blockPark.Len())

	before := h.rec.getDataCount()

	h.sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return h.rec.askedForSince(before, front) }, 5*time.Second),
		"a block given up on must go back into the download walk, or nothing ever asks for it again")
}

// TestSyncManager_AParkedBlockThatWillNotReadBackIsAskedForAgain covers the
// drain's read failure: the blob has gone or will not decode, so there is
// nothing to commit and nothing to put back. The block has to re-enter the
// download walk or it is simply lost.
func TestSyncManager_AParkedBlockThatWillNotReadBackIsAskedForAgain(t *testing.T) {
	h := newParkWiringHarness(t, true)

	child := h.blocks[1].MsgBlock().BlockHash()
	parent := h.blocks[0].MsgBlock().BlockHash()

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	// The blob is destroyed under the park: a truncated write, a disk that lost
	// the file, a sweep that took it.
	require.NoError(t, os.Remove(filepath.Join(h.parkDir, child.String()+".block")))

	before := h.rec.getDataCount()

	// See drainOneParkCommit's own doc comment: the parent's arrival resolves
	// its own parent (genesis, always in the real chain) immediately, so
	// handleBlockOnDiskMsg posts straight to a channel rather than committing
	// inline, and this drains it the way a running node's consumer loop would.
	h.sm.parkCommits = make(chan parkCommit, parkSweepRPCBudget)

	// The parent commits, so the drain reaches for the child and finds nothing.
	require.NoError(t, h.deliver(t, 0))
	h.drainOneParkCommit(t)

	h.requireCommitted(t, parent)
	require.Zero(t, h.sm.blockPark.Len(), "a block that cannot be read back must not stay in the index")

	h.sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return h.rec.askedForSince(before, child) }, 5*time.Second),
		"a parked block that will not read back must be asked for again")
}

// TestSyncManager_AParkedBlockThatWillNotCommitIsGivenUpAndRejected covers the
// rest of the drain's failure branch, none of which any test reached: the blob
// is dropped, the block goes back into the download walk, and the peer that
// actually sent it — not a fallback peer, and not nobody — is told it was
// rejected.
//
// Whether the peer is told at all is the node's state and not the error's, and
// both sides of that are driven here. handleBlockMsg suppresses every reject
// while the node is catching blocks, because then it is replaying history rather
// than judging a peer's tip — and during initial sync this drain is the MAIN
// commit path, so a parked block must not earn its peer a reject that the same
// block delivered live would not.
func TestSyncManager_AParkedBlockThatWillNotCommitIsGivenUpAndRejected(t *testing.T) {
	for _, tc := range []struct {
		name         string
		fsmState     blockchain2.FSMStateType
		expectReject bool
	}{
		{
			name:         "catching blocks, so the peer is not blamed",
			fsmState:     blockchain2.FSMStateCATCHINGBLOCKS,
			expectReject: false,
		},
		{
			name:         "running, so the peer is told the block was bad",
			fsmState:     blockchain2.FSMStateRUNNING,
			expectReject: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newParkWiringHarnessInState(t, true, tc.fsmState)

			child := h.blocks[1].MsgBlock().BlockHash()
			parent := h.blocks[0].MsgBlock().BlockHash()

			// Block validation's verdict on the child, at the drain's real
			// commit attempt below: a fault that is the block's and not the
			// local node's, so the error itself is a judgement on the block.
			h.validation.failOnce(child, errors.NewBlockInvalidError("this block is not one we can take"))

			require.NoError(t, h.deliver(t, 1))
			require.Equal(t, 1, h.sm.blockPark.Len())

			before := h.rec.getDataCount()

			// See drainOneParkCommit's own doc comment: the parent's arrival
			// resolves its own parent (genesis, always in the real chain)
			// immediately, so handleBlockOnDiskMsg posts straight to a channel
			// rather than committing inline.
			h.sm.parkCommits = make(chan parkCommit, parkSweepRPCBudget)

			require.NoError(t, h.deliver(t, 0))
			h.drainOneParkCommit(t)

			h.requireCommitted(t, parent)
			require.Equal(t, 1, h.validation.callsFor(child), "the child reached block validation, which is what judged it")

			exists, err := h.chain.GetBlockExists(h.sm.ctx, &child)
			require.NoError(t, err)
			require.False(t, exists, "a refused block is not in the chain")

			require.Zero(t, h.sm.blockPark.Len(), "a block that will not commit must not stay parked")

			for _, name := range parkDirEntries(t, h.parkDir) {
				require.NotContains(t, name, child.String(), "a block given up on must not leave its blob behind")
			}

			if tc.expectReject {
				require.True(t, WaitUntil(func() bool { return h.rec.wasRejected(child) }, 5*time.Second),
					"the peer that sent the block must be the one told it was rejected")
			} else {
				require.False(t, h.rec.wasRejected(child),
					"while catching blocks no reject is sent, whether the block is committed from the wire or from the park")
			}

			_, failed := h.sm.recentlyFailedBlocks.Get(child)
			require.True(t, failed, "a block written off must be remembered so its descendants are short-circuited")

			h.sm.fetchHeaderBlocks()

			require.False(t, WaitUntil(func() bool { return h.rec.askedForSince(before, child) }, time.Second),
				"a block written off as invalid must not be asked for again while recentlyFailedBlocks still names it")
		})
	}
}

// TestSyncManager_AParkedBlockWhoseParentGoesMissingAgainStaysParked covers one
// of the ways a drain declines to commit without giving the block up (the table in
// block_park_policy.go has three keep rows: RetryLater, ParentGone, ParentNotMinedYet):
// HandleConvertedBlock's own GetBlockHeader on the previous hash answers
// ErrBlockNotFound, and parkCommitFailure maps that to the ParentGone row. A
// missing parent says nothing about the child, so the child has to go back in
// the index with its blob intact rather than be written off and re-downloaded.
func TestSyncManager_AParkedBlockWhoseParentGoesMissingAgainStaysParked(t *testing.T) {
	h := newParkWiringHarness(t, true)

	child := h.blocks[1].MsgBlock().BlockHash()
	parent := h.blocks[0].MsgBlock().BlockHash()

	// The parent's commit reports success, but the chain never gets the row,
	// so the drain the parent's commit triggers reaches the child and the
	// child's own parent lookup still fails. This is a fault injection to get
	// the drain onto the ParentGone row; see recordOnly's comment for why no
	// store path (a reorg included) has been traced to produce it.
	h.validation.recordOnlyFor(parent)

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	parkedBytes := h.sm.blockPark.Bytes()

	require.NoError(t, h.deliver(t, 0))

	require.Equal(t, 1, h.validation.callsFor(parent), "sanity: the parent's commit was reported")
	require.Equal(t, 1, h.sm.blockPark.Len(), "a block whose parent went missing again must stay parked")
	require.False(t, h.parkedEntry(t, child).parentMissingAt.IsZero(), "the ParentGone row stamped the entry, so the next turn is not spent on it")
	require.Zero(t, h.validation.callsFor(child), "a block whose parent is missing never reaches block validation")
	require.Equal(t, parkedBytes, h.sm.blockPark.Bytes(), "putting a block back must not lose or double its budget")
	require.Contains(t, parkDirEntries(t, h.parkDir), child.String()+".block",
		"the blob must still be on disk for the retry")

	_, failed := h.sm.recentlyFailedBlocks.Get(child)
	require.False(t, failed, "a block nobody could commit yet must not be written off as a failure")

	require.False(t, h.rec.wasRejected(child), "a missing parent is not the peer's fault, so it must not be told the block was bad")
}

// TestSyncManager_AParkedBlockIsKeptWhenTheCommitIsCancelled covers the
// RetryLater row. On shutdown the commit is cancelled mid-flight; the block has not been
// judged, so it must be left where the restart scan will find it rather than
// deleted and re-downloaded.
func TestSyncManager_AParkedBlockIsKeptWhenTheCommitIsCancelled(t *testing.T) {
	h := newParkWiringHarness(t, true)

	child := h.blocks[1].MsgBlock().BlockHash()
	parent := h.blocks[0].MsgBlock().BlockHash()

	// The child's commit is cancelled mid-flight at block validation; the
	// parent's goes through.
	h.validation.failOnce(child, errors.NewContextCanceledError("shutting down", context.Canceled))

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	require.NoError(t, h.deliver(t, 0))

	h.requireCommitted(t, parent)
	require.Equal(t, 1, h.sm.blockPark.Len(), "a cancelled commit must leave the block parked for the restart scan")
	require.Contains(t, parkDirEntries(t, h.parkDir), child.String()+".block")

	_, failed := h.sm.recentlyFailedBlocks.Get(child)
	require.False(t, failed, "a cancelled commit is not a verdict on the block")
}

// TestSyncManager_AParkedRecordWhoseDataFileIsGoneIsReDownloadedNotRejected is
// the FilesGone row end to end. A converted record is a few hundred bytes
// naming subtree files that carry their own delete-at-height; the record does
// not, so it can outlive the files it points at. Before this, the commit path
// handed such a record to block validation, whose read of the missing file came
// back as a not-found that parkCommitFailure's default read as a judgement on
// the block: the only copy deleted, the hash frozen in recentlyFailedBlocks for
// ten minutes, and a reject sent to an honest peer once RUNNING.
//
// Now the record's completeness is checked on the commit path, before anything
// is validated, and a record whose files are gone is dropped without a mark or
// a reject so the next wanted-range pass downloads the block again.
//
// The FSM is RUNNING so a reject, were one sent, would not be suppressed.
// The spy block validation reads no files, so with the completeness check
// removed the child COMMITS: callsFor(child) and GetBlockExists(child) are the
// assertions that tell the two apart.
func TestSyncManager_AParkedRecordWhoseDataFileIsGoneIsReDownloadedNotRejected(t *testing.T) {
	h := newParkWiringHarnessInState(t, true, blockchain2.FSMStateRUNNING, withTransactions(1))

	child := h.blocks[1].MsgBlock().BlockHash()
	parent := h.blocks[0].MsgBlock().BlockHash()

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	record, err := h.sm.blockPark.ReadConverted(h.sm.ctx, child)
	require.NoError(t, err)
	require.NotEmpty(t, record.Subtrees, "sanity: the record must name a subtree, or there is no file to lose")

	// The data file expires under the record, the way the subtree store's own
	// delete-at-height removes each file on its own.
	require.NoError(t, h.store.Del(h.sm.ctx, record.Subtrees[0][:], fileformat.FileTypeSubtreeData))

	before := h.rec.getDataCount()

	// See drainOneParkCommit's own doc comment: the parent's arrival resolves
	// its own parent (genesis) immediately, so handleBlockOnDiskMsg posts to a
	// channel rather than committing inline.
	h.sm.parkCommits = make(chan parkCommit, parkSweepRPCBudget)

	require.NoError(t, h.deliver(t, 0))
	h.drainOneParkCommit(t)

	h.requireCommitted(t, parent)

	require.Zero(t, h.validation.callsFor(child), "a record whose files are gone must never reach block validation")

	exists, err := h.chain.GetBlockExists(h.sm.ctx, &child)
	require.NoError(t, err)
	require.False(t, exists, "a record whose files are gone cannot have been committed")

	require.Zero(t, h.sm.blockPark.Len(), "the record is dropped, not kept")

	for _, name := range parkDirEntries(t, h.parkDir) {
		require.NotContains(t, name, child.String(), "the dropped record must not leave its blob behind")
	}

	require.False(t, h.rec.wasRejected(child), "the files going missing on this node is not the peer's fault")

	_, failed := h.sm.recentlyFailedBlocks.Get(child)
	require.False(t, failed, "a block nobody judged must not be written off, or the wanted range skips it for the TTL")

	h.sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return h.rec.askedForSince(before, child) }, 5*time.Second),
		"a record whose files are gone must be downloaded again at once")
}

// TestSyncManager_AParkedRecordWhoseFilesCannotBeCheckedIsKept is the other
// half of the completeness check: a stat that could not run is not a stat that
// said absent. The file store takes a read permit before every Exists and gives
// up on it after its deadline with a StorageError, and the same call is
// cancelled at shutdown; reading either as "the files are gone" would delete
// the only copy of a downloaded block over a condition that is over in
// seconds, the regression the policy file was written to stop. The block stays
// parked, unjudged, for the sweep to try again.
func TestSyncManager_AParkedRecordWhoseFilesCannotBeCheckedIsKept(t *testing.T) {
	h := newParkWiringHarnessInState(t, true, blockchain2.FSMStateRUNNING, withTransactions(1))

	child := h.blocks[1].MsgBlock().BlockHash()
	parent := h.blocks[0].MsgBlock().BlockHash()

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	parkedBytes := h.sm.blockPark.Bytes()

	record, err := h.sm.blockPark.ReadConverted(h.sm.ctx, child)
	require.NoError(t, err)
	require.NotEmpty(t, record.Subtrees, "sanity: the record must name a subtree, or there is no stat to fault")

	// The shape File.Exists returns when acquireReadPermit runs out of time:
	// a StorageError around a ServiceUnavailable. Armed for the child's
	// subtree key only, so the parent's own sink writes below are untouched.
	h.store.failExistsFor(record.Subtrees[0][:],
		errors.NewStorageError("[File][Exists] failed to acquire read permit", errors.NewServiceUnavailableError("read permit not acquired within 25s")))

	h.sm.parkCommits = make(chan parkCommit, parkSweepRPCBudget)

	require.NoError(t, h.deliver(t, 0))
	h.drainOneParkCommit(t)

	h.requireCommitted(t, parent)

	require.Zero(t, h.validation.callsFor(child), "a record whose completeness is unknown must not be handed to block validation either")

	// Disarmed before the end-state check: requireStillParked proves the block
	// is not asked for again through holdsBlock, which stats the same files.
	h.store.failExistsFor(nil, nil)

	h.requireStillParked(t, child, parkedBytes)
}

// TestSyncManager_ACorruptVerdictFromTheParkDropsTheRecordAndBlamesNobody is
// the RecordCorrupt row end to end: block validation's corrupt verdict on a
// record this node wrote drops the record, writes nothing off and tells the
// peer nothing, and the block is downloaded again. The FSM is RUNNING so the
// reject the default row would send is not suppressed by the catching-blocks
// rule.
func TestSyncManager_ACorruptVerdictFromTheParkDropsTheRecordAndBlamesNobody(t *testing.T) {
	h := newParkWiringHarnessInState(t, true, blockchain2.FSMStateRUNNING, withTransactions(1))

	child := h.blocks[1].MsgBlock().BlockHash()
	parent := h.blocks[0].MsgBlock().BlockHash()

	// What a quick validation that finds a structure file not hashing to its
	// key returns, after quarantining the file.
	h.validation.failOnce(child, errors.NewBlockCorruptError("[processBlockFound][%s] a subtree file this node wrote does not hash to its key", child.String()))

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	before := h.rec.getDataCount()

	h.sm.parkCommits = make(chan parkCommit, parkSweepRPCBudget)

	require.NoError(t, h.deliver(t, 0))
	h.drainOneParkCommit(t)

	h.requireCommitted(t, parent)
	require.Equal(t, 1, h.validation.callsFor(child), "the child reached block validation, which is what returned the verdict")

	exists, err := h.chain.GetBlockExists(h.sm.ctx, &child)
	require.NoError(t, err)
	require.False(t, exists, "a block with a corrupt verdict is not in the chain")

	require.Zero(t, h.sm.blockPark.Len(), "the record is dropped so the block is downloaded again")

	for _, name := range parkDirEntries(t, h.parkDir) {
		require.NotContains(t, name, child.String(), "the dropped record must not leave its blob behind")
	}

	require.False(t, h.rec.wasRejected(child), "the body was verified at the sink; a corrupt record is this node's fault, not the peer's")

	_, failed := h.sm.recentlyFailedBlocks.Get(child)
	require.False(t, failed, "a corrupt verdict on this node's own record must not write the block off")

	h.sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return h.rec.askedForSince(before, child) }, 5*time.Second),
		"a block whose record was corrupt must be downloaded again")
}

// TestSyncManager_AMissingParentOutputKeepsTheParkedBlock is the LocalUtxoFault
// row end to end: the UTXO store reports an output the block spends as not
// found, and the block stays parked, unjudged and not re-asked, with the
// parent-missing stamp so the drain does not spend every turn on it.
func TestSyncManager_AMissingParentOutputKeepsTheParkedBlock(t *testing.T) {
	h := newParkWiringHarnessInState(t, true, blockchain2.FSMStateRUNNING, withTransactions(1))

	child := h.blocks[1].MsgBlock().BlockHash()
	parent := h.blocks[0].MsgBlock().BlockHash()

	// The SQL store's batched spend shape: a UtxoError whose chain carries
	// the per-spend TxNotFound, under quickValidateBlock's wrap.
	h.validation.failOnce(child, errors.NewProcessingError("[quickValidateBlock][%s] failed to process block subtrees", child.String(),
		errors.NewUtxoError("error in sql spend (batched mode) - errors", errors.NewTxNotFoundError("output 0 of %s not found", "abcd"))))

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	parkedBytes := h.sm.blockPark.Bytes()

	require.NoError(t, h.deliver(t, 0))

	h.requireCommitted(t, parent)
	require.Equal(t, 1, h.validation.callsFor(child), "the child reached block validation, which is where the UTXO set came up short")

	h.requireStillParked(t, child, parkedBytes)
	require.False(t, h.parkedEntry(t, child).parentMissingAt.IsZero(), "the row stamps the entry so the next turn is not spent on it")
}

// TestSyncManager_AnInvalidTransactionVerdictFromTheFullRouteIsAJudgementOnTheBlock
// is the BlockRejected row for the shape the full route now returns for a legacy
// block with a consensus-invalid transaction: ErrBlockInvalid around subtree
// validation's processing wrap around ErrTxInvalid, with no corrupt code in the
// chain. Before the producer was corrected the same block came back corrupt and
// landed on RecordCorrupt: dropped, unmarked, re-asked on the next wanted-range
// pass, and judged corrupt again, without bound. Here the record is dropped,
// the hash is written off, the peer is told once RUNNING, and the block is not
// asked for again while recentlyFailedBlocks names it.
//
// This test pins the row for the shape. It does not fail when the producer in
// block validation is reverted, because the spy returns what the test injects;
// TestProcessBlockFound_AnInvalidTransactionOnTheLegacyFullRouteIsAJudgementNotACorruptRecord
// in services/blockvalidation is the test that does.
func TestSyncManager_AnInvalidTransactionVerdictFromTheFullRouteIsAJudgementOnTheBlock(t *testing.T) {
	h := newParkWiringHarnessInState(t, true, blockchain2.FSMStateRUNNING, withTransactions(1))

	child := h.blocks[1].MsgBlock().BlockHash()
	parent := h.blocks[0].MsgBlock().BlockHash()

	verdict := errors.NewBlockInvalidError("[ValidateBlock][%s] block contains invalid transactions", child.String(),
		errors.NewProcessingError("[CheckBlockSubtrees] failed to process transactions",
			errors.NewTxInvalidError("transaction in subtree is invalid")))
	require.False(t, errors.IsBlockCorrupt(verdict), "fixture: the full route's legacy verdict carries no corrupt code")

	h.validation.failOnce(child, verdict)

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	before := h.rec.getDataCount()

	h.sm.parkCommits = make(chan parkCommit, parkSweepRPCBudget)

	require.NoError(t, h.deliver(t, 0))
	h.drainOneParkCommit(t)

	h.requireCommitted(t, parent)
	require.Equal(t, 1, h.validation.callsFor(child), "the child reached block validation, which is what judged it")

	exists, err := h.chain.GetBlockExists(h.sm.ctx, &child)
	require.NoError(t, err)
	require.False(t, exists, "a block with an invalid transaction is not in the chain")

	require.Zero(t, h.sm.blockPark.Len(), "a judged block does not stay parked")

	for _, name := range parkDirEntries(t, h.parkDir) {
		require.NotContains(t, name, child.String(), "a judged block must not leave its blob behind")
	}

	require.True(t, WaitUntil(func() bool { return h.rec.wasRejected(child) }, 5*time.Second),
		"the sink bound the body to the header, so the verdict is about the block the peer sent and the peer is told")

	_, failed := h.sm.recentlyFailedBlocks.Get(child)
	require.True(t, failed, "a judged block is written off so its descendants are short-circuited and it is not re-asked")

	h.sm.fetchHeaderBlocks()

	require.False(t, WaitUntil(func() bool { return h.rec.askedForSince(before, child) }, time.Second),
		"a block written off must not be downloaded again while recentlyFailedBlocks names it; that re-ask is the loop this closes")
}
