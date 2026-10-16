package netsync

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

var subtreeArtefactTypes = []fileformat.FileType{fileformat.FileTypeSubtreeData, fileformat.FileTypeSubtreeMeta, fileformat.FileTypeSubtreeToCheck}

// emitSubtree runs one subtree through w the way the stream builder does: the data file is
// streamed into a pending file, then Emit names it and writes the other two.
func emitSubtree(t *testing.T, ctx context.Context, w *subtreeWriter, leaves int) {
	t.Helper()

	st, data, meta := oneSubtree(t, leaves)

	sink, err := w.OpenData(ctx)(0)
	require.NoError(t, err)
	require.NotNil(t, sink)
	require.NoError(t, data.WriteTransactionsToWriter(sink, 0, st.Length()))
	require.NoError(t, w.Emit(ctx)(0, st, data, meta))
}

// Two blocks that carry an identical run of transactions name the same subtree files. Block A
// creates them and block B adopts them; B's record, and once B commits the block persister,
// rely on files A created. A's conversion then fails and its cleanup runs. It used to delete
// every file A created, leaving B naming subtree data that was gone, with no second source.
// Over one registry, A's cleanup leaves the files B adopted. Removing the adoption check from
// DeleteAll fails this test.
func TestSubtreeWriter_ACleanupLeavesTheFilesAnotherBlockAdopted(t *testing.T) {
	ctx := context.Background()
	store := exclusiveFileStore(t)
	reg := newSubtreeAdoptions()

	a := newSubtreeWriter(ulogger.TestLogger{}, settings.NewSettings(), store, 800000)
	a.adoptions = reg

	b := newSubtreeWriter(ulogger.TestLogger{}, settings.NewSettings(), store, 800001)
	b.adoptions = reg

	emitSubtree(t, ctx, a, 8)
	emitSubtree(t, ctx, b, 8)

	require.Len(t, a.written, len(subtreeArtefactTypes), "A created all three files")
	require.Empty(t, b.written, "B created none")
	require.Len(t, b.adopted, len(subtreeArtefactTypes), "B adopted all three")

	st, _, _ := oneSubtree(t, 8)
	root := st.RootHash()

	// B is handed over and commits, so its writer is never asked to delete. A fails.
	b.finish()
	require.NoError(t, a.DeleteAll(ctx))

	for _, ft := range subtreeArtefactTypes {
		exists, err := store.Exists(ctx, root[:], ft)
		require.NoError(t, err)
		require.True(t, exists, "A's cleanup must leave the %s that B adopted", ft)
	}

	require.Zero(t, reg.size(), "with A's conversion over, nothing is left that could delete the files")
}

// A hold matters only while a creator of the file is still converting, because only a creator
// deletes, and only inside its own conversion. So holds do not outlive the conversions they
// guard against: racing copies of one block, which adopt every file the first copy wrote and
// are common during IBD, leave nothing behind once both have converted, and a writer that adopts
// a file whose creator has already finished takes no hold at all.
func TestSubtreeAdoptions_HoldsEndWithTheCreatorsTheyGuardAgainst(t *testing.T) {
	ctx := context.Background()
	store := exclusiveFileStore(t)
	reg := newSubtreeAdoptions()

	first := newSubtreeWriter(ulogger.TestLogger{}, settings.NewSettings(), store, 800000)
	first.adoptions = reg

	racing := newSubtreeWriter(ulogger.TestLogger{}, settings.NewSettings(), store, 800000)
	racing.adoptions = reg

	emitSubtree(t, ctx, first, 8)
	emitSubtree(t, ctx, racing, 8)

	require.Len(t, racing.adopted, len(subtreeArtefactTypes), "the racing copy holds what the converting first copy wrote")

	racing.finish()
	require.Positive(t, reg.size(), "while the first copy converts, the holds stand")

	first.finish()
	require.Zero(t, reg.size(), "once both copies have converted, nothing is held")

	late := newSubtreeWriter(ulogger.TestLogger{}, settings.NewSettings(), store, 800000)
	late.adoptions = reg

	emitSubtree(t, ctx, late, 8)
	late.finish()

	require.Empty(t, late.adopted, "no creator is converting, so a late adopter needs no hold")
	require.Zero(t, reg.size())
}

// A writer that created its files and that nobody adopted still removes them on failure, and a
// failed adopter gives its marks back, so a creator that fails after it removes its own files.
// Leaving files of a failed block to their delete-at-height would let a peer that keeps sending
// bodies which fail the merkle check fill the disk until retention passes.
func TestSubtreeWriter_ACleanupStillRemovesFilesNoLiveBlockAdopted(t *testing.T) {
	ctx := context.Background()
	store := exclusiveFileStore(t)
	reg := newSubtreeAdoptions()

	a := newSubtreeWriter(ulogger.TestLogger{}, settings.NewSettings(), store, 800000)
	a.adoptions = reg

	b := newSubtreeWriter(ulogger.TestLogger{}, settings.NewSettings(), store, 800001)
	b.adoptions = reg

	emitSubtree(t, ctx, a, 8)
	emitSubtree(t, ctx, b, 8)

	// B fails first: its record is never read, so it gives its holds back.
	require.NoError(t, b.DeleteAll(ctx))
	require.Empty(t, b.adopted, "a failed adopter holds nothing")

	require.NoError(t, a.DeleteAll(ctx))
	require.Zero(t, reg.size(), "nothing is tracked once both conversions are over")

	st, _, _ := oneSubtree(t, 8)
	root := st.RootHash()

	for _, ft := range subtreeArtefactTypes {
		exists, err := store.Exists(ctx, root[:], ft)
		require.NoError(t, err)
		require.False(t, exists, "with no live adopter the creator's cleanup removes %s", ft)
	}
}

// convertedCopy stands in for one pipelineBlockSink call that converted: a writer of the
// manager's registry emits the subtree and is then held open until settled, as the sink's
// deferred holdUntilSettled does.
func convertedCopy(t *testing.T, ctx context.Context, sm *SyncManager, hash chainhash.Hash) *subtreeWriter {
	t.Helper()

	w := newSubtreeWriter(ulogger.TestLogger{}, settings.NewSettings(), sm.subtreeStore, 800000)
	w.adoptions = sm.subtreeAdoptionsFor()

	emitSubtree(t, ctx, w, 8)
	sm.holdUntilSettled(hash, w)

	return w
}

func requireSubtreeFiles(t *testing.T, ctx context.Context, sm *SyncManager, want bool, msg string) {
	t.Helper()

	st, _, _ := oneSubtree(t, 8)
	root := st.RootHash()

	for _, ft := range subtreeArtefactTypes {
		exists, err := sm.subtreeStore.Exists(ctx, root[:], ft)
		require.NoError(t, err)
		require.Equal(t, want, exists, "%s: %s", msg, ft)
	}
}

// pipelineBlockDelete runs after the sink, in the same wire read, when the wire layer refuses a
// copy that converted. It runs in the order production uses: the copy's writer is still held
// open (holdUntilSettled), so a hold another block took on a file this copy created stands, and
// the files that block adopted are left. Before, the sink finished its writer first and this
// delete removed them.
func TestPipelineBlockDelete_ARefusedCopyLeavesTheFilesAnotherBlockAdopted(t *testing.T) {
	ctx := context.Background()
	sm := &SyncManager{ctx: ctx, subtreeStore: exclusiveFileStore(t)}

	refused, adopter := chainhash.Hash{1}, chainhash.Hash{2}

	convertedCopy(t, ctx, sm, refused)

	b := convertedCopy(t, ctx, sm, adopter)
	require.Len(t, b.adopted, len(subtreeArtefactTypes), "B adopted while the refused copy was still open")

	sm.settleAccepted(adopter, false)

	require.NoError(t, sm.pipelineBlockDelete(refused, true))

	requireSubtreeFiles(t, ctx, sm, true, "the refused copy's cleanup must leave the file B adopted")
	require.Zero(t, sm.subtreeAdoptionsFor().size(), "both copies are settled, so nothing is tracked")
}

// A refused copy deletes only what it created, never a file it adopted from a block whose
// conversion has already finished.
func TestPipelineBlockDelete_ARefusedCopyLeavesTheFilesItAdopted(t *testing.T) {
	ctx := context.Background()
	sm := &SyncManager{ctx: ctx, subtreeStore: exclusiveFileStore(t)}

	creator, refused := chainhash.Hash{1}, chainhash.Hash{2}

	convertedCopy(t, ctx, sm, creator)
	sm.settleAccepted(creator, false)

	d := convertedCopy(t, ctx, sm, refused)
	require.Empty(t, d.written, "the refused copy created nothing")

	require.NoError(t, sm.pipelineBlockDelete(refused, true))

	requireSubtreeFiles(t, ctx, sm, true, "the refused copy must leave the files it only adopted")
}

// A refused copy that created its files and that nobody adopted still removes them.
func TestPipelineBlockDelete_ARefusedCopyRemovesWhatOnlyItUsed(t *testing.T) {
	ctx := context.Background()
	sm := &SyncManager{ctx: ctx, subtreeStore: exclusiveFileStore(t)}

	refused := chainhash.Hash{1}

	convertedCopy(t, ctx, sm, refused)

	require.NoError(t, sm.pipelineBlockDelete(refused, true))

	requireSubtreeFiles(t, ctx, sm, false, "a refused copy's own files are removed")
	require.Zero(t, sm.subtreeAdoptionsFor().size())
}

// A converted copy's writer is held open until the wire layer settles it. An accepted copy is
// settled by QueueBlockOnDisk; a copy whose verdict never arrives is settled by the park sweep
// once settleBackstop has passed. Either way nothing is left tracked.
func TestSyncManager_AConvertedCopyIsAlwaysSettled(t *testing.T) {
	ctx := context.Background()
	sm := &SyncManager{ctx: ctx, subtreeStore: exclusiveFileStore(t)}

	accepted := chainhash.Hash{1}

	convertedCopy(t, ctx, sm, accepted)
	require.Positive(t, sm.subtreeAdoptionsFor().size(), "the converted copy's files are live until it is settled")

	// Shutdown set only so the queue send is skipped; the settle comes before it.
	atomic.StoreInt32(&sm.shutdown, 1)
	sm.QueueBlockOnDisk(peerpkg.BlockBody{Hash: accepted, Converted: true}, nil)
	require.Zero(t, waitingFor(sm, accepted), "an accepted copy is settled when it is queued")
	require.Zero(t, sm.subtreeAdoptionsFor().size())

	other := &SyncManager{ctx: ctx, subtreeStore: exclusiveFileStore(t)}
	lost := chainhash.Hash{2}

	convertedCopy(t, ctx, other, lost)

	other.settleStale(time.Now())
	require.Positive(t, other.subtreeAdoptionsFor().size(), "a copy converted moments ago is not stale yet")

	other.settleStale(time.Now().Add(settleBackstop))
	require.Zero(t, waitingFor(other, lost), "the sweep settles a copy whose verdict never arrived")
	require.Zero(t, other.subtreeAdoptionsFor().size())
}

// waitingFor is the number of converted copies of hash whose writers wait for a verdict.
func waitingFor(sm *SyncManager, hash chainhash.Hash) int {
	sm.settlingMu.Lock()
	defer sm.settlingMu.Unlock()

	return len(sm.settling[hash])
}

// A second copy of a block can convert after the first copy's sink returned and before the
// first copy's verdict arrives, so two writers can wait under one hash. A refusal then cannot
// say which copy it is about. It used to take whichever writer was waiting, which could be the
// second copy's, and delete that copy's files and its fresh record. Now it deletes nothing and
// leaves both writers waiting; acceptance settles the oldest, and the sweep the rest.
func TestPipelineBlockDelete_ARefusalThatCannotNameItsCopyDeletesNothing(t *testing.T) {
	ctx := context.Background()
	sm := &SyncManager{ctx: ctx, subtreeStore: exclusiveFileStore(t), logger: ulogger.TestLogger{}}

	hash := chainhash.Hash{1}

	first := convertedCopy(t, ctx, sm, hash)
	require.Len(t, first.written, len(subtreeArtefactTypes), "the first copy created the files")

	second := convertedCopy(t, ctx, sm, hash)
	require.Len(t, second.adopted, len(subtreeArtefactTypes), "the second copy adopted them while the first was open")
	require.Equal(t, 2, waitingFor(sm, hash))

	require.NoError(t, sm.pipelineBlockDelete(hash, true))

	requireSubtreeFiles(t, ctx, sm, true, "a refusal that cannot name its copy deletes nothing")
	require.Equal(t, 2, waitingFor(sm, hash), "and settles neither writer")

	sm.settleAccepted(hash, false)
	require.Equal(t, 1, waitingFor(sm, hash), "acceptance settles the oldest")

	sm.settleStale(time.Now().Add(settleBackstop))
	require.Zero(t, waitingFor(sm, hash))
	require.Zero(t, sm.subtreeAdoptionsFor().size())
}
