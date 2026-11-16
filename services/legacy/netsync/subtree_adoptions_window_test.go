package netsync

import (
	"context"
	"io"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// pausingStore is the real file store with a hook that runs after a SetFromReader has
// published, before the call returns to the writer. It embeds the blob.Store interface, not
// *File, so the subtree writer's pendingFileStore assertion fails and every artefact, the data
// file included, goes through put and so through this hook.
type pausingStore struct {
	blob.Store
	after func(key []byte, fileType fileformat.FileType)
}

func (s *pausingStore) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	err := s.Store.SetFromReader(ctx, key, fileType, reader, opts...)
	if err == nil && s.after != nil {
		s.after(key, fileType)
	}

	return err
}

// External review of e4698b219, P1. A creator's file is on disk the moment its publish
// returns, but the registry learned it was a creator only afterwards, in created(). An adopter
// that met the file in between counted no live creator and took no hold, so the creator's
// failure cleanup then deleted a file the adopter's record named. Pinned at the registry: the
// creator is still pending when the adopter adopts.
func TestSubtreeAdoptions_AnAdoptionBeforeTheCreatorRegistersIsHeld(t *testing.T) {
	ctx := context.Background()
	store := exclusiveFileStore(t)
	reg := newSubtreeAdoptions()

	key := writtenSubtree{Hash: chainhash.Hash{7}, FileType: fileformat.FileTypeSubtreeData}
	require.NoError(t, store.Set(ctx, key.Hash[:], key.FileType, []byte("published by A"), options.WithDeleteAt(100)))

	reg.attempt(key) // A, whose publish has just succeeded and who has not yet said so
	reg.attempt(key) // B

	_, held := reg.adopted(key) // B finds A's file
	require.True(t, held, "B adopted a file whose creator was still unresolved, so B must hold it")

	reg.created(key) // A registers

	left, err := reg.deleteUnlessAdopted(ctx, store, key) // A fails
	require.NoError(t, err)
	require.True(t, left)

	exists, err := store.Exists(ctx, key.Hash[:], key.FileType)
	require.NoError(t, err)
	require.True(t, exists, "A's cleanup must leave the file B adopted")
}

// The same window through two real writers: A publishes its data file and is paused before
// it returns, B converts the same subtree in full and adopts A's file, then A resumes and its
// conversion fails. The file B adopted must still be on disk.
func TestSubtreeWriter_AnAdoptionInsideTheCreatorsPublishSurvivesItsCleanup(t *testing.T) {
	ctx := context.Background()
	base := exclusiveFileStore(t)
	reg := newSubtreeAdoptions()

	published := make(chan struct{})
	resume := make(chan struct{})

	var once sync.Once

	aStore := &pausingStore{Store: base, after: func(_ []byte, ft fileformat.FileType) {
		if ft == fileformat.FileTypeSubtreeData {
			once.Do(func() {
				close(published)
				<-resume
			})
		}
	}}

	a := newSubtreeWriter(ulogger.TestLogger{}, settings.NewSettings(), aStore, 800000)
	a.adoptions = reg

	b := newSubtreeWriter(ulogger.TestLogger{}, settings.NewSettings(), &pausingStore{Store: base}, 800001)
	b.adoptions = reg

	st, data, meta := oneSubtree(t, 8)
	root := st.RootHash()

	aDone := make(chan error, 1)

	go func() { aDone <- a.Emit(ctx)(0, st, data, meta) }()

	<-published

	require.NoError(t, b.Emit(ctx)(0, st, data, meta))

	close(resume)
	require.NoError(t, <-aDone)

	require.Contains(t, a.written, writtenSubtree{Hash: *root, FileType: fileformat.FileTypeSubtreeData}, "A created the data file")

	// B converted and is waiting for its verdict; A fails.
	require.NoError(t, a.DeleteAll(ctx))

	exists, err := base.Exists(ctx, root[:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)
	require.True(t, exists, "A's cleanup must leave the data file B adopted while A was publishing it")

	b.finish()
	require.Zero(t, reg.size())
}

// A hold names the entry it was taken on. Once every creator of a file has finished, the entry
// and its holds go; if the file is later written afresh (its delete-at-height removed it and a
// new block carried the same run), a new entry starts. A stale hold given back must not take a
// hold from the new entry, or the new adopter is left unprotected against the new creator.
func TestSubtreeAdoptions_AStaleReleaseDoesNotTakeANewHold(t *testing.T) {
	ctx := context.Background()
	store := exclusiveFileStore(t)
	reg := newSubtreeAdoptions()

	key := writtenSubtree{Hash: chainhash.Hash{9}, FileType: fileformat.FileTypeSubtreeMeta}

	// Generation one: A creates, B holds, A finishes.
	reg.attempt(key)
	reg.created(key)
	reg.attempt(key)

	stale, held := reg.adopted(key)
	require.True(t, held)

	reg.finished(key)
	require.Zero(t, reg.size())

	// Generation two: C creates the file afresh, D holds against C.
	require.NoError(t, store.Set(ctx, key.Hash[:], key.FileType, []byte("written again by C"), options.WithDeleteAt(100)))
	reg.attempt(key)
	reg.created(key)
	reg.attempt(key)

	_, held = reg.adopted(key)
	require.True(t, held)

	// B fails long after and gives back its generation-one hold.
	reg.release(key, stale)

	left, err := reg.deleteUnlessAdopted(ctx, store, key) // C fails
	require.NoError(t, err)
	require.True(t, left, "D's hold must survive B's stale release")
}

// Writers of one subtree run their whole conversion at once, as racing copies and blocks
// carrying the same run do: each emits, then either commits (its writer finishes) or fails (its
// cleanup runs), in any order. Whatever the interleaving, a block that committed must find
// every subtree file on disk. Run with -race -count=300 -cpu 1,2,8.
func TestSubtreeWriter_ACommittedBlockKeepsItsFilesUnderAnyInterleaving(t *testing.T) {
	ctx := context.Background()
	base := exclusiveFileStore(t)
	reg := newSubtreeAdoptions()

	const writers = 6

	st, _, _ := oneSubtree(t, 8)
	root := st.RootHash()

	// Each writer serialises its own copy, as each conversion builds its own subtree.
	type built struct {
		st   *subtreepkg.Subtree
		data *subtreepkg.Data
		meta *subtreepkg.Meta
	}

	copies := make([]built, writers)
	for i := range copies {
		copies[i].st, copies[i].data, copies[i].meta = oneSubtree(t, 8)
	}

	ws := make([]*subtreeWriter, writers)
	for i := range ws {
		ws[i] = newSubtreeWriter(ulogger.TestLogger{}, settings.NewSettings(), &pausingStore{Store: base}, uint32(800000+i)) //nolint:gosec // small test index
		ws[i].adoptions = reg
	}

	var wg sync.WaitGroup

	for i := range ws {
		wg.Add(1)

		go func(w *subtreeWriter, b built, commit bool) {
			defer wg.Done()

			if err := w.Emit(ctx)(0, b.st, b.data, b.meta); err != nil {
				t.Errorf("emit: %v", err)

				return
			}

			if commit {
				w.finish()
			} else if err := w.DeleteAll(ctx); err != nil {
				t.Errorf("cleanup: %v", err)
			}
		}(ws[i], copies[i], i%2 == 0)
	}

	wg.Wait()

	for _, ft := range subtreeArtefactTypes {
		exists, err := base.Exists(ctx, root[:], ft)
		require.NoError(t, err)
		require.True(t, exists, "a committed block's %s must still be on disk", ft)
	}

	require.Zero(t, reg.size(), "every conversion is over, so nothing is tracked")
}
