package netsync

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/stretchr/testify/require"
)

// slowDelStore is a real in-memory blob store whose Del waits until the test lets it go, as a
// file-store Del does while it waits up to 25 s for a write permit.
type slowDelStore struct {
	blob.Store

	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newSlowDelStore() *slowDelStore {
	return &slowDelStore{Store: memory.New(), entered: make(chan struct{}), release: make(chan struct{})}
}

func (s *slowDelStore) Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
	s.once.Do(func() { close(s.entered) })
	<-s.release

	return s.Store.Del(ctx, key, fileType, opts...)
}

// returnsWithin reports whether fn returns within d.
func returnsWithin(d time.Duration, fn func()) bool {
	done := make(chan struct{})

	go func() {
		fn()
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// A slow delete of one subtree file held the adoption registry's lock for as long as the store
// took, so every writer of every other file waited on it (review of 76be891d6). The delete now
// runs outside the lock: another file's attempt and finish go straight through, while an attempt
// on the file being deleted waits for the delete to end, so it can never find the file in the
// unlink window and adopt it.
func TestSubtreeAdoptions_ASlowDeleteHoldsUpOnlyItsOwnFile(t *testing.T) {
	ctx := context.Background()
	store := newSlowDelStore()
	reg := newSubtreeAdoptions()

	doomed := writtenSubtree{Hash: chainhash.Hash{0x01}, FileType: fileformat.FileTypeSubtree}
	other := writtenSubtree{Hash: chainhash.Hash{0x02}, FileType: fileformat.FileTypeSubtree}

	require.NoError(t, store.Store.Set(ctx, doomed.Hash[:], doomed.FileType, []byte("doomed")))

	deleted := make(chan error, 1)

	go func() {
		_, err := reg.deleteUnlessAdopted(ctx, store, doomed)
		deleted <- err
	}()

	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the delete never reached the store")
	}

	require.True(t, returnsWithin(time.Second, func() {
		reg.attempt(other)
		reg.created(other)
		reg.finished(other)
	}), "a writer of another file must not wait for a slow delete")

	attempted := make(chan struct{})

	go func() {
		reg.attempt(doomed)
		close(attempted)
	}()

	select {
	case <-attempted:
		close(store.release)
		t.Fatal("a writer of the file being deleted must wait for the delete, or it can adopt a file that is about to go")
	case <-time.After(200 * time.Millisecond):
	}

	close(store.release)
	require.NoError(t, <-deleted)

	select {
	case <-attempted:
	case <-time.After(5 * time.Second):
		t.Fatal("the writer never started once the delete ended")
	}

	exists, err := store.Exists(ctx, doomed.Hash[:], doomed.FileType)
	require.NoError(t, err)
	require.False(t, exists, "the writer starts after the delete, so it finds no file and creates its own")

	reg.abandoned(doomed)
	require.Zero(t, reg.size(), "nothing is left in the registry")
}

// settleStale finished its writers while it held settlingMu, and finish takes the registry's
// lock, so anything holding the registry stalled every read loop's settle with it (review of
// 76be891d6). The stale writers are now finished after settlingMu is released.
func TestSettleStale_FinishesItsWritersOutsideTheSettlingLock(t *testing.T) {
	sm := &SyncManager{}
	reg := sm.subtreeAdoptionsFor()

	key := writtenSubtree{Hash: chainhash.Hash{0x03}, FileType: fileformat.FileTypeSubtree}

	w := &subtreeWriter{adoptions: reg}
	reg.attempt(key)
	w.created(key)

	stale := chainhash.Hash{0x04}
	sm.holdUntilSettled(stale, w)

	// Something holds the registry: the slow delete above, before this change.
	reg.mu.Lock()

	settled := make(chan struct{})

	go func() {
		sm.settleStale(time.Now().Add(2 * settleBackstop))
		close(settled)
	}()

	require.Eventually(t, func() bool { return blockedIn("netsync.(*subtreeAdoptions).finished") }, 5*time.Second, time.Millisecond,
		"the sweep reaches the stale writer's finish")

	ok := returnsWithin(time.Second, func() { sm.acceptancePending(stale) })

	reg.mu.Unlock()
	<-settled

	require.True(t, ok, "a read loop's settle must not wait while the sweep finishes a stale writer")
	require.Zero(t, reg.size(), "the stale writer's window is closed")
}
