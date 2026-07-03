package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newBlobDeletionTestStore spins up a fresh in-memory SQL store for blob-deletion tests.
func newBlobDeletionTestStore(t *testing.T) *SQL {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	return s
}

func TestScheduleAndGetPendingBlobDeletions(t *testing.T) {
	s := newBlobDeletionTestStore(t)
	ctx := context.Background()

	// Schedule three deletions at ascending heights.
	id100, err := s.ScheduleBlobDeletion(ctx, &ScheduleRequest{BlobKey: []byte("a"), FileType: "subtree", StoreType: 1, DeleteAtHeight: 100})
	require.NoError(t, err)
	require.NotZero(t, id100)

	id200, err := s.ScheduleBlobDeletion(ctx, &ScheduleRequest{BlobKey: []byte("b"), FileType: "subtree", StoreType: 1, DeleteAtHeight: 200})
	require.NoError(t, err)

	_, err = s.ScheduleBlobDeletion(ctx, &ScheduleRequest{BlobKey: []byte("c"), FileType: "block", StoreType: 2, DeleteAtHeight: 300})
	require.NoError(t, err)

	// Only deletions with delete_at_height <= 200 are pending at height 200.
	pending, err := s.GetPendingBlobDeletions(ctx, 200, 10)
	require.NoError(t, err)
	require.Len(t, pending, 2)
	// Ordered by delete_at_height ASC.
	assert.Equal(t, id100, pending[0].ID)
	assert.Equal(t, []byte("a"), pending[0].BlobKey)
	assert.Equal(t, "subtree", pending[0].FileType)
	assert.Equal(t, int32(1), pending[0].StoreType)
	assert.Equal(t, uint32(100), pending[0].DeleteAtHeight)
	assert.Equal(t, id200, pending[1].ID)

	// The limit is honoured.
	limited, err := s.GetPendingBlobDeletions(ctx, 300, 1)
	require.NoError(t, err)
	require.Len(t, limited, 1)
	assert.Equal(t, id100, limited[0].ID)

	// Nothing is due below the earliest height.
	none, err := s.GetPendingBlobDeletions(ctx, 50, 10)
	require.NoError(t, err)
	assert.Empty(t, none)
}

func TestScheduleBlobDeletionUpsert(t *testing.T) {
	s := newBlobDeletionTestStore(t)
	ctx := context.Background()

	req := &ScheduleRequest{BlobKey: []byte("dup"), FileType: "subtree", StoreType: 1, DeleteAtHeight: 100}
	id1, err := s.ScheduleBlobDeletion(ctx, req)
	require.NoError(t, err)

	// Bump the retry count so we can prove the upsert resets it.
	_, _, err = s.IncrementBlobDeletionRetry(ctx, id1, 5)
	require.NoError(t, err)

	// Re-schedule the same (blob_key, file_type, store_type) with a new height.
	req.DeleteAtHeight = 500
	id2, err := s.ScheduleBlobDeletion(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, id1, id2, "upsert must return the existing row id")

	list, total, err := s.ListScheduledBlobDeletions(ctx, &ListFilters{Limit: 10})
	require.NoError(t, err)
	require.Equal(t, 1, total, "upsert must not create a second row")
	require.Len(t, list, 1)
	assert.Equal(t, uint32(500), list[0].DeleteAtHeight, "height should be updated")
	assert.Equal(t, 0, list[0].RetryCount, "retry_count should be reset to 0")
}

func TestCancelBlobDeletion(t *testing.T) {
	s := newBlobDeletionTestStore(t)
	ctx := context.Background()

	_, err := s.ScheduleBlobDeletion(ctx, &ScheduleRequest{BlobKey: []byte("x"), FileType: "subtree", StoreType: 1, DeleteAtHeight: 100})
	require.NoError(t, err)

	// Cancelling an existing scheduled deletion succeeds.
	err = s.CancelBlobDeletion(ctx, []byte("x"), "subtree", 1)
	require.NoError(t, err)

	// It is gone afterwards.
	pending, err := s.GetPendingBlobDeletions(ctx, 100, 10)
	require.NoError(t, err)
	assert.Empty(t, pending)

	// Cancelling a non-existent deletion returns a not-found error.
	err = s.CancelBlobDeletion(ctx, []byte("missing"), "subtree", 1)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errors.ErrNotFound), "expected not-found error, got %v", err)
}

func TestRemoveBlobDeletion(t *testing.T) {
	s := newBlobDeletionTestStore(t)
	ctx := context.Background()

	id, err := s.ScheduleBlobDeletion(ctx, &ScheduleRequest{BlobKey: []byte("r"), FileType: "subtree", StoreType: 1, DeleteAtHeight: 100})
	require.NoError(t, err)

	require.NoError(t, s.RemoveBlobDeletion(ctx, id))

	pending, err := s.GetPendingBlobDeletions(ctx, 100, 10)
	require.NoError(t, err)
	assert.Empty(t, pending)

	// Removing a non-existent id is a no-op, not an error.
	require.NoError(t, s.RemoveBlobDeletion(ctx, 999999))
}

func TestIncrementBlobDeletionRetry(t *testing.T) {
	s := newBlobDeletionTestStore(t)
	ctx := context.Background()

	id, err := s.ScheduleBlobDeletion(ctx, &ScheduleRequest{BlobKey: []byte("y"), FileType: "subtree", StoreType: 1, DeleteAtHeight: 100})
	require.NoError(t, err)

	// First retry: below the max, should not signal removal.
	shouldRemove, count, err := s.IncrementBlobDeletionRetry(ctx, id, 2)
	require.NoError(t, err)
	assert.False(t, shouldRemove)
	assert.Equal(t, 1, count)

	// Second retry: reaches the max, should signal removal.
	shouldRemove, count, err = s.IncrementBlobDeletionRetry(ctx, id, 2)
	require.NoError(t, err)
	assert.True(t, shouldRemove)
	assert.Equal(t, 2, count)
}

func TestListScheduledBlobDeletionsFilters(t *testing.T) {
	s := newBlobDeletionTestStore(t)
	ctx := context.Background()

	// Seed a spread of heights and store types.
	seed := []*ScheduleRequest{
		{BlobKey: []byte("h100"), FileType: "subtree", StoreType: 1, DeleteAtHeight: 100},
		{BlobKey: []byte("h200"), FileType: "subtree", StoreType: 1, DeleteAtHeight: 200},
		{BlobKey: []byte("h300"), FileType: "block", StoreType: 2, DeleteAtHeight: 300},
		{BlobKey: []byte("h400"), FileType: "block", StoreType: 2, DeleteAtHeight: 400},
	}
	for _, r := range seed {
		_, err := s.ScheduleBlobDeletion(ctx, r)
		require.NoError(t, err)
	}

	// No filters: everything, ordered by height.
	all, total, err := s.ListScheduledBlobDeletions(ctx, &ListFilters{Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, 4, total)
	require.Len(t, all, 4)
	assert.Equal(t, uint32(100), all[0].DeleteAtHeight)

	// Height window filter.
	windowed, total, err := s.ListScheduledBlobDeletions(ctx, &ListFilters{MinHeight: 200, MaxHeight: 300, Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	require.Len(t, windowed, 2)
	assert.Equal(t, uint32(200), windowed[0].DeleteAtHeight)
	assert.Equal(t, uint32(300), windowed[1].DeleteAtHeight)

	// Store-type filter.
	byStore, total, err := s.ListScheduledBlobDeletions(ctx, &ListFilters{FilterByStore: true, StoreType: 2, Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	require.Len(t, byStore, 2)
	for _, d := range byStore {
		assert.Equal(t, int32(2), d.StoreType)
	}

	// Limit + offset paginate over the ordered set (total still reflects the full match).
	page, total, err := s.ListScheduledBlobDeletions(ctx, &ListFilters{Limit: 2, Offset: 2})
	require.NoError(t, err)
	assert.Equal(t, 4, total)
	require.Len(t, page, 2)
	assert.Equal(t, uint32(300), page[0].DeleteAtHeight)
	assert.Equal(t, uint32(400), page[1].DeleteAtHeight)
}

func TestCompleteBlobDeletions(t *testing.T) {
	s := newBlobDeletionTestStore(t)
	ctx := context.Background()

	schedule := func(key string, height uint32) int64 {
		id, err := s.ScheduleBlobDeletion(ctx, &ScheduleRequest{BlobKey: []byte(key), FileType: "subtree", StoreType: 1, DeleteAtHeight: height})
		require.NoError(t, err)
		return id
	}

	// Empty input is a no-op.
	removed, retried, err := s.CompleteBlobDeletions(ctx, nil, nil, 3)
	require.NoError(t, err)
	assert.Equal(t, 0, removed)
	assert.Equal(t, 0, retried)

	completed1 := schedule("c1", 100)
	completed2 := schedule("c2", 100)
	failedRetry := schedule("f1", 100)  // below max -> retry incremented
	failedRemove := schedule("f2", 100) // at max -> removed

	// Push failedRemove to one below its max so this completion pass trips it over.
	_, _, err = s.IncrementBlobDeletionRetry(ctx, failedRemove, 2)
	require.NoError(t, err)

	removed, retried, err = s.CompleteBlobDeletions(ctx, []int64{completed1, completed2}, []int64{failedRetry, failedRemove}, 2)
	require.NoError(t, err)
	assert.Equal(t, 3, removed, "2 completed + 1 failed-at-max removed")
	assert.Equal(t, 1, retried, "1 failed below max retried")

	// Only the retried row remains.
	list, total, err := s.ListScheduledBlobDeletions(ctx, &ListFilters{Limit: 10})
	require.NoError(t, err)
	require.Equal(t, 1, total)
	require.Len(t, list, 1)
	assert.Equal(t, []byte("f1"), list[0].BlobKey)
	assert.Equal(t, 1, list[0].RetryCount)
}

func TestAcquireBlobDeletionBatch(t *testing.T) {
	s := newBlobDeletionTestStore(t)
	ctx := context.Background()

	for _, h := range []uint32{100, 150, 250} {
		_, err := s.ScheduleBlobDeletion(ctx, &ScheduleRequest{BlobKey: []byte{byte(h)}, FileType: "subtree", StoreType: 1, DeleteAtHeight: h})
		require.NoError(t, err)
	}

	// Acquire everything due at or below height 200, ordered, honouring the limit.
	batch, err := s.AcquireBlobDeletionBatch(ctx, 200, 10, 30)
	require.NoError(t, err)
	require.Len(t, batch, 2)
	assert.Equal(t, uint32(100), batch[0].DeleteAtHeight)
	assert.Equal(t, uint32(150), batch[1].DeleteAtHeight)

	// Nothing due below the earliest height.
	empty, err := s.AcquireBlobDeletionBatch(ctx, 50, 10, 30)
	require.NoError(t, err)
	assert.Empty(t, empty)
}
