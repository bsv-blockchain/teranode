package blobfault

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/stretchr/testify/require"
)

var (
	keyA = []byte("key-a")
	keyB = []byte("key-b")
)

func TestStore_FailTimesThenSucceeds(t *testing.T) {
	s := New(blob_memory.New())
	s.OnSet(keyA, fileformat.FileTypeSubtree, Rule{FailTimes: 2})

	require.Error(t, s.Set(t.Context(), keyA, fileformat.FileTypeSubtree, []byte{1}))
	require.Error(t, s.Set(t.Context(), keyA, fileformat.FileTypeSubtree, []byte{1}))
	require.NoError(t, s.Set(t.Context(), keyA, fileformat.FileTypeSubtree, []byte{1}))

	exists, err := s.Exists(t.Context(), keyA, fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, exists, "the third Set must reach the inner store")
	require.Equal(t, 3, s.SetCalls(keyA, fileformat.FileTypeSubtree))
}

func TestStore_FailAlways(t *testing.T) {
	s := New(blob_memory.New())
	s.OnSet(keyA, fileformat.FileTypeSubtreeMeta, Rule{FailTimes: Always})

	for range 5 {
		require.Error(t, s.Set(t.Context(), keyA, fileformat.FileTypeSubtreeMeta, []byte{1}))
	}

	exists, err := s.Exists(t.Context(), keyA, fileformat.FileTypeSubtreeMeta)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestStore_RuleIsPerKeyAndType(t *testing.T) {
	s := New(blob_memory.New())
	s.OnSet(keyA, fileformat.FileTypeSubtree, Rule{FailTimes: Always})

	require.NoError(t, s.Set(t.Context(), keyB, fileformat.FileTypeSubtree, []byte{1}), "another key is not affected")
	require.NoError(t, s.Set(t.Context(), keyA, fileformat.FileTypeSubtreeMeta, []byte{1}), "another file type is not affected")
	require.Equal(t, 1, s.SetCalls(keyB, fileformat.FileTypeSubtree))
}

func TestStore_OnSetAnyAppliesToEveryKeyOfAType(t *testing.T) {
	s := New(blob_memory.New())
	s.OnSetAny(fileformat.FileTypeSubtree, Rule{FailTimes: Always})

	require.Error(t, s.Set(t.Context(), keyA, fileformat.FileTypeSubtree, []byte{1}))
	require.Error(t, s.Set(t.Context(), keyB, fileformat.FileTypeSubtree, []byte{1}))
	require.NoError(t, s.Set(t.Context(), keyA, fileformat.FileTypeSubtreeMeta, []byte{1}))
}

// A blocked Set models a synchronous filesystem write: cancelling its context does not end it.
func TestStore_BlockIgnoresContext(t *testing.T) {
	s := New(blob_memory.New())
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	s.OnSet(keyA, fileformat.FileTypeSubtree, Rule{Block: release, Entered: entered})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() { done <- s.Set(ctx, keyA, fileformat.FileTypeSubtree, []byte{1}) }()

	<-entered
	cancel()

	select {
	case <-done:
		t.Fatal("a blocked Set must outlive its cancelled context")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	require.NoError(t, <-done, "after release the write completes")

	exists, err := s.Exists(t.Context(), keyA, fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestStore_SetFromReaderFollowsTheSameRules(t *testing.T) {
	s := New(blob_memory.New())
	s.OnSet(keyA, fileformat.FileTypeSubtree, Rule{FailTimes: 1})

	reader := func() io.ReadCloser { return io.NopCloser(bytes.NewReader([]byte{1, 2})) }

	require.Error(t, s.SetFromReader(t.Context(), keyA, fileformat.FileTypeSubtree, reader()))
	require.NoError(t, s.SetFromReader(t.Context(), keyA, fileformat.FileTypeSubtree, reader()))
	require.Equal(t, 2, s.SetCalls(keyA, fileformat.FileTypeSubtree))
}
