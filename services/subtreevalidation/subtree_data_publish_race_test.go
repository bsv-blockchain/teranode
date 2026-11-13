package subtreevalidation

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blobfile "github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/blob/storetypes"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// This test runs over the REAL file store (stores/blob/file) in a temporary directory,
// decorated with one hook so a second writer publishes the key at the first read of the body,
// which is after the store's pre-check and before its publish. Nothing about storage is faked.
// This writer does not ask for the file store's exclusive publish, so both writers publish and
// the later rename replaces the file, with the same bytes.

type noopDeletionScheduler struct{}

func (noopDeletionScheduler) ScheduleBlobDeletion(context.Context, []byte, string, storetypes.BlobStoreType, uint32) (int64, bool, error) {
	return 0, true, nil
}

func (noopDeletionScheduler) CancelBlobDeletion(context.Context, []byte, string, storetypes.BlobStoreType) (bool, error) {
	return true, nil
}

type publishRaceStore struct {
	*blobfile.File
	compete func(key []byte, fileType fileformat.FileType)
}

func (s *publishRaceStore) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	return s.File.SetFromReader(ctx, key, fileType, &competeOnFirstRead{ReadCloser: reader, hook: func() { s.compete(key, fileType) }}, opts...)
}

type competeOnFirstRead struct {
	io.ReadCloser
	hook func()
	once sync.Once
}

func (r *competeOnFirstRead) Read(p []byte) (int, error) {
	r.once.Do(r.hook)

	return r.ReadCloser.Read(p)
}

// TestProcessSubtreeDataStream_ARacingWriterOfTheSameSubtreeIsNotAFailure: a sibling block
// validating the same subtree publishes its subtreeData while this call is streaming. Every
// transaction has been parsed from the stream and both writers wrote the same transactions,
// so the call succeeds and the file holds them, whichever publish came last.
func TestProcessSubtreeDataStream_ARacingWriterOfTheSameSubtreeIsNotAFailure(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	storeURL, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	plain, err := blobfile.New(ulogger.TestLogger{}, storeURL, options.WithBlobDeletionScheduler(noopDeletionScheduler{}))
	require.NoError(t, err)

	tx1, err := createTestTransaction("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
	require.NoError(t, err)

	tx2, err := createTestTransaction("6359f0868171b1d194cbee1af2f16ea598ae8fad666d9b012c8ed2b79a236ec4")
	require.NoError(t, err)

	subtreeData := bytes.Join([][]byte{tx1.Bytes(), tx2.Bytes()}, nil)

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddNode(*tx1.TxIDChainHash(), 1, 1))
	require.NoError(t, subtree.AddNode(*tx2.TxIDChainHash(), 1, 2))

	// The hook runs on the storage goroutine, so it records and the test asserts.
	var competeErr error

	store := &publishRaceStore{File: plain}
	store.compete = func(key []byte, fileType fileformat.FileType) {
		competeErr = plain.Set(context.Background(), key, fileType, subtreeData, options.WithDeleteAt(100))
	}

	server.subtreeStore = store

	var allTransactions []*bt.Tx

	err = server.processSubtreeDataStream(context.Background(), subtree, io.NopCloser(bytes.NewReader(subtreeData)), &allTransactions, 100, nil)
	require.NoError(t, competeErr, "the other writer's publish must succeed")
	require.NoError(t, err, "racing a writer of the same bytes is not a failure")
	require.Len(t, allTransactions, 2, "every transaction was parsed from the stream")

	got, err := plain.Get(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)
	require.Equal(t, subtreeData, got, "the file holds the subtree's transactions")
}

// TestProcessSubtreeDataStream_ParseFailureAttribution pins who a parse failure is charged to. The
// tee hands every byte to the store before the parser sees it, so a store that refuses the key up
// front closes the pipe and fails the parse, and so does a cancelled context. Neither is the peer's
// doing, so neither may carry a peer code. Bytes the peer got wrong still do.
func TestProcessSubtreeDataStream_ParseFailureAttribution(t *testing.T) {
	tx1, err := createTestTransaction("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
	require.NoError(t, err)

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddNode(*tx1.TxIDChainHash(), 1, 1))
	require.NoError(t, subtree.AddNode(chainhash.HashH([]byte("second")), 1, 2))

	t.Run("store refused the key up front", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		require.NoError(t, server.subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData, []byte("theirs")))

		var allTransactions []*bt.Tx
		err := server.processSubtreeDataStream(context.Background(), subtree, io.NopCloser(bytes.NewReader(tx1.Bytes())), &allTransactions, 100, nil)
		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrNetworkInvalidResponse), "our store refusing the write is not the peer's fault")
	})

	t.Run("context cancelled", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		var allTransactions []*bt.Tx
		err := server.processSubtreeDataStream(ctx, subtree, io.NopCloser(&cancelledReader{ctx: ctx}), &allTransactions, 100, nil)
		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrNetworkInvalidResponse), "a cancelled context is not the peer's fault")
	})

	t.Run("peer served bad bytes", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		var allTransactions []*bt.Tx
		err := server.processSubtreeDataStream(context.Background(), subtree, io.NopCloser(bytes.NewReader([]byte{0x01, 0x02, 0x03})), &allTransactions, 100, nil)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNetworkInvalidResponse))
	})
}

// cancelledReader fails every read with its context's error, the way an HTTP body does once the
// request context is cancelled.
type cancelledReader struct {
	ctx context.Context
}

func (r *cancelledReader) Read([]byte) (int, error) {
	return 0, r.ctx.Err()
}
