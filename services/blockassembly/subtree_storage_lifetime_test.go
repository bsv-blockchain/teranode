package blockassembly

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/util/testutil"
	"github.com/stretchr/testify/require"
)

type snapshotBlockingBlobStore struct {
	blob.Store
	started chan struct{}
	proceed chan struct{}
}

func (s *snapshotBlockingBlobStore) Set(ctx context.Context, key []byte, kind fileformat.FileType, value []byte, opts ...options.FileOption) error {
	close(s.started)
	select {
	case <-s.proceed:
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.Store.Set(ctx, key, kind, value, opts...)
}

func newRetiredStorageRequest(t *testing.T) (*BlockAssembly, subtreeprocessor.NewSubtreeRequest, string) {
	t.Helper()
	common := testutil.NewCommonTestSetup(t)
	common.Settings.BlockAssembly.InitialMerkleItemsPerSubtree = 2
	common.Settings.BlockAssembly.StoreTxInpointsForSubtreeMeta = false
	directory := t.TempDir()
	blockchainClient := testutil.NewMemorySQLiteBlockchainClient(common.Logger, common.Settings, t)
	utxoStore := testutil.NewSQLiteMemoryUTXOStore(common.Ctx, common.Logger, common.Settings, t)
	subtreeStore := testutil.NewMemoryBlobStore()
	requests := make(chan subtreeprocessor.NewSubtreeRequest, 1)
	processor, err := subtreeprocessor.NewSubtreeProcessor(common.Ctx, common.Logger, common.Settings, subtreeStore, blockchainClient, utxoStore, requests, subtreeprocessor.WithMmapDir(directory))
	require.NoError(t, err)
	require.NoError(t, processor.AddDirectly(&subtreepkg.Node{Hash: chainhash.HashH([]byte("storage snapshot")), Fee: 1, SizeInBytes: 100}, &subtreepkg.TxInpoints{}, true))
	queued := <-requests
	request, retained := queued.TakeStorageOwnership()
	require.True(t, retained)
	// The processor may retire its storage while the asynchronous writer still
	// needs it. The request must now be its sole owner.
	processor.Stop(context.Background())
	request.ErrChan <- nil
	server := New(common.Logger, common.Settings, nil, utxoStore, subtreeStore, blockchainClient)
	server.blockAssembler = &BlockAssembler{utxoStore: utxoStore}
	t.Cleanup(func() { request.Release(); require.NoError(t, server.Stop(context.Background())) })
	return server, request, directory
}

func TestSubtreeStorageRetainsMmapThroughCompletionCallback(t *testing.T) {
	server, request, directory := newRetiredStorageRequest(t)
	blocking := &snapshotBlockingBlobStore{Store: server.subtreeStore, started: make(chan struct{}), proceed: make(chan struct{})}
	server.subtreeStore = blocking
	callbackEntered := make(chan struct{})
	callbackFinish := make(chan struct{})
	var releaseCallback sync.Once
	t.Cleanup(func() { releaseCallback.Do(func() { close(callbackFinish) }) })
	request.OnStorageComplete = func() {
		close(callbackEntered)
		<-callbackFinish
	}
	_, allDone, err := server.storeSubtreeData(t.Context(), request, make(chan *subtreeRetrySend, 1))
	require.NoError(t, err)
	select {
	case <-blocking.started:
	case <-time.After(time.Second):
		t.Fatal("storage did not start")
	}
	files, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.Len(t, files, 1, "background storage must keep retired nodes mapped")
	close(blocking.proceed)
	select {
	case <-callbackEntered:
	case <-time.After(time.Second):
		t.Fatal("storage callback did not start")
	}
	files, err = os.ReadDir(directory)
	require.NoError(t, err)
	require.Len(t, files, 1, "completion callback must finish before storage is unmapped")
	require.Equal(t, subtreepkg.CoinbasePlaceholderHashValue, request.Subtree.Nodes[0].Hash)
	releaseCallback.Do(func() { close(callbackFinish) })
	select {
	case <-allDone:
	case <-time.After(time.Second):
		t.Fatal("storage did not finish")
	}
	files, err = os.ReadDir(directory)
	require.NoError(t, err)
	require.Empty(t, files, "finished storage must release the retired mapping")
}

func TestSubtreeStorageEarlyReturnReleasesMmap(t *testing.T) {
	server, request, directory := newRetiredStorageRequest(t)
	bytes, err := request.Subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, server.subtreeStore.Set(t.Context(), request.Subtree.RootHash()[:], fileformat.FileTypeSubtree, bytes))
	_, allDone, err := server.storeSubtreeData(t.Context(), request, make(chan *subtreeRetrySend, 1))
	require.Error(t, err, "already-stored subtree follows the synchronous early return")
	require.Nil(t, allDone)
	files, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.Empty(t, files, "early return must release storage ownership")
}
