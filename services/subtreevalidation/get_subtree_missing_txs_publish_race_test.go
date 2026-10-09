package subtreevalidation

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blobfile "github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/require"
)

// Set is the whole-value write getSubtreeMissingTxs uses. (*blobfile.File).Set calls the
// embedded File's SetFromReader, not the decorator's, so the hook has to be attached here.
// The competitor publishes first, after the caller's Exists check and before the store's
// pre-write check, which then refuses this write: the refusal a caller of the file store
// gets without options.WithExclusivePublish.
func (s *publishRaceStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	s.compete(key, fileType)

	return s.File.Set(ctx, key, fileType, value, opts...)
}

// TestGetSubtreeMissingTxs_ALostPublishRaceReadsTheOtherWritersFile: the subtreeData file
// is absent at the Exists check, the whole file is fetched from the peer, and while this
// call stores it another writer publishes the same key. The file store refuses this Set at
// its pre-write check with ErrBlobAlreadyExists. The file on disk holds the subtree's transactions, so
// the missing transactions are read from it and not fetched one by one over HTTP.
func TestGetSubtreeMissingTxs_ALostPublishRaceReadsTheOtherWritersFile(t *testing.T) {
	txMetaStore, validatorClient, _, _, blockchainClient, deferFunc := setup(t)
	defer deferFunc()

	storeURL, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	plain, err := blobfile.New(ulogger.TestLogger{}, storeURL, options.WithBlobDeletionScheduler(noopDeletionScheduler{}))
	require.NoError(t, err)

	subtree, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddNode(*hash1, 121, 0))
	require.NoError(t, subtree.AddNode(*hash2, 122, 0))
	require.NoError(t, subtree.AddNode(*hash3, 123, 0))
	require.NoError(t, subtree.AddNode(*hash4, 123, 0))

	subtreeDataBytes := bytes.Join([][]byte{tx1.ExtendedBytes(), tx2.ExtendedBytes(), tx3.ExtendedBytes(), tx4.ExtendedBytes()}, nil)
	allTxs := []chainhash.Hash{*hash1, *hash2, *hash3, *hash4}

	// Two of four missing is 50%, above the 20% threshold, so the whole file is fetched.
	unresolved := []utxo.UnresolvedMetaData{{Hash: *hash1}, {Hash: *hash2}}

	httpmock.RegisterResponder("GET",
		fmt.Sprintf("%s/subtree_data/%s", testPeerURL, subtree.RootHash().String()),
		httpmock.NewBytesResponder(http.StatusOK, subtreeDataBytes))

	// The per-transaction fallback. It answers correctly so that, if it is reached, the only
	// thing that fails is the assertion that it was never reached. An exact URL beats the
	// regexp responder setup registered.
	var perTxCalls atomic.Int32

	httpmock.RegisterResponder("POST",
		fmt.Sprintf("%s/subtree/%s/txs", testPeerURL, subtree.RootHash().String()),
		func(*http.Request) (*http.Response, error) {
			perTxCalls.Add(1)

			return httpmock.NewBytesResponse(http.StatusOK, bytes.Join([][]byte{tx1.ExtendedBytes(), tx2.ExtendedBytes()}, nil)), nil
		})

	// The hook runs on the storing goroutine, so it records and the test asserts.
	var competeErr error

	store := &publishRaceStore{File: plain}
	store.compete = func(key []byte, fileType fileformat.FileType) {
		competeErr = plain.Set(context.Background(), key, fileType, subtreeDataBytes, options.WithDeleteAt(100))
	}

	s := &Server{
		logger:           ulogger.TestLogger{},
		settings:         test.CreateBaseTestSettings(t),
		utxoStore:        txMetaStore,
		subtreeStore:     store,
		validatorClient:  validatorClient,
		blockchainClient: blockchainClient,
	}

	missingTxs, err := s.getSubtreeMissingTxs(context.Background(), *subtree.RootHash(), subtree, unresolved, allTxs, testPeerURL, "")
	require.NoError(t, competeErr, "the other writer's publish must succeed")
	require.NoError(t, err, "losing the publish to a writer of the same key is not a failure")

	require.Len(t, missingTxs, 2)
	require.Equal(t, *hash1, *missingTxs[0].tx.TxIDChainHash())
	require.Equal(t, *hash2, *missingTxs[1].tx.TxIDChainHash())

	require.Zero(t, perTxCalls.Load(), "the missing txs must come from the file on disk, not one by one over HTTP")

	got, err := plain.Get(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)
	require.Equal(t, subtreeDataBytes, got, "the other writer's file stands")
}
