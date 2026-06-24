package subtreeprocessor

import (
	"context"
	"io"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// countingBlobStore wraps a blob.Store and counts how many io.ReadCloser handles
// returned by GetIoReader are closed. Used by leak tests to detect callers that
// open a reader but never call Close — in production this drains the file-store
// global read-permit semaphore.
type countingBlobStore struct {
	blob.Store
	opens  atomic.Int64
	closes atomic.Int64
}

func newCountingBlobStore(inner blob.Store) *countingBlobStore {
	return &countingBlobStore{Store: inner}
}

func (c *countingBlobStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	rc, err := c.Store.GetIoReader(ctx, key, fileType, opts...)
	if err != nil {
		return nil, err
	}

	c.opens.Add(1)

	return &countingReadCloser{ReadCloser: rc, parent: c}, nil
}

type countingReadCloser struct {
	io.ReadCloser
	parent *countingBlobStore
	once   atomic.Bool
}

func (r *countingReadCloser) Close() error {
	if r.once.CompareAndSwap(false, true) {
		r.parent.closes.Add(1)
	}

	return r.ReadCloser.Close()
}

// TestSubtreeProcessor_getConflictingNodes_closesReader regression test for the
// production observation on teranode-mainnet-eu-1 where block-assembly drained
// the file-store read-permit semaphore. getConflictingNodes opened the subtree
// reader inside an errgroup goroutine but never closed it, leaking one permit
// per subtree per moveForwardBlock call.
func TestSubtreeProcessor_getConflictingNodes_closesReader(t *testing.T) {
	inner := blob_memory.New()
	counting := newCountingBlobStore(inner)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 4

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(t.Context(), ulogger.TestLogger{}, tSettings, utxoStoreURL)
	require.NoError(t, err)

	blockchainClient := &blockchain.Mock{}

	newSubtreeChan := make(chan NewSubtreeRequest, 1)

	stp, err := NewSubtreeProcessor(t.Context(), ulogger.TestLogger{}, tSettings, counting, blockchainClient, utxoStore, newSubtreeChan)
	require.NoError(t, err)

	s1 := createSubtree(t, 4, true)
	s1Bytes, err := s1.Serialize()
	require.NoError(t, err)
	require.NoError(t, inner.Set(t.Context(), s1.RootHash()[:], fileformat.FileTypeSubtree, s1Bytes))

	s2 := createSubtree(t, 4, false)
	s2Bytes, err := s2.Serialize()
	require.NoError(t, err)
	require.NoError(t, inner.Set(t.Context(), s2.RootHash()[:], fileformat.FileTypeSubtree, s2Bytes))

	block := &model.Block{
		Header:   prevBlockHeader,
		Subtrees: []*chainhash.Hash{s1.RootHash(), s2.RootHash()},
	}

	_, err = stp.getConflictingNodes(t.Context(), block)
	require.NoError(t, err)

	opens := counting.opens.Load()
	closes := counting.closes.Load()

	require.Greater(t, opens, int64(0), "test exercise didn't open any readers — assertion vacuous")
	require.Equal(t, opens, closes,
		"read-permit leak in getConflictingNodes: opens=%d closes=%d (every GetIoReader must close its ReadCloser)",
		opens, closes)
}

// TestSubtreeProcessor_moveBackBlockGetSubtrees_closesSubtreeMetaReader
// exercises the moveBackBlock GetSubtrees branch with
// StoreTxInpointsForSubtreeMeta enabled. The branch opens both a subtree reader
// (closed via defer) AND a subtree-meta reader. The meta reader was never
// closed, leaking permits during every reorg.
func TestSubtreeProcessor_moveBackBlockGetSubtrees_closesSubtreeMetaReader(t *testing.T) {
	inner := blob_memory.New()
	counting := newCountingBlobStore(inner)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 4
	tSettings.BlockAssembly.StoreTxInpointsForSubtreeMeta = true

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(t.Context(), ulogger.TestLogger{}, tSettings, utxoStoreURL)
	require.NoError(t, err)

	newSubtreeChan := make(chan NewSubtreeRequest, 1)

	stp, err := NewSubtreeProcessor(t.Context(), ulogger.TestLogger{}, tSettings, counting, nil, utxoStore, newSubtreeChan)
	require.NoError(t, err)

	s1 := createSubtree(t, 4, true)
	s1Bytes, err := s1.Serialize()
	require.NoError(t, err)
	require.NoError(t, inner.Set(t.Context(), s1.RootHash()[:], fileformat.FileTypeSubtree, s1Bytes))

	sm1 := createSubtreeMeta(t, s1)
	sm1Bytes, err := sm1.Serialize()
	require.NoError(t, err)
	require.NoError(t, inner.Set(t.Context(), s1.RootHash()[:], fileformat.FileTypeSubtreeMeta, sm1Bytes))

	s2 := createSubtree(t, 4, false)
	s2Bytes, err := s2.Serialize()
	require.NoError(t, err)
	require.NoError(t, inner.Set(t.Context(), s2.RootHash()[:], fileformat.FileTypeSubtree, s2Bytes))

	sm2 := createSubtreeMeta(t, s2)
	sm2Bytes, err := sm2.Serialize()
	require.NoError(t, err)
	require.NoError(t, inner.Set(t.Context(), s2.RootHash()[:], fileformat.FileTypeSubtreeMeta, sm2Bytes))

	_, _, _, err = stp.moveBackBlockGetSubtrees(t.Context(), &model.Block{
		Header:   prevBlockHeader,
		Subtrees: []*chainhash.Hash{s1.RootHash(), s2.RootHash()},
	})
	require.NoError(t, err)

	opens := counting.opens.Load()
	closes := counting.closes.Load()

	require.GreaterOrEqual(t, opens, int64(4), "expected at least 4 opens (2 subtrees × subtree+meta)")
	require.Equal(t, opens, closes,
		"read-permit leak in moveBackBlockGetSubtrees: opens=%d closes=%d", opens, closes)
}

// TestSubtreeProcessor_getSubtreeAndConflictingTransactionsMap_closesReader
// regression test for the third leak site: function fetches the subtree but
// never closes the reader.
func TestSubtreeProcessor_getSubtreeAndConflictingTransactionsMap_closesReader(t *testing.T) {
	inner := blob_memory.New()
	counting := newCountingBlobStore(inner)

	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(t.Context(), ulogger.TestLogger{}, tSettings, utxoStoreURL)
	require.NoError(t, err)

	newSubtreeChan := make(chan NewSubtreeRequest, 1)

	stp, err := NewSubtreeProcessor(t.Context(), ulogger.TestLogger{}, tSettings, counting, nil, utxoStore, newSubtreeChan)
	require.NoError(t, err)

	s, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)

	tx1 := chainhash.HashH([]byte("leak-test-tx1"))
	tx2 := chainhash.HashH([]byte("leak-test-tx2"))
	tx3 := chainhash.HashH([]byte("leak-test-tx3"))
	require.NoError(t, s.AddNode(tx1, 1, 100))
	require.NoError(t, s.AddNode(tx2, 2, 200))
	require.NoError(t, s.AddNode(tx3, 3, 300))

	sBytes, err := s.Serialize()
	require.NoError(t, err)
	require.NoError(t, inner.Set(t.Context(), s.RootHash()[:], fileformat.FileTypeSubtree, sBytes))

	_, _, err = stp.getSubtreeAndConflictingTransactionsMap(t.Context(), s.RootHash(), []chainhash.Hash{tx1, tx3})
	require.NoError(t, err)

	opens := counting.opens.Load()
	closes := counting.closes.Load()

	require.Greater(t, opens, int64(0), "test didn't exercise GetIoReader")
	require.Equal(t, opens, closes,
		"read-permit leak in getSubtreeAndConflictingTransactionsMap: opens=%d closes=%d", opens, closes)
}
