package model

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// mockSubtreeStoreWriter implements SubtreeStoreWriter for testing
type mockSubtreeStoreWriter struct {
	storedMeta  map[string][]byte
	subtreeData map[string][]byte
	getErr      error
	setErr      error
}

func newMockSubtreeStoreWriter() *mockSubtreeStoreWriter {
	return &mockSubtreeStoreWriter{
		storedMeta:  make(map[string][]byte),
		subtreeData: make(map[string][]byte),
	}
}

func (m *mockSubtreeStoreWriter) GetIoReader(_ context.Context, key []byte, fileType fileformat.FileType, _ ...options.FileOption) (io.ReadCloser, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}

	keyStr := string(key) + "." + string(fileType)
	if data, ok := m.subtreeData[keyStr]; ok {
		return io.NopCloser(newBytesReader(data)), nil
	}
	return nil, errors.NewNotFoundError("not found")
}

func (m *mockSubtreeStoreWriter) Set(_ context.Context, key []byte, fileType fileformat.FileType, value []byte, _ ...options.FileOption) error {
	if m.setErr != nil {
		return m.setErr
	}
	m.storedMeta[string(key)+"."+string(fileType)] = value
	return nil
}

type bytesReader struct {
	data   []byte
	offset int
}

func newBytesReader(data []byte) *bytesReader {
	return &bytesReader{data: data}
}

func (r *bytesReader) Read(p []byte) (n int, err error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}

// createTestSubtree creates a simple subtree for testing
func createTestSubtree(txHashes []chainhash.Hash) *subtreepkg.Subtree {
	nodes := make([]subtreepkg.Node, len(txHashes)+1)
	// First node is coinbase placeholder
	nodes[0] = subtreepkg.Node{Hash: subtreepkg.CoinbasePlaceholderHashValue}
	for i, h := range txHashes {
		nodes[i+1] = subtreepkg.Node{Hash: h}
	}
	return &subtreepkg.Subtree{Nodes: nodes}
}

// createTestTransaction creates a simple transaction for testing
func createTestTransaction(t *testing.T, prevTxIDHex string, prevVout uint32) *bt.Tx {
	t.Helper()

	prevTxID, err := chainhash.NewHashFromStr(prevTxIDHex)
	require.NoError(t, err)

	tx := bt.NewTx()
	tx.Inputs = []*bt.Input{{
		UnlockingScript:    &bscript.Script{},
		PreviousTxOutIndex: prevVout,
	}}
	err = tx.Inputs[0].PreviousTxIDAdd(prevTxID)
	require.NoError(t, err)

	err = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000)
	require.NoError(t, err)

	return tx
}

func TestSubtreeMetaRegenerator_RegenerateMeta_FromLocal(t *testing.T) {
	// Create test transactions
	prevTxID1 := "0000000000000000000000000000000000000000000000000000000000000001"
	prevTxID2 := "0000000000000000000000000000000000000000000000000000000000000002"

	tx1 := createTestTransaction(t, prevTxID1, 0)
	tx2 := createTestTransaction(t, prevTxID2, 1)

	txHash1 := *tx1.TxIDChainHash()
	txHash2 := *tx2.TxIDChainHash()

	// Create subtree with the transaction hashes
	subtree := createTestSubtree([]chainhash.Hash{txHash1, txHash2})
	subtreeHash := subtree.RootHash()

	// Create subtree data containing the transactions
	subtreeData := subtreepkg.NewSubtreeData(subtree)
	subtreeData.Txs[1] = tx1
	subtreeData.Txs[2] = tx2

	// Serialize subtree data for the mock store
	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)

	// Setup mock store with subtree data
	mockStore := newMockSubtreeStoreWriter()
	mockStore.subtreeData[string(subtreeHash[:])+"."+string(fileformat.FileTypeSubtreeData)] = subtreeDataBytes

	logger := ulogger.TestLogger{}

	regenerator := NewSubtreeMetaRegenerator(logger, mockStore, nil, "")

	// Test regeneration
	meta, err := regenerator.RegenerateMeta(context.Background(), subtreeHash, subtree)

	require.NoError(t, err)
	require.NotNil(t, meta)

	// Verify meta contains correct inpoints
	inpoints1, err := meta.GetTxInpoints(1)
	require.NoError(t, err)
	require.NotNil(t, inpoints1)

	inpoints2, err := meta.GetTxInpoints(2)
	require.NoError(t, err)
	require.NotNil(t, inpoints2)

	// Verify meta was stored
	require.Len(t, mockStore.storedMeta, 1)
}

func TestSubtreeMetaRegenerator_RegenerateMeta_FromPeer(t *testing.T) {
	// Create test transactions
	prevTxID1 := "0000000000000000000000000000000000000000000000000000000000000001"
	prevTxID2 := "0000000000000000000000000000000000000000000000000000000000000002"

	tx1 := createTestTransaction(t, prevTxID1, 0)
	tx2 := createTestTransaction(t, prevTxID2, 1)

	txHash1 := *tx1.TxIDChainHash()
	txHash2 := *tx2.TxIDChainHash()

	// Create subtree with the transaction hashes
	subtree := createTestSubtree([]chainhash.Hash{txHash1, txHash2})
	subtreeHash := subtree.RootHash()

	// Create subtree data containing the transactions
	subtreeData := subtreepkg.NewSubtreeData(subtree)
	subtreeData.Txs[1] = tx1
	subtreeData.Txs[2] = tx2

	// Serialize subtree data for HTTP response
	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)

	// Create mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expectedPath := "/api/v1/subtree_data/" + subtreeHash.String()
		if r.URL.Path == expectedPath {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(subtreeDataBytes)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	// Setup mock store without local subtree data (so it falls back to peer)
	mockStore := newMockSubtreeStoreWriter()
	logger := ulogger.TestLogger{}

	regenerator := NewSubtreeMetaRegenerator(logger, mockStore, []string{server.URL}, "/api/v1")

	// Test regeneration
	meta, err := regenerator.RegenerateMeta(context.Background(), subtreeHash, subtree)

	require.NoError(t, err)
	require.NotNil(t, meta)

	// Verify meta contains correct inpoints
	inpoints1, err := meta.GetTxInpoints(1)
	require.NoError(t, err)
	require.NotNil(t, inpoints1)

	inpoints2, err := meta.GetTxInpoints(2)
	require.NoError(t, err)
	require.NotNil(t, inpoints2)

	// Verify meta was stored
	require.Len(t, mockStore.storedMeta, 1)
}

func TestSubtreeMetaRegenerator_RegenerateMeta_AllSourcesFail(t *testing.T) {
	tx1 := createTestTransaction(t, "0000000000000000000000000000000000000000000000000000000000000001", 0)
	txHash1 := *tx1.TxIDChainHash()

	subtree := createTestSubtree([]chainhash.Hash{txHash1})
	subtreeHash := subtree.RootHash()

	// Create mock HTTP server that always returns 404
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	// Setup mock store without subtree data
	mockStore := newMockSubtreeStoreWriter()
	logger := ulogger.TestLogger{}

	regenerator := NewSubtreeMetaRegenerator(logger, mockStore, []string{server.URL}, "/api/v1")

	// Test regeneration should fail
	meta, err := regenerator.RegenerateMeta(context.Background(), subtreeHash, subtree)

	require.Error(t, err)
	require.Nil(t, meta)
	require.Contains(t, err.Error(), "subtreedata not available locally or from peers")
}

func TestSubtreeMetaRegenerator_RegenerateMeta_NilStore_PeerFallback(t *testing.T) {
	// Create test transaction
	prevTxID1 := "0000000000000000000000000000000000000000000000000000000000000001"
	tx1 := createTestTransaction(t, prevTxID1, 0)
	txHash1 := *tx1.TxIDChainHash()

	// Create subtree
	subtree := createTestSubtree([]chainhash.Hash{txHash1})
	subtreeHash := subtree.RootHash()

	// Create subtree data
	subtreeData := subtreepkg.NewSubtreeData(subtree)
	subtreeData.Txs[1] = tx1
	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)

	// Create mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(subtreeDataBytes)
	}))
	defer server.Close()

	logger := ulogger.TestLogger{}

	// Create regenerator with nil store - should still work via peer
	regenerator := NewSubtreeMetaRegenerator(logger, nil, []string{server.URL}, "/api/v1")

	meta, err := regenerator.RegenerateMeta(context.Background(), subtreeHash, subtree)

	require.NoError(t, err)
	require.NotNil(t, meta)
}

func TestSubtreeMetaRegenerator_StoreRegeneratedMeta_Success(t *testing.T) {
	mockStore := newMockSubtreeStoreWriter()
	logger := ulogger.TestLogger{}

	regenerator := &SubtreeMetaRegenerator{
		logger:       logger,
		subtreeStore: mockStore,
	}

	// Create a simple subtree and meta
	hash1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	subtree := &subtreepkg.Subtree{
		Nodes: []subtreepkg.Node{
			{Hash: subtreepkg.CoinbasePlaceholderHashValue},
			{Hash: *hash1},
		},
	}
	subtreeHash := subtree.RootHash()

	meta := subtreepkg.NewSubtreeMeta(subtree)
	// Initialize TxInpoints for non-coinbase nodes to make serialization happy
	// The first node (coinbase placeholder) is at index 0, so we need to set index 1
	meta.TxInpoints[1] = subtreepkg.TxInpoints{
		ParentTxHashes: []chainhash.Hash{},
	}

	regenerator.storeRegeneratedMeta(context.Background(), subtreeHash, meta)

	// Verify meta was stored
	require.Len(t, mockStore.storedMeta, 1)
}

func TestSubtreeMetaRegenerator_StoreRegeneratedMeta_NilStore(t *testing.T) {
	logger := ulogger.TestLogger{}

	regenerator := &SubtreeMetaRegenerator{
		logger:       logger,
		subtreeStore: nil, // No store
	}

	hash1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	subtree := &subtreepkg.Subtree{
		Nodes: []subtreepkg.Node{
			{Hash: subtreepkg.CoinbasePlaceholderHashValue},
			{Hash: *hash1},
		},
	}
	subtreeHash := subtree.RootHash()

	meta := subtreepkg.NewSubtreeMeta(subtree)
	meta.TxInpoints[1] = subtreepkg.TxInpoints{}

	// Should not panic with nil store
	regenerator.storeRegeneratedMeta(context.Background(), subtreeHash, meta)
}

func TestSubtreeStoreAdapter(t *testing.T) {
	// Create a mock SubtreeStore
	mockStore := NewLocalSubtreeStore()

	adapter := &SubtreeStoreAdapter{SubtreeStore: mockStore}

	// Test Set (should be no-op)
	err := adapter.Set(context.Background(), []byte("key"), fileformat.FileTypeSubtreeMeta, []byte("value"))
	require.NoError(t, err)

	// Verify nothing was stored (adapter's Set is a no-op)
	require.Empty(t, mockStore.FileData)
}
