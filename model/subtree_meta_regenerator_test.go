package model

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
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

// allowLoopbackHTTP disables the util HTTP client's SSRF protection for the
// duration of a test that talks to a localhost httptest server.
func allowLoopbackHTTP(t *testing.T) {
	t.Helper()
	util.SetSSRFProtection(false)
	t.Cleanup(func() { util.SetSSRFProtection(true) })
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

	regenerator := NewSubtreeMetaRegenerator(logger, mockStore, nil, func() uint32 { return 100 }, 288, 0)

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

// TestSubtreeMetaRegenerator_RegenerateMeta_FromPeer also pins the peer URL
// contract: the URL handed to the regenerator is the announcing peer's DataHub
// URL, which already ends in the API prefix (every asset_httpAddress /
// asset_httpPublicAddress form in settings.conf embeds ${asset_apiPrefix}). The
// regenerator must request <peerURL>/subtree_data/<hash> exactly like
// check_block_subtrees.go and peer_cache_bypass.go do — appending a second
// prefix 404s on every real peer, which is why the handler below serves only
// /api/v1/subtree_data/<hash> and 404s everything else.
func TestSubtreeMetaRegenerator_RegenerateMeta_FromPeer(t *testing.T) {
	allowLoopbackHTTP(t)

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

	regenerator := NewSubtreeMetaRegenerator(logger, mockStore, []string{server.URL + "/api/v1"}, func() uint32 { return 100 }, 288, 0)

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
	allowLoopbackHTTP(t)

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

	regenerator := NewSubtreeMetaRegenerator(logger, mockStore, []string{server.URL + "/api/v1"}, func() uint32 { return 100 }, 288, 0)

	// Test regeneration should fail
	meta, err := regenerator.RegenerateMeta(context.Background(), subtreeHash, subtree)

	require.Error(t, err)
	require.Nil(t, meta)
	require.Contains(t, err.Error(), "subtreedata not available locally or from peers")
}

func TestSubtreeMetaRegenerator_RegenerateMeta_NilStore_PeerFallback(t *testing.T) {
	allowLoopbackHTTP(t)

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
	regenerator := NewSubtreeMetaRegenerator(logger, nil, []string{server.URL + "/api/v1"}, func() uint32 { return 100 }, 288, 0)

	meta, err := regenerator.RegenerateMeta(context.Background(), subtreeHash, subtree)

	require.NoError(t, err)
	require.NotNil(t, meta)
}

func TestSubtreeMetaRegenerator_StoreRegeneratedMeta_Success(t *testing.T) {
	mockStore := newMockSubtreeStoreWriter()
	logger := ulogger.TestLogger{}

	regenerator := &SubtreeMetaRegenerator{
		logger:               logger,
		subtreeStore:         mockStore,
		getBlockHeight:       func() uint32 { return 100 },
		blockHeightRetention: 288,
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

// buildPeerSubtreeData builds a one-tx subtree and its serialized subtreeData,
// the payload a peer's asset service would serve for /subtree_data/<hash>.
func buildPeerSubtreeData(t *testing.T) (*subtreepkg.Subtree, *chainhash.Hash, []byte) {
	t.Helper()

	tx1 := createTestTransaction(t, "0000000000000000000000000000000000000000000000000000000000000001", 0)
	subtree := createTestSubtree([]chainhash.Hash{*tx1.TxIDChainHash()})

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	subtreeData.Txs[1] = tx1

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)

	return subtree, subtree.RootHash(), subtreeDataBytes
}

// TestSubtreeMetaRegenerator_RetriesOn503 verifies the peer fetch backs off and
// retries when the peer's asset service rejects under admission control while
// it generates subtree_data on demand — the same 503 semantics
// check_block_subtrees.go handles via util.DoHTTPRequestBodyReaderWithRetry.
func TestSubtreeMetaRegenerator_RetriesOn503(t *testing.T) {
	allowLoopbackHTTP(t)

	subtree, subtreeHash, subtreeDataBytes := buildPeerSubtreeData(t)

	var attempts atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(subtreeDataBytes)
	}))
	defer server.Close()

	mockStore := newMockSubtreeStoreWriter()
	logger := ulogger.TestLogger{}

	regenerator := NewSubtreeMetaRegenerator(logger, mockStore, []string{server.URL}, func() uint32 { return 100 }, 288, 0)

	meta, err := regenerator.RegenerateMeta(context.Background(), subtreeHash, subtree)

	require.NoError(t, err)
	require.NotNil(t, meta)
	require.GreaterOrEqual(t, attempts.Load(), int32(2), "the 503 must be retried, not returned")
}

// TestSubtreeMetaRegenerator_NoPeers_CleanError pins the error shape on the
// gRPC validation path, which builds the regenerator with no peer URLs. The
// returned error must not carry fmt artifacts like "%!(EXTRA <nil>)" from
// wrapping a nil cause, and it must still name why the local lookup missed —
// with no peers, the local failure is the only diagnostic there is.
func TestSubtreeMetaRegenerator_NoPeers_CleanError(t *testing.T) {
	subtree, subtreeHash, _ := buildPeerSubtreeData(t)

	mockStore := newMockSubtreeStoreWriter()
	logger := ulogger.TestLogger{}

	regenerator := NewSubtreeMetaRegenerator(logger, mockStore, nil, func() uint32 { return 100 }, 288, 0)

	meta, err := regenerator.RegenerateMeta(context.Background(), subtreeHash, subtree)

	require.Error(t, err)
	require.Nil(t, meta)
	require.NotContains(t, err.Error(), "%!", "no-peers error must not wrap a nil cause")
	require.Contains(t, err.Error(), "not found",
		"the local store's cause must survive into the returned error, not be logged and dropped")
}

// TestSubtreeMetaRegenerator_IncompletePeerBody_IsTransient pins the completeness
// check on the peer body. go-subtree's deserializer stops at a clean io.EOF and
// reports success, so a truncated or zero-byte 200 leaves the tail Txs nil and
// produces a meta whose GetParentTxHashes returns nil with no error. Block
// validation reads that as "transaction could not be found in tx meta data",
// raises ErrBlockInvalid and calls storeInvalidBlock — permanently invalidating a
// perfectly valid block. Regeneration must fail transiently instead.
//
// This path was unreachable before the peer URL fix in this branch: every peer
// fetch requested /api/v1/api/v1/... and 404ed. Repairing the URL makes it live.
// A zero-byte 200 is a documented real case — see the proxy-cache note at
// services/blockvalidation/get_blocks.go:641-646.
func TestSubtreeMetaRegenerator_IncompletePeerBody_IsTransient(t *testing.T) {
	allowLoopbackHTTP(t)

	// Two real transactions so a body can be truncated cleanly between them —
	// the deserializer must accept what it reads and stop at EOF, leaving the
	// second node with no inpoints.
	tx1 := createTestTransaction(t, "0000000000000000000000000000000000000000000000000000000000000001", 0)
	tx2 := createTestTransaction(t, "0000000000000000000000000000000000000000000000000000000000000002", 0)
	subtree := createTestSubtree([]chainhash.Hash{*tx1.TxIDChainHash(), *tx2.TxIDChainHash()})
	subtreeHash := subtree.RootHash()

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	subtreeData.Txs[1] = tx1
	subtreeData.Txs[2] = tx2

	fullBody, err := subtreeData.Serialize()
	require.NoError(t, err)

	firstTxOnly := tx1.SerializeBytes()
	require.Less(t, len(firstTxOnly), len(fullBody), "sanity: truncation actually drops the second tx")

	tests := []struct {
		name string
		body []byte
	}{
		{name: "zero-byte 200", body: []byte{}},
		{name: "truncated at a transaction boundary", body: firstTxOnly},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body := tc.body

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/v1/subtree_data/"+subtreeHash.String() {
					w.WriteHeader(http.StatusNotFound)
					return
				}

				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(body)
			}))
			defer server.Close()

			mockStore := newMockSubtreeStoreWriter()
			regenerator := NewSubtreeMetaRegenerator(ulogger.TestLogger{}, mockStore,
				[]string{server.URL + "/api/v1"}, func() uint32 { return 100 }, 288, 0)

			meta, err := regenerator.RegenerateMeta(context.Background(), subtreeHash, subtree)

			require.Error(t, err, "an incomplete body must not yield a meta")
			require.Nil(t, meta)
			require.False(t, errors.Is(err, errors.ErrBlockInvalid),
				"the error must stay transient — ErrBlockInvalid would poison a valid block")
			require.Empty(t, mockStore.storedMeta,
				"an incomplete meta must never reach the store, where it would overwrite an intact file")
		})
	}
}

// TestSubtreeMetaRegenerator_StalledPeer_IsBounded exercises the per-peer
// deadline against a peer that accepts the request and then never responds.
// The constructor-field test below only proves the value was stored; deleting
// the context.WithTimeout in getSubtreeDataFromPeer leaves that test green but
// fails this one, because the fetch would then inherit the shared client's
// streaming timeout and hold block validation open for minutes.
func TestSubtreeMetaRegenerator_StalledPeer_IsBounded(t *testing.T) {
	allowLoopbackHTTP(t)

	subtree, subtreeHash, _ := buildPeerSubtreeData(t)

	released := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// Hold the response open; the regenerator's own deadline is what must
		// end this, not the peer.
		select {
		case <-released:
		case <-r.Context().Done():
		}
	}))

	// Registered before the close below so it runs after it: Close waits for the
	// handler to return, and the handler only returns once released is closed.
	defer server.Close()
	defer close(released)

	regenerator := NewSubtreeMetaRegenerator(ulogger.TestLogger{}, newMockSubtreeStoreWriter(),
		[]string{server.URL + "/api/v1"}, func() uint32 { return 100 }, 288, 750*time.Millisecond)

	done := make(chan error, 1)

	go func() {
		_, err := regenerator.RegenerateMeta(context.Background(), subtreeHash, subtree)
		done <- err
	}()

	select {
	case err := <-done:
		require.Error(t, err, "a stalled peer must not hang block validation")
	case <-time.After(20 * time.Second):
		t.Fatal("the per-peer deadline did not bound the fetch against a stalled peer")
	}
}

// TestSubtreeMetaRegenerator_PeerFetchTimeoutFallback pins the fail-closed
// contract on the configurable per-peer bound: a non-positive setting must fall
// back to the default rather than leaving the fetch unbounded, since this fetch
// runs inline in block validation.
func TestSubtreeMetaRegenerator_PeerFetchTimeoutFallback(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := newMockSubtreeStoreWriter()
	height := func() uint32 { return 100 }

	for _, configured := range []time.Duration{0, -1 * time.Second} {
		r := NewSubtreeMetaRegenerator(logger, mockStore, nil, height, 288, configured)
		require.Equal(t, DefaultPeerFetchTimeout, r.peerFetchTimeout,
			"a non-positive timeout must fall back to the default, never to no limit")
	}

	r := NewSubtreeMetaRegenerator(logger, mockStore, nil, height, 288, 90*time.Second)
	require.Equal(t, 90*time.Second, r.peerFetchTimeout, "an explicit timeout must be honoured")
}
