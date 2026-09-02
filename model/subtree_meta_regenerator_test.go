package model

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
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
	meta, err := regenerator.RegenerateMeta(context.Background(), subtreeHash, subtree, true)

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
	meta, err := regenerator.RegenerateMeta(context.Background(), subtreeHash, subtree, true)

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
	meta, err := regenerator.RegenerateMeta(context.Background(), subtreeHash, subtree, true)

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

	meta, err := regenerator.RegenerateMeta(context.Background(), subtreeHash, subtree, true)

	require.NoError(t, err)
	require.NotNil(t, meta)
}

// TestSubtreeMetaRegenerator_RegenerateMeta_RejectsIncompleteSubtreeData pins the
// completeness of a regenerated meta. go-subtree's data deserializer stops at io.EOF
// without checking it filled every node, so a truncated .subtreeData file yields
// trailing nil transactions. Those become nodes with no recorded parent hashes, and
// feeding such a meta to the within-block duplicate-inputs check sends it down the
// nil-parents path, which rejects the whole block as invalid. Regeneration has to fail
// loudly rather than hand back a partial meta while logging success.
func TestSubtreeMetaRegenerator_RegenerateMeta_RejectsIncompleteSubtreeData(t *testing.T) {
	ctx := context.Background()

	tx1 := createTestTransaction(t, "0000000000000000000000000000000000000000000000000000000000000001", 0)
	tx2 := createTestTransaction(t, "0000000000000000000000000000000000000000000000000000000000000002", 1)

	subtree := createTestSubtree([]chainhash.Hash{*tx1.TxIDChainHash(), *tx2.TxIDChainHash()})
	subtreeHash := subtree.RootHash()

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	subtreeData.Txs[1] = tx1
	subtreeData.Txs[2] = tx2

	full, err := subtreeData.Serialize()
	require.NoError(t, err)

	// Cut the file after the first transaction. Subtree data is bare concatenated
	// transactions with no header, so this is exactly the shape a short write leaves
	// on disk: tx1 parses, then EOF, and node 2 never gets its inpoints.
	truncated := full[:len(tx1.SerializeBytes())]
	require.Less(t, len(truncated), len(full), "fixture must actually be short")

	store := memory.New()
	require.NoError(t, store.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData, truncated))

	regenerator := NewSubtreeMetaRegenerator(ulogger.TestLogger{}, store, nil, func() uint32 { return 100 }, 288, 0)

	meta, err := regenerator.RegenerateMeta(ctx, subtreeHash, subtree, true)

	require.Error(t, err, "regeneration from truncated subtree data must fail, not return a partial meta")
	require.Nil(t, meta)

	// A meta that failed the completeness check must not reach disk either.
	_, err = store.Get(ctx, subtreeHash[:], fileformat.FileTypeSubtreeMeta)
	require.Error(t, err, "incomplete meta must not be persisted")
}

// TestSubtreeMetaRegenerator_RejectsMissingInpointsAtNodeZero covers the one node the
// completeness check would otherwise let through. Meta.Serialize exempts index 0
// unconditionally, because the FIRST subtree of a block carries the coinbase
// placeholder there — but every other subtree has a real transaction at index 0. An
// empty .subtreeData for such a subtree used to rebuild into a meta with no recorded
// parents that serialized cleanly and then overwrote the intact file on disk, leaving a
// poisoned cache that rejects a valid block on every restart.
func TestSubtreeMetaRegenerator_RejectsMissingInpointsAtNodeZero(t *testing.T) {
	ctx := context.Background()

	tx1 := createTestTransaction(t, "0000000000000000000000000000000000000000000000000000000000000001", 0)

	// No coinbase placeholder: a single real transaction at index 0, as every
	// subtree after the first one has.
	subtree := &subtreepkg.Subtree{Nodes: []subtreepkg.Node{{Hash: *tx1.TxIDChainHash()}}}
	subtreeHash := subtree.RootHash()

	// An empty subtree data file: the deserializer breaks on io.EOF without error,
	// so every node comes back nil.
	store := memory.New()
	require.NoError(t, store.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData, []byte{}))

	// An intact meta the poisoned rebuild must not be allowed to replace.
	intact := subtreepkg.NewSubtreeMeta(subtree)
	require.NoError(t, intact.SetTxInpointsFromTx(tx1))

	intactBytes, err := intact.Serialize()
	require.NoError(t, err)
	require.NoError(t, store.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeMeta, intactBytes))

	regenerator := NewSubtreeMetaRegenerator(ulogger.TestLogger{}, store, nil, func() uint32 { return 100 }, 288, 0)

	meta, err := regenerator.RegenerateMeta(ctx, subtreeHash, subtree, true)
	require.Error(t, err, "a rebuild with no inpoints for the real transaction at node 0 must fail")
	require.Nil(t, meta)

	stored, err := store.Get(ctx, subtreeHash[:], fileformat.FileTypeSubtreeMeta)
	require.NoError(t, err)
	require.Equal(t, intactBytes, stored, "the failed rebuild must not overwrite the intact meta")
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

	cached, err := regenerator.storeRegeneratedMeta(context.Background(), subtreeHash, meta)
	require.NoError(t, err)
	require.True(t, cached, "a successful write must be reported as cached")

	// Verify meta was stored
	require.Len(t, mockStore.storedMeta, 1)
}

// TestSubtreeMetaRegenerator_StoreRegeneratedMeta_ReplacesCorruptFile pins the repair
// down to disk. Regeneration now also runs for a file that is present but corrupt, not
// only for a missing one, so the store write has to be allowed to overwrite. A blob
// store that refuses the overwrite leaves the corrupt bytes in place and the node
// rebuilds the same meta on every read, forever.
func TestSubtreeMetaRegenerator_StoreRegeneratedMeta_ReplacesCorruptFile(t *testing.T) {
	store := memory.New()
	ctx := context.Background()

	regenerator := &SubtreeMetaRegenerator{
		logger:               ulogger.TestLogger{},
		subtreeStore:         store,
		getBlockHeight:       func() uint32 { return 100 },
		blockHeightRetention: 288,
	}

	hash1, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	subtree := &subtreepkg.Subtree{
		Nodes: []subtreepkg.Node{
			{Hash: subtreepkg.CoinbasePlaceholderHashValue},
			{Hash: *hash1},
		},
	}
	subtreeHash := subtree.RootHash()

	// A torn meta file is already on disk under the key regeneration will write to.
	corrupt := []byte("torn meta file")
	require.NoError(t, store.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeMeta, corrupt))

	meta := subtreepkg.NewSubtreeMeta(subtree)
	meta.TxInpoints[1] = subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}}

	cached, err := regenerator.storeRegeneratedMeta(ctx, subtreeHash, meta)
	require.NoError(t, err)
	require.True(t, cached, "the overwrite must be reported as cached")

	expected, err := meta.Serialize()
	require.NoError(t, err)

	stored, err := store.Get(ctx, subtreeHash[:], fileformat.FileTypeSubtreeMeta)
	require.NoError(t, err)
	require.Equal(t, expected, stored, "regenerated meta must replace the corrupt file on disk")
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

	// A nil store is not an error: there is nowhere to cache the meta, so the
	// caller carries on with the regenerated one in memory rather than failing.
	// It is not a repair either, so cached is false.
	cached, err := regenerator.storeRegeneratedMeta(context.Background(), subtreeHash, meta)
	require.NoError(t, err)
	require.False(t, cached, "nothing was cached, so it must not be reported as cached")
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

	meta, err := regenerator.RegenerateMeta(context.Background(), subtreeHash, subtree, true)

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

	meta, err := regenerator.RegenerateMeta(context.Background(), subtreeHash, subtree, true)

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

			meta, err := regenerator.RegenerateMeta(context.Background(), subtreeHash, subtree, true)

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
		_, err := regenerator.RegenerateMeta(context.Background(), subtreeHash, subtree, true)
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

// TestSubtreeMetaRegenerator_TruncatedLocalFallsBackToPeer pins that an
// incomplete local rebuild does not short-circuit the peer loop. The data
// deserializer stops at io.EOF without reporting a short fill, so a truncated
// .subtreeData reads back with a nil error and only fails the completeness
// check. Returning that failure directly — as this code did when the
// completeness check was first added — strands a block permanently even though
// a peer holds an intact copy of exactly the file needed to repair it.
func TestSubtreeMetaRegenerator_TruncatedLocalFallsBackToPeer(t *testing.T) {
	allowLoopbackHTTP(t)

	ctx := context.Background()

	tx1 := createTestTransaction(t, "0000000000000000000000000000000000000000000000000000000000000001", 0)
	tx2 := createTestTransaction(t, "0000000000000000000000000000000000000000000000000000000000000002", 1)

	subtree := createTestSubtree([]chainhash.Hash{*tx1.TxIDChainHash(), *tx2.TxIDChainHash()})
	subtreeHash := subtree.RootHash()

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	subtreeData.Txs[1] = tx1
	subtreeData.Txs[2] = tx2

	full, err := subtreeData.Serialize()
	require.NoError(t, err)

	// Local copy is short; the peer's is intact.
	store := memory.New()
	require.NoError(t, store.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData, full[:len(tx1.SerializeBytes())]))

	var peerHits int

	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		peerHits++

		_, _ = w.Write(full)
	}))
	defer peer.Close()

	regenerator := NewSubtreeMetaRegenerator(ulogger.TestLogger{}, store, []string{peer.URL}, func() uint32 { return 100 }, 288, 0)

	meta, err := regenerator.RegenerateMeta(ctx, subtreeHash, subtree, true)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.Equal(t, 1, peerHits, "the peer must be asked once the local rebuild comes back incomplete")

	parents, err := meta.GetParentTxHashes(2)
	require.NoError(t, err)
	require.NotEmpty(t, parents, "the repaired meta must carry the tail node's inpoints")

	// The repaired meta replaces the unusable local file.
	stored, err := store.Get(ctx, subtreeHash[:], fileformat.FileTypeSubtreeMeta)
	require.NoError(t, err)
	require.NotEmpty(t, stored)
}

// TestSubtreeMetaRegenerator_PlaceholderExemptionMatchesValidateSubtree pins that
// node 0 is exempt only where validateSubtree also skips it — the block's first
// subtree. Exempting node 0 of any subtree that happens to carry a placeholder
// leaves TxInpoints[0] unset on a later subtree that validateSubtree does not
// skip, and GetParentTxHashes returning nil there is a BlockInvalidError on a
// valid block: a consensus-shaped verdict manufactured by the two predicates
// disagreeing.
func TestSubtreeMetaRegenerator_PlaceholderExemptionMatchesValidateSubtree(t *testing.T) {
	ctx := context.Background()

	tx1 := createTestTransaction(t, "0000000000000000000000000000000000000000000000000000000000000001", 0)

	// Node 0 is the coinbase placeholder, node 1 a real transaction.
	subtree := &subtreepkg.Subtree{Nodes: []subtreepkg.Node{
		{Hash: subtreepkg.CoinbasePlaceholderHashValue},
		{Hash: *tx1.TxIDChainHash()},
	}}
	subtreeHash := subtree.RootHash()

	// Subtree data carrying only the real transaction — what a placeholder at
	// node 0 always produces, since a placeholder has no transaction.
	data := subtreepkg.NewSubtreeData(subtree)
	data.Txs[1] = tx1

	serialized, err := data.Serialize()
	require.NoError(t, err)

	store := memory.New()
	require.NoError(t, store.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData, serialized))

	regenerator := NewSubtreeMetaRegenerator(ulogger.TestLogger{}, store, nil, func() uint32 { return 100 }, 288, 0)

	t.Run("the first subtree exempts node 0", func(t *testing.T) {
		meta, err := regenerator.RegenerateMeta(ctx, subtreeHash, subtree, true)
		require.NoError(t, err)
		require.NotNil(t, meta)
	})

	t.Run("a later subtree does not", func(t *testing.T) {
		// validateSubtree would not skip node 0 here, so a meta with no inpoints
		// there gets the block condemned. Failing regeneration keeps the error
		// transient instead.
		meta, err := regenerator.RegenerateMeta(ctx, subtreeHash, subtree, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no transaction for node 0")
		require.Nil(t, meta)
	})
}

// TestSubtreeMetaRegenerator_ZeroInputTxIsRejected pins that a node whose
// transaction has no inputs fails regeneration rather than producing a meta that
// can never be written.
//
// The earlier version of this test asserted the opposite, and put the zero-input
// transaction at index 0 — the one index Meta.Serialize exempts from its
// nil-parents check — so it passed over the bug it was meant to cover. Anywhere
// else, such a node builds a meta that serializes with an error, which
// storeRegeneratedMeta used to swallow: RegenerateMeta returned success, nothing
// reached the store, and every later read rebuilt from .subtreeData and paid the
// peer fetch again. That is the rebuild-forever loop WithAllowOverwrite was added
// to break.
func TestSubtreeMetaRegenerator_ZeroInputTxIsRejected(t *testing.T) {
	ctx := context.Background()

	tx0 := createTestTransaction(t, "0000000000000000000000000000000000000000000000000000000000000001", 0)
	noInputs := &bt.Tx{}

	subtree := &subtreepkg.Subtree{Nodes: []subtreepkg.Node{
		{Hash: *tx0.TxIDChainHash()},
		{Hash: *noInputs.TxIDChainHash()},
	}}
	subtreeHash := subtree.RootHash()

	data := subtreepkg.NewSubtreeData(subtree)
	data.Txs[0] = tx0
	data.Txs[1] = noInputs

	serialized, err := data.Serialize()
	require.NoError(t, err)

	store := memory.New()
	require.NoError(t, store.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData, serialized))

	regenerator := NewSubtreeMetaRegenerator(ulogger.TestLogger{}, store, nil, func() uint32 { return 100 }, 288, 0)

	meta, err := regenerator.RegenerateMeta(ctx, subtreeHash, subtree, false)
	require.Error(t, err, "a meta that cannot be serialized must not be returned as success")
	require.Nil(t, meta)

	// And nothing half-written was left behind.
	_, err = store.Get(ctx, subtreeHash[:], fileformat.FileTypeSubtreeMeta)
	require.Error(t, err)
}

// TestSubtreeMetaRegenerator_RejectsInternalPeer is the SSRF regression test for the peer
// fetch path: peerURLs come straight from peer block/subtree announcements. The fetch must be
// refused after DNS resolution, so a hostname that only resolves to an internal address is no
// better for an attacker than an internal literal - and the target sees no request even
// though it is serving exactly what the regenerator wants.
//
// The guard now comes from util's shared client (DoHTTPRequestBodyReaderWithRetry); this pins
// the property at this layer so a future change of client cannot quietly drop it.
func TestSubtreeMetaRegenerator_RejectsInternalPeer(t *testing.T) {
	tx1 := createTestTransaction(t, "0000000000000000000000000000000000000000000000000000000000000001", 0)
	subtree := createTestSubtree([]chainhash.Hash{*tx1.TxIDChainHash()})
	subtreeHash := subtree.RootHash()

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	subtreeData.Txs[1] = tx1
	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)

	var hits atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(subtreeDataBytes)
	}))
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	_, port, err := net.SplitHostPort(serverURL.Host)
	require.NoError(t, err)

	tests := map[string]string{
		// A hostname: passes the static check (no DNS there), refused at dial time once
		// resolution reveals the loopback address. This is the case the guard exists for.
		"http://localhost:" + port + "/api/v1": "loopback address",
		// A literal cloud metadata endpoint, refused earlier by the static ValidateURL
		// pre-check without a connection being attempted at all.
		"http://169.254.169.254/api/v1": "blocked IP address",
	}

	for peerURL, reason := range tests {
		t.Run(peerURL, func(t *testing.T) {
			regenerator := NewSubtreeMetaRegenerator(ulogger.TestLogger{}, newMockSubtreeStoreWriter(),
				[]string{peerURL}, func() uint32 { return 100 }, 288, 5*time.Second)

			data, err := regenerator.getSubtreeDataFromPeer(context.Background(), subtreeHash, subtree, peerURL)
			require.Error(t, err)
			require.Nil(t, data)
			require.Contains(t, err.Error(), reason)
		})
	}

	require.Zero(t, hits.Load(), "the fetch must not reach the internal target")
}

// TestStoreRegeneratedMeta_UnserializableIsAnError pins the second of the two
// layers guarding the rebuild-forever loop. buildMetaFromSubtreeData rejects the
// input that produces an unserializable meta, so this path should be
// unreachable — but it used to swallow a serialize failure as a Warnf and report
// success, which is what let a meta that could never be written look like a
// successful regeneration. Tested directly because, with both layers in place,
// no input reaches it.
func TestStoreRegeneratedMeta_UnserializableIsAnError(t *testing.T) {
	ctx := context.Background()

	tx0 := createTestTransaction(t, "0000000000000000000000000000000000000000000000000000000000000001", 0)
	noInputs := &bt.Tx{}

	subtree := &subtreepkg.Subtree{Nodes: []subtreepkg.Node{
		{Hash: *tx0.TxIDChainHash()},
		{Hash: *noInputs.TxIDChainHash()},
	}}

	// Node 1 left with no inpoints: Meta.Serialize rejects nil parents for every
	// index except 0.
	meta := subtreepkg.NewSubtreeMeta(subtree)
	require.NoError(t, meta.SetTxInpointsFromTx(tx0))

	_, err := meta.Serialize()
	require.Error(t, err, "fixture must actually be unserializable or this tests nothing")

	store := memory.New()
	regenerator := NewSubtreeMetaRegenerator(ulogger.TestLogger{}, store, nil, func() uint32 { return 100 }, 288, 0)

	cached, err := regenerator.storeRegeneratedMeta(ctx, subtree.RootHash(), meta)
	require.Error(t, err, "a meta that cannot be serialized must not be reported as stored")
	require.False(t, cached)

	_, err = store.Get(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta)
	require.Error(t, err, "nothing should have been written")
}

// metaWriteFailingStore serves reads from a real in-memory blob store but refuses
// to write a .subtreeMeta. That is the shape ChiR1 is about: the rebuild itself
// works, the cache write does not, and whatever was already on disk under that
// key survives untouched.
type metaWriteFailingStore struct {
	*memory.Memory
	setErr error
}

func (s *metaWriteFailingStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	if fileType == fileformat.FileTypeSubtreeMeta {
		return s.setErr
	}

	return s.Memory.Set(ctx, key, fileType, value, opts...)
}

// capturingLogger records the Warnf and Errorf lines so a test can assert on what
// an operator would actually grep for. New and Duplicate return the same logger:
// NewSubtreeMetaRegenerator calls New, and ulogger.TestLogger's New hands back a
// fresh no-op logger that would capture nothing.
type capturingLogger struct {
	ulogger.TestLogger
	mu    sync.Mutex
	lines []string
}

func (l *capturingLogger) New(_ string, _ ...ulogger.Option) ulogger.Logger { return l }

func (l *capturingLogger) Duplicate(_ ...ulogger.Option) ulogger.Logger { return l }

func (l *capturingLogger) WithTraceContext(_ context.Context) ulogger.Logger { return l }

func (l *capturingLogger) Warnf(format string, args ...interface{}) { l.record(format, args...) }

func (l *capturingLogger) Errorf(format string, args ...interface{}) { l.record(format, args...) }

func (l *capturingLogger) record(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *capturingLogger) contains(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, line := range l.lines {
		if strings.Contains(line, substr) {
			return true
		}
	}

	return false
}

// TestSubtreeMetaRegenerator_FailedCacheWriteIsNotARepair pins the end state when
// the rebuild succeeds and the cache write does not.
//
// storeRegeneratedMeta used to log a Warnf and return nil there, so
// buildAndStoreMeta went on to log "successfully regenerated meta" for a subtree
// whose rejected file was still on disk. With regeneration now running for a
// present-but-rejected file as well as a missing one, that line told an operator a
// rebuild loop had been broken when nothing had changed: the next read fails the
// same header check, re-enters RegenerateMeta and pays the .subtreeData read and,
// on a local miss, the peer fetch again.
//
// The write failure stays non-fatal on purpose. Returning it would send
// RegenerateMeta on to the peers, and no peer can fix a local write.
func TestSubtreeMetaRegenerator_FailedCacheWriteIsNotARepair(t *testing.T) {
	ctx := context.Background()

	tx0 := createTestTransaction(t, "0000000000000000000000000000000000000000000000000000000000000001", 0)
	tx1 := createTestTransaction(t, "0000000000000000000000000000000000000000000000000000000000000002", 0)

	subtree := &subtreepkg.Subtree{Nodes: []subtreepkg.Node{
		{Hash: *tx0.TxIDChainHash()},
		{Hash: *tx1.TxIDChainHash()},
	}}
	subtreeHash := subtree.RootHash()

	data := subtreepkg.NewSubtreeData(subtree)
	data.Txs[0] = tx0
	data.Txs[1] = tx1

	serialized, err := data.Serialize()
	require.NoError(t, err)

	inner := memory.New()
	require.NoError(t, inner.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData, serialized))

	// The rejected file this regeneration is supposed to replace, already on disk.
	poisoned := []byte("rejected meta file")
	require.NoError(t, inner.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeMeta, poisoned))

	store := &metaWriteFailingStore{Memory: inner, setErr: errors.NewStorageError("disk full")}
	logger := &capturingLogger{}

	regenerator := NewSubtreeMetaRegenerator(logger, store, nil, func() uint32 { return 100 }, 288, 0)

	meta, err := regenerator.RegenerateMeta(ctx, subtreeHash, subtree, false)

	// The meta is still handed back and still usable for this validation.
	require.NoError(t, err)
	require.NotNil(t, meta)

	parents, err := meta.GetParentTxHashes(1)
	require.NoError(t, err)
	require.NotEmpty(t, parents, "the regenerated meta must be usable even though it was not cached")

	// End state on disk: the rejected file is exactly where it was, so the next
	// read rebuilds again.
	onDisk, err := inner.Get(ctx, subtreeHash[:], fileformat.FileTypeSubtreeMeta)
	require.NoError(t, err)
	require.Equal(t, poisoned, onDisk, "a failed write must leave the rejected file untouched")

	// End state in the return: storeRegeneratedMeta reports not-cached, with no error.
	cached, err := regenerator.storeRegeneratedMeta(ctx, subtreeHash, meta)
	require.NoError(t, err, "a failed cache write must not be returned as an error")
	require.False(t, cached, "a failed cache write must not be reported as cached")

	// End state in the logs: the success line an operator greps for is absent, and
	// the line that is there names the consequence.
	require.False(t, logger.contains("successfully regenerated meta"),
		"a rebuild that never reached the store must not be logged as a successful regeneration")
	require.True(t, logger.contains("did not cache it"),
		"the not-cached outcome needs its own line")
	require.True(t, logger.contains("rebuilt on every read"),
		"the write failure must name what it costs")
}
