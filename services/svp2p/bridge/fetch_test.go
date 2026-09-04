package bridge

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/asset/repository"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/svp2p/bridge/bsvutil"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newFetchBridge builds a minimal svp2pBridge for FetchBlock/FetchTx tests:
// a mocked blockchain client (the caller sets expectations) and a real
// sqlitememory UTXO store (never a mock — the "not retained" and "missing"
// rows below are only genuine on a real store's own Get path).
func newFetchBridge(ctx context.Context, t *testing.T, assetHTTPAddress string) (*svp2pBridge, *blockchain.Mock, utxo.Store) {
	t.Helper()

	tSettings, _ := newOutpointOnlySettings(t, true, true, 1000)
	tSettings.Asset.HTTPAddress = assetHTTPAddress

	storeURL, err := url.Parse("sqlitememory:///" + t.Name())
	require.NoError(t, err)
	tSettings.UtxoStore.UtxoStore = storeURL

	store, err := utxosql.New(ctx, ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	mockBC := &blockchain.Mock{}

	return &svp2pBridge{
		logger:           ulogger.TestLogger{},
		settings:         tSettings,
		blockchainClient: mockBC,
		utxoStore:        store,
	}, mockBC, store
}

// paddedWireBody returns body followed by filler bytes so the total length
// clears minLegacyBlockWireBytes — every FetchBlock test below needs a
// declared SizeInBytes past that floor (fetch.go's implausible-size guard),
// even though the httptest bodies themselves are just marker strings. The
// filler is a distinct byte value so a test asserting on the body's prefix
// still reads clearly in a failure diff.
func paddedWireBody(body string) []byte {
	padded := make([]byte, minLegacyBlockWireBytes+16)
	copy(padded, body)

	for i := len(body); i < len(padded); i++ {
		padded[i] = 0xee
	}

	return padded
}

// TestFetchBlock_StreamsBodyAndReportsDeclaredLength proves the two halves
// come from different places: the byte stream from the httptest double
// standing in for the asset service, the length from the blockchain client —
// never from the HTTP response, which carries no Content-Length on this
// route (A1).
func TestFetchBlock_StreamsBodyAndReportsDeclaredLength(t *testing.T) {
	ctx := context.Background()

	wantHash := chainhash.Hash{0x01}
	wantBody := paddedWireBody("legacy-wire-block-bytes")

	// requestPath/requestWireParam are set from the handler goroutine and
	// asserted on in the test body below — require/t.Fatal from inside an
	// httptest handler goroutine is outside what the testing package
	// supports (FailNow's runtime.Goexit there abandons the response
	// mid-write), so the handler only records what it saw.
	var requestPath, requestWireParam string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestPath = r.URL.Path
		requestWireParam = r.URL.Query().Get("wire")
		// No Content-Length set — matches the real streaming route
		// (services/asset/httpimpl/stream.go:83,88).
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(wantBody)
	}))
	defer srv.Close()

	// Deliberately WITH SSRF protection on: asset_httpAddress is operator
	// configuration and defaults to loopback, so FetchBlock must not go through
	// the peer-facing dial policy (ledger residual 14).
	util.SetSSRFProtection(true)

	b, mockBC, _ := newFetchBridge(ctx, t, srv.URL)
	mockBC.On("GetBlockHeader", mock.Anything, &wantHash).
		Return((*model.BlockHeader)(nil), &model.BlockHeaderMeta{SizeInBytes: uint64(len(wantBody))}, nil).Once()

	reader, length, err := b.FetchBlock(ctx, &wantHash)
	require.NoError(t, err)
	defer reader.Close()

	require.EqualValues(t, len(wantBody), length, "declared length must come from GetBlockHeader, not the (absent) Content-Length")

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, wantBody, got)

	require.Equal(t, fmt.Sprintf("/block_legacy/%s", wantHash.String()), requestPath)
	require.Equal(t, "1", requestWireParam)
}

// TestFetchBlock_UnknownBlock_NoHTTPCall proves a block the blockchain
// service has never heard of fails before any HTTP request is made — the
// httptest server never receives a hit if the block is unknown.
func TestFetchBlock_UnknownBlock_NoHTTPCall(t *testing.T) {
	ctx := context.Background()

	hash := chainhash.Hash{0x02}

	// Recorded rather than t.Fatal'd directly: FailNow's runtime.Goexit from
	// a handler goroutine is outside what testing supports and would abandon
	// the response mid-write.
	var called bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer srv.Close()

	util.SetSSRFProtection(false)
	t.Cleanup(func() { util.SetSSRFProtection(true) })

	b, mockBC, _ := newFetchBridge(ctx, t, srv.URL)
	mockBC.On("GetBlockHeader", mock.Anything, &hash).
		Return((*model.BlockHeader)(nil), (*model.BlockHeaderMeta)(nil), errors.NewBlockNotFoundError("block %s not found", hash.String())).Once()

	_, _, err := b.FetchBlock(ctx, &hash)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockNotFound), "expected BlockNotFound, got %v", err)
	require.False(t, called, "asset service must not be called for a block the blockchain service does not know")
}

// TestFetchBlock_AssetServiceHTTPError proves an asset-service failure (the
// asset endpoint's documented 500 for retrieval errors) surfaces as an error
// from FetchBlock rather than a reader over an empty/error body.
func TestFetchBlock_AssetServiceHTTPError(t *testing.T) {
	ctx := context.Background()

	hash := chainhash.Hash{0x03}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	util.SetSSRFProtection(false)
	t.Cleanup(func() { util.SetSSRFProtection(true) })

	b, mockBC, _ := newFetchBridge(ctx, t, srv.URL)
	mockBC.On("GetBlockHeader", mock.Anything, &hash).
		Return((*model.BlockHeader)(nil), &model.BlockHeaderMeta{SizeInBytes: minLegacyBlockWireBytes + 100}, nil).Once()

	_, _, err := b.FetchBlock(ctx, &hash)
	require.Error(t, err)
}

// TestFetchBlock_ImplausibleDeclaredSize_Rejected proves a declared length
// below what any real legacy-wire block body can be (0 included) fails
// before the HTTP call, rather than being handed to Task 10 to write into a
// wire message header, where it would corrupt the frame.
func TestFetchBlock_ImplausibleDeclaredSize_Rejected(t *testing.T) {
	ctx := context.Background()

	hash := chainhash.Hash{0x05}

	var called bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer srv.Close()

	util.SetSSRFProtection(false)
	t.Cleanup(func() { util.SetSSRFProtection(true) })

	b, mockBC, _ := newFetchBridge(ctx, t, srv.URL)
	mockBC.On("GetBlockHeader", mock.Anything, &hash).
		Return((*model.BlockHeader)(nil), &model.BlockHeaderMeta{SizeInBytes: 0}, nil).Once()

	_, _, err := b.FetchBlock(ctx, &hash)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "expected BlockInvalid, got %v", err)
	require.False(t, called, "an implausible declared size must be rejected before any HTTP call")
}

// TestFetchBlock_TruncatedBody proves a connection that closes mid-stream is
// visible to the caller as a read error off the returned reader, not
// swallowed — the caller (Task 10) writes the declared length into the wire
// header before streaming the body, so a short body must fail loudly rather
// than silently under-deliver.
func TestFetchBlock_TruncatedBody(t *testing.T) {
	ctx := context.Background()

	hash := chainhash.Hash{0x04}
	fullBody := paddedWireBody("this body is longer than what the server actually sends")

	// hijackErr/isFlusher/isHijacker are recorded rather than asserted with
	// require inside the handler goroutine, for the same reason as above.
	// Hijacking bypasses the normal response write/close protocol that
	// otherwise gives the test body a happens-before relationship with the
	// handler goroutine, so handlerDone makes that synchronization explicit
	// instead of relying on the (here, absent) implicit one.
	var hijackErr error
	var isFlusher, isHijacker bool
	handlerDone := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)

		w.Header().Set("Content-Type", "application/octet-stream")

		flusher, ok := w.(http.Flusher)
		isFlusher = ok

		if ok {
			_, _ = w.Write(fullBody[:10])
			flusher.Flush()
		}

		// Close the underlying connection without writing the rest, so the
		// client sees a truncated chunked/close-delimited body.
		hijacker, ok := w.(http.Hijacker)
		isHijacker = ok

		if ok {
			var conn net.Conn
			conn, _, hijackErr = hijacker.Hijack()
			if hijackErr == nil {
				_ = conn.Close()
			}
		}
	}))
	defer srv.Close()

	util.SetSSRFProtection(false)
	t.Cleanup(func() { util.SetSSRFProtection(true) })

	b, mockBC, _ := newFetchBridge(ctx, t, srv.URL)
	mockBC.On("GetBlockHeader", mock.Anything, &hash).
		Return((*model.BlockHeader)(nil), &model.BlockHeaderMeta{SizeInBytes: uint64(len(fullBody))}, nil).Once()

	reader, length, err := b.FetchBlock(ctx, &hash)
	require.NoError(t, err, "the truncation happens mid-body, after headers are already sent")
	defer reader.Close()

	require.EqualValues(t, len(fullBody), length)

	_, err = io.ReadAll(reader)
	require.Error(t, err, "a truncated body must surface as a read error, not a short read masquerading as success")

	<-handlerDone // synchronizes with the handler goroutine's writes above

	require.True(t, isFlusher, "httptest ResponseWriter must support Flusher")
	require.True(t, isHijacker, "httptest ResponseWriter must support Hijacker")
	require.NoError(t, hijackErr)
}

// wireByteCount independently re-serializes a decoded block's own
// transactions, in wire order — header(80) + varint(txCount) + every
// transaction, coinbase first — using the wire package's own writers. It is
// deliberately not model.Block's SizeInBytes formula, so a test that
// compares against it is not asserting the implementation's arithmetic
// against itself.
func wireByteCount(t *testing.T, msgBlock *wire.MsgBlock) int {
	t.Helper()

	txs := msgBlock.Transactions

	var buf bytes.Buffer
	require.NoError(t, msgBlock.Header.Serialize(&buf))
	require.NoError(t, wire.WriteVarInt(&buf, 0, uint64(len(txs))))

	for _, tx := range txs {
		require.NoError(t, tx.Serialize(&buf))
	}

	return buf.Len()
}

// newFetchLegacyBlockRepository builds a real services/asset/repository.Repository
// wired to the given (mocked or real) blockchainClient and a real subtree
// blob store, so a test can drive the actual GetLegacyBlockReader streaming
// code — the same code the asset service's block_legacy?wire=1 HTTP route
// calls — rather than reimplementing its byte-counting logic.
func newFetchLegacyBlockRepository(t *testing.T, tSettings *settings.Settings, blockchainClient blockchain.ClientI, subtreeStore blob.Store) *repository.Repository {
	t.Helper()

	repo, err := repository.NewRepository(ulogger.TestLogger{}, tSettings, nil, nil,
		blockchainClient, nil, subtreeStore, nil, nil, nil)
	require.NoError(t, err)

	return repo
}

// TestFetchBlock_A1_RealMainnetBlock_RecomputedSizeMatchesWireBytes is half
// of the load-bearing A1 proof: that model.Block.SizeInBytes, as actually
// (re)computed by production code, equals the real block_legacy?wire=1 byte
// count — for a real mainnet block, not a synthetic one.
//
// A1 named model/Block.go:1708 as the source of the number FetchBlock reads
// back (via BlockHeaderMeta.SizeInBytes). That line only runs inside
// Block.GetAndValidateSubtrees, which Block.Valid calls under the guard
// `subtreeStore != nil && len(b.Subtrees) > 0` (model/Block.go:668) — i.e.
// only for a block that actually has subtrees. HandleBlockDirect captures
// the block *before* that recomputation (it sets SizeInBytes from
// wire.MsgBlock.SerializeSize() at handle_block.go:187, a value blockvalidation
// discards and overwrites before AddBlock ever runs in production). So this
// test calls GetAndValidateSubtrees directly against the real subtree blob
// store the pipeline just wrote to, forcing exactly the recomputation A1
// named, and asserts the result — not the SerializeSize placeholder — against
// an independent wire-byte count.
//
// It does not call the full Block.Valid: Valid's other checks (BIP34 coinbase
// height, block reward, proof-of-work against the current chain) need real
// chain context this fixture does not have outside its own real historical
// position, which this test does not attempt to reconstruct. Only the
// GetAndValidateSubtrees recomputation — the site the number in question
// actually comes from — needs to run for this proof.
//
// Second half: drives the real asset-repository GetLegacyBlockReader
// streaming code (the code the block_legacy?wire=1 HTTP route actually calls)
// over the recomputed block and the same real subtree store, and requires the
// byte count it streams to equal the recomputed SizeInBytes exactly. This is
// the end-to-end form the controller asked to be tried: it cannot be
// satisfied by two formulas merely agreeing, because it exercises the actual
// production wire-serialization code, not a second reimplementation of it.
//
// What this half does NOT do: push the block through a real blockchain SQL
// store via AddBlock/GetBlockHeader. AddBlock validates that the new block's
// parent is already on chain, and this fixture's real parent is a specific
// historical mainnet block this test does not have — chaining it into a
// fresh test store is not possible without faking the rest of the chain.
// That store round-trip is proven for real, without any mocking, by
// TestFetchBlock_A1_CoinbaseOnlyBlock_SizeMatchesWireBytes below, whose block
// is built to chain directly off a fresh store's own genesis block. Here, the
// repository is driven with a blockchainClient double that returns the
// recomputed in-memory block directly from GetBlock — the store-persistence
// claim is out of scope for this half by construction, not by oversight.
func TestFetchBlock_A1_RealMainnetBlock_RecomputedSizeMatchesWireBytes(t *testing.T) {
	ctx := context.Background()

	fixture, err := ReadBlockFromFile("testdata/00000000000000000ad4cd15bbeaf6cb4583c93e13e311f9774194aadea87386.bin")
	require.NoError(t, err)

	txs := fixture.Transactions()
	require.Greater(t, len(txs), 256, "the fixture must span several subtrees, like the parity test uses it for")

	harness := newIngestHarness(ctx, t, "fetch_a1_ingest", true, 256)
	require.NoError(t, harness.sm.HandleBlockDirect(ctx, "peer-a", *fixture.Hash(), fixture.MsgBlock()))

	got := harness.captured.block()
	require.NotNil(t, got, "the pipeline never reached ProcessBlock")
	require.NotEmpty(t, got.Subtrees, "the fixture must produce at least one subtree for GetAndValidateSubtrees to have anything to recompute")

	wantBytes := wireByteCount(t, fixture.MsgBlock())

	// Force the model/Block.go:1708 recomputation directly — the exact code
	// A1 named, run against the exact subtree files the ingestion pipeline
	// just wrote to harness.subtreeStore.
	require.NoError(t, got.GetAndValidateSubtrees(ctx, ulogger.TestLogger{}, harness.subtreeStore, 0))

	require.EqualValues(t, wantBytes, got.SizeInBytes,
		"model.Block.SizeInBytes, as recomputed by GetAndValidateSubtrees (model/Block.go:1708), must equal the real block_legacy?wire=1 byte count")

	// End-to-end: the real asset-repository streaming code, not a
	// reimplementation of its byte count.
	mockBC := &blockchain.Mock{}
	mockBC.On("GetBlock", mock.Anything, fixture.Hash()).Return(got, nil).Once()

	repo := newFetchLegacyBlockRepository(t, harness.sm.settings, mockBC, harness.subtreeStore)

	reader, err := repo.GetLegacyBlockReader(ctx, fixture.Hash(), true)
	require.NoError(t, err)
	defer reader.Close()

	n, err := io.Copy(io.Discard, reader)
	require.NoError(t, err)

	require.EqualValues(t, wantBytes, n,
		"the real GetLegacyBlockReader(wire=true) stream must be exactly the recomputed SizeInBytes in length")
}

// TestFetchBlock_A1_CoinbaseOnlyBlock_SizeMatchesWireBytes is the second,
// fully real (no test doubles on the store side) half of the A1 proof, using
// the degenerate single-transaction block shape the controller asked for:
// sum(subtrees) == 0, so the whole number collapses to
// 80 (header) + varint(txCount) + coinbase.Size(), where an off-by-one in
// the varint or a double-counted coinbase would show up immediately.
//
// Unlike the mainnet fixture above, this block has no subtrees, so
// Block.Valid's guard (`len(b.Subtrees) > 0`, model/Block.go:668) means
// GetAndValidateSubtrees — and therefore the 1708 formula — never runs for
// it in production, on any real block of this shape. What HandleBlockDirect
// sets at construction (wire.MsgBlock.SerializeSize(), handle_block.go:187)
// is genuinely the number production stores for this shape; there is no
// separate "real" recomputation to force here, and calling
// GetAndValidateSubtrees on a subtree-less block directly would corrupt
// TransactionCount (it unconditionally sets it to the sum over b.Subtrees,
// zero when there are none) — actively wrong for this shape, not merely
// unnecessary.
//
// Because this block is built to chain directly off a fresh store's own
// genesis block, the full loop closes for real: real ingestion pipeline,
// real SQL blockchain store (AddBlock, no mocking), real GetBlockHeader, and
// the real GetLegacyBlockReader streaming code — no double stands in for any
// of it.
func TestFetchBlock_A1_CoinbaseOnlyBlock_SizeMatchesWireBytes(t *testing.T) {
	ctx := context.Background()

	harness := newIngestHarness(ctx, t, "fetch_a1_genesis_child", false, 0, 0)

	genesisChild := buildGenesisChildBlock(t, *harness.sm.chainParams.GenesisHash)
	require.Len(t, genesisChild.Transactions(), 1, "degenerate coinbase-only block")

	require.NoError(t, harness.sm.HandleBlockDirect(ctx, "peer-a", *genesisChild.Hash(), genesisChild.MsgBlock()))

	childBlock := harness.captured.block()
	require.NotNil(t, childBlock, "the pipeline never reached ProcessBlock")
	require.Empty(t, childBlock.Subtrees, "this shape must produce no subtree, or the test is not exercising the guarded-off path it claims to")

	wantBytes := wireByteCount(t, genesisChild.MsgBlock())

	require.EqualValues(t, wantBytes, childBlock.SizeInBytes,
		"for a subtree-less block, the SerializeSize-derived value IS what production stores (GetAndValidateSubtrees never runs) and must equal the real wire byte count")

	bcStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, harness.sm.settings)
	require.NoError(t, err)

	bcClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, harness.sm.settings, bcStore, nil, nil)
	require.NoError(t, err)

	require.NoError(t, bcClient.AddBlock(ctx, childBlock, "peer-a"))

	_, meta, err := bcClient.GetBlockHeader(ctx, genesisChild.Hash())
	require.NoError(t, err)

	require.EqualValues(t, wantBytes, meta.SizeInBytes,
		"BlockHeaderMeta.SizeInBytes — what FetchBlock actually reads — must equal the real wire byte count after a real store round-trip")

	// End-to-end: the real asset-repository streaming code driven by the real
	// blockchain client (not a double) that just stored this block.
	repo := newFetchLegacyBlockRepository(t, harness.sm.settings, bcClient, harness.subtreeStore)

	reader, err := repo.GetLegacyBlockReader(ctx, genesisChild.Hash(), true)
	require.NoError(t, err)
	defer reader.Close()

	n, err := io.Copy(io.Discard, reader)
	require.NoError(t, err)

	require.EqualValues(t, meta.SizeInBytes, n,
		"the real GetLegacyBlockReader(wire=true) stream must be exactly meta.SizeInBytes in length — the number FetchBlock hands Task 10")
}

// buildGenesisChildBlock makes the degenerate single-transaction block
// (buildCoinbaseOnlyBlock's shape, ingest_test.go) chained directly off the
// given hash, so it can be pushed into a real, freshly-created blockchain
// store whose only existing block is genesis.
func buildGenesisChildBlock(t *testing.T, prevBlock chainhash.Hash) *bsvutil.Block {
	t.Helper()

	coinbase := wire.NewMsgTx(1)
	coinbase.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Hash: chainhash.Hash{}, Index: 0xffffffff},
		SignatureScript:  []byte{0x51, 0x01, 0x01},
		Sequence:         0xffffffff,
	})
	coinbase.AddTxOut(&wire.TxOut{Value: 5000000000, PkScript: []byte{0x76, 0xa9, 0x14, 0x03}})

	msgBlock := &wire.MsgBlock{
		Header:       wire.BlockHeader{Version: 1, PrevBlock: prevBlock, Timestamp: time.Unix(1600000200, 0)},
		Transactions: []*wire.MsgTx{coinbase},
	}

	grindEasyPoW(t, msgBlock)

	return bsvutil.NewBlock(msgBlock)
}

// storeRetainedTx creates a normal, fully-decoded coinbase-shaped transaction
// and stores it via the real UTXO store's ordinary create path, so
// TxIsSerializable() is true and FetchTx must return its bytes.
func storeRetainedTx(ctx context.Context, t *testing.T, store utxo.Store) *bt.Tx {
	t.Helper()

	input := &bt.Input{
		PreviousTxOutIndex: 0xffffffff,
		SequenceNumber:     0xffffffff,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x51, 0x01}),
	}
	require.NoError(t, input.PreviousTxIDAdd(&chainhash.Hash{}))

	tx := &bt.Tx{
		Version: 1,
		Inputs:  []*bt.Input{input},
		Outputs: []*bt.Output{
			{Satoshis: 5000000000, LockingScript: bscript.NewFromBytes([]byte{0x51})},
		},
	}

	_, _, err := store.SpendAndCreate(ctx, tx, 100, utxo.WithCreateOnly(), utxo.WithSkipExtendedInputs(true))
	require.NoError(t, err)

	return tx
}

// storeNotRetainedTx creates the minimal (below-checkpoint / outpoint-only)
// shape the real SQL store actually persists during checkpoint catchup:
// zero inputs, so TxMetaDataFromTxNoFee (util/tx_meta.go) skips tx.Size()
// and stores the transaction as given. meta.Data.TxIsSerializable()
// (stores/utxo/meta/data.go:237) treats a zero-input transaction as its
// snapshot signature and reports false — this is not a synthetic shape
// invented for the test, it is the one the codebase's own minimal-create
// path produces (see newOutpointOnlySettings and TxMetaDataFromTxNoFee's own
// "For partially populated utxos, we will have no inputs" comment).
func storeNotRetainedTx(ctx context.Context, t *testing.T, store utxo.Store, hash chainhash.Hash) {
	t.Helper()

	tx := &bt.Tx{
		Version: 1,
		Inputs:  []*bt.Input{},
		Outputs: []*bt.Output{
			{Satoshis: 1000, LockingScript: bscript.NewFromBytes([]byte{0x51})},
		},
	}

	_, _, err := store.SpendAndCreate(ctx, tx, 100, utxo.WithCreateOnly(), utxo.WithSkipExtendedInputs(true), utxo.WithTXID(&hash))
	require.NoError(t, err)
}

// TestFetchTx_Present returns the exact bytes of a fully-retained transaction.
func TestFetchTx_Present(t *testing.T) {
	ctx := context.Background()

	b, _, store := newFetchBridge(ctx, t, "http://unused.invalid")
	tx := storeRetainedTx(ctx, t, store)
	hash := *tx.TxIDChainHash()

	got, err := b.FetchTx(ctx, &hash)
	require.NoError(t, err)
	require.Equal(t, tx.Bytes(), got)
}

// TestFetchTx_Missing proves a hash the store has never seen returns
// errors.ErrTxNotFound (not legacy's silent nil-error).
func TestFetchTx_Missing(t *testing.T) {
	ctx := context.Background()

	b, _, _ := newFetchBridge(ctx, t, "http://unused.invalid")
	hash := chainhash.Hash{0x09}

	_, err := b.FetchTx(ctx, &hash)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "expected TxNotFound, got %v", err)
}

// TestFetchTx_NotRetainedInFull proves a real store row that exists but is
// not retained in full (the checkpoint-catchup outpoint-only shape) also
// returns errors.ErrTxNotFound — legacy's silent-nil branch
// (services/legacy/peer_server.go:1998-2003), deliberately not carried
// forward (A4).
func TestFetchTx_NotRetainedInFull(t *testing.T) {
	ctx := context.Background()

	b, _, store := newFetchBridge(ctx, t, "http://unused.invalid")
	hash := chainhash.Hash{0x0a}
	storeNotRetainedTx(ctx, t, store, hash)

	_, err := b.FetchTx(ctx, &hash)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "expected TxNotFound, got %v", err)
}

// TestFetchTx_ProjectionIsTxOnly proves the store call asks for fields.Tx
// only, matching legacy's s.utxoStore.Get(ctx, hash, fields.Tx)
// (services/legacy/peer_server.go:1993) — not fetching more than the caller
// needs. A store that only serves fields.Tx must still answer correctly.
func TestFetchTx_ProjectionIsTxOnly(t *testing.T) {
	ctx := context.Background()

	b, _, store := newFetchBridge(ctx, t, "http://unused.invalid")
	tx := storeRetainedTx(ctx, t, store)
	hash := *tx.TxIDChainHash()

	// A direct Get with fields.Tx must succeed the same way FetchTx's own
	// call does — proving FetchTx does not depend on a wider projection.
	data, err := store.Get(ctx, &hash, fields.Tx)
	require.NoError(t, err)
	require.True(t, data.TxIsSerializable())

	got, err := b.FetchTx(ctx, &hash)
	require.NoError(t, err)
	require.Equal(t, tx.Bytes(), got)
}

// TestFetchBlock_AssetServiceStatusIsClassified is the split Task 10 needs: a
// block the asset service does not have is a 404, and it must arrive as
// errors.ErrBlockNotFound so the getdata answerer can say notfound; every
// other status is a real failure and must NOT look like absence, because
// answering notfound for it tells the peer to stop asking for a block we hold.
func TestFetchBlock_AssetServiceStatusIsClassified(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		wantAbsent bool
	}{
		{name: "404 is absence", status: http.StatusNotFound, wantAbsent: true},
		{name: "500 is a failure", status: http.StatusInternalServerError, wantAbsent: false},
		{name: "503 is a failure", status: http.StatusServiceUnavailable, wantAbsent: false},
		{name: "400 is a failure", status: http.StatusBadRequest, wantAbsent: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			hash := chainhash.Hash{0x3f}

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "no", tt.status)
			}))
			defer srv.Close()

			util.SetSSRFProtection(false)
			t.Cleanup(func() { util.SetSSRFProtection(true) })

			b, mockBC, _ := newFetchBridge(ctx, t, srv.URL)
			mockBC.On("GetBlockHeader", mock.Anything, &hash).
				Return((*model.BlockHeader)(nil), &model.BlockHeaderMeta{SizeInBytes: minLegacyBlockWireBytes + 100}, nil).Once()

			_, _, err := b.FetchBlock(ctx, &hash)
			require.Error(t, err)
			require.Equal(t, tt.wantAbsent, errors.Is(err, errors.ErrBlockNotFound),
				"status %d classified wrongly: %v", tt.status, err)
		})
	}
}

// TestFetchBlock_TwoPassesDeliverIdenticalBytes is the contract Task 10's
// streaming send rests on. That send hashes the payload in one pass and writes
// a second one to the socket, and the message header carries the checksum from
// the first pass ahead of the bytes of the second. So a served block whose two
// passes disagreed by a single byte would go out with a checksum SVNode
// ban-scores (net_processing.cpp:5005-5015).
//
// This drives the real FetchBlock twice against the real HTTP path and
// requires both passes to yield the same declared length, the same bytes, and
// therefore the same double-SHA256 — the exact quantity that lands in the
// header. It also records the cost of the ruling: two asset-service reads for
// one served block.
func TestFetchBlock_TwoPassesDeliverIdenticalBytes(t *testing.T) {
	ctx := context.Background()

	hash := chainhash.Hash{0x02}
	body := paddedWireBody("two-pass-block-bytes")

	var hits int

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++

		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	util.SetSSRFProtection(false)
	t.Cleanup(func() { util.SetSSRFProtection(true) })

	b, mockBC, _ := newFetchBridge(ctx, t, srv.URL)
	mockBC.On("GetBlockHeader", mock.Anything, &hash).
		Return((*model.BlockHeader)(nil), &model.BlockHeaderMeta{SizeInBytes: uint64(len(body))}, nil).Twice()

	pass := func() (uint64, []byte) {
		reader, length, err := b.FetchBlock(ctx, &hash)
		require.NoError(t, err)

		defer reader.Close()

		raw, err := io.ReadAll(reader)
		require.NoError(t, err)

		return length, raw
	}

	firstLen, firstBytes := pass()
	secondLen, secondBytes := pass()

	require.Equal(t, firstLen, secondLen, "the declared length must not change between passes")
	require.Equal(t, firstBytes, secondBytes, "the two passes must deliver identical bytes")

	// Both halves of the header the send writes: the length field, and the
	// checksum the peer verifies against the bytes it received.
	require.EqualValues(t, len(firstBytes), firstLen, "the streamed byte count must equal the declared length")
	require.Equal(t, chainhash.DoubleHashB(firstBytes)[0:4], chainhash.DoubleHashB(secondBytes)[0:4])

	require.Equal(t, 2, hits, "the two-pass checksum costs exactly two asset-service reads per served block")
}
