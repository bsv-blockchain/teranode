package repository

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	memory_blob "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// setupWithSettings mirrors setup() but lets a test adjust the settings before
// the repository (and therefore its semaphores) are built.
func setupWithSettings(t *testing.T, adjust func(s *settings.Settings)) *testContext {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	adjust(tSettings)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	repo, err := NewRepository(logger, tSettings, utxoStore, memory_blob.New(), &blockchain.Mock{}, nil,
		memory_blob.New(), memory_blob.New(), nil, nil)
	require.NoError(t, err)

	return &testContext{repo: repo, logger: logger, settings: tSettings}
}

// shortCtx returns a context that expires quickly, so a test can tell "blocked on
// a permit" apart from "returned straight away" without waiting out the 30s
// fallback deadline in acquireSemaphorePermit.
func shortCtx(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	t.Cleanup(cancel)

	return ctx
}

// TestGetLegacyBlockReaderHoldsPermitForStreamLifetime proves the legacy-block
// concurrency permit covers the producer, not just the setup. With a limit of 1,
// a second request must be refused while the first stream is still live, and must
// succeed again once that stream has been drained.
func TestGetLegacyBlockReaderHoldsPermitForStreamLifetime(t *testing.T) {
	tracing.SetupMockTracer()

	tc := setupWithSettings(t, func(s *settings.Settings) {
		s.Asset.ConcurrencyGetLegacyBlockReader = 1
	})

	block, st := newBlock(tc, t, params)

	blockchainClientMock := tc.repo.BlockchainClient.(*blockchain.Mock)
	blockchainClientMock.On("GetBlock", mock.Anything, mock.Anything).Return(block, nil)

	subtreeData := subtreepkg.NewSubtreeData(st)
	for i, tx := range params.txs {
		if i != 0 {
			require.NoError(t, subtreeData.AddTx(tx, i))
		}
	}

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)
	require.NoError(t, tc.repo.SubtreeStore.Set(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes))

	first, err := tc.repo.GetLegacyBlockReader(t.Context(), &chainhash.Hash{})
	require.NoError(t, err)

	// The first stream has not been read at all, so its producer is still live.
	_, err = tc.repo.GetLegacyBlockReader(shortCtx(t), &chainhash.Hash{})
	require.Error(t, err, "second legacy block stream must not be admitted while the first is live")

	// Drain and close the first stream; the permit must come back.
	_, err = io.Copy(io.Discard, first)
	require.NoError(t, err)
	require.NoError(t, first.Close())

	require.Eventually(t, func() bool {
		r, err := tc.repo.GetLegacyBlockReader(shortCtx(t), &chainhash.Hash{})
		if err != nil {
			return false
		}
		_, _ = io.Copy(io.Discard, r)
		_ = r.Close()

		return true
	}, 5*time.Second, 20*time.Millisecond, "permit must be released once the stream is drained")
}

// setupLegacyBlockReaderPoolTest builds a repository with both legacy-block-reader
// pools capped at 1 and a legacy block ready to serve, and holds the anonymous
// (non-marked) pool open with a live, undrained stream. Every
// TestGetLegacyBlockReader*Pool* test shares this setup: only the ctx a second
// call is made with differs.
func setupLegacyBlockReaderPoolTest(t *testing.T) *testContext {
	t.Helper()

	tracing.SetupMockTracer()

	tc := setupWithSettings(t, func(s *settings.Settings) {
		s.Asset.ConcurrencyGetLegacyBlockReader = 1
		s.Asset.ConcurrencyGetLegacyBlockReaderPeer = 1
	})

	block, st := newBlock(tc, t, params)

	blockchainClientMock := tc.repo.BlockchainClient.(*blockchain.Mock)
	blockchainClientMock.On("GetBlock", mock.Anything, mock.Anything).Return(block, nil)

	subtreeData := subtreepkg.NewSubtreeData(st)
	for i, tx := range params.txs {
		if i != 0 {
			require.NoError(t, subtreeData.AddTx(tx, i))
		}
	}

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)
	require.NoError(t, tc.repo.SubtreeStore.Set(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes))

	// Hold the anonymous (unmarked-ctx) pool with a live, undrained stream.
	anon, err := tc.repo.GetLegacyBlockReader(t.Context(), &chainhash.Hash{})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = io.Copy(io.Discard, anon)
		_ = anon.Close()
	})

	// A second, unmarked request is refused: the anonymous pool is exhausted. This
	// pins the precondition every test below depends on.
	_, err = tc.repo.GetLegacyBlockReader(shortCtx(t), &chainhash.Hash{})
	require.Error(t, err, "second anonymous legacy block stream must not be admitted while the first is live")

	return tc
}

// TestGetLegacyBlockReaderPeerPoolIsSeparateFromAnonymousPool proves a ctx marked
// by WithLegacyBlockReaderPeerPool draws from the separate peer semaphore: with
// the anonymous pool fully held by a live, undrained stream, a marked request
// must still be admitted.
func TestGetLegacyBlockReaderPeerPoolIsSeparateFromAnonymousPool(t *testing.T) {
	tc := setupLegacyBlockReaderPoolTest(t)

	peerCtx := WithLegacyBlockReaderPeerPool(shortCtx(t), true)

	peer, err := tc.repo.GetLegacyBlockReader(peerCtx, &chainhash.Hash{}, true)
	require.NoError(t, err, "a ctx marked for the peer pool must still acquire a permit while the anonymous pool is exhausted")

	_, err = io.Copy(io.Discard, peer)
	require.NoError(t, err)
	require.NoError(t, peer.Close())
}

// TestGetLegacyBlockReaderWireBlockAloneDoesNotClaimThePeerPool is the exact
// vulnerability the peer-pool separation must not reintroduce: wireBlock=true
// (?wire=1 on the HTTP route) is client-controlled and, on its own, must not
// grant the peer pool. Only httpimpl.GetLegacyBlock's loopback check does that,
// by marking ctx. A caller that passes wireBlock=true without marking ctx — as
// any anonymous HTTP client requesting ?wire=1 does — must still be refused
// while the anonymous pool is exhausted.
func TestGetLegacyBlockReaderWireBlockAloneDoesNotClaimThePeerPool(t *testing.T) {
	tc := setupLegacyBlockReaderPoolTest(t)

	_, err := tc.repo.GetLegacyBlockReader(shortCtx(t), &chainhash.Hash{}, true)
	require.Error(t, err, "wireBlock=true without a ctx marked for the peer pool must not bypass the exhausted anonymous pool")
}

// TestSubtreeNodeHashesStreamConcurrencyCap proves the new subtree stream cap is
// held for the lifetime of the returned reader.
func TestSubtreeNodeHashesStreamConcurrencyCap(t *testing.T) {
	tc := setupWithSettings(t, func(s *settings.Settings) {
		s.Asset.SubtreeStreamConcurrency = 1
	})

	st := storeTestSubtree(t, tc, 4)

	first, err := tc.repo.GetSubtreeNodeHashesReader(t.Context(), st.RootHash())
	require.NoError(t, err)

	_, err = tc.repo.GetSubtreeNodeHashesReader(shortCtx(t), st.RootHash())
	require.Error(t, err, "second subtree stream must not be admitted while the first is open")

	require.NoError(t, first.Close())

	second, err := tc.repo.GetSubtreeNodeHashesReader(shortCtx(t), st.RootHash())
	require.NoError(t, err, "permit must be released when the reader is closed")
	require.NoError(t, second.Close())
}

// TestSubtreeStreamConcurrencyUnlimitedByDefault guards the catchup path: the new
// cap must be off unless an operator turns it on.
func TestSubtreeStreamConcurrencyUnlimitedByDefault(t *testing.T) {
	tc := setupWithSettings(t, func(_ *settings.Settings) {})
	require.Equal(t, 0, tc.settings.Asset.SubtreeStreamConcurrency)

	st := storeTestSubtree(t, tc, 4)

	readers := make([]io.ReadCloser, 0, 64)
	for i := 0; i < 64; i++ {
		r, err := tc.repo.GetSubtreeNodeHashesReader(shortCtx(t), st.RootHash())
		require.NoError(t, err)

		readers = append(readers, r)
	}

	for _, r := range readers {
		require.NoError(t, r.Close())
	}
}

// TestGetSubtreeDataReaderMissingHashDoesNotQueueOnReaderPermit proves the cheap
// existence check runs before the reader permit is taken, so a flood of requests
// for hashes this node does not have cannot occupy the streaming budget.
func TestGetSubtreeDataReaderMissingHashDoesNotQueueOnReaderPermit(t *testing.T) {
	tc := setupWithSettings(t, func(s *settings.Settings) {
		s.Asset.ConcurrencyGetSubtreeDataReader = 1
	})

	st := storeTestSubtree(t, tc, 2)
	require.NoError(t, tc.repo.SubtreeStore.Set(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtreeData, []byte("data")))

	// Occupy the single reader permit with a live stream.
	held, err := tc.repo.GetSubtreeDataReader(t.Context(), st.RootHash())
	require.NoError(t, err)

	defer func() {
		require.NoError(t, held.Close())
	}()

	missing := chainhash.HashH([]byte("no such subtree"))

	start := time.Now()
	_, err = tc.repo.GetSubtreeDataReader(shortCtx(t), &missing)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrNotFound), "expected NotFound, got %v", err)
	require.Less(t, time.Since(start), 200*time.Millisecond, "404 must not wait on the reader permit")
}

// countingReadSeeker records how many bytes were actually pulled from the
// underlying data, so a pagination test can assert the offset was skipped rather
// than read and thrown away.
type countingReadSeeker struct {
	r    *bytes.Reader
	read int64
}

func (c *countingReadSeeker) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.read += int64(n)

	return n, err
}

func (c *countingReadSeeker) Seek(offset int64, whence int) (int64, error) {
	return c.r.Seek(offset, whence)
}

func (c *countingReadSeeker) Close() error { return nil }

// TestSubtreeNodesPageSeeksPastOffset proves a deep page no longer reads and
// discards every preceding node record when the store reader can seek.
func TestSubtreeNodesPageSeeksPastOffset(t *testing.T) {
	const totalNodes = 4096

	serialized := serializeTestSubtreeStream(t, totalNodes)
	counting := &countingReadSeeker{r: bytes.NewReader(serialized)}

	nodes, total, err := readSubtreeNodesPageFromReader(t.Context(), counting, totalNodes-1, 1)
	require.NoError(t, err)
	require.Equal(t, totalNodes, total)
	require.Len(t, nodes, 1)

	// Without a seek this reads the whole file; the buffered reader means the
	// honest ceiling for header + one record is a couple of buffer fills.
	require.Less(t, counting.read, int64(4*subtreeStreamBufferSize),
		"deep page must seek past the offset instead of scanning it")

	// The seek must land on exactly the same record the scan would have reached.
	scanned, _, err := readSubtreeNodesPageFromReader(t.Context(), onlyReader{bytes.NewReader(serialized)}, totalNodes-1, 1)
	require.NoError(t, err)
	require.Equal(t, scanned, nodes)
}

// onlyReader hides the Seek method of the wrapped reader, forcing the discard
// fallback used by stores that hand back non-seekable readers.
type onlyReader struct {
	io.Reader
}

// TestSubtreePageSeeksPastOffset is the same assertion for the sibling page API
// behind /subtree/:hash/json.
func TestSubtreePageSeeksPastOffset(t *testing.T) {
	const totalNodes = 4096

	serialized := serializeTestSubtreeStream(t, totalNodes)
	counting := &countingReadSeeker{r: bytes.NewReader(serialized)}

	st, offset, total, err := readSubtreePageFromReader(t.Context(), counting, totalNodes-1, 1)
	require.NoError(t, err)
	require.Equal(t, totalNodes-1, offset)
	require.Equal(t, totalNodes, total)
	require.Len(t, st.Nodes, 1)

	require.Less(t, counting.read, int64(4*subtreeStreamBufferSize),
		"deep page must seek past the offset instead of scanning it")

	scanned, _, _, err := readSubtreePageFromReader(t.Context(), onlyReader{bytes.NewReader(serialized)}, totalNodes-1, 1)
	require.NoError(t, err)
	require.Equal(t, scanned.Nodes, st.Nodes)
}

// unserializableReconstructionStore hands back a decorated record whose
// transaction is the snapshot-reconstruction shape: it serializes cleanly but has
// no inputs, so it does not hash to the requested txid.
type unserializableReconstructionStore struct {
	utxo.Store
}

func (s *unserializableReconstructionStore) BatchDecorate(ctx context.Context, items []*utxo.UnresolvedMetaData, f ...fields.FieldName) error {
	if err := s.Store.BatchDecorate(ctx, items, f...); err != nil {
		return err
	}

	for _, item := range items {
		if item.Data == nil || item.Data.Tx == nil {
			continue
		}

		// Strip the inputs, exactly what a UTXO-set snapshot leaves behind.
		stripped := bt.NewTx()
		stripped.Version = item.Data.Tx.Version
		stripped.LockTime = item.Data.Tx.LockTime
		stripped.Outputs = item.Data.Tx.Outputs

		shallow := *item.Data
		shallow.Tx = stripped
		item.Data = &shallow
	}

	return nil
}

// TestGetTxsRejectsIncompleteReconstruction proves the batch reconstruction path
// applies the same completeness/identity gate as the single-transaction path, so
// an incomplete reconstruction is never streamed or persisted.
func TestGetTxsRejectsIncompleteReconstruction(t *testing.T) {
	tracing.SetupMockTracer()

	tc := setup(t)
	tc.repo.UtxoStore = &unserializableReconstructionStore{Store: tc.repo.UtxoStore}

	_, _, err := tc.repo.UtxoStore.SpendAndCreate(t.Context(), tx1, 1, utxo.WithCreateOnly())
	require.NoError(t, err)

	hashes := []chainhash.Hash{*tx1.TxIDChainHash()}
	metaSlice := make([]*meta.Data, len(hashes))

	missed, err := tc.repo.getTxs(t.Context(), hashes, metaSlice)
	require.NoError(t, err)
	require.Equal(t, 1, missed, "an incomplete reconstruction must count as missing, not be written out")
	require.Nil(t, metaSlice[0], "a rejected reconstruction must not be handed to the writer")
}

// storeTestSubtree writes a subtree file with numNodes leaves into the subtree store.
func storeTestSubtree(t *testing.T, tc *testContext, numNodes int) *subtreepkg.Subtree {
	t.Helper()

	st, err := subtreepkg.NewTreeByLeafCount(numNodes)
	require.NoError(t, err)

	require.NoError(t, st.AddCoinbaseNode())

	for i := 1; i < numNodes; i++ {
		require.NoError(t, st.AddNode(chainhash.HashH([]byte{byte(i)}), uint64(i), uint64(i)))
	}

	serialized, err := st.Serialize()
	require.NoError(t, err)

	require.NoError(t, tc.repo.SubtreeStore.Set(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtree, serialized))

	return st
}

// serializeTestSubtreeStream builds the on-disk subtree byte stream for numNodes
// leaves without going through the store.
func serializeTestSubtreeStream(t *testing.T, numNodes int) []byte {
	t.Helper()

	st, err := subtreepkg.NewTreeByLeafCount(numNodes)
	require.NoError(t, err)

	require.NoError(t, st.AddCoinbaseNode())

	for i := 1; i < numNodes; i++ {
		require.NoError(t, st.AddNode(chainhash.HashH([]byte{byte(i), byte(i >> 8)}), uint64(i), uint64(i)))
	}

	serialized, err := st.Serialize()
	require.NoError(t, err)

	return serialized
}

// uniqueStreamTestTx builds a transaction that is unique per index and complete
// enough to pass the repository's reconstruction gate. A bare &bt.Tx{} with no
// inputs is exactly the UTXO-snapshot shape that isRequestedTransaction rejects,
// so streaming fixtures have to look like real transactions.
func uniqueStreamTestTx(t *testing.T, i int) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000001",
		uint32(i), "76a914000000000000000000000000000000000000000088ac", 1000)) //nolint:gosec

	lockingScript, err := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
	require.NoError(t, err)

	tx.AddOutput(&bt.Output{Satoshis: 900, LockingScript: lockingScript})

	unlockingScript, err := bscript.NewFromHexString("0101")
	require.NoError(t, err)

	tx.Inputs[0].UnlockingScript = unlockingScript

	tx.Version = uint32(i)  //nolint:gosec
	tx.LockTime = uint32(i) //nolint:gosec

	return tx
}
