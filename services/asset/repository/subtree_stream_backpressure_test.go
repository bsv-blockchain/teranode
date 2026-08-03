package repository

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// headStallingStore wraps a utxo.Store and stalls the BatchDecorate call belonging
// to the first subtree chunk (the one containing headHash) until released, while
// letting every other chunk's fetch complete immediately. This reproduces the
// head-of-line blocking condition: the in-order chunk is the slowest, so all the
// later out-of-order chunks pile up waiting to be written.
type headStallingStore struct {
	utxo.Store

	headHash        *chainhash.Hash
	release         chan struct{}
	othersCompleted atomic.Int64
}

func (s *headStallingStore) BatchDecorate(ctx context.Context, items []*utxo.UnresolvedMetaData, f ...fields.FieldName) error {
	isHead := false

	for _, it := range items {
		if bytes.Equal(it.Hash[:], s.headHash[:]) {
			isHead = true
			break
		}
	}

	if isHead {
		// Stall the head chunk. It is released either when the test's timer fires
		// (the only completion signal the in-order chunk gets under a correct,
		// backpressured scheduler) or when the stream is torn down via context
		// cancellation (which is how the buggy scheduler's abort frees us).
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	err := s.Store.BatchDecorate(ctx, items, f...)

	if !isHead {
		s.othersCompleted.Add(1)
	}

	return err
}

// TestSubtreeStreamingHeadOfLineBackpressure proves the asset subtree-data streaming
// path does not truncate a large response when the first (in-order) chunk is the
// slowest to fetch. Before the windowed-backpressure fix the scheduler ran arbitrarily
// far ahead of the chunk being written, overflowed the `2*concurrency` pending buffer,
// and aborted the whole response mid-stream ("pending chunk buffer exceeded cap N").
// With backpressure the scheduler never runs more than the window ahead of nextChunk,
// so the stream always completes in order.
func TestSubtreeStreamingHeadOfLineBackpressure(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := setup(t)

	const concurrency = 4
	const chunkSize = 4

	ctx.settings.Asset.SubtreeDataStreamingConcurrency = concurrency
	ctx.settings.Asset.SubtreeDataStreamingChunkSize = chunkSize

	// 64 leaves (coinbase + 63 txs) / chunkSize 4 => 16 chunks. With the head chunk
	// stalled, 15 later chunks can complete — far past the cap of 2*concurrency = 8.
	numTxs := 63
	txs := make([]*bt.Tx, numTxs+1)
	txs[0] = coinbase

	for i := 1; i <= numTxs; i++ {
		txs[i] = &bt.Tx{
			Version:  uint32(i), //nolint:gosec
			LockTime: uint32(i), //nolint:gosec
			Inputs:   []*bt.Input{},
			Outputs:  []*bt.Output{},
		}
	}

	testParams := blockInfo{
		version:           1,
		bits:              "2000ffff",
		previousBlockHash: "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206",
		height:            1,
		nonce:             2083236893,
		timestamp:         uint32(time.Now().Unix()), //nolint:gosec
		txs:               txs,
	}

	block, subtree := newBlockWithCorrectMerkleRoot(ctx, t, testParams)

	blockchainClientMock := ctx.repo.BlockchainClient.(*blockchain.Mock)
	blockchainClientMock.On("GetBlock", mock.Anything, mock.Anything).Return(block, nil).Once()

	// Populate the real utxo store, then wrap it so the head chunk stalls.
	for i := 1; i < len(txs); i++ {
		_, _, err := ctx.repo.UtxoStore.SpendAndCreate(context.Background(), txs[i], testParams.height, utxo.WithCreateOnly())
		require.NoError(t, err)
	}

	// Store the subtree so GetSubtreeTxIDsReader can stream its tx IDs.
	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, ctx.repo.SubtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	stalling := &headStallingStore{
		Store:    ctx.repo.UtxoStore,
		headHash: txs[1].TxIDChainHash(), // txs[1] lives in chunk 0 (leaves 0..3)
		release:  make(chan struct{}),
	}
	ctx.repo.UtxoStore = stalling

	// The in-order head chunk has no upstream completion event of its own under a
	// correct scheduler — release it after a bounded delay that is comfortably longer
	// than the time the buggy scheduler needs to overflow its pending buffer (which it
	// does autonomously in milliseconds on in-memory stores). The buggy path therefore
	// aborts long before this fires; this timer only drives the fixed (passing) path.
	releaseTimer := time.AfterFunc(800*time.Millisecond, func() { close(stalling.release) })
	defer releaseTimer.Stop()

	r, err := ctx.repo.GetLegacyBlockReader(context.Background(), &chainhash.Hash{})
	require.NoError(t, err)

	// Skip the legacy block header: magic(4)+size(4), block header(80), tx count varint.
	buf := make([]byte, 4096)
	_, err = io.ReadFull(r, buf[:8])
	require.NoError(t, err)
	_, err = io.ReadFull(r, buf[:80])
	require.NoError(t, err)
	_, err = r.Read(buf[:10])
	require.NoError(t, err)

	txCount, _ := bt.NewVarIntFromBytes(buf[:10])
	require.Equal(t, uint64(len(txs)), uint64(txCount))

	// The stream must deliver every transaction — no mid-stream abort/truncation. The
	// assertion now says what it always claimed: a clean end, not io.ErrClosedPipe, which
	// was the producer reporting its own success as a failure.
	allTxData, err := io.ReadAll(r)
	require.NoError(t, err, "stream did not complete cleanly (truncated/aborted)")

	offset := 0
	for i := 0; i < len(txs); i++ {
		parsedTx, size, parseErr := bt.NewTxFromStream(allTxData[offset:])
		require.NoError(t, parseErr, "failed to parse transaction %d — response was truncated", i)
		require.NotNil(t, parsedTx)

		if i == 0 {
			require.Equal(t, coinbase.TxID(), parsedTx.TxID(), "coinbase mismatch at position 0")
		} else {
			require.Equal(t, uint32(i), parsedTx.Version, "transaction at position %d out of order", i)
			require.Equal(t, txs[i].TxID(), parsedTx.TxID(), "transaction %d TxID mismatch", i)
		}

		offset += size
	}

	require.Equal(t, len(allTxData), offset, "not all transaction data was consumed")
	require.GreaterOrEqual(t, stalling.othersCompleted.Load(), int64(concurrency),
		"expected the scheduler to fetch later chunks while the head chunk stalled")
}
