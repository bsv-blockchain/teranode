package blockpersister

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// gatedDecorateStore answers BatchDecorate from txs, one tx per batch, and holds batch 0 back
// until every batch that can be fetched while it runs has been. With an errgroup limit of L, those
// are batches 1 to L-1, which are inside the writer's window and park, and batches L to 2L-2,
// which take the slots the parked ones freed and then wait on the window. Batch 0 then completes,
// or fails with batch0Err. Only BatchDecorate is implemented.
type gatedDecorateStore struct {
	utxo.Store

	txs       []*bt.Tx
	threshold int64
	others    atomic.Int64
	released  chan struct{}
	batch0Err error
}

func (s *gatedDecorateStore) BatchDecorate(_ context.Context, unresolved []*utxo.UnresolvedMetaData, _ ...fields.FieldName) error {
	if unresolved[0].Idx == 0 {
		select {
		case <-s.released:
		case <-time.After(30 * time.Second):
			return errors.NewProcessingError("only %d of %d batches were fetched while batch 0 was held back", s.others.Load(), s.threshold)
		}

		if s.batch0Err != nil {
			return s.batch0Err
		}
	}

	for _, missing := range unresolved {
		missing.Data = &meta.Data{Tx: s.txs[missing.Idx]}
	}

	if unresolved[0].Idx != 0 && s.others.Add(1) == s.threshold {
		close(s.released)
	}

	return nil
}

// createSubtreeDataWithBatch0HeldBack runs CreateSubtreeDataFileStreaming on a subtree of one-tx
// batches whose batch 0 is fetched last, and fails if it does not return within a minute.
func createSubtreeDataWithBatch0HeldBack(t *testing.T, batch0Err error) (subtreeStore *memory.Memory, subtree *subtreepkg.Subtree, txs []*bt.Tx, err error) {
	t.Helper()

	limit := subtreeDataFetchConcurrency()
	txCount := 3 * limit

	txs = make([]*bt.Tx, txCount)
	for i := range txs {
		txs[i] = bt.NewTx()
		require.NoError(t, txs[i].From("0000000000000000000000000000000000000000000000000000000000000001", uint32(i), "76a914000000000000000000000000000000000000000088ac", 2000))
		require.NoError(t, txs[i].AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))
	}

	subtree, err = subtreepkg.NewTreeByLeafCount(subtreepkg.CeilPowerOfTwo(txCount))
	require.NoError(t, err)

	for _, tx := range txs {
		require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 1, uint64(tx.Size())))
	}

	subtreeStore = memory.New()
	t.Cleanup(func() { _ = subtreeStore.Close(context.Background()) })

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	utxoStore := &gatedDecorateStore{
		txs:       txs,
		threshold: int64(2*limit - 2),
		released:  make(chan struct{}),
		batch0Err: batch0Err,
	}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Block.ProcessTxMetaUsingStoreBatchSize = 1

	persister := New(t.Context(), ulogger.TestLogger{}, tSettings, nil, subtreeStore, utxoStore, nil)

	// No coinbase placeholder, so the block is only used for logging.
	block := &model.Block{
		Header: &model.BlockHeader{
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
		},
		Subtrees: []*chainhash.Hash{subtree.RootHash()},
	}

	done := make(chan error, 1)

	go func() { done <- persister.CreateSubtreeDataFileStreaming(t.Context(), *subtree.RootHash(), block, 1) }()

	select {
	case err = <-done:
	case <-time.After(time.Minute):
		t.Fatal("CreateSubtreeDataFileStreaming did not return")
	}

	return subtreeStore, subtree, txs, err
}

// TestCreateSubtreeDataFileStreaming_WritesInOrderWhenBatch0FinishesLast holds batch 0 back until
// every later batch that can be fetched alongside it has been: some park in the writer, the rest
// wait on its window. The file must still hold every transaction in node order.
func TestCreateSubtreeDataFileStreaming_WritesInOrderWhenBatch0FinishesLast(t *testing.T) {
	subtreeStore, subtree, txs, err := createSubtreeDataWithBatch0HeldBack(t, nil)
	require.NoError(t, err)

	data, err := subtreeStore.Get(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)

	var want []byte
	for _, tx := range txs {
		want = append(want, tx.Bytes()...)
	}

	require.Equal(t, want, data)
}

// TestCreateSubtreeDataFileStreaming_ReturnsWhenBatch0FailsWhileOthersWait is the failure issue
// 1916 warns about. Batch 0 fails after the batches beyond the window are already waiting for it.
// It never reaches the writer, so only the errgroup's cancellation can wake them, and the call
// must return batch 0's error instead of hanging. No file may be left behind.
func TestCreateSubtreeDataFileStreaming_ReturnsWhenBatch0FailsWhileOthersWait(t *testing.T) {
	batch0Err := errors.NewServiceError("batch 0 failed to fetch")

	subtreeStore, subtree, _, err := createSubtreeDataWithBatch0HeldBack(t, batch0Err)
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to batch decorate")
	require.ErrorContains(t, err, "batch 0 failed to fetch")

	exists, err := subtreeStore.Exists(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)
	require.False(t, exists)
}
