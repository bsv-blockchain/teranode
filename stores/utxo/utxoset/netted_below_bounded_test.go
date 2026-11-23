package utxoset

import (
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

// A chunk ends at nettedBelowMaxChunkOutputs outputs, not only at a transaction count. On mainnet
// at block 814,043 on 2026-10-08 the 16 parallel chunks of a 30,595-transaction block held about
// 28 million outputs, and the netted write used 13 GB until the OOM killer stopped the node. Each
// workload transaction has two outputs, so with a limit of three each chunk has one transaction.
func TestNettedBelowChunksEndAtTheOutputLimit(t *testing.T) {
	s, ctx := newTestStore(t)

	const height = 950

	old := nettedBelowMaxChunkOutputs
	nettedBelowMaxChunkOutputs = 3

	t.Cleanup(func() { nettedBelowMaxChunkOutputs = old; nettedBelowFault = nil })

	var (
		mu     sync.Mutex
		chunks [][]int
	)

	nettedBelowFault = func(chunk []int) error {
		mu.Lock()
		defer mu.Unlock()

		chunks = append(chunks, append([]int(nil), chunk...))

		return nil
	}

	w := tests.BuildMultiWorkload(t, 0x7a, 3, 4)
	w.StoreRoots(t, s, height-1)

	list := outpointOnly(w.Txs)

	results, err := s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
	require.NoError(t, err)

	for i, r := range results {
		require.Equal(t, utxo.MultiTxCreated, r.Status, "tx %d: %v", i, r.Err)
	}

	require.Len(t, chunks, len(list), "one transaction for each chunk")

	for _, c := range chunks {
		require.Len(t, c, 1)
	}

	require.Equal(t, expectedNetState(w.Roots, list), readNetState(t, s, ctx, w.Roots, list, height))
}

// The coins of a chunk are written in statements of at most nettedCoinsBatchRows rows, so one
// transaction with millions of outputs does not build one statement of all of them. The net
// effect is the same.
func TestNettedBelowWritesTheCoinsInBatches(t *testing.T) {
	s, ctx := newTestStore(t)

	const height = 960

	old := nettedCoinsBatchRows
	nettedCoinsBatchRows = 1

	t.Cleanup(func() { nettedCoinsBatchRows = old })

	w := tests.BuildMultiWorkload(t, 0x7b, 3, 4)
	w.StoreRoots(t, s, height-1)

	list := outpointOnly(w.Txs)

	_, err := s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
	require.NoError(t, err)

	require.Equal(t, expectedNetState(w.Roots, list), readNetState(t, s, ctx, w.Roots, list, height))
}

// A coin statement ends at nettedCoinsBatchBytes as well as at nettedCoinsBatchRows. On mainnet at
// block 863,817 on 2026-10-09, 50,000 rows with large scripts went past the Postgres limit of 1 GB
// for one protocol message ("write failed: message body too large"), and the block failed on
// every attempt.
func TestCoinBatchesEndAtTheByteLimit(t *testing.T) {
	scripts := [][]byte{make([]byte, 100), make([]byte, 100), make([]byte, 100), make([]byte, 10), make([]byte, 10)}
	rows := []int{0, 1, 2, 3, 4}

	// Each row costs its script and coinRowOverhead bytes. With room for two 100-byte rows, rows 2
	// and 3 fit together and row 4 does not.
	batches := coinBatches(rows, scripts, 10, 2*(100+coinRowOverhead))
	require.Equal(t, [][]int{{0, 1}, {2, 3}, {4}}, batches)

	require.Equal(t, [][]int{{0, 1}, {2, 3}, {4}}, coinBatches(rows, scripts, 2, 1<<30), "the row limit still applies")

	require.Equal(t, [][]int{{0}, {1}, {2}, {3}, {4}}, coinBatches(rows, scripts, 10, 1), "a row larger than the limit is a batch of its own")

	require.Empty(t, coinBatches(nil, scripts, 10, 1<<30))
}

// Coins written in batches cut by bytes give the same net effect.
func TestNettedBelowWritesTheCoinsInByteBoundedBatches(t *testing.T) {
	s, ctx := newTestStore(t)

	const height = 970

	old := nettedCoinsBatchBytes
	nettedCoinsBatchBytes = 1

	t.Cleanup(func() { nettedCoinsBatchBytes = old })

	w := tests.BuildMultiWorkload(t, 0x7c, 3, 4)
	w.StoreRoots(t, s, height-1)

	list := outpointOnly(w.Txs)

	_, err := s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
	require.NoError(t, err)

	require.Equal(t, expectedNetState(w.Roots, list), readNetState(t, s, ctx, w.Roots, list, height))
}
