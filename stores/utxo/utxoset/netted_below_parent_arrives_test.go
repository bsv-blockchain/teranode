package utxoset

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

// A parent in the list can hold coins the netting never wrote: it reached the store unmined,
// which writes every output as a coin. An output a transaction of the list spends must not stay
// a coin once the list has applied.

// The child's chunk commits and the parent's does not. Before the repeat, the parent arrives
// unmined through propagation, which writes all its outputs as coins. On the repeat the child
// already has its record and is skipped, so the parent's output it spends must still go.
func TestNettedBelowParentArrivingBetweenAttempts(t *testing.T) {
	s, ctx := newTestStore(t)

	const height = 970

	withNettedBelowChunkTxs(t, 1)

	w := tests.BuildMultiWorkload(t, 0x7c, 2, 2)
	w.StoreRoots(t, s, height-1)

	list := outpointOnly(w.Txs)
	parent := w.Txs[0]

	// The parent's chunk fails, but only once the child's chunk has committed: the chunks run
	// at the same time, and the first failure cancels the ones not yet started.
	childDone := make(chan struct{})
	crash := errors.NewProcessingError("injected crash")
	nettedBelowFault = func(chunk []int) error {
		switch chunk[0] {
		case 0:
			select {
			case <-childDone:
			case <-time.After(10 * time.Second):
			}

			return crash
		case 2:
			defer close(childDone)
		}

		return nil
	}

	t.Cleanup(func() { nettedBelowFault = nil })

	_, err := s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
	require.Error(t, err)

	nettedBelowFault = nil

	var childMined int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM tx_mined WHERE txid = $1 AND mined_height = $2`,
		w.Txs[2].TxIDChainHash()[:], int32(height)).Scan(&childMined))
	require.Equal(t, 1, childMined, "the child's chunk committed before the crash")

	var parentMined int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM tx_mined WHERE txid = $1`, parent.TxIDChainHash()[:]).Scan(&parentMined))
	require.Zero(t, parentMined, "the parent's chunk did not")

	// The parent arrives unmined: its own spends made, every output a coin.
	_, _, err = s.SpendAndCreate(ctx, parent, height-1)
	require.NoError(t, err)

	results, err := s.SpendAndCreateMulti(ctx, outpointOnly(w.Txs), height, belowCheckpointOptions(height)...)
	require.NoError(t, err)

	for i, r := range results {
		require.Contains(t, []utxo.SpendAndCreateMultiStatus{utxo.MultiTxCreated, utxo.MultiTxExisted}, r.Status, "tx %d: %v", i, r.Err)
	}

	require.Equal(t, expectedNetState(w.Roots, list).coins, readNetState(t, s, ctx, w.Roots, list, height).coins,
		"an output the list spends is not a coin")
}
