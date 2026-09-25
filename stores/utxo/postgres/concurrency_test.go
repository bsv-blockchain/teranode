package postgres

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// TestFreezeUTXOsConcurrentSingleWinner is a regression test for the FreezeUTXOs
// double-freeze race: the freeze decision and the write are now a single guarded
// UPDATE, so of N concurrent freezes of the same UTXO exactly one succeeds and the
// rest are rejected. With the old SELECT-then-UPDATE, several could pass the check
// and all "succeed".
func TestFreezeUTXOsConcurrentSingleWinner(t *testing.T) {
	store, ctx := setupTestStore(t)

	tx := testExtendedTx(t)
	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()
	utxoHash, err := util.UTXOHashFromOutput(txHash, tx.Outputs[0], 0)
	require.NoError(t, err)
	spend := &utxo.Spend{TxID: txHash, Vout: 0, UTXOHash: utxoHash}

	const n = 16
	var (
		wg        sync.WaitGroup
		successes atomic.Int32
	)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if err := store.FreezeUTXOs(ctx, []*utxo.Spend{spend}, nil); err == nil {
				successes.Add(1)
			}
		}()
	}
	wg.Wait()

	require.Equal(t, int32(1), successes.Load(), "exactly one concurrent freeze should succeed")

	// Verify frozen state via the txs.out_frozens packed bitmap (bit 0 = vout 0).
	var frozen bool
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT out_frozens IS NOT NULL AND get_bit(out_frozens, 0) = 1 FROM txs WHERE hash = $1`, txHash[:]).Scan(&frozen))
	require.True(t, frozen, "the UTXO must end up frozen")
}

// TestUnsetMinedConcurrentNoLostBlock is a regression test for the unsetMinedMulti
// TOCTOU: a reorg unset of one block_id must not clobber a SetMinedMulti append of
// a different block that interleaves with it. The unset is now a single atomic
// UPDATE (array_remove + position-matched re-aggregation), so it re-evaluates
// against the locked row version and preserves the concurrent append. The old
// SELECT-then-UPDATE could read [1,2], have [1,2,3] appended underneath it, then
// write back [2] — silently dropping block 3.
func TestUnsetMinedConcurrentNoLostBlock(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(200))

	const iters = 40
	for i := 0; i < iters; i++ {
		tx := testExtendedTx(t)
		tx.LockTime = uint32(i + 1) // unique txid per iteration
		_, err := store.Create(ctx, tx, 100)
		require.NoError(t, err)
		h := tx.TxIDChainHash()

		// Mine into blocks 1 and 2.
		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{h},
			utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100, OnLongestChain: true})
		require.NoError(t, err)
		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{h},
			utxo.MinedBlockInfo{BlockID: 2, BlockHeight: 101, OnLongestChain: true})
		require.NoError(t, err)

		// Concurrently: unset block 1 (reorg) and append block 3 (new mine).
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = store.SetMinedMulti(ctx, []*chainhash.Hash{h},
				utxo.MinedBlockInfo{BlockID: 1, UnsetMined: true})
		}()
		go func() {
			defer wg.Done()
			_, _ = store.SetMinedMulti(ctx, []*chainhash.Hash{h},
				utxo.MinedBlockInfo{BlockID: 3, BlockHeight: 102, OnLongestChain: true})
		}()
		wg.Wait()

		got, err := store.Get(ctx, h, fields.BlockIDs)
		require.NoError(t, err)
		require.Contains(t, got.BlockIDs, uint32(3),
			"iter %d: concurrently-appended block 3 was lost: %v", i, got.BlockIDs)
		require.NotContains(t, got.BlockIDs, uint32(1),
			"iter %d: unset block 1 still present: %v", i, got.BlockIDs)
	}
}
