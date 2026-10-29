package utxoset

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// TestSetLockedConcurrentBatchesDoNotDeadlock pins the rule that lets the lock batcher run
// batches in parallel. Many callers set and clear the lock on overlapping sets of transactions
// at once, through the batcher (one hash) and the direct path (many hashes, in random order).
// Two batches naming the same transactions in different orders deadlock, and PostgreSQL stops
// one; without the retry in setLockedDirect that call fails. Every call must succeed, and at the
// end every transaction reads back unlocked.
func TestSetLockedConcurrentBatchesDoNotDeadlock(t *testing.T) {
	s, ctx := newTestStoreWith(t, func(ts *settings.Settings) {
		ts.UtxoStore.LockedBatcherSize = 16
		ts.UtxoStore.LockedBatcherDurationMillis = 1
		ts.UtxoStore.BatcherMaxConcurrent = 8
		ts.BatcherBackground = true
	})
	require.NotNil(t, s.lockBatcher)

	const n = 40

	txs := make([]chainhash.Hash, n)

	for i := range txs {
		tx := mkTx(t, 3, uint64(10_000+i))
		_, err := s.Create(ctx, tx, 100)
		require.NoError(t, err)

		txs[i] = *tx.TxIDChainHash()
	}

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)

	for w := 0; w < 32; w++ {
		w := w

		wg.Add(1)

		go func() {
			defer wg.Done()

			rng := rand.New(rand.NewSource(int64(w))) //nolint:gosec // test ordering only

			for round := 0; round < 25; round++ {
				var err error

				if w%2 == 0 {
					err = s.SetLocked(ctx, []chainhash.Hash{txs[rng.Intn(n)]}, round%2 == 0)
				} else {
					picks := rng.Perm(n)[:8]
					hs := make([]chainhash.Hash, 0, len(picks))

					for _, p := range picks {
						hs = append(hs, txs[p])
					}

					err = s.SetLocked(ctx, hs, round%2 == 0)
				}

				if err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
			}
		}()
	}

	wg.Wait()
	require.Empty(t, errs, "a concurrent SetLocked failed, which is how a deadlock shows")

	require.NoError(t, s.SetLocked(ctx, txs, false))

	for i := range txs {
		got, err := s.Get(ctx, &txs[i], fields.Locked)
		require.NoError(t, err)
		require.False(t, got.Locked)
	}
}

// TestSetLockedConcurrentWithMiningDoesNotDeadlock: a block that contains a transaction can arrive
// while that transaction's two-phase-commit release is in flight, and the mine releases the lock
// too (releaseLocksOnMine). The two touch the same identity and containment rows, and so do two
// mines of competing blocks. Many releases and many mines of overlapping sets run at once here.
// They can deadlock, and every call must still succeed through the retry. The retry itself is
// pinned by TestWithDeadlockRetry, because a run here is not certain to deadlock.
func TestSetLockedConcurrentWithMiningDoesNotDeadlock(t *testing.T) {
	s, ctx := newTestStoreWith(t, func(ts *settings.Settings) {
		withCheckpoints(ts, nil)
		ts.UtxoStore.LockedBatcherSize = 16
		ts.UtxoStore.LockedBatcherDurationMillis = 1
		ts.UtxoStore.BatcherMaxConcurrent = 8
		ts.BatcherBackground = true
	})

	const n = 60

	txs := make([]chainhash.Hash, n)

	for i := range txs {
		tx := mkTx(t, 2, uint64(20_000+i))
		_, err := s.Create(ctx, tx, 1_000, utxo.WithLocked(true))
		require.NoError(t, err)

		txs[i] = *tx.TxIDChainHash()
	}

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)

	record := func(err error) {
		if err != nil {
			mu.Lock()
			errs = append(errs, err)
			mu.Unlock()
		}
	}

	for w := 0; w < 24; w++ {
		w := w

		wg.Add(1)

		go func() {
			defer wg.Done()

			rng := rand.New(rand.NewSource(int64(w))) //nolint:gosec // test ordering only

			for round := 0; round < 15; round++ {
				picks := rng.Perm(n)[:10]

				if w%3 == 0 {
					hs := make([]*chainhash.Hash, 0, len(picks))
					for _, p := range picks {
						hs = append(hs, &txs[p])
					}

					_, err := s.SetMinedMulti(ctx, hs, utxo.MinedBlockInfo{
						BlockID: uint32(100 + w*20 + round), BlockHeight: 1_001, OnLongestChain: true}) //nolint:gosec // small
					record(err)

					continue
				}

				if w%2 == 0 {
					record(s.SetLocked(ctx, []chainhash.Hash{txs[picks[0]]}, round%2 == 0))

					continue
				}

				hs := make([]chainhash.Hash, 0, len(picks))
				for _, p := range picks {
					hs = append(hs, txs[p])
				}

				record(s.SetLocked(ctx, hs, round%2 == 0))
			}
		}()
	}

	wg.Wait()
	require.Empty(t, errs, "a concurrent release or mine failed, which is how a deadlock shows")

}
