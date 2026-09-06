package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// liveRows counts UTXO rows for a transaction.
func liveRows(t *testing.T, s *Store, ctx context.Context, txid []byte) int {
	t.Helper()

	var n int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM utxo WHERE txid = $1`, txid).Scan(&n))

	return n
}

// TestApplyBlockReplayDoesNotDuplicateOutputs is the coin-inflation guard.
//
// The UTXO table's ukey is a 96-bit prefix and is deliberately NON-UNIQUE, so createSQL
// has no ON CONFLICT to make an insert idempotent. Replay is routine — catchup, a
// restart mid-window, the post-restart unrequested-block storm — so without a durable
// record of which blocks have been applied, re-offering a block inserts every output a
// second time and nothing rejects it. Those duplicate rows are independently spendable.
//
// The applied_block ledger is what makes application idempotent, authorised by ground
// truth (a row written in the same transaction as the work it describes) rather than by
// a counter or a clock.
func TestApplyBlockReplayDoesNotDuplicateOutputs(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 2, 5_000)
	txid := tx.TxIDChainHash()
	blockHash := chainhash.Hash{0xab, 0xcd}

	apply := func() (bool, error) {
		return s.ApplyBlock(ctx, &blockHash, 100, func(dbTx pgx.Tx) error {
			_, err := s.createIn(ctx, dbTx, tx, 100, 100)
			return err
		})
	}

	applied, err := apply()
	require.NoError(t, err)
	require.True(t, applied, "first application must run")
	require.Equal(t, 2, liveRows(t, s, ctx, txid[:]), "two spendable outputs")

	applied, err = apply()
	require.NoError(t, err)
	require.Equal(t, 2, liveRows(t, s, ctx, txid[:]),
		"replay must not resurrect or duplicate outputs")
	require.False(t, applied, "replay of an applied block must be skipped")
}

// TestApplyBlockFailureLeavesBlockRetryable is the mirror image of the inflation guard.
//
// If a failed application still marked the block as applied, the block could never be
// retried: its outputs would never exist and every later block spending them would fail
// forever. That is silent loss rather than silent inflation, and it is the more dangerous
// of the two because nothing downstream would flag it. The claim and the work must
// therefore commit or roll back together.
func TestApplyBlockFailureLeavesBlockRetryable(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 2, 5_000)
	txid := tx.TxIDChainHash()
	blockHash := chainhash.Hash{0x11, 0x22}
	boom := errors.NewProcessingError("chunk failed midway")

	// The application creates its outputs and then fails.
	applied, err := s.ApplyBlock(ctx, &blockHash, 100, func(dbTx pgx.Tx) error {
		if _, cErr := s.createIn(ctx, dbTx, tx, 100, 100); cErr != nil {
			return cErr
		}

		return boom
	})
	require.Error(t, err)
	require.False(t, applied)
	require.Equal(t, 0, liveRows(t, s, ctx, txid[:]),
		"a failed application must leave no rows behind")

	// The block must still be offerable, and must apply cleanly this time.
	applied, err = s.ApplyBlock(ctx, &blockHash, 100, func(dbTx pgx.Tx) error {
		_, cErr := s.createIn(ctx, dbTx, tx, 100, 100)
		return cErr
	})
	require.NoError(t, err)
	require.True(t, applied, "a failed block must remain retryable")
	require.Equal(t, 2, liveRows(t, s, ctx, txid[:]))
}

// TestBeginBlockApplySkipsOnlyCompletedBlocks pins the semantics the ledger must have
// when the caller cannot wrap the whole block in one transaction.
//
// Block application in teranode is not one unit of work: creates and spends run as
// separate phases across thousands of goroutines, so ApplyBlock's single-transaction
// shape does not fit. The claim and the completion have to be separate calls.
//
// That makes the completed flag load-bearing. A block claimed and finished is safe to
// skip. A block claimed and NOT finished died part-way through, and skipping it would
// leave its outputs missing forever and every later block spending them failing: silent
// loss, which is worse than the duplication it would be avoiding. So an incomplete claim
// must re-apply.
func TestBeginBlockApplySkipsOnlyCompletedBlocks(t *testing.T) {
	s, ctx := newTestStore(t)

	done := chainhash.Hash{0x01}
	partial := chainhash.Hash{0x02}

	// A block that finishes is skipped on re-offer.
	skip, err := s.BeginBlockApply(ctx, &done, 10)
	require.NoError(t, err)
	require.False(t, skip, "first offer must apply")
	require.NoError(t, s.CompleteBlockApply(ctx, &done))

	skip, err = s.BeginBlockApply(ctx, &done, 10)
	require.NoError(t, err)
	require.True(t, skip, "a completed block must never be applied twice")

	// A block that dies part-way through must be offered again, not skipped.
	skip, err = s.BeginBlockApply(ctx, &partial, 11)
	require.NoError(t, err)
	require.False(t, skip)

	skip, err = s.BeginBlockApply(ctx, &partial, 11)
	require.NoError(t, err)
	require.False(t, skip, "an unfinished block must re-apply, or its outputs are lost forever")

	require.NoError(t, s.CompleteBlockApply(ctx, &partial))

	skip, err = s.BeginBlockApply(ctx, &partial, 11)
	require.NoError(t, err)
	require.True(t, skip)
}
