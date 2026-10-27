package aerospike

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func preserveTxIDs(n int) []chainhash.Hash {
	txIDs := make([]chainhash.Hash, n)
	for i := range txIDs {
		txIDs[i] = chainhash.Hash{byte(i + 1)}
	}

	return txIDs
}

// PreserveTransactions must never send more than preserveTransactionsChunkSize records in one
// BatchOperate, on either the UDF or the filter-expression path, and must still cover every txID.
func TestPreserveTransactions_SendsBoundedChunks(t *testing.T) {
	prev := preserveTransactionsChunkSize
	preserveTransactionsChunkSize = 3

	t.Cleanup(func() { preserveTransactionsChunkSize = prev })

	for _, expressions := range []bool{false, true} {
		name := "udf"
		if expressions {
			name = "expressions"
		}

		t.Run(name, func(t *testing.T) {
			s := newTestStoreForGet(t)
			s.settings.Aerospike.EnablePreserveFilterExpressions = expressions

			var sizes []int

			seen := make(map[string]bool)

			s.batchOperateFn = func(_ *aerospike.BatchPolicy, recs []aerospike.BatchRecordIfc) aerospike.Error {
				sizes = append(sizes, len(recs))

				for _, r := range recs {
					seen[string(r.BatchRec().Key.Digest())] = true
					r.BatchRec().Record = &aerospike.Record{Bins: aerospike.BinMap{}} // a write always answers with a record
				}

				return nil
			}

			require.NoError(t, s.PreserveTransactions(context.Background(), preserveTxIDs(7), 500))
			require.Equal(t, []int{3, 3, 1}, sizes)
			require.Len(t, seen, 7, "every txID is sent exactly once")
		})
	}
}

// A failing chunk ends the call with its error; later chunks are not sent.
func TestPreserveTransactions_StopsAtFirstFailedChunk(t *testing.T) {
	prev := preserveTransactionsChunkSize
	preserveTransactionsChunkSize = 3

	t.Cleanup(func() { preserveTransactionsChunkSize = prev })

	s := newTestStoreForGet(t)

	calls := 0

	s.batchOperateFn = func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error {
		calls++
		return aerospike.ErrNetwork
	}

	require.Error(t, s.PreserveTransactions(context.Background(), preserveTxIDs(7), 500))
	require.Equal(t, 1, calls)
}

// A cancelled context stops before the next chunk.
func TestPreserveTransactions_StopsOnCancelledContext(t *testing.T) {
	s := newTestStoreForGet(t)

	s.batchOperateFn = func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error {
		t.Fatal("no batch may be sent after cancellation")
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, s.PreserveTransactions(ctx, preserveTxIDs(2), 500), context.Canceled)
}
