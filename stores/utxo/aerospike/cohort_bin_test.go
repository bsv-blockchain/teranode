package aerospike_test

import (
	"os"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	teranodeaerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util/cohort"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestGetBinsToStore_Cohort covers the issue-556 cohort bin without an Aerospike
// container: the round-trip half of the conformance suite only runs when Docker
// is available, so the write side is pinned here instead.
//
// Three things matter. The bin must carry the exact stamped value; cohort.Unset
// must write no bin at all, so a record created with the feature flag off is
// identical to what a pre-cohort writer produced; and the bin must sit on
// record 0 only, because record 0 is the master and classifyCreateBatchResults
// decides "did I create this transaction" from record 0's result alone.
func TestGetBinsToStore_Cohort(t *testing.T) {
	teranodeaerospike.InitPrometheusMetrics()

	loadTx := func(t *testing.T) *bt.Tx {
		t.Helper()

		txHex, err := os.ReadFile("testdata/fbebcc148e40cb6c05e57c6ad63abd49d5e18b013c82f704601bc4ba567dfb90.hex")
		require.NoError(t, err)

		tx, err := bt.NewTxFromString(string(txHex))
		require.NoError(t, err)

		return tx
	}

	findCohortBin := func(bins []*aerospike.Bin) (aerospike.Value, bool) {
		for _, b := range bins {
			if b.Name == fields.Cohort.String() {
				return b.Value, true
			}
		}

		return nil, false
	}

	newStore := func(t *testing.T, utxoBatchSize int) *teranodeaerospike.Store {
		t.Helper()

		s := &teranodeaerospike.Store{}
		s.SetUtxoBatchSize(utxoBatchSize)
		s.SetSettings(test.CreateBaseTestSettings(t))

		return s
	}

	t.Run("a stamped cohort lands on the master record", func(t *testing.T) {
		s := newStore(t, 100)
		tx := loadTx(t)

		stamp := cohort.ID(1_700_000_000)

		bins, err := s.GetBinsToStore(tx, 0, nil, nil, nil, false, tx.TxIDChainHash(), false, false, false, stamp, nil)
		require.NoError(t, err)
		require.NotEmpty(t, bins)

		value, ok := findCohortBin(bins[0])
		require.True(t, ok, "a stamped create must write the cohort bin")
		require.Equal(t, aerospike.NewIntegerValue(int(stamp)), value)
	})

	t.Run("cohort.Unset writes no bin", func(t *testing.T) {
		s := newStore(t, 100)
		tx := loadTx(t)

		bins, err := s.GetBinsToStore(tx, 0, nil, nil, nil, false, tx.TxIDChainHash(), false, false, false, cohort.Unset, nil)
		require.NoError(t, err)
		require.NotEmpty(t, bins)

		_, ok := findCohortBin(bins[0])
		require.False(t, ok, "with no cohort stamped the record must be unchanged from the pre-cohort shape")
	})

	t.Run("the cohort bin is only on record 0", func(t *testing.T) {
		tx := loadTx(t)

		// A batch size of one output per record forces the transaction to split
		// across several records, so the child records can be checked.
		s := newStore(t, 1)

		stamp := cohort.ID(1_700_000_001)

		bins, err := s.GetBinsToStore(tx, 0, nil, nil, nil, false, tx.TxIDChainHash(), false, false, false, stamp, nil)
		require.NoError(t, err)
		require.Greater(t, len(bins), 1, "this transaction must split into more than one record for the check to mean anything")

		_, ok := findCohortBin(bins[0])
		require.True(t, ok, "the master record must carry the cohort bin")

		for i := 1; i < len(bins); i++ {
			_, ok := findCohortBin(bins[i])
			require.False(t, ok, "record %d is a child record and must not carry the cohort bin", i)
		}
	})
}
