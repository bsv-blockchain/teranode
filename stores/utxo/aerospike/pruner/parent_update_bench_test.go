package pruner

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
)

// parentUpdateResultsForBench builds n parent-update results in the mix the
// pruner sees on a live cluster: half answer OK and half come back
// KEY_NOT_FOUND because the parent was already deleted.
func parentUpdateResultsForBench(b *testing.B, n int) []aerospike.BatchRecordIfc {
	b.Helper()

	key, err := aerospike.NewKey("test", "utxo", []byte("parent"))
	if err != nil {
		b.Fatal(err)
	}

	records := make([]aerospike.BatchRecordIfc, n)

	for i := range records {
		rec := aerospike.NewBatchWrite(aerospike.NewBatchWritePolicy(), key, aerospike.TouchOp())

		if i%2 == 0 {
			rec.Record = &aerospike.Record{Bins: aerospike.BinMap{
				luaSuccessBin: map[interface{}]interface{}{fieldStatus: statusOK},
			}}
		} else {
			rec.Err = &aerospike.AerospikeError{ResultCode: types.KEY_NOT_FOUND_ERROR}
		}

		records[i] = rec
	}

	return records
}

// BenchmarkTallyParentUpdateResults measures classifying one chunk's worth of
// mod-teranode parent-update results.
func BenchmarkTallyParentUpdateResults(b *testing.B) {
	records := parentUpdateResultsForBench(b, 1024)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = tallyParentUpdateResults(records, true, nil)
	}
}
