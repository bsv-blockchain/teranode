package fields

import "testing"

// sinkStrings forces the FieldNamesToStrings result to escape so the benchmark
// measures the real heap allocation (in production the slice escapes into the
// aerospike BatchRead record); without the sink, escape analysis stack-
// allocates the discarded result and reports a misleading 0 allocs/op.
var sinkStrings []string

// BenchmarkFieldNamesToStrings measures the allocation of converting a field
// set to its wire string names. BatchDecorate calls this once per item even
// though every item in a call usually requests the same field set, so the
// per-call cost is N times this. Tier A hoists it to once per distinct field
// set per call; this benchmark is the per-conversion baseline.
func BenchmarkFieldNamesToStrings(b *testing.B) {
	// Representative expanded set (BlockIDs prefetch expands to ~4 fields; Tx
	// expands to ~8). Use the larger to bound the cost.
	fieldSet := []FieldName{Fee, SizeInBytes, TxInpoints, Inputs, Outputs, Version, LockTime, External}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sinkStrings = FieldNamesToStrings(fieldSet)
	}
}
