package aerospike

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// prunerParentRecordForBench builds one pruner parent-update record the way the
// pruner's BuildAddDeletedChildrenRecord hook does for a parent with the given
// children, including encoding the child hashes into the op's payload.
func prunerParentRecordForBench(s *Store, key *aerospike.Key, children []*chainhash.Hash) aerospike.BatchRecordIfc {
	return s.prunerAddDeletedChildrenRecord(aerospike.NewBatchUDFPolicy(), key, children)
}

// BenchmarkPrunerParentRecord measures building the native addDeletedChildren
// record for a parent with one child, the common case on the pruner's hot path.
func BenchmarkPrunerParentRecord(b *testing.B) {
	s := &Store{nativeOpBatchWritePolicy: aerospike.NewBatchWritePolicy()}
	s.useNativeTeranodeOps.Store(true)

	key, err := aerospike.NewKey("test", "utxo", []byte("parent"))
	if err != nil {
		b.Fatal(err)
	}

	var child chainhash.Hash
	for i := range child {
		child[i] = byte(i)
	}

	children := []*chainhash.Hash{&child}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = prunerParentRecordForBench(s, key, children)
	}
}

// BenchmarkDemoteNativeOnUnsupportedKeyNotFound measures the demotion check on
// the routine pruner result: a parent the pruner already deleted.
func BenchmarkDemoteNativeOnUnsupportedKeyNotFound(b *testing.B) {
	s := &Store{}
	s.useNativeTeranodeOps.Store(true)

	var err error = &aerospike.AerospikeError{ResultCode: types.KEY_NOT_FOUND_ERROR}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.demoteNativeOnUnsupported(err)
	}
}
