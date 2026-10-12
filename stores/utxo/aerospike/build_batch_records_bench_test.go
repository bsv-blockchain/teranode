package aerospike

import (
	"encoding/binary"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
)

// BenchmarkBuildBatchRecords measures the per-call / per-item allocations of the
// BatchDecorate setup loop (the network-free construction phase): one
// aerospike.Key per item, addAbstractedBins expanding the field set per item,
// FieldNamesToStrings converting it per item, and one aerospike.BatchRead per
// item. This is the Tier-A allocation surface — Key/BatchRead pooling and
// hoisting the per-item field-set/string-slice work out of the loop should drop
// allocs/op here. No live Aerospike is needed; buildBatchRecords does no I/O.
//
// Field-set scenarios mirror real callers:
//   - default: caller passes no fields → the hardcoded default bin set
//   - prefetch_parents: subtreevalidation's parent prefetch (BlockIDs+Tx)
//   - tx: block persister / asset (full transaction)
func BenchmarkBuildBatchRecords(b *testing.B) {
	tSettings := settings.NewSettings()
	s := &Store{namespace: "test", setName: "utxo", settings: tSettings}
	policy := util.GetAerospikeBatchReadPolicy(tSettings)

	scenarios := []struct {
		name           string
		optionalFields []fields.FieldName
	}{
		{"default", nil},
		{"prefetch_parents", []fields.FieldName{fields.BlockIDs, fields.BlockHeights, fields.Tx}},
		{"tx", []fields.FieldName{fields.Tx}},
	}

	sizes := []int{128, 1024}

	for _, sc := range scenarios {
		for _, n := range sizes {
			items := make([]*utxo.UnresolvedMetaData, n)
			for i := range items {
				var h chainhash.Hash
				binary.LittleEndian.PutUint32(h[:], uint32(i+1))
				items[i] = &utxo.UnresolvedMetaData{Hash: h, Idx: i}
			}

			b.Run(sc.name+"/"+itoa(n), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					// Each production BatchDecorate call receives fresh items with
					// no expanded Fields yet; reset to model that (zero-alloc).
					for _, it := range items {
						it.Fields = nil
					}

					recs, err := s.buildBatchRecords(items, policy, sc.optionalFields)
					if err != nil {
						b.Fatalf("buildBatchRecords: %v", err)
					}

					if len(recs) != n {
						b.Fatalf("expected %d records, got %d", n, len(recs))
					}
				}

				b.ReportMetric(float64(n), "records/op")
			})
		}
	}
}

// itoa avoids a fmt import for the sub-benchmark labels.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}

	var buf [20]byte

	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}

	return string(buf[i:])
}
