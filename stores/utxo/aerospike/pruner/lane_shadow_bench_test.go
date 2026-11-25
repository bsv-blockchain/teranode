package pruner

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// BenchmarkLaneShadowRun times the lane rule's whole per-record cost on Aerospike outside the rule's
// lookups: reading the parent's position and each spent output's spender and slot position from the
// query's bins, then judging. The index answers every lookup from the floor, the cheap case, so
// ns/op less BenchmarkPruneJudgeDesignRate/retention=288 (three floor lookups) is the decoding the
// store adds to the rule. Each record holds two outputs, both spent, by two spenders. ns/op is per
// record.
func BenchmarkLaneShadowRun(b *testing.B) {
	installLaneIndex(b, laneIndexForTests(b))

	svc := &Service{
		namespace:           "test",
		set:                 "test",
		fieldTxID:           fields.TxID.String(),
		fieldUtxos:          fields.Utxos.String(),
		fieldTotalExtraRecs: fields.TotalExtraRecs.String(),
		fieldConflicting:    fields.Conflicting.String(),
		laneRetention:       288,
	}

	spenderA := chainhash.HashH([]byte("spender a"))
	spenderB := chainhash.HashH([]byte("spender b"))

	chunk := make([]*aerospike.Result, 1_000)

	for i := range chunk {
		txID := chainhash.HashH([]byte{byte(i), byte(i >> 8)})
		key, err := aerospike.NewKey(svc.namespace, svc.set, uaerospike.CalculateKeySourceInternal(&txID, 0))
		require.NoError(b, err)

		chunk[i] = &aerospike.Result{Record: &aerospike.Record{Key: key, Bins: aerospike.BinMap{
			svc.fieldTxID:                         txID.CloneBytes(),
			svc.fieldTotalExtraRecs:               0,
			fields.LanePosition.String():          int(lane.ToSigned(laneSettledParent)),
			svc.fieldUtxos:                        []interface{}{spentUtxo("a", spenderA), spentUtxo("b", spenderB)},
			fields.SpendingLanePositions.String(): slotPositions(laneSettled, laneSettled),
		}}}
	}

	b.ResetTimer()

	for b.Loop() {
		l := svc.newLaneShadow(chunk)
		for _, rec := range chunk {
			l.markDeleted(rec)
		}

		l.run()
	}

	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(len(chunk)), "ns/record")
}
