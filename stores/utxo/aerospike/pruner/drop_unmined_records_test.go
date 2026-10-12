package pruner

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestDropUnminedRecords(t *testing.T) {
	svc := newTestService(t)
	svc.fieldConflicting = "conflicting"

	txID := chainhash.HashH([]byte("tx"))

	record := func(num uint32, bins aerospike.BinMap) *aerospike.Result {
		key, err := aerospike.NewKey(svc.namespace, svc.set, uaerospike.CalculateKeySourceInternal(&txID, num))
		require.NoError(t, err)

		bins[svc.fieldTxID] = txID.CloneBytes()

		return &aerospike.Result{Record: &aerospike.Record{Key: key, Bins: bins}}
	}

	master := func(bins aerospike.BinMap) *aerospike.Result {
		bins[svc.fieldTotalExtraRecs] = 1
		return record(0, bins)
	}

	notFound := &aerospike.AerospikeError{ResultCode: types.KEY_NOT_FOUND_ERROR}
	other := &aerospike.AerospikeError{ResultCode: types.TIMEOUT}

	unmined := aerospike.BinMap{svc.fieldUnminedSince: 7}
	unminedConflicting := aerospike.BinMap{svc.fieldUnminedSince: 7, svc.fieldConflicting: true}

	tests := []struct {
		name          string
		rec           *aerospike.Result
		reads         []masterRead
		batchErr      error
		wantDeletable bool
		wantReads     bool
	}{
		{name: "mined master", rec: master(aerospike.BinMap{}), wantDeletable: true},
		{name: "unmined master", rec: master(unmined), wantDeletable: false},
		{name: "unmined conflicting master", rec: master(unminedConflicting), wantDeletable: true},
		{name: "legacy master without totalExtraRecs, mined", rec: record(0, aerospike.BinMap{}), wantDeletable: true},
		{name: "legacy master without totalExtraRecs, unmined", rec: record(0, aerospike.BinMap{svc.fieldUnminedSince: 7}), wantDeletable: false},
		{name: "legacy master without totalExtraRecs, unmined conflicting", rec: record(0, aerospike.BinMap{svc.fieldUnminedSince: 7, svc.fieldConflicting: true}), wantDeletable: true},
		{name: "page of mined master", rec: record(1, aerospike.BinMap{}), reads: []masterRead{{record: &aerospike.Record{Bins: aerospike.BinMap{}}}}, wantDeletable: true, wantReads: true},
		{name: "page of unmined master", rec: record(1, aerospike.BinMap{}), reads: []masterRead{{record: &aerospike.Record{Bins: aerospike.BinMap{svc.fieldUnminedSince: 7}}}}, wantDeletable: false, wantReads: true},
		{name: "page of conflicting master", rec: record(1, aerospike.BinMap{}), reads: []masterRead{{record: &aerospike.Record{Bins: aerospike.BinMap{svc.fieldUnminedSince: 7, svc.fieldConflicting: true}}}}, wantDeletable: true, wantReads: true},
		{name: "orphan page", rec: record(1, aerospike.BinMap{}), reads: []masterRead{{err: notFound}}, wantDeletable: true, wantReads: true},
		{name: "page, master read errors", rec: record(1, aerospike.BinMap{}), reads: []masterRead{{err: other}}, wantDeletable: false, wantReads: true},
		{name: "page, whole batch fails", rec: record(1, aerospike.BinMap{}), batchErr: other, wantDeletable: false, wantReads: true},
		{name: "page, neither error nor record", rec: record(1, aerospike.BinMap{}), reads: []masterRead{{}}, wantDeletable: false, wantReads: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			svc.readMastersFn = func(keys []*aerospike.Key) ([]masterRead, error) {
				called = true
				require.Len(t, keys, 1)

				return tt.reads, tt.batchErr
			}

			before := testutil.ToFloat64(prometheusUtxoRecordsSkippedUnmined)

			kept, dropped := svc.dropUnminedRecords([]*aerospike.Result{tt.rec})

			require.Equal(t, tt.wantReads, called)

			if tt.wantDeletable {
				require.Len(t, kept, 1)
				require.Zero(t, dropped)
			} else {
				require.Empty(t, kept)
				require.Equal(t, 1, dropped)
			}

			require.Equal(t, float64(dropped), testutil.ToFloat64(prometheusUtxoRecordsSkippedUnmined)-before)
		})
	}
}
