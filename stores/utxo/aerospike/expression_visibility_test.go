package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestExpressionSeesInFlightOperateChanges pins, against a real server, what an ExpWriteOp
// sees of earlier ops in the same operate call. The DAH expression builders (set mined,
// spend, expired preservation) depend on this: the answer is that expressions see the
// in-flight record (earlier writes in the same operate are visible), NOT the pre-operate one.
func TestExpressionSeesInFlightOperateChanges(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, _, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()
	cleanDB(t, client)

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), []byte("visibility-probe"))
	require.NoError(t, err)

	require.NoError(t, client.PutBins(nil, key,
		aerospike.NewBin("x", 5),
		aerospike.NewBin("list", []int{}),
	))

	flags := aerospike.ExpWriteFlagCreateOnly | aerospike.ExpWriteFlagEvalNoFail

	// 1. a Put-nil of bin x earlier in the operate is visible to the expression.
	// 2. a ListAppend earlier in the operate is visible to the expression.
	_, err = client.Operate(nil, key,
		aerospike.PutOp(aerospike.NewBin("x", nil)),
		aerospike.ListAppendOp("list", 42),
		aerospike.ExpWriteOp("sawXGone", aerospike.ExpCond(
			aerospike.ExpBinExists("x"), aerospike.ExpIntVal(0),
			aerospike.ExpIntVal(1),
		), flags),
		aerospike.ExpWriteOp("sawListLen", aerospike.ExpListSize(aerospike.ExpListBin("list")), flags),
	)
	require.NoError(t, err)

	rec, err := client.Get(nil, key)
	require.NoError(t, err)
	require.Equal(t, 1, rec.Bins["sawXGone"], "expression must see an earlier Put-nil of the bin (in-flight visibility)")
	require.Equal(t, 1, rec.Bins["sawListLen"], "expression must see an earlier ListAppend (in-flight visibility)")
}
