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

// TestExpressionMissingBinSemantics records, against a real server, what an ExpWriteOp with
// ExpWriteFlagEvalNoFail does when the expression reads a bin that does not exist, and whether
// ExpAnd short-circuits past a failing read (it does). It documents why buildDeleteAtHeightExpressions
// guards every counter/list read (Lua treats a nil bin as false/0/empty, an unguarded read
// makes the whole expression unknown, which is a silent no-op under EvalNoFail).
func TestExpressionMissingBinSemantics(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, _, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()
	cleanDB(t, client)

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), []byte("missing-bin-probe"))
	require.NoError(t, err)
	require.NoError(t, client.PutBins(nil, key, aerospike.NewBin("present", 1)))

	flags := aerospike.ExpWriteFlagCreateOnly | aerospike.ExpWriteFlagEvalNoFail
	missing := aerospike.ExpIntBin("missing")

	// the observed semantics are server behaviour, so every failure names the server build
	build := "unknown"
	if nodes := client.GetNodes(); len(nodes) > 0 {
		if info, infoErr := nodes[0].RequestInfo(aerospike.NewInfoPolicy(), "build"); infoErr == nil {
			build = info["build"]
		}
	}

	_, err = client.Operate(nil, key,
		// unguarded comparison on a missing bin
		aerospike.ExpWriteOp("unguardedEq", aerospike.ExpCond(
			aerospike.ExpEq(missing, aerospike.ExpIntVal(1)), aerospike.ExpIntVal(1),
			aerospike.ExpIntVal(0)), flags),
		// ExpAnd(exists, eq(missing)): does it short-circuit?
		aerospike.ExpWriteOp("andGuarded", aerospike.ExpCond(
			aerospike.ExpAnd(aerospike.ExpBinExists("missing"), aerospike.ExpEq(missing, aerospike.ExpIntVal(1))), aerospike.ExpIntVal(1),
			aerospike.ExpIntVal(0)), flags),
		// guard via ExpCond branches
		aerospike.ExpWriteOp("condGuarded", aerospike.ExpCond(
			aerospike.ExpBinExists("missing"), aerospike.ExpEq(missing, aerospike.ExpIntVal(1)),
			aerospike.ExpBoolVal(false)), flags),
		// missing list size
		aerospike.ExpWriteOp("listSizeMissing", aerospike.ExpCond(
			aerospike.ExpGreater(aerospike.ExpListSize(aerospike.ExpListBin("missingList")), aerospike.ExpIntVal(0)), aerospike.ExpIntVal(1),
			aerospike.ExpIntVal(0)), flags),
	)
	require.NoError(t, err)

	rec, err := client.Get(nil, key)
	require.NoError(t, err)

	// Observed on the real server (asserted so a server change is noticed):
	//   unguarded read of a missing bin -> expression unknown -> write skipped (bin absent)
	//   ExpAnd short-circuits            -> a false ExpBinExists on the left guards the read
	//   ExpCond branch guard             -> works (evaluates lazily)
	_, unguarded := rec.Bins["unguardedEq"]
	_, andGuarded := rec.Bins["andGuarded"]
	_, condGuarded := rec.Bins["condGuarded"]
	_, listSizeMissing := rec.Bins["listSizeMissing"]
	t.Logf("unguardedEq written=%v andGuarded written=%v condGuarded written=%v listSizeMissing written=%v",
		unguarded, andGuarded, condGuarded, listSizeMissing)

	require.False(t, unguarded, "an unguarded read of a missing bin is a silent no-op under EvalNoFail (server build %s)", build)
	require.True(t, andGuarded, "ExpAnd short-circuits, so ExpAnd(ExpBinExists, read) is a valid guard (server build %s)", build)
	require.True(t, condGuarded, "ExpCond branch guard works (server build %s)", build)
	require.False(t, listSizeMissing, "ExpListSize of a missing list bin makes the expression unknown (server build %s)", build)
}

// TestExpressionSeesInFlightOperateChangesBatch is the BatchOperate variant of
// TestExpressionSeesInFlightOperateChanges. Production writes go through BatchOperate
// (BatchWrite), not Operate, so the visibility guarantee is pinned on that path too.
func TestExpressionSeesInFlightOperateChangesBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, _, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()
	cleanDB(t, client)

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), []byte("visibility-probe-batch"))
	require.NoError(t, err)
	require.NoError(t, client.PutBins(nil, key, aerospike.NewBin("x", 5), aerospike.NewBin("list", []int{})))

	flags := aerospike.ExpWriteFlagCreateOnly | aerospike.ExpWriteFlagEvalNoFail
	br := aerospike.NewBatchWrite(aerospike.NewBatchWritePolicy(), key,
		aerospike.PutOp(aerospike.NewBin("x", nil)),
		aerospike.ListAppendOp("list", 42),
		aerospike.ExpWriteOp("sawXGone", aerospike.ExpCond(
			aerospike.ExpBinExists("x"), aerospike.ExpIntVal(0), aerospike.ExpIntVal(1)), flags),
		aerospike.ExpWriteOp("sawListLen", aerospike.ExpListSize(aerospike.ExpListBin("list")), flags),
	)
	require.NoError(t, client.BatchOperate(nil, []aerospike.BatchRecordIfc{br}))
	require.NoError(t, br.BatchRec().Err)

	rec, err := client.Get(nil, key)
	require.NoError(t, err)
	require.Equal(t, 1, rec.Bins["sawXGone"])
	require.Equal(t, 1, rec.Bins["sawListLen"])
}
