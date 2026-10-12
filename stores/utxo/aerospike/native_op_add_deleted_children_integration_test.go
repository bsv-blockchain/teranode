//go:build aerospike

package aerospike

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// sumNamespaceStat reads a numeric Aerospike namespace statistic, summed across
// every node in the cluster.
//
// It requires the statistic to exist on at least one node. Returning a silent 0
// for an unknown stat name would make the caller's before/after comparison pass
// vacuously if the counter were ever renamed or version-gated away — the
// comparison would be 0 == 0 while proving nothing.
func sumNamespaceStat(t *testing.T, db *Store, stat string) int64 {
	t.Helper()

	cmd := "namespace/" + db.namespace

	var (
		total int64
		found bool
	)

	nodes := db.client.Cluster().GetNodes()
	require.NotEmpty(t, nodes, "cluster must expose at least one node")

	infoPolicy := aerospike.NewInfoPolicy()

	for _, nd := range nodes {
		info, err := nd.RequestInfo(infoPolicy, cmd)
		require.NoError(t, err)

		for _, kv := range strings.Split(info[cmd], ";") {
			name, value, hasValue := strings.Cut(kv, "=")
			if !hasValue || name != stat {
				continue
			}

			v, perr := strconv.ParseInt(value, 10, 64)
			require.NoError(t, perr)

			total += v
			found = true
		}
	}

	require.Truef(t, found, "namespace statistic %q not reported by any node — the before/after comparison would be vacuous", stat)

	return total
}

// TestNativeOp_AddDeletedChildren_Integration verifies against a real Teraspike
// server (the BSV fork of aerospike-server with native operate-path support,
// wire op 200) that the pruner's addDeletedChildren parent-update runs as a
// NATIVE batch write (zero batch_sub_udf), correctly mutates the parent's
// deletedChildren map, and answers with a response the pruner can actually read.
//
// It builds the record via exactly the call GetPrunerService's
// BuildAddDeletedChildrenRecord uses, so it exercises the shared native path the
// pruner relies on. Skips cleanly on a stock Aerospike image (native-op probe
// fails); point at a Teraspike build with AEROSPIKE_CONTAINER_IMAGE to run it.
func TestNativeOp_AddDeletedChildren_Integration(t *testing.T) {
	InitPrometheusMetrics()

	logger := ulogger.NewErrorTestLogger(t)
	ctx := context.Background()

	container, err := runAerospikeTestContainer(ctx)
	if err != nil {
		t.Skipf("Skipping: Aerospike container not available (%v)", err)
	}

	t.Cleanup(func() { require.NoError(t, container.Terminate(ctx)) })

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.ServicePort(ctx)
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Aerospike.UseNativeTeranodeOps = true

	aeroURL, err := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=test&externalStore=file://./data/externalStore", host, port))
	require.NoError(t, err)

	db, err := New(ctx, logger, tSettings, aeroURL)
	require.NoError(t, err)

	db.SetExternalStore(memory.New())

	if !db.useNativeTeranodeOps.Load() {
		t.Skipf("native ops not enabled (image %q is not a Teraspike build); set AEROSPIKE_CONTAINER_IMAGE to a native-op server", os.Getenv("AEROSPIKE_CONTAINER_IMAGE"))
	}

	var childHash chainhash.Hash
	childHash[0] = 0xCD

	childList := []interface{}{childHash.String()}

	// buildRecord mirrors GetPrunerService.BuildAddDeletedChildrenRecord exactly,
	// so the test exercises the record the pruner actually emits rather than a
	// lookalike — including the shared UPDATE_ONLY policy initNativeTeranodeOps
	// installs, which is what stops a missing parent from being created.
	buildRecord := func(key *aerospike.Key) aerospike.BatchRecordIfc {
		return db.teranodeBatchRecord(
			aerospike.NewBatchUDFPolicy(),
			LuaPackage,
			key,
			subOpAddDeletedChildren,
			"addDeletedChildren",
			childList,
		)
	}

	t.Run("existing parent is mutated natively", func(t *testing.T) {
		var parentHash chainhash.Hash
		parentHash[0] = 0xAB

		key, err := aerospike.NewKey(db.namespace, db.setName, parentHash[:])
		require.NoError(t, err)
		require.NoError(t, db.client.PutBins(nil, key, aerospike.NewBin("seed", 1)))

		udfBefore := sumNamespaceStat(t, db, "batch_sub_udf_complete")

		rec := buildRecord(key)

		_, builtAsUDF := rec.(*aerospike.BatchUDF)
		require.False(t, builtAsUDF, "a native-op store must build addDeletedChildren as a BatchWrite, not a NewBatchUDF")

		require.NoError(t, db.client.BatchOperate(nil, []aerospike.BatchRecordIfc{rec}))
		require.NoError(t, rec.BatchRec().Err, "native addDeletedChildren batch operation must succeed")

		// Pin the response shape. The pruner now fails CLOSED on a response it
		// cannot read, so a dispatcher whose encoding drifts from what
		// ParseLuaMapResponse accepts must break here rather than at runtime.
		resp := rec.BatchRec().Record.Bins[nativeOpResultBin]
		parsed, perr := db.ParseLuaMapResponse(resp)
		require.NoErrorf(t, perr, "native addDeletedChildren response bin is not parseable (%T)", resp)
		require.Equal(t, LuaStatusOK, parsed.Status)

		// The parent's deletedChildren map must now contain the child hash — proof
		// the native subOpAddDeletedChildren dispatcher executed server-side.
		parent, err := db.client.Get(nil, key, fields.DeletedChildren.String())
		require.NoError(t, err)
		require.NotNil(t, parent)

		deletedChildren, ok := parent.Bins[fields.DeletedChildren.String()].(map[interface{}]interface{})
		require.Truef(t, ok, "deletedChildren must be a map, got %T", parent.Bins[fields.DeletedChildren.String()])
		require.Equal(t, true, deletedChildren[childHash.String()], "child hash must be recorded in the parent's deletedChildren map")

		udfAfter := sumNamespaceStat(t, db, "batch_sub_udf_complete")

		// The decisive check: the parent was mutated (native dispatcher ran) yet the
		// server's Lua-UDF sub-transaction counter did not move at all. The native
		// wire-op-200 path is deliberately not counted under batch_sub_udf.
		require.Equalf(t, udfBefore, udfAfter, "addDeletedChildren must NOT invoke a Lua UDF on a native-op store (batch_sub_udf_complete went %d -> %d)", udfBefore, udfAfter)
	})

	// The routine case for the pruner: the parent was already deleted, normally by
	// the pruner itself. The Lua path this replaces returns TX_NOT_FOUND and writes
	// nothing. The native path must not create the record instead — a resurrected
	// parent would hold only deletedChildren (no txid, no deleteAtHeight), so the
	// DAH scan could never reclaim it and any later read would fail in
	// extractTxHash. This is what the updateOnly policy exists to guarantee, and it
	// holds regardless of how the server-fork dispatcher handles a missing record.
	t.Run("missing parent is not created", func(t *testing.T) {
		var absentHash chainhash.Hash
		absentHash[0] = 0xEF

		key, err := aerospike.NewKey(db.namespace, db.setName, absentHash[:])
		require.NoError(t, err)

		// Make sure it really is absent.
		existing, err := db.client.Get(nil, key)
		require.NoError(t, err)
		require.Nil(t, existing, "precondition: the parent key must not exist")

		rec := buildRecord(key)
		require.NoError(t, db.client.BatchOperate(nil, []aerospike.BatchRecordIfc{rec}))

		// The op may report the miss either way — as a KEY_NOT_FOUND batch error
		// (the updateOnly policy rejecting the write) or as an ERROR/TX_NOT_FOUND
		// response map (the dispatcher guarding existence itself). The pruner
		// counts both as a skipped parent; what must never happen is a silent
		// success that leaves a record behind.
		if batchErr := rec.BatchRec().Err; batchErr != nil {
			require.Truef(t, batchErr.Matches(aerospike.ErrKeyNotFound.ResultCode),
				"a missing parent must surface as KEY_NOT_FOUND, got %v", batchErr)
		} else {
			resp := rec.BatchRec().Record.Bins[nativeOpResultBin]
			parsed, perr := db.ParseLuaMapResponse(resp)
			require.NoErrorf(t, perr, "response bin is not parseable (%T)", resp)
			require.Equal(t, LuaStatusError, parsed.Status, "a missing parent must not report success")
			require.Equal(t, LuaErrorCodeTxNotFound, parsed.ErrorCode)
		}

		// The decisive assertion: no record was created.
		after, err := db.client.Get(nil, key)
		require.NoError(t, err)
		require.Nilf(t, after, "addDeletedChildren must not create a missing parent, but a record now exists: %v", after)
	})
}
