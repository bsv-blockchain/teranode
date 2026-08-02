package pruner

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// makeParentUpdates builds n distinct parentUpdateInfo entries with valid keys.
func makeParentUpdates(t *testing.T, n int) map[string]*parentUpdateInfo {
	t.Helper()

	updates := make(map[string]*parentUpdateInfo, n)

	for i := 0; i < n; i++ {
		var h chainhash.Hash
		h[0] = byte(i + 1)

		key, err := aerospike.NewKey("test", "utxo", h[:])
		require.NoError(t, err)

		updates[h.String()] = &parentUpdateInfo{key: key, childHashes: []*chainhash.Hash{&h}}
	}

	return updates
}

// isUDF reports whether a batch record is a Lua NewBatchUDF invocation (as
// opposed to the native/BatchWrite operate-path).
func isUDF(rec aerospike.BatchRecordIfc) bool {
	_, ok := rec.(*aerospike.BatchUDF)
	return ok
}

// The combined cleanup path (defensive mode off) must route parent updates
// through the injected native-op builder when it is present, NOT through a raw
// Lua NewBatchUDF — even though luaPackage is also set as the fallback. This is
// the regression that left the pruner emitting a per-block batch_sub_udf burst
// on native-op-enabled deployments running with defensive mode off.
func TestBuildCombinedCleanupRecords_PrefersNativeBuilderOverUDF(t *testing.T) {
	nativeCalls := 0

	s := &Service{
		luaPackage: "teranode", // provider always sets this as the UDF fallback
		buildAddDeletedChildrenRecord: func(p *aerospike.BatchUDFPolicy, key *aerospike.Key, childHashes []interface{}) aerospike.BatchRecordIfc {
			nativeCalls++
			return aerospike.NewBatchWrite(aerospike.NewBatchWritePolicy(), key, aerospike.TouchOp())
		},
	}

	updates := makeParentUpdates(t, 2)

	records, parentEnd, usesModTeranode := s.buildCombinedCleanupRecords(updates, nil)

	require.Equal(t, len(updates), nativeCalls, "native builder must be invoked once per parent update")
	require.Equal(t, len(updates), parentEnd, "parentEnd must cover exactly the parent-update records")
	require.True(t, usesModTeranode, "native-op path must be flagged as mod-teranode for SUCCESS-map result parsing")

	for i := 0; i < parentEnd; i++ {
		require.Falsef(t, isUDF(records[i]), "parent update %d must not be a NewBatchUDF when the native builder is present", i)
	}
}

// With no native builder but a lua package configured, the combined path still
// falls back to the Lua UDF call and flags the mod-teranode SUCCESS-map shape.
func TestBuildCombinedCleanupRecords_FallsBackToUDFWhenNoNativeBuilder(t *testing.T) {
	s := &Service{luaPackage: "teranode"}

	updates := makeParentUpdates(t, 2)

	records, parentEnd, usesModTeranode := s.buildCombinedCleanupRecords(updates, nil)

	require.Equal(t, len(updates), parentEnd)
	require.True(t, usesModTeranode, "UDF path is also a mod-teranode SUCCESS-map path")

	for i := 0; i < parentEnd; i++ {
		require.Truef(t, isUDF(records[i]), "parent update %d must be a NewBatchUDF when only luaPackage is set", i)
	}
}

// With neither a native builder nor a lua package, the combined path uses the
// plain MapPutItems BatchWrite and reports it is NOT a mod-teranode path (so
// results are parsed via KEY_NOT_FOUND, not a SUCCESS map).
func TestBuildCombinedCleanupRecords_PlainMapWriteWhenNoLuaNoNative(t *testing.T) {
	s := &Service{fieldDeletedChildren: "deletedChildren"}

	updates := makeParentUpdates(t, 2)

	records, parentEnd, usesModTeranode := s.buildCombinedCleanupRecords(updates, nil)

	require.Equal(t, len(updates), parentEnd)
	require.False(t, usesModTeranode, "plain MapPutItems path must not be flagged as mod-teranode")

	for i := 0; i < parentEnd; i++ {
		require.Falsef(t, isUDF(records[i]), "parent update %d must be a BatchWrite, not a UDF", i)
	}
}
