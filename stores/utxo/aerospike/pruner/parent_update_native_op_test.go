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

		info := &parentUpdateInfo{key: key, seen: make(map[chainhash.Hash]struct{}, 1)}
		info.add(&h)

		updates[h.String()] = info
	}

	return updates
}

// isUDF reports whether a batch record is a Lua NewBatchUDF invocation (as
// opposed to the native/BatchWrite operate-path).
func isUDF(rec aerospike.BatchRecordIfc) bool {
	_, ok := rec.(*aerospike.BatchUDF)
	return ok
}

// buildParentUpdateRecords must route parent updates through the injected
// native-op builder when it is present, NOT through a raw Lua NewBatchUDF — even
// though luaPackage is also set as the fallback. This is the regression that
// left the pruner emitting a per-block batch_sub_udf burst on native-op-enabled
// deployments.
func TestBuildParentUpdateRecords_PrefersNativeBuilderOverUDF(t *testing.T) {
	nativeCalls := 0

	s := &Service{
		luaPackage: "teranode", // provider always sets this as the UDF fallback
		buildAddDeletedChildrenRecord: func(p *aerospike.BatchUDFPolicy, key *aerospike.Key, childHashes []interface{}) aerospike.BatchRecordIfc {
			nativeCalls++
			return aerospike.NewBatchWrite(aerospike.NewBatchWritePolicy(), key, aerospike.TouchOp())
		},
	}

	updates := makeParentUpdates(t, 2)

	records, infos := s.buildParentUpdateRecords(updates)

	require.Equal(t, len(updates), nativeCalls, "native builder must be invoked once per parent update")
	require.Len(t, records, len(updates))

	for i := 0; i < len(records); i++ {
		require.Falsef(t, isUDF(records[i]), "parent update %d must not be a NewBatchUDF when the native builder is present", i)
	}

	// infos is index-aligned and names the same children the record protects.
	require.Len(t, infos, len(records))
	for i, info := range infos {
		require.NotNil(t, info.key, "each info must carry the parent key")
		require.Lenf(t, info.childHashes, 1, "record %d must protect exactly one child", i)
	}
}

// With no native builder but a lua package configured, buildParentUpdateRecords
// falls back to the Lua UDF call.
func TestBuildParentUpdateRecords_FallsBackToUDFWhenNoNativeBuilder(t *testing.T) {
	s := &Service{luaPackage: "teranode"}

	updates := makeParentUpdates(t, 2)

	records, infos := s.buildParentUpdateRecords(updates)

	require.Len(t, records, len(updates))
	require.Len(t, infos, len(records))

	for i := 0; i < len(records); i++ {
		require.Truef(t, isUDF(records[i]), "parent update %d must be a NewBatchUDF when only luaPackage is set", i)
	}
}

// parentUpdateInfo.add must dedup repeated children of the same parent. A
// multi-input consolidation transaction that spends many outputs of one parent
// is a single (parent, child) marker, and one transaction being the input of
// several parents queues one hash per distinct parent.
func TestParentUpdateInfoAddDedupsChildren(t *testing.T) {
	key, err := aerospike.NewKey("test", "utxo", []byte("parent"))
	require.NoError(t, err)

	info := &parentUpdateInfo{key: key, seen: make(map[chainhash.Hash]struct{}, 1)}

	var child1, child2 chainhash.Hash
	child1[0] = 1
	child2[0] = 2

	info.add(&child1)
	info.add(&child2)
	info.add(&child1) // duplicate of an already-queued child
	info.add(&child2)

	require.Len(t, info.childHashes, 2, "duplicate children of one parent must be coalesced")
	require.Contains(t, info.childHashes, &child1)
	require.Contains(t, info.childHashes, &child2)
}
