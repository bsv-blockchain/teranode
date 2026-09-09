package pruner

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
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

// Parent updates must prefer the native-op builder when it is available.
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

	records, _ := s.buildParentUpdateRecords(updates)
	require.Len(t, records, len(updates))

	require.Equal(t, len(updates), nativeCalls, "native builder must be invoked once per parent update")

	for i := 0; i < len(records); i++ {
		require.Falsef(t, isUDF(records[i]), "parent update %d must not be a NewBatchUDF when the native builder is present", i)
	}
}

// With no native builder but a lua package configured, the parent update path
// falls back to the Lua UDF call.
func TestBuildParentUpdateRecords_FallsBackToUDFWhenNoNativeBuilder(t *testing.T) {
	s := &Service{luaPackage: "teranode"}

	updates := makeParentUpdates(t, 2)

	records, _ := s.buildParentUpdateRecords(updates)
	require.Len(t, records, len(updates))

	for i := 0; i < len(records); i++ {
		require.Truef(t, isUDF(records[i]), "parent update %d must be a NewBatchUDF when only luaPackage is set", i)
	}
}

// With neither a native builder nor a lua package, the fallback uses the
// plain MapPutItems BatchWrite (so
// results are parsed via KEY_NOT_FOUND, not a SUCCESS map).
func TestBuildParentUpdateRecords_PlainMapWriteWhenNoLuaNoNative(t *testing.T) {
	s := &Service{fieldDeletedChildren: "deletedChildren"}

	updates := makeParentUpdates(t, 2)

	records, _ := s.buildParentMapUpdateRecords(updates)
	require.Len(t, records, len(updates))

	for i := 0; i < len(records); i++ {
		require.Falsef(t, isUDF(records[i]), "parent update %d must be a BatchWrite, not a UDF", i)
	}
}

// TestAddParentUpdatesForInput covers what the pruner asks Aerospike to write for
// one spent outpoint: which parent records get a marker, and how many copies of
// the child hash each one receives.
func TestAddParentUpdatesForInput(t *testing.T) {
	var parent chainhash.Hash

	parent[0] = 0xAA

	var child chainhash.Hash

	child[0] = 0xBB

	newService := func(defensive bool) *Service {
		return &Service{namespace: "test", set: "utxo", utxoBatchSize: 128, defensiveEnabled: defensive}
	}

	masterKey := string(parent.CloneBytes())
	pageKey := string(uaerospike.CalculateKeySource(&parent, 300, 128))

	require.NotEqual(t, masterKey, pageKey, "fixture must use a vout past the first page")

	t.Run("either mode writes only the page the spend path reads", func(t *testing.T) {
		for _, defensive := range []bool{false, true} {
			updates := map[string]*parentUpdateInfo{}
			require.NoError(t, newService(defensive).addParentUpdatesForInput(updates, &parent, 300, &child))
			require.Len(t, updates, 1)
			require.Contains(t, updates, pageKey)
			require.NotContains(t, updates, masterKey,
				"the master holds outputs 0..batchSize-1 only, so a marker for a higher output is read by nothing there and grows the record without bound")
		}
	})

	t.Run("first page needs one write in either mode", func(t *testing.T) {
		for _, defensive := range []bool{false, true} {
			updates := map[string]*parentUpdateInfo{}
			require.NoError(t, newService(defensive).addParentUpdatesForInput(updates, &parent, 7, &child))
			require.Len(t, updates, 1)
			require.Contains(t, updates, masterKey, "vout 0..127 lives on the master record")
		}
	})

	t.Run("a consolidation spend queues one child hash, not one per input", func(t *testing.T) {
		// 10k outputs of the same paginated parent, spread across pages: every
		// input asks for the same (parent, child) marker. Without dedup this
		// produced a 10,000-element list of the identical 64-char hex string in
		// one batch record.
		s := newService(true)
		updates := map[string]*parentUpdateInfo{}

		for vout := uint32(0); vout < 10_000; vout++ {
			require.NoError(t, s.addParentUpdatesForInput(updates, &parent, vout, &child))
		}

		require.Len(t, updates, 79, "one entry per output page (page 0 is the master)")

		for source, info := range updates {
			require.Lenf(t, info.childHashes, 1, "parent record %x queued %d copies of the same child", source, len(info.childHashes))
		}
	})
}
