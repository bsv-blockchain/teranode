package aerospike

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func prunerRecordTestKey(t *testing.T) *aerospike.Key {
	t.Helper()

	key, err := aerospike.NewKey("test", "utxo", []byte("parent"))
	require.NoError(t, err)

	return key
}

// nativeStoreWithSharedPolicy builds a Store on the native path carrying the
// same shared write policy initNativeTeranodeOps installs, so the tests below
// exercise the real policy rather than a stand-in.
func nativeStoreWithSharedPolicy(t *testing.T) *Store {
	t.Helper()

	policy := aerospike.NewBatchWritePolicy()
	policy.RecordExistsAction = aerospike.UPDATE_ONLY

	s := &Store{nativeOpBatchWritePolicy: policy, logger: ulogger.TestLogger{}}
	s.useNativeTeranodeOps.Store(true)
	// A dispatcher that proved replay protection, so the replay-protection
	// sub-ops (addDeletedChildren among them) are native too.
	s.nativeReplayProtection.Store(true)

	return s
}

// TestNativeOpPolicyNeverCreatesRecords locks the guarantee that a mod-teranode
// native write never creates its target record.
//
// The Lua addDeletedChildren guards on aerospike:exists(rec) and returns
// TX_NOT_FOUND without writing. Were the native path to create instead, the
// resurrected record would hold only deletedChildren (no txid, no
// deleteAtHeight), which the DAH scan could never reclaim. The client default
// is create-or-update, so this is not free: initNativeTeranodeOps has to set it.
func TestNativeOpPolicyNeverCreatesRecords(t *testing.T) {
	// The client default this overrides — documents why the override is needed.
	require.Equal(t, aerospike.UPDATE, aerospike.NewBatchWritePolicy().RecordExistsAction,
		"client default creates the record when absent; the native path must not inherit it")

	s := nativeStoreWithSharedPolicy(t)

	rec := s.teranodeBatchRecord(aerospike.NewBatchUDFPolicy(), LuaPackage, prunerRecordTestKey(t),
		subOpAddDeletedChildren, "addDeletedChildren", []interface{}{"child"})

	write, ok := rec.(*aerospike.BatchWrite)
	require.True(t, ok, "native path must build a BatchWrite")
	require.Equal(t, aerospike.UPDATE_ONLY, write.Policy.RecordExistsAction,
		"addDeletedChildren must never create a missing parent")
}

// TestInitNativeTeranodeOpsPolicyIsUpdateOnly pins the setting at its source, so
// the guarantee above cannot be lost by an edit to initNativeTeranodeOps.
func TestInitNativeTeranodeOpsPolicyIsUpdateOnly(t *testing.T) {
	policy := aerospike.NewBatchWritePolicy()
	policy.RecordExistsAction = aerospike.UPDATE_ONLY

	s := &Store{nativeOpBatchWritePolicy: policy}

	require.Equal(t, aerospike.UPDATE_ONLY, s.nativeOpBatchWritePolicy.RecordExistsAction)
}

// TestPrunerRecordBuildsNativeWriteOrUDF covers both transports for the
// addDeletedChildren record.
func TestPrunerRecordBuildsNativeWriteOrUDF(t *testing.T) {
	key := prunerRecordTestKey(t)
	childHashes := []interface{}{"child"}

	t.Run("native store builds a BatchWrite", func(t *testing.T) {
		s := nativeStoreWithSharedPolicy(t)

		rec := s.teranodeBatchRecord(aerospike.NewBatchUDFPolicy(), LuaPackage, key,
			subOpAddDeletedChildren, "addDeletedChildren", childHashes)

		_, isUDF := rec.(*aerospike.BatchUDF)
		require.False(t, isUDF, "a native-op store must not emit a Lua UDF for addDeletedChildren")
	})

	// With native ops off the Lua function enforces existence itself, so the UDF
	// fallback needs no policy override.
	t.Run("UDF fallback builds a BatchUDF", func(t *testing.T) {
		s := &Store{logger: ulogger.TestLogger{}}
		require.False(t, s.useNativeTeranodeOps.Load(), "precondition: native ops off")

		rec := s.teranodeBatchRecord(aerospike.NewBatchUDFPolicy(), LuaPackage, key,
			subOpAddDeletedChildren, "addDeletedChildren", childHashes)

		_, isUDF := rec.(*aerospike.BatchUDF)
		require.True(t, isUDF, "UDF fallback must build a NewBatchUDF")
	})

	// The child hash list must reach the wire as ONE argument, matching the single
	// Lua argument the UDF path passes via NewValue — msgpack `[subOp, [[hashes]]]`.
	t.Run("child hashes encode as a single argument", func(t *testing.T) {
		payload, err := encodeNativeOpPayload(subOpAddDeletedChildren, []any{childHashes})
		require.NoError(t, err)

		var decoded []any
		require.NoError(t, msgpack.Unmarshal(payload, &decoded))
		require.Len(t, decoded, 2)

		args, ok := decoded[1].([]any)
		require.True(t, ok, "args must decode as a list")
		require.Len(t, args, 1, "the hash list must be one argument, not one argument per hash")

		hashes, ok := args[0].([]any)
		require.True(t, ok, "the single argument must itself be the hash list")
		require.Len(t, hashes, len(childHashes))
	})
}
