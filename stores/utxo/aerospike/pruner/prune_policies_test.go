package pruner

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// The pruner removes records that are already provably safe to drop, and every
// removal is idempotent — a replica that misses the write just gets re-pruned
// on the next scan. Waiting for a full-replication ACK buys nothing, so the
// pruner's own removals default to COMMIT_MASTER, and
// pruner_relaxRemovalCommitLevel=false puts them back on COMMIT_ALL.

func TestRemovalCommitLevel(t *testing.T) {
	require.Equal(t, aerospike.COMMIT_MASTER, removalCommitLevel(true))
	require.Equal(t, aerospike.COMMIT_ALL, removalCommitLevel(false))
}

func TestNewPrunerDeletePolicyCarriesCommitLevel(t *testing.T) {
	require.Equal(t, aerospike.COMMIT_MASTER, newPrunerDeletePolicy(aerospike.COMMIT_MASTER).CommitLevel)
	require.Equal(t, aerospike.COMMIT_ALL, newPrunerDeletePolicy(aerospike.COMMIT_ALL).CommitLevel)
}

func TestNewPrunerTTLTouchPolicyCarriesCommitLevel(t *testing.T) {
	require.Equal(t, aerospike.COMMIT_MASTER, newPrunerTTLTouchPolicy(aerospike.COMMIT_MASTER).CommitLevel)
	require.Equal(t, aerospike.COMMIT_ALL, newPrunerTTLTouchPolicy(aerospike.COMMIT_ALL).CommitLevel)
}

// The TTL path replaces a hard delete with a 1-second expiry on an existing
// record; losing UPDATE_ONLY would resurrect deleted records as empty stubs,
// and losing Expiration=1 would leave them alive forever.
func TestNewPrunerTTLTouchPolicyKeepsExpirySemantics(t *testing.T) {
	policy := newPrunerTTLTouchPolicy(aerospike.COMMIT_MASTER)

	require.Equal(t, aerospike.UPDATE_ONLY, policy.RecordExistsAction)
	require.Equal(t, uint32(1), policy.Expiration)
}

func testKeys(t *testing.T, n int) []*aerospike.Key {
	t.Helper()

	keys := make([]*aerospike.Key, n)

	for i := range keys {
		key, err := aerospike.NewKey("test", "utxo", i)
		require.NoError(t, err)

		keys[i] = key
	}

	return keys
}

// Proves the commit level actually reaches the records the pruner puts on the
// wire, not just the policy constructors.
func TestBuildDeletionBatchRecordsHardDelete(t *testing.T) {
	for _, commitLevel := range []aerospike.CommitLevel{aerospike.COMMIT_MASTER, aerospike.COMMIT_ALL} {
		keys := testKeys(t, 3)

		records := buildDeletionBatchRecords(keys, false, commitLevel)

		require.Len(t, records, 3)

		for i, record := range records {
			batchDelete, ok := record.(*aerospike.BatchDelete)
			require.True(t, ok, "record %d should be a BatchDelete when utxoSetTTL is off", i)
			require.Equal(t, keys[i], batchDelete.Key)
			require.Equal(t, commitLevel, batchDelete.Policy.CommitLevel)
		}
	}
}

func TestBuildDeletionBatchRecordsTTLTouch(t *testing.T) {
	for _, commitLevel := range []aerospike.CommitLevel{aerospike.COMMIT_MASTER, aerospike.COMMIT_ALL} {
		keys := testKeys(t, 3)

		records := buildDeletionBatchRecords(keys, true, commitLevel)

		require.Len(t, records, 3)

		for i, record := range records {
			batchWrite, ok := record.(*aerospike.BatchWrite)
			require.True(t, ok, "record %d should be a BatchWrite when utxoSetTTL is on", i)
			require.Equal(t, keys[i], batchWrite.Key)
			require.Equal(t, commitLevel, batchWrite.Policy.CommitLevel)
			require.Equal(t, uint32(1), batchWrite.Policy.Expiration)
			require.Equal(t, aerospike.UPDATE_ONLY, batchWrite.Policy.RecordExistsAction)
			require.Len(t, batchWrite.Ops, 1)
		}
	}
}

func TestBuildDeletionBatchRecordsEmpty(t *testing.T) {
	require.Empty(t, buildDeletionBatchRecords(nil, false, aerospike.COMMIT_MASTER))
	require.Empty(t, buildDeletionBatchRecords(nil, true, aerospike.COMMIT_MASTER))
}

func testParentUpdates(t *testing.T, n int) map[string]*parentUpdateInfo {
	t.Helper()

	updates := make(map[string]*parentUpdateInfo, n)

	for i := range n {
		key, err := aerospike.NewKey("test", "utxo", 1000+i)
		require.NoError(t, err)

		updates[key.String()] = &parentUpdateInfo{key: key, childHashes: []*chainhash.Hash{{}}}
	}

	return updates
}

// Parent updates mutate records that SURVIVE the prune, so they must stay on
// COMMIT_ALL even when removals are relaxed. Asserted against the records the
// pruner actually builds — asserting the aerospike client's constructor
// defaults instead would guard nothing about this package.
func TestBuildCombinedCleanupRecordsKeepsParentUpdatesCommitAll(t *testing.T) {
	updates := testParentUpdates(t, 2)
	keys := testKeys(t, 3)

	// No native builder and no lua package → the plain MapPutItems BatchWrite
	// parent path.
	s := &Service{
		utxoSetTTL:           false,
		removalCommitLevel:   aerospike.COMMIT_MASTER,
		fieldDeletedChildren: "deletedChildren",
	}

	batchRecords, parentEnd, parentUsesModTeranode := s.buildCombinedCleanupRecords(updates, keys)

	require.False(t, parentUsesModTeranode)
	require.Len(t, batchRecords, len(updates)+len(keys))
	require.Equal(t, len(updates), parentEnd)

	for i, record := range batchRecords[:parentEnd] {
		batchWrite, ok := record.(*aerospike.BatchWrite)
		require.True(t, ok, "parent record %d should be a BatchWrite", i)
		require.Equal(t, aerospike.COMMIT_ALL, batchWrite.Policy.CommitLevel, "parent update %d must not inherit the relaxed removal commit level", i)
		require.Equal(t, aerospike.UPDATE_ONLY, batchWrite.Policy.RecordExistsAction)
	}

	for i, record := range batchRecords[parentEnd:] {
		batchDelete, ok := record.(*aerospike.BatchDelete)
		require.True(t, ok, "child record %d should be a BatchDelete", i)
		require.Equal(t, aerospike.COMMIT_MASTER, batchDelete.Policy.CommitLevel)
	}
}

// Same guarantee on the mod-teranode parent path: the addDeletedChildren UDF
// records keep COMMIT_ALL.
func TestBuildParentUpdateRecordsKeepCommitAll(t *testing.T) {
	updates := testParentUpdates(t, 2)

	s := &Service{
		removalCommitLevel:   aerospike.COMMIT_MASTER,
		fieldDeletedChildren: "deletedChildren",
		luaPackage:           "teranode",
	}

	batchRecords := s.buildParentUpdateRecords(updates)

	require.Len(t, batchRecords, len(updates))

	for i, record := range batchRecords {
		batchUDF, ok := record.(*aerospike.BatchUDF)
		require.True(t, ok, "parent record %d should be a BatchUDF", i)
		require.Equal(t, aerospike.COMMIT_ALL, batchUDF.Policy.CommitLevel, "parent update %d must not inherit the relaxed removal commit level", i)
	}
}

// A relaxed removal commit level must not leak into the combined batch's parent
// half when removals are NOT relaxed either — both halves are COMMIT_ALL.
func TestBuildCombinedCleanupRecordsUnrelaxed(t *testing.T) {
	updates := testParentUpdates(t, 1)
	keys := testKeys(t, 2)

	s := &Service{
		utxoSetTTL:           true,
		removalCommitLevel:   aerospike.COMMIT_ALL,
		fieldDeletedChildren: "deletedChildren",
	}

	batchRecords, parentEnd, _ := s.buildCombinedCleanupRecords(updates, keys)

	require.Len(t, batchRecords, len(updates)+len(keys))

	for i, record := range batchRecords[parentEnd:] {
		batchWrite, ok := record.(*aerospike.BatchWrite)
		require.True(t, ok, "child record %d should be a BatchWrite when utxoSetTTL is on", i)
		require.Equal(t, aerospike.COMMIT_ALL, batchWrite.Policy.CommitLevel)
	}
}
