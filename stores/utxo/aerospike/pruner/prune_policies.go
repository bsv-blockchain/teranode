package pruner

import (
	"github.com/bsv-blockchain/aerospike-client-go/v8"
)

// The pruner only removes records that are already provably safe to drop, and
// every removal is idempotent: a replica that misses the write is caught by the
// next partition scan and re-pruned. Blocking on a full-replication ACK
// therefore buys no correctness, so removals default to COMMIT_MASTER.
// pruner_relaxRemovalCommitLevel=false puts them back on COMMIT_ALL without a
// code change, for when the relaxation has to be ruled out in the field.
//
// This applies to the pruner's own record removals ONLY. Parent updates (the
// deletedChildren map writes and the addDeletedChildren UDF) mutate records that
// survive the prune, so they keep COMMIT_ALL, as does every write outside the
// pruner — see util.GetAerospikeWritePolicy.

// removalCommitLevel maps pruner_relaxRemovalCommitLevel onto the commit level
// used for the pruner's own record removals.
func removalCommitLevel(relax bool) aerospike.CommitLevel {
	if relax {
		return aerospike.COMMIT_MASTER
	}

	return aerospike.COMMIT_ALL
}

// newPrunerDeletePolicy builds the per-record policy for hard-deleting a pruned
// UTXO record.
func newPrunerDeletePolicy(commitLevel aerospike.CommitLevel) *aerospike.BatchDeletePolicy {
	policy := aerospike.NewBatchDeletePolicy()
	policy.CommitLevel = commitLevel

	return policy
}

// newPrunerTTLTouchPolicy builds the per-record policy used when
// pruner_utxoSetTTL is enabled: instead of deleting, the record's TTL is set to
// one second and Aerospike's nsup thread expires it. UPDATE_ONLY keeps the
// touch from resurrecting an already-deleted record as an empty stub.
func newPrunerTTLTouchPolicy(commitLevel aerospike.CommitLevel) *aerospike.BatchWritePolicy {
	policy := aerospike.NewBatchWritePolicy()
	policy.RecordExistsAction = aerospike.UPDATE_ONLY
	policy.Expiration = 1
	policy.CommitLevel = commitLevel

	return policy
}

// buildDeletionBatchRecords turns the pruned keys into the batch records that
// remove them: a hard delete per key, or a 1-second TTL touch when
// pruner_utxoSetTTL is enabled. Shared by executeBatchDeletions and the child
// half of executeBatchCleanupCombined so both paths get the same durability.
func buildDeletionBatchRecords(keys []*aerospike.Key, utxoSetTTL bool, commitLevel aerospike.CommitLevel) []aerospike.BatchRecordIfc {
	batchRecords := make([]aerospike.BatchRecordIfc, len(keys))

	if utxoSetTTL {
		ttlWritePolicy := newPrunerTTLTouchPolicy(commitLevel)
		for i, key := range keys {
			batchRecords[i] = aerospike.NewBatchWrite(ttlWritePolicy, key, aerospike.TouchOp())
		}

		return batchRecords
	}

	batchDeletePolicy := newPrunerDeletePolicy(commitLevel)
	for i, key := range keys {
		batchRecords[i] = aerospike.NewBatchDelete(batchDeletePolicy, key)
	}

	return batchRecords
}
