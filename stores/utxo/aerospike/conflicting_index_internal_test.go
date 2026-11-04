package aerospike

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// TestConflictingIndexNotReadable pins which query errors from the client mean
// the index is missing or still building, so the store falls back, as opposed
// to a query failure, which is an error.
func TestConflictingIndexNotReadable(t *testing.T) {
	require.True(t, conflictingIndexNotReadable(&aerospike.AerospikeError{ResultCode: types.INDEX_NOTFOUND}))
	require.True(t, conflictingIndexNotReadable(&aerospike.AerospikeError{ResultCode: types.INDEX_NOTREADABLE}))
	require.False(t, conflictingIndexNotReadable(&aerospike.AerospikeError{ResultCode: types.TIMEOUT}))
	require.False(t, conflictingIndexNotReadable(&aerospike.AerospikeError{ResultCode: types.PARAMETER_ERROR}))
}

// TestConflictingIndex_WithoutAClient pins that a store without a server
// connection creates no index, starts no self-test and answers not indexed.
func TestConflictingIndex_WithoutAClient(t *testing.T) {
	s := &Store{client: &uaerospike.Client{}}

	require.NoError(t, s.createExpressionIndexIfNotExists(conflictingIndexName, conflictingIndexExpression()))

	s.startConflictingIndexSelfTest(context.Background())
	require.Nil(t, s.conflictingIndexDone, "a self-test started without a client")
	s.stopConflictingIndexSelfTest()

	hashes, indexed, err := s.ConflictingTxHashes(context.Background())
	require.NoError(t, err)
	require.False(t, indexed)
	require.Nil(t, hashes)
}

// TestRecordConflictingIndexProbe pins that a probe the index fails is retried
// before the index is given up for the process: a miss can be transient, for
// example while partitions migrate, and giving up is permanent.
func TestRecordConflictingIndexProbe(t *testing.T) {
	t.Run("misses until the limit give up", func(t *testing.T) {
		s := &Store{logger: ulogger.TestLogger{}}

		for i := 1; i < conflictingIndexProbeMissLimit; i++ {
			require.False(t, s.recordConflictingIndexProbe(false), "gave up after %d misses", i)
			require.Equal(t, conflictingIndexUnchecked, s.conflictingIndexState.Load())
		}

		require.True(t, s.recordConflictingIndexProbe(false))
		require.Equal(t, conflictingIndexUnusable, s.conflictingIndexState.Load())
	})

	t.Run("a pass after a miss trusts the index", func(t *testing.T) {
		s := &Store{logger: ulogger.TestLogger{}}

		require.False(t, s.recordConflictingIndexProbe(false))
		require.True(t, s.recordConflictingIndexProbe(true))
		require.Equal(t, conflictingIndexUsable, s.conflictingIndexState.Load())
	})
}
