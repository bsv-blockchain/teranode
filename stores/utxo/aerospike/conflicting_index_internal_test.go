package aerospike

import (
	"context"
	"testing"
	"time"

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

// TestReadConflictingIndexResults_StalledQuery pins that a cancelled read of the conflicting
// index returns at once although a node never answers. Closing a recordset waits for every node
// goroutine, and one stuck in a socket read holds that until the query policy's socket timeout;
// the close has to happen off the caller's path, but it still has to happen.
func TestReadConflictingIndexResults_StalledQuery(t *testing.T) {
	stalled := make(chan *aerospike.Result) // a node that never answers
	unblockClose := make(chan struct{})
	closed := make(chan struct{})

	closeRecordset := func() {
		<-unblockClose // Close waits for the stuck node goroutine
		close(closed)
	}

	ctx, cancel := context.WithCancel(context.Background())

	returned := make(chan error, 1)

	go func() {
		_, _, err := readConflictingIndexResults(ctx, stalled, closeRecordset, func(*aerospike.Record) bool { return false })
		returned <- err
	}()

	cancel()

	select {
	case err := <-returned:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "a cancelled read waited for the stalled query to close")
	}

	close(unblockClose)

	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the recordset of a cancelled read was never closed")
	}
}

// TestReadConflictingIndexResults_DrainsWhileClosing pins that an early return does not leave
// the background close waiting forever on a node goroutine blocked sending into a full results
// buffer, as the client's error path does without watching for cancellation: the results have
// to be drained while the recordset closes.
func TestReadConflictingIndexResults_DrainsWhileClosing(t *testing.T) {
	nodeErr := &aerospike.Result{Err: &aerospike.AerospikeError{ResultCode: types.TIMEOUT}}

	results := make(chan *aerospike.Result, 1)
	results <- nodeErr // one node's error ends the read early

	producerDone := make(chan struct{})

	go func() {
		// Another node's error, sent into the buffer once it is full again, blocks until
		// someone reads.
		results <- nodeErr
		results <- nodeErr
		close(producerDone)
	}()

	closed := make(chan struct{})
	closeRecordset := func() {
		<-producerDone // Close waits for every node goroutine
		close(results) // which then ends the results
		close(closed)
	}

	_, _, err := readConflictingIndexResults(context.Background(), results, closeRecordset, func(*aerospike.Record) bool { return false })
	require.Error(t, err)

	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the background close waited forever on a blocked node goroutine")
	}
}

// TestReadConflictingIndexResults_Outcomes pins the answer for each way a read ends, and that
// the recordset is closed every time.
func TestReadConflictingIndexResults_Outcomes(t *testing.T) {
	record := &aerospike.Record{}

	tests := map[string]struct {
		results      []*aerospike.Result
		wantFound    bool
		wantReadable bool
		wantErr      bool
	}{
		"records then end":    {results: []*aerospike.Result{{Record: record}}, wantFound: true, wantReadable: true},
		"no records":          {wantReadable: true},
		"index not readable":  {results: []*aerospike.Result{{Err: &aerospike.AerospikeError{ResultCode: types.INDEX_NOTREADABLE}}}},
		"node error":          {results: []*aerospike.Result{{Err: &aerospike.AerospikeError{ResultCode: types.TIMEOUT}}}, wantErr: true},
		"error after records": {results: []*aerospike.Result{{Record: record}, {Err: &aerospike.AerospikeError{ResultCode: types.TIMEOUT}}}, wantErr: true},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			results := make(chan *aerospike.Result, len(tc.results))
			for _, r := range tc.results {
				results <- r
			}

			close(results)

			closed := make(chan struct{})

			found, readable, err := readConflictingIndexResults(context.Background(), results, func() { close(closed) }, func(*aerospike.Record) bool { return true })
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tc.wantFound && !tc.wantErr, found)
			require.Equal(t, tc.wantReadable, readable)

			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				require.FailNow(t, "the recordset was never closed")
			}
		})
	}
}

// TestReadQueryResults_WaitsOnErrorForTheClose pins that a WAL read ending on a node error returns
// only once its recordset has closed. Its caller retries, and a retry that started while the last
// walk's node reads still ran would overlap two namespace walks.
func TestReadQueryResults_WaitsOnErrorForTheClose(t *testing.T) {
	newRead := func() (chan *aerospike.Result, chan struct{}, func()) {
		results := make(chan *aerospike.Result, 1)
		results <- &aerospike.Result{Err: &aerospike.AerospikeError{ResultCode: types.TIMEOUT}}

		unblockClose := make(chan struct{})
		closeRecordset := func() {
			<-unblockClose // Close waits for a node goroutine stuck in a socket read
			close(results)
		}

		return results, unblockClose, closeRecordset
	}

	t.Run("waits for the close", func(t *testing.T) {
		results, unblockClose, closeRecordset := newRead()

		returned := make(chan error, 1)

		go func() {
			_, _, err := readQueryResults(context.Background(), "wal read failed", true, results, closeRecordset, func(*aerospike.Record) bool { return false })
			returned <- err
		}()

		select {
		case <-returned:
			require.FailNow(t, "the read returned while its recordset was still closing")
		case <-time.After(100 * time.Millisecond):
		}

		close(unblockClose)

		select {
		case err := <-returned:
			require.ErrorContains(t, err, "wal read failed")
		case <-time.After(5 * time.Second):
			require.FailNow(t, "the read never returned after the close")
		}
	})

	t.Run("a cancelled context ends the wait", func(t *testing.T) {
		results, unblockClose, closeRecordset := newRead()
		defer close(unblockClose)

		ctx, cancel := context.WithCancel(context.Background())
		returned := make(chan error, 1)

		go func() {
			_, _, err := readQueryResults(ctx, "wal read failed", true, results, closeRecordset, func(*aerospike.Record) bool { return false })
			returned <- err
		}()

		cancel()

		select {
		case <-returned:
		case <-time.After(5 * time.Second):
			require.FailNow(t, "a cancelled read kept waiting for the close")
		}
	})
}
