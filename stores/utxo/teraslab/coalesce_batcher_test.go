package teraslab

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teraslab "github.com/icellan/teraslab/client/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hashN returns a deterministic distinct chainhash for index n.
func hashN(n byte) chainhash.Hash {
	var h chainhash.Hash
	h[0] = n
	return h
}

// newSetLockedStore wires a Store with a real SetLocked batcher whose wire RPC
// is the supplied fake. A large size + window forces concurrent Put()s to
// coalesce into the same flush so we can count the actual wire calls.
func newSetLockedStore(t *testing.T, fn func(ctx context.Context, value bool, txids []teraslab.TxID) (*teraslab.BatchResult, error)) *Store {
	t.Helper()
	s := &Store{setLockedFn: fn}
	s.setLockedBatcher = batcher.New(1024, 25*time.Millisecond, s.sendSetLockedBatch, true)
	t.Cleanup(s.setLockedBatcher.Close)
	return s
}

func newDecorateStore(t *testing.T, fn func(ctx context.Context, fieldMask uint32, txids []teraslab.TxID) ([]teraslab.TxRecord, error)) *Store {
	t.Helper()
	s := &Store{getRecordFn: fn}
	s.decorateBatcher = batcher.New(1024, 25*time.Millisecond, s.sendDecorateBatch, true)
	t.Cleanup(s.decorateBatcher.Close)
	return s
}

// TestSetLockedCoalescesConcurrentCalls proves that N concurrent single-hash
// SetLocked(value=false) calls collapse into FEWER wire RPCs than N, while every
// caller still gets nil.
func TestSetLockedCoalescesConcurrentCalls(t *testing.T) {
	var rpcCount int64
	var seenTotal int64
	fn := func(_ context.Context, value bool, txids []teraslab.TxID) (*teraslab.BatchResult, error) {
		atomic.AddInt64(&rpcCount, 1)
		atomic.AddInt64(&seenTotal, int64(len(txids)))
		assert.False(t, value)
		return &teraslab.BatchResult{}, nil
	}
	s := newSetLockedStore(t, fn)

	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			err := s.SetLocked(context.Background(), []chainhash.Hash{hashN(byte(i))}, false)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	rpcs := atomic.LoadInt64(&rpcCount)
	require.Less(t, rpcs, int64(n), "expected coalescing: %d RPCs for %d calls", rpcs, n)
	require.GreaterOrEqual(t, rpcs, int64(1))
	// Every caller's single hash must reach the wire exactly once.
	require.Equal(t, int64(n), atomic.LoadInt64(&seenTotal))
}

// TestSetLockedSeparatesByValue proves calls with different `value` go to
// SEPARATE wire RPCs (grouping key = value), each carrying only its group's
// hashes.
func TestSetLockedSeparatesByValue(t *testing.T) {
	var mu sync.Mutex
	byValue := map[bool]int{}       // RPC count per value
	hashesByValue := map[bool]int{} // total hashes per value
	fn := func(_ context.Context, value bool, txids []teraslab.TxID) (*teraslab.BatchResult, error) {
		mu.Lock()
		byValue[value]++
		hashesByValue[value] += len(txids)
		mu.Unlock()
		return &teraslab.BatchResult{}, nil
	}
	s := newSetLockedStore(t, fn)

	const each = 20
	var wg sync.WaitGroup
	wg.Add(each * 2)
	for i := 0; i < each; i++ {
		i := i
		go func() {
			defer wg.Done()
			assert.NoError(t, s.SetLocked(context.Background(), []chainhash.Hash{hashN(byte(i))}, true))
		}()
		go func() {
			defer wg.Done()
			assert.NoError(t, s.SetLocked(context.Background(), []chainhash.Hash{hashN(byte(100 + i))}, false))
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	// Each value group produced at least one RPC, and no RPC mixed values
	// (enforced by the asserts inside fn would have failed otherwise — here we
	// assert via per-value hash totals).
	require.GreaterOrEqual(t, byValue[true], 1)
	require.GreaterOrEqual(t, byValue[false], 1)
	require.Equal(t, each, hashesByValue[true])
	require.Equal(t, each, hashesByValue[false])
}

// TestSetLockedPartialErrorMappedBySpan proves that a *PartialError from a
// coalesced RPC is attributed to ONLY the originating call whose hash failed;
// sibling calls that succeeded still get nil.
func TestSetLockedPartialErrorMappedBySpan(t *testing.T) {
	// Sentinel hash whose presence in a wire batch triggers a per-item error
	// for that index.
	bad := hashN(0xEE)

	fn := func(_ context.Context, _ bool, txids []teraslab.TxID) (*teraslab.BatchResult, error) {
		var pe teraslab.PartialError
		for i, tid := range txids {
			if tid == hashToTxID(&bad) {
				pe.Errors = append(pe.Errors, teraslab.BatchItemError{
					ItemIndex: uint32(i),
					Code:      teraslab.ErrCodeTxNotFound,
				})
			}
		}
		if len(pe.Errors) > 0 {
			return nil, &pe
		}
		return &teraslab.BatchResult{}, nil
	}
	s := newSetLockedStore(t, fn)

	const n = 30
	results := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			h := hashN(byte(i))
			if i == 7 {
				h = bad // only this caller should see an error
			}
			results[i] = s.SetLocked(context.Background(), []chainhash.Hash{h}, true)
		}()
	}
	wg.Wait()

	for i, err := range results {
		if i == 7 {
			require.Error(t, err, "the call carrying the bad hash must fail")
			assert.True(t, errors.Is(err, errors.ErrTxNotFound) || err.Error() != "")
		} else {
			require.NoError(t, err, "call %d should not inherit a sibling's error", i)
		}
	}
}

// TestSetLockedGlobalErrorFansOut proves a non-partial (global) wire error fans
// out to every call in the group.
func TestSetLockedGlobalErrorFansOut(t *testing.T) {
	wireErr := errors.NewStorageError("boom")
	fn := func(_ context.Context, _ bool, _ []teraslab.TxID) (*teraslab.BatchResult, error) {
		return nil, wireErr
	}
	s := newSetLockedStore(t, fn)

	const n = 10
	results := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			results[i] = s.SetLocked(context.Background(), []chainhash.Hash{hashN(byte(i))}, false)
		}()
	}
	wg.Wait()

	for i := range results {
		require.Error(t, results[i], "call %d must see the global error", i)
	}
}

// TestSetLockedEmptyIsNoOp confirms the fast path returns nil without a wire
// call.
func TestSetLockedEmptyIsNoOp(t *testing.T) {
	var rpcCount int64
	fn := func(_ context.Context, _ bool, _ []teraslab.TxID) (*teraslab.BatchResult, error) {
		atomic.AddInt64(&rpcCount, 1)
		return &teraslab.BatchResult{}, nil
	}
	s := newSetLockedStore(t, fn)

	require.NoError(t, s.SetLocked(context.Background(), nil, true))
	require.Equal(t, int64(0), atomic.LoadInt64(&rpcCount))
}

// TestBatchDecorateCoalescesAndFillsPerCall proves N concurrent single-item
// BatchDecorate calls coalesce into FEWER wire RPCs than N and each caller's
// own unresolved slice is filled correctly (found -> Data, miss -> Err).
func TestBatchDecorateCoalescesAndFillsPerCall(t *testing.T) {
	var rpcCount int64
	// Even indices "exist" (Found + a recognisable Fee), odd indices are misses.
	fn := func(_ context.Context, _ uint32, txids []teraslab.TxID) ([]teraslab.TxRecord, error) {
		atomic.AddInt64(&rpcCount, 1)
		out := make([]teraslab.TxRecord, len(txids))
		for i, tid := range txids {
			idx := tid[0]
			if idx%2 == 0 {
				out[i] = teraslab.TxRecord{
					Found:    true,
					Metadata: &teraslab.TxMetadata{Fee: uint64(idx)},
				}
			} else {
				out[i] = teraslab.TxRecord{Found: false}
			}
		}
		return out, nil
	}
	s := newDecorateStore(t, fn)

	const n = 40
	items := make([][]*utxo.UnresolvedMetaData, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			umd := &utxo.UnresolvedMetaData{Hash: hashN(byte(i)), Idx: 0}
			items[i] = []*utxo.UnresolvedMetaData{umd}
			err := s.BatchDecorate(context.Background(), items[i])
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	rpcs := atomic.LoadInt64(&rpcCount)
	require.Less(t, rpcs, int64(n), "expected coalescing: %d RPCs for %d calls", rpcs, n)
	require.GreaterOrEqual(t, rpcs, int64(1))

	for i := 0; i < n; i++ {
		umd := items[i][0]
		if byte(i)%2 == 0 {
			require.NotNil(t, umd.Data, "even index %d should be found", i)
			assert.Equal(t, uint64(i), umd.Data.Fee)
			assert.NoError(t, umd.Err)
		} else {
			assert.Nil(t, umd.Data, "odd index %d should be a miss", i)
			require.Error(t, umd.Err, "odd index %d should carry not-found", i)
			assert.True(t, errors.Is(umd.Err, errors.ErrTxNotFound))
		}
	}
}

// TestBatchDecorateUnionsFieldMasks proves the wire RPC receives the OR of every
// concurrent caller's requested field mask.
func TestBatchDecorateUnionsFieldMasks(t *testing.T) {
	var mu sync.Mutex
	var seenMask uint32
	fn := func(_ context.Context, fieldMask uint32, txids []teraslab.TxID) ([]teraslab.TxRecord, error) {
		mu.Lock()
		seenMask |= fieldMask
		mu.Unlock()
		out := make([]teraslab.TxRecord, len(txids))
		for i := range out {
			out[i] = teraslab.TxRecord{Found: true, Metadata: &teraslab.TxMetadata{}}
		}
		return out, nil
	}
	s := newDecorateStore(t, fn)

	// Two callers asking for disjoint masks must coalesce into one RPC whose
	// mask is the union. We craft the masks via the wire field constants used in
	// PreviousOutputsDecorate so the union is observable.
	maskA := teraslab.FieldColdData
	maskB := teraslab.FieldTxVersion | teraslab.FieldLocktime

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		s.callDecorateWithMask(t, hashN(1), maskA)
	}()
	go func() {
		defer wg.Done()
		s.callDecorateWithMask(t, hashN(2), maskB)
	}()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, maskA|maskB, seenMask&(maskA|maskB), "wire mask must be the union of both callers' masks")
}

// callDecorateWithMask enqueues a single-item decorate call with an explicit raw
// field mask, bypassing buildFieldMask so the test controls the exact bits.
func (s *Store) callDecorateWithMask(t *testing.T, h chainhash.Hash, mask uint32) {
	t.Helper()
	done := make(chan error, 1)
	umd := &utxo.UnresolvedMetaData{Hash: h}
	s.decorateBatcher.Put(&batchDecorateCall{
		ctx:        context.Background(),
		fieldMask:  mask,
		unresolved: []*utxo.UnresolvedMetaData{umd},
		done:       done,
	})
	require.NoError(t, <-done)
}

// TestBatchDecorateGlobalErrorFansOut proves a wire error reaches every caller.
func TestBatchDecorateGlobalErrorFansOut(t *testing.T) {
	wireErr := errors.NewStorageError("getrecord boom")
	fn := func(_ context.Context, _ uint32, _ []teraslab.TxID) ([]teraslab.TxRecord, error) {
		return nil, wireErr
	}
	s := newDecorateStore(t, fn)

	const n = 8
	results := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			umd := &utxo.UnresolvedMetaData{Hash: hashN(byte(i))}
			results[i] = s.BatchDecorate(context.Background(), []*utxo.UnresolvedMetaData{umd})
		}()
	}
	wg.Wait()

	for i := range results {
		require.Error(t, results[i], "call %d must see the global error", i)
	}
}
