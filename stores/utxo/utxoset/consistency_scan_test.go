package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// drainScan reads an iterator to exhaustion and returns everything it yielded.
func drainScan(t *testing.T, it utxo.ConsistencyScanIterator) []*utxo.InconsistentTxRecord {
	t.Helper()

	var all []*utxo.InconsistentTxRecord

	for {
		batch, err := it.Next(t.Context())
		require.NoError(t, err)

		if batch == nil {
			break
		}

		all = append(all, batch...)
	}

	require.NoError(t, it.Err())

	return all
}

// plantIdent inserts an identity row directly, so a test can build the exact combination of
// mempool marker and block membership it needs without driving the whole write path.
func plantIdent(t *testing.T, s *Store, ctx context.Context, txid []byte, membership []byte, offChain *int32) {
	t.Helper()

	_, err := s.pool.Exec(ctx, `
        INSERT INTO tx_ident (leaf, txid, created_height, membership, off_chain_since)
        VALUES ($1, $2, 100, $3, $4)`, LeafFor(txid), txid, membership, offChain)
	require.NoError(t, err)
}

func idBytes(n byte) []byte {
	b := make([]byte, 32)
	for i := range b {
		b[i] = n
	}

	b[0] = n

	return b
}

func ptrI32(v int32) *int32 { return &v }

// TestConsistencyScanYieldsOnlyTheRowsWorthRepairing.
//
// The repair this feeds fixes transactions that carry block membership while still marked as
// waiting to be mined. A row with no membership cannot be one, and a row with no marker is not
// waiting, so yielding either would put work on the wire for the caller to throw away.
//
// A zero-length membership is deliberately excluded too. The length constraint admits it,
// because zero is a multiple of twelve, and it names no block, so it can never be repaired.
func TestConsistencyScanYieldsOnlyTheRowsWorthRepairing(t *testing.T) {
	s, ctx := newTestStore(t)

	wanted := idBytes(0x11)
	plantIdent(t, s, ctx, wanted, packTriples(t, [3]uint32{7, 700, 0}), ptrI32(100))

	// Settled: mined and no longer waiting.
	plantIdent(t, s, ctx, idBytes(0x12), packTriples(t, [3]uint32{7, 700, 0}), nil)
	// Ordinary mempool transaction: waiting, but in no block.
	plantIdent(t, s, ctx, idBytes(0x13), nil, ptrI32(100))
	// Membership present but empty, which names no block and can never be repaired.
	plantIdent(t, s, ctx, idBytes(0x14), []byte{}, ptrI32(100))

	it, err := s.ScanInconsistentUnminedTxs()
	require.NoError(t, err)

	defer func() { require.NoError(t, it.Close()) }()

	got := drainScan(t, it)

	require.Len(t, got, 1, "only the row that carries a block AND is still waiting")

	var want chainhash.Hash

	copy(want[:], wanted)
	require.Equal(t, want, got[0].Hash)
	require.Equal(t, []uint32{7}, got[0].BlockIDs, "and it carries every block it names")
	require.Equal(t, 100, got[0].UnminedSince)
}

// TestConsistencyScanIncludesConflictingRows. This is the one class the scan finds that the
// ordinary waiting-transaction iterator cannot, because that one masks conflicting rows out. A
// scan trusting the same predicate as the thing it double-checks would be decoration.
func TestConsistencyScanIncludesConflictingRows(t *testing.T) {
	s, ctx := newTestStore(t)

	txid := idBytes(0x21)
	plantIdent(t, s, ctx, txid, packTriples(t, [3]uint32{9, 900, 1}), ptrI32(100))

	_, err := s.pool.Exec(ctx, `UPDATE tx_ident SET flags = flags | $2 WHERE txid = $1`,
		txid, FlagConflicting)
	require.NoError(t, err)

	it, err := s.ScanInconsistentUnminedTxs()
	require.NoError(t, err)

	defer func() { require.NoError(t, it.Close()) }()

	require.Len(t, drainScan(t, it), 1, "a conflicting row is exactly what this scan is for")
}

// TestConsistencyScanYieldsAZeroMarker.
//
// Zero is a REAL height in this store: a transaction created before any block state has been
// pushed gets a marker of zero. The interface carries the marker as a plain int with no way to
// say "absent", and the caller treats zero as absent and drops the row. The scan must still
// yield it, so that the loss is the caller's decision and is visible, rather than the store
// quietly deciding some inconsistencies do not count.
func TestConsistencyScanYieldsAZeroMarker(t *testing.T) {
	s, ctx := newTestStore(t)

	plantIdent(t, s, ctx, idBytes(0x31), packTriples(t, [3]uint32{5, 500, 0}), ptrI32(0))

	it, err := s.ScanInconsistentUnminedTxs()
	require.NoError(t, err)

	defer func() { require.NoError(t, it.Close()) }()

	got := drainScan(t, it)
	require.Len(t, got, 1)
	require.Equal(t, 0, got[0].UnminedSince, "a zero marker is a real height and must reach the caller")
}

// TestConsistencyScanBatchesAtItsBound pins the memory bound at the boundary rather than only
// at zero rows. The scan exists because materialising the whole answer caused memory blowups,
// so a change that removed the bound and still passed every other test would be a regression
// nothing caught.
func TestConsistencyScanBatchesAtItsBound(t *testing.T) {
	s, ctx := newTestStore(t)

	for i := byte(0x41); i <= 0x43; i++ {
		plantIdent(t, s, ctx, idBytes(i), packTriples(t, [3]uint32{7, 700, 0}), ptrI32(100))
	}

	it, err := s.ScanInconsistentUnminedTxs()
	require.NoError(t, err)

	defer func() { require.NoError(t, it.Close()) }()

	scan, ok := it.(*consistencyScanIterator)
	require.True(t, ok)

	scan.batchSize = 2

	first, err := it.Next(ctx)
	require.NoError(t, err)
	require.Len(t, first, 2, "a batch must stop at the bound, not at the end of the answer")

	second, err := it.Next(ctx)
	require.NoError(t, err)
	require.Len(t, second, 1)

	third, err := it.Next(ctx)
	require.NoError(t, err)
	require.Nil(t, third, "exhaustion is a nil batch, never a non-nil empty one")
}

// TestConsistencyScanCountsWhatItYieldedUnderConcurrentReads.
//
// The caller polls the counter every ten seconds from a separate goroutine while the main one
// sits inside Next. Reading it without synchronisation is a data race that the race detector
// catches only if a test actually reads it concurrently, so this one does.
func TestConsistencyScanCountsWhatItYieldedUnderConcurrentReads(t *testing.T) {
	s, ctx := newTestStore(t)

	for i := byte(0x51); i <= 0x55; i++ {
		plantIdent(t, s, ctx, idBytes(i), packTriples(t, [3]uint32{7, 700, 0}), ptrI32(100))
	}

	it, err := s.ScanInconsistentUnminedTxs()
	require.NoError(t, err)

	defer func() { require.NoError(t, it.Close()) }()

	done := make(chan struct{})
	stopped := make(chan struct{})

	go func() {
		defer close(stopped)

		for {
			select {
			case <-done:
				return
			default:
				_ = it.TotalScanned()
			}
		}
	}()

	got := drainScan(t, it)

	close(done)
	<-stopped

	require.Len(t, got, 5)
	require.Equal(t, int64(5), it.TotalScanned(), "the counter reports what was yielded")
}

// TestConsistencyScanStopsWhenItsContextIsCancelled. A full reset can be abandoned while the
// scan is running, and reporting the cancellation as exhaustion would have the caller log that
// it found nothing wrong after a scan that never finished.
func TestConsistencyScanStopsWhenItsContextIsCancelled(t *testing.T) {
	s, ctx := newTestStore(t)

	for i := byte(0x61); i <= 0x63; i++ {
		plantIdent(t, s, ctx, idBytes(i), packTriples(t, [3]uint32{7, 700, 0}), ptrI32(100))
	}

	it, err := s.ScanInconsistentUnminedTxs()
	require.NoError(t, err)

	defer func() { _ = it.Close() }()

	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	_, err = it.Next(cancelled)
	require.Error(t, err, "a cancelled scan must not read as a clean finish")
	require.Error(t, it.Err(), "and the iterator must stay failed")
}
