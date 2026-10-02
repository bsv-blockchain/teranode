//go:build aerospike

package aerospike_test

import (
	"context"
	crand "crypto/rand"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	aerostore "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	utxotesthelper "github.com/bsv-blockchain/teranode/test/longtest/stores/utxo"
	"github.com/stretchr/testify/require"
)

// newStoreForBatchTests spins up a testcontainers Aerospike instance and returns
// a ready *aerostore.Store plus a cleanup function. It reuses initAerospike from
// container_helper_test.go which is compiled into this test binary.
//
// The returned store has an in-memory external store and all test-safe defaults.
func newStoreForBatchTests(t *testing.T) (*aerostore.Store, func()) {
	t.Helper()

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	_, store, _, cleanup := initAerospike(t, tSettings, logger)

	return store, cleanup
}

// seedParentRecord creates a minimal tx with the given number of outputs,
// stores it via s.Create, and returns the tx's hash. The record is stored
// as an unmined transaction (blockHeight=0, no block info) so that the test
// is not sensitive to block-height assignment logic.
func seedParentRecord(t *testing.T, s *aerostore.Store, numOutputs int) *chainhash.Hash {
	t.Helper()

	tx, err := utxotesthelper.CreateTransaction(uint64(numOutputs)) //nolint:gosec
	require.NoError(t, err)

	ctx := context.Background()
	_, err = s.Create(ctx, tx, 0)
	require.NoError(t, err)

	return tx.TxIDChainHash()
}

// seedParentTx creates a minimal tx with the given number of outputs, stores
// it via s.Create, and returns the full *bt.Tx so callers can compute
// UTXOHashes and build *utxo.Spend entries.
func seedParentTx(t *testing.T, s *aerostore.Store, numOutputs int) *bt.Tx {
	t.Helper()

	tx, err := utxotesthelper.CreateTransaction(uint64(numOutputs)) //nolint:gosec
	require.NoError(t, err)

	ctx := context.Background()
	_, err = s.Create(ctx, tx, 0)
	require.NoError(t, err)

	return tx
}

// randSpendingHash returns a random 32-byte chainhash suitable for use as a
// spending-tx hash in test SpendingData.
func randSpendingHash(t *testing.T) *chainhash.Hash {
	t.Helper()
	b := make([]byte, 32)
	_, err := crand.Read(b)
	require.NoError(t, err)
	h, err := chainhash.NewHash(b)
	require.NoError(t, err)
	return h
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestBatchGetParents_AllPresent(t *testing.T) {
	ctx := context.Background()
	s, cleanup := newStoreForBatchTests(t)
	defer cleanup()

	parents := []*chainhash.Hash{
		seedParentRecord(t, s, 1),
		seedParentRecord(t, s, 2),
		seedParentRecord(t, s, 3),
	}

	hashes := make([][]byte, len(parents))
	for i, p := range parents {
		b := p.CloneBytes()
		hashes[i] = b
	}

	got, missing, err := s.BatchGetParents(ctx, hashes)
	require.NoError(t, err)
	require.Empty(t, missing)
	require.Len(t, got, 3)

	for _, p := range parents {
		var key [32]byte
		copy(key[:], p[:])
		_, ok := got[key]
		require.True(t, ok, "expected parent %x to be present in result", p[:])
	}
}

func TestBatchGetParents_SomeMissing(t *testing.T) {
	ctx := context.Background()
	s, cleanup := newStoreForBatchTests(t)
	defer cleanup()

	present := seedParentRecord(t, s, 1)
	var missingHash chainhash.Hash
	missingHash[0] = 0xff
	missingHash[1] = 0xee

	got, missing, err := s.BatchGetParents(ctx, [][]byte{present[:], missingHash[:]})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Len(t, missing, 1)
	require.Equal(t, missingHash[:], missing[0])
}

func TestBatchGetParents_EmptyInput(t *testing.T) {
	ctx := context.Background()
	s, cleanup := newStoreForBatchTests(t)
	defer cleanup()

	got, missing, err := s.BatchGetParents(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, got)
	require.Empty(t, missing)
}

// ---------------------------------------------------------------------------
// BatchSpend tests
// ---------------------------------------------------------------------------

func TestBatchSpend_AllSucceed(t *testing.T) {
	ctx := context.Background()
	s, cleanup := newStoreForBatchTests(t)
	defer cleanup()

	// Seed a parent with 2 outputs and build proper spend entries.
	parentTx := seedParentTx(t, s, 2)
	parentHash := parentTx.TxIDChainHash()
	spendingHash := randSpendingHash(t)

	sp0Hash, err := util.UTXOHashFromOutput(parentHash, parentTx.Outputs[0], 0)
	require.NoError(t, err)
	sp1Hash, err := util.UTXOHashFromOutput(parentHash, parentTx.Outputs[1], 1)
	require.NoError(t, err)

	spends := []*utxo.Spend{
		{
			TxID:         parentHash,
			Vout:         0,
			UTXOHash:     sp0Hash,
			SpendingData: spendpkg.NewSpendingData(spendingHash, 0),
		},
		{
			TxID:         parentHash,
			Vout:         1,
			UTXOHash:     sp1Hash,
			SpendingData: spendpkg.NewSpendingData(spendingHash, 1),
		},
	}

	results, err := s.BatchSpend(ctx, spends, 1)
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.NoError(t, results[0].Err)
	require.NoError(t, results[1].Err)
}

func TestBatchSpend_PerRecordErrorMapsToInput(t *testing.T) {
	ctx := context.Background()
	s, cleanup := newStoreForBatchTests(t)
	defer cleanup()

	// Seed a parent with 1 output, then spend it with a wrong UTXOHash — the
	// Lua will return UTXO_HASH_MISMATCH which surfaces as results[0].Err.
	parentTx := seedParentTx(t, s, 1)
	parentHash := parentTx.TxIDChainHash()
	spendingHash := randSpendingHash(t)

	// Build a deliberately wrong UTXOHash (all zeros).
	wrongHash, err := chainhash.NewHash(make([]byte, 32))
	require.NoError(t, err)

	spends := []*utxo.Spend{
		{
			TxID:         parentHash,
			Vout:         0,
			UTXOHash:     wrongHash,
			SpendingData: spendpkg.NewSpendingData(spendingHash, 0),
		},
	}

	results, err := s.BatchSpend(ctx, spends, 1)
	require.NoError(t, err, "whole-call err must be nil for per-record Lua failures")
	require.Len(t, results, 1)
	require.Error(t, results[0].Err, "expected per-record error for hash mismatch")
}

func TestBatchSpend_EmptyInput(t *testing.T) {
	ctx := context.Background()
	s, cleanup := newStoreForBatchTests(t)
	defer cleanup()

	got, err := s.BatchSpend(ctx, nil, 1)
	require.NoError(t, err)
	require.Empty(t, got)
}

// ---------------------------------------------------------------------------
// BatchCreate tests
// ---------------------------------------------------------------------------

// buildMinimalTx returns a minimal, self-consistent *bt.Tx with the given
// number of outputs. Each call produces a unique transaction because
// utxotesthelper.CreateTransaction generates a random input txid. The tx is
// NOT seeded into the store; callers are responsible for creating it.
func buildMinimalTx(t *testing.T, numOutputs int) *bt.Tx {
	t.Helper()
	tx, err := utxotesthelper.CreateTransaction(uint64(numOutputs)) //nolint:gosec
	require.NoError(t, err)
	return tx
}

func TestBatchCreate_AllSucceed(t *testing.T) {
	ctx := context.Background()
	s, cleanup := newStoreForBatchTests(t)
	defer cleanup()

	txs := []*bt.Tx{buildMinimalTx(t, 1), buildMinimalTx(t, 1), buildMinimalTx(t, 1)}

	results, err := s.BatchCreate(ctx, txs, 100, true)
	require.NoError(t, err)
	require.Len(t, results, 3)
	for i, r := range results {
		require.NoError(t, r.Err, "index %d", i)
	}

	// Spot-check: Get should now return the first record with Locked=true.
	md, err := s.Get(ctx, txs[0].TxIDChainHash())
	require.NoError(t, err)
	require.NotNil(t, md)
	require.True(t, md.Locked, "expected lockedTrue")
}

func TestBatchCreate_CreateOnlyViolation(t *testing.T) {
	ctx := context.Background()
	s, cleanup := newStoreForBatchTests(t)
	defer cleanup()

	tx := buildMinimalTx(t, 1)

	// First create succeeds.
	r1, err := s.BatchCreate(ctx, []*bt.Tx{tx}, 100, true)
	require.NoError(t, err)
	require.NoError(t, r1[0].Err)

	// Second create on same hash must fail per-record (not whole-call).
	r2, err := s.BatchCreate(ctx, []*bt.Tx{tx}, 100, true)
	require.NoError(t, err, "whole-call err must be nil for per-record CREATE_ONLY violations")
	require.Len(t, r2, 1)
	require.Error(t, r2[0].Err)
}

func TestBatchCreate_EmptyInput(t *testing.T) {
	ctx := context.Background()
	s, cleanup := newStoreForBatchTests(t)
	defer cleanup()

	results, err := s.BatchCreate(ctx, nil, 0, false)
	require.NoError(t, err)
	require.Empty(t, results)
}

// ---------------------------------------------------------------------------
// BatchSetLocked tests
// ---------------------------------------------------------------------------

func TestBatchSetLocked_Unlock(t *testing.T) {
	ctx := context.Background()
	s, cleanup := newStoreForBatchTests(t)
	defer cleanup()

	// Seed two records with locked=true via BatchCreate.
	txs := []*bt.Tx{buildMinimalTx(t, 1), buildMinimalTx(t, 1)}
	createRes, err := s.BatchCreate(ctx, txs, 100, true)
	require.NoError(t, err)
	for i, r := range createRes {
		require.NoError(t, r.Err, "index %d", i)
	}

	hashes := make([][]byte, len(txs))
	for i, tx := range txs {
		h := tx.TxIDChainHash()
		hashes[i] = h[:]
	}

	results, err := s.BatchSetLocked(ctx, hashes, false)
	require.NoError(t, err)
	require.Len(t, results, 2)
	for i, r := range results {
		require.NoError(t, r.Err, "index %d", i)
	}

	// Spot-check via Get: locked flag must now be false.
	md, err := s.Get(ctx, txs[0].TxIDChainHash())
	require.NoError(t, err)
	require.False(t, md.Locked)
}

func TestBatchSetLocked_MissingRecordPerRecord(t *testing.T) {
	ctx := context.Background()
	s, cleanup := newStoreForBatchTests(t)
	defer cleanup()

	// 32 bytes that were never seeded into the store.
	never := make([]byte, 32)
	never[0] = 0x77

	results, err := s.BatchSetLocked(ctx, [][]byte{never}, false)
	require.NoError(t, err, "whole-call err must be nil for per-record key-not-found")
	require.Len(t, results, 1)
	require.Error(t, results[0].Err)
}

func TestBatchSetLocked_EmptyInput(t *testing.T) {
	ctx := context.Background()
	s, cleanup := newStoreForBatchTests(t)
	defer cleanup()

	results, err := s.BatchSetLocked(ctx, nil, false)
	require.NoError(t, err)
	require.Empty(t, results)
}

// ---------------------------------------------------------------------------
// BatchSetDAH tests
// ---------------------------------------------------------------------------

func TestBatchSetDAH_HappyPath(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, s, ctx, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()

	// Seed one record via BatchCreate.
	tx := buildMinimalTx(t, 1)
	_, err := s.BatchCreate(ctx, []*bt.Tx{tx}, 100, false)
	require.NoError(t, err)

	h := tx.TxIDChainHash()
	items := []aerostore.SetDAHItem{{TxHash: h[:], DAH: 42}}

	results, err := s.BatchSetDAH(ctx, items)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.NoError(t, results[0].Err)

	// Verify the raw bin via the Aerospike client — meta.Data does not expose DAH.
	key, err := aerospike.NewKey(s.GetNamespace(), s.GetName(), h[:])
	require.NoError(t, err)

	record, err := client.Get(util.GetAerospikeReadPolicy(tSettings), key, fields.DeleteAtHeight.String())
	require.NoError(t, err)
	require.NotNil(t, record)
	require.Equal(t, 42, record.Bins[fields.DeleteAtHeight.String()])
}

func TestBatchSetDAH_MissingRecordPerRecord(t *testing.T) {
	ctx := context.Background()
	s, cleanup := newStoreForBatchTests(t)
	defer cleanup()

	// 32 bytes that were never seeded into the store.
	never := make([]byte, 32)
	never[0] = 0x88

	items := []aerostore.SetDAHItem{{TxHash: never, DAH: 99}}
	results, err := s.BatchSetDAH(ctx, items)
	require.NoError(t, err, "whole-call err must be nil for per-record key-not-found")
	require.Len(t, results, 1)
	require.Error(t, results[0].Err)
}

func TestBatchSetDAH_EmptyInput(t *testing.T) {
	ctx := context.Background()
	s, cleanup := newStoreForBatchTests(t)
	defer cleanup()

	results, err := s.BatchSetDAH(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, results)
}
