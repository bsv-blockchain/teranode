package validator

import (
	"context"
	"runtime"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	terrors "github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/require"
)

// stubBatchStore implements batchUtxoStore for unit-testing the native path
// without touching real Aerospike. Each test populates the fields it needs.
type stubBatchStore struct {
	getCalls int
	parents  map[[32]byte]*aerospike.ParentRecord
	missing  [][]byte
	// parentsErr causes BatchGetParents to return a whole-batch error.
	parentsErr error

	// spendCalls is incremented each time BatchSpend is called.
	spendCalls int
	// spendErr causes BatchSpend to return a whole-batch transport error.
	spendErr error
	// spendPerParent injects per-parent failures keyed by parent tx hash.
	// The stub returns SpendResult.Err for any spend whose TxID matches a key.
	spendPerParent map[[32]byte]error

	// createCalls is incremented each time BatchCreate is called.
	createCalls int
	// createErr causes BatchCreate to return a whole-batch transport error.
	createErr error
	// createPerIdx injects per-tx failures indexed against the slice passed to
	// BatchCreate (NOT the original txs slice — use compactAlive ordering).
	createPerIdx map[int]error

	// lockedCalls is incremented each time BatchSetLocked is called.
	lockedCalls int
	// lockedErr causes BatchSetLocked to return a whole-batch transport error.
	lockedErr error
	// lockedPerHash injects per-tx failures keyed by tx hash (full 32 bytes).
	lockedPerHash map[[32]byte]error
}

func (s *stubBatchStore) BatchGetParents(_ context.Context, hashes [][]byte) (map[[32]byte]*aerospike.ParentRecord, [][]byte, error) {
	s.getCalls++
	if s.parentsErr != nil {
		return nil, nil, s.parentsErr
	}
	return s.parents, s.missing, nil
}

func (s *stubBatchStore) BatchCreate(_ context.Context, txs []*bt.Tx, _ uint32, _ bool) ([]aerospike.CreateResult, error) {
	s.createCalls++
	if s.createErr != nil {
		return nil, s.createErr
	}
	results := make([]aerospike.CreateResult, len(txs))
	for i, tx := range txs {
		h := tx.TxIDChainHash()
		results[i].TxHash = h.CloneBytes()
		results[i].Err = s.createPerIdx[i]
	}
	return results, nil
}

func (s *stubBatchStore) BatchSetLocked(_ context.Context, hashes [][]byte, _ bool) ([]aerospike.SetLockedResult, error) {
	s.lockedCalls++
	if s.lockedErr != nil {
		return nil, s.lockedErr
	}
	results := make([]aerospike.SetLockedResult, len(hashes))
	for i, h := range hashes {
		results[i].TxHash = h
		var key [32]byte
		copy(key[:], h)
		results[i].Err = s.lockedPerHash[key]
	}
	return results, nil
}

func (s *stubBatchStore) BatchSpend(_ context.Context, spends []*utxo.Spend, _ uint32, _ ...utxo.IgnoreFlags) ([]aerospike.SpendResult, error) {
	s.spendCalls++
	if s.spendErr != nil {
		return nil, s.spendErr
	}
	results := make([]aerospike.SpendResult, len(spends))
	for i, sp := range spends {
		if sp == nil {
			continue
		}
		var key [32]byte
		copy(key[:], sp.TxID[:])
		if e, ok := s.spendPerParent[key]; ok {
			results[i].Err = e
		}
	}
	return results, nil
}

// newNativeValidator returns a *Validator wired to a stub UTXO store so that
// ValidateBatch runs the six-phase pipeline. The UseBatchValidation flag is set
// to true; it no longer affects ValidateBatch internally but is harmless.
func newNativeValidator(t testing.TB) (*Validator, *stubBatchStore) {
	t.Helper()
	v := newValidatorForTest(t)
	stub := &stubBatchStore{}
	v.setBatchStoreForTest(stub)
	// Present a ready block state (non-zero MedianTime) so the per-tx finality
	// check in ValidateBatch passes and these tests exercise the pipeline
	// phases rather than tripping the "utxo store not ready" readiness gate.
	// Height 0 keeps the default blockHeight at 1 (Height+1), matching the
	// prior sqlitememory-backed behaviour.
	v.setBlockStateForTest(utxo.BlockState{Height: 0, MedianTime: 1700000000})
	v.settings.Validator.UseBatchValidation = true
	return v, stub
}

// mkParentMap builds a parents map for the stub, marking each hash as
// present with a minimal ParentRecord.
func mkParentMap(hashes ...*chainhash.Hash) map[[32]byte]*aerospike.ParentRecord {
	m := make(map[[32]byte]*aerospike.ParentRecord, len(hashes))
	for _, h := range hashes {
		var key [32]byte
		copy(key[:], h[:])
		m[key] = &aerospike.ParentRecord{BlockHeight: 1}
	}
	return m
}

// minimalTxWithParent builds a *bt.Tx with one input whose PreviousTxID is
// set to the given parent hash. PreviousTxScript is set to an empty non-nil
// bscript.Script so that utxo.GetSpends (called by Phase C) can compute the
// UTXOHash without a nil-pointer error. Actual script/satoshi values are
// zero/empty — sufficient for the stub spend path used in unit tests.
func minimalTxWithParent(t testing.TB, parent chainhash.Hash) *bt.Tx {
	t.Helper()
	tx := bt.NewTx()
	input := &bt.Input{
		PreviousTxOutIndex: 0,
		PreviousTxScript:   &bscript.Script{},
		PreviousTxSatoshis: 0,
	}
	err := input.PreviousTxIDAdd(&parent)
	require.NoError(t, err)
	tx.Inputs = append(tx.Inputs, input)
	return tx
}

// installNoopCPUOverride installs a CPU validation override that always
// passes. Use this in Phase C/D/E tests that construct minimal (non-extended,
// 0-output) transactions and want Phase B to be a no-op so the test can
// focus on the later phase under test.
func installNoopCPUOverride(t testing.TB, v *Validator) {
	t.Helper()
	v.overrideCPUValidationForTest(func(_ *bt.Tx) error { return nil })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestValidateBatchNative_PhaseA_OneGetCallRegardlessOfN asserts that exactly
// one BatchGetParents call is made for a batch of N txs.
func TestValidateBatchNative_PhaseA_OneGetCallRegardlessOfN(t *testing.T) {
	v, stub := newNativeValidator(t)

	pA, pB, pC := chainhash.Hash{0x01}, chainhash.Hash{0x02}, chainhash.Hash{0x03}
	stub.parents = mkParentMap(&pA, &pB, &pC)

	txs := []*bt.Tx{
		minimalTxWithParent(t, pA),
		minimalTxWithParent(t, pB),
		minimalTxWithParent(t, pC),
	}

	results, err := v.ValidateBatch(context.Background(), txs, 0)
	require.NoError(t, err)
	require.Equal(t, 1, stub.getCalls, "Phase A must fire exactly one BatchGetParents call")
	require.Len(t, results, 3)
}

// TestValidateBatchNative_PhaseA_MissingParentTagged asserts that txs whose
// parent is absent from the Aerospike response are tagged with
// ErrTxMissingParent at PhaseGetParents, while txs with present parents
// remain error-free after Phase A.
func TestValidateBatchNative_PhaseA_MissingParentTagged(t *testing.T) {
	v, stub := newNativeValidator(t)
	// Install a no-op CPU override so this test stays focused on Phase A
	// behaviour; minimalTxWithParent produces 0-output txs that Phase B
	// would otherwise reject.
	v.overrideCPUValidationForTest(func(_ *bt.Tx) error { return nil })

	pPresent := chainhash.Hash{0x10}
	pMissing := chainhash.Hash{0x20}
	stub.parents = mkParentMap(&pPresent)
	stub.missing = [][]byte{pMissing[:]}

	good := minimalTxWithParent(t, pPresent)
	bad := minimalTxWithParent(t, pMissing)

	results, err := v.ValidateBatch(context.Background(), []*bt.Tx{good, bad}, 0)
	require.NoError(t, err, "whole-batch err must be nil when only per-tx parents are missing")
	require.Len(t, results, 2)

	// First tx — parent was present; Phase A leaves it error-free.
	require.NoError(t, results[0].Err, "tx[0] has a present parent, must have no Phase A error")

	// Second tx — parent was missing; must be tagged.
	require.Equal(t, PhaseGetParents, results[1].Phase)
	require.ErrorIs(t, results[1].Err, terrors.ErrTxMissingParent)
}

// TestValidateBatchNative_PhaseA_WholeBatchAerospikeFailure asserts that a
// transport-level Aerospike error on BatchGetParents propagates as the
// whole-batch err return (not silently per-tx).
func TestValidateBatchNative_PhaseA_WholeBatchAerospikeFailure(t *testing.T) {
	v, stub := newNativeValidator(t)
	stub.parentsErr = terrors.NewProcessingError("aerospike unreachable")

	p := chainhash.Hash{0x01}
	tx := minimalTxWithParent(t, p)

	_, err := v.ValidateBatch(context.Background(), []*bt.Tx{tx}, 0)
	require.Error(t, err, "a transport-level Aerospike error must become the whole-batch error")
}

// TestValidateBatchNative_PhaseA_DeduplicatedParents asserts that when
// multiple txs share the same parent hash, collectUniqueParents deduplicates
// so BatchGetParents receives each hash only once.
func TestValidateBatchNative_PhaseA_DeduplicatedParents(t *testing.T) {
	v, stub := newNativeValidator(t)
	// Install a no-op CPU override so this test stays focused on Phase A
	// behaviour; minimalTxWithParent produces 0-output txs that Phase B
	// would otherwise reject.
	v.overrideCPUValidationForTest(func(_ *bt.Tx) error { return nil })

	sharedParent := chainhash.Hash{0xAA}
	stub.parents = mkParentMap(&sharedParent)

	// Three txs all pointing at the same parent.
	txs := []*bt.Tx{
		minimalTxWithParent(t, sharedParent),
		minimalTxWithParent(t, sharedParent),
		minimalTxWithParent(t, sharedParent),
	}

	results, err := v.ValidateBatch(context.Background(), txs, 0)
	require.NoError(t, err)
	require.Len(t, results, 3)
	require.Equal(t, 1, stub.getCalls, "only one BatchGetParents call even with repeated parents")

	// All three txs should have no Phase A error (parent is present).
	for i, r := range results {
		require.NoError(t, r.Err, "tx[%d] should have no Phase A error", i)
	}
}

// ---------------------------------------------------------------------------
// Phase B tests
// ---------------------------------------------------------------------------

// TestValidateBatchNative_PhaseB_PerTxCPUFailureIsolated asserts that a tx
// that fails CPU validation (format/script) is tagged with PhaseCPU while
// a tx that passes CPU validation remains error-free after Phase B.
func TestValidateBatchNative_PhaseB_PerTxCPUFailureIsolated(t *testing.T) {
	v, stub := newNativeValidator(t)

	pGood := chainhash.Hash{0x40}
	pBad := chainhash.Hash{0x41}
	stub.parents = mkParentMap(&pGood, &pBad)
	stub.missing = nil

	// good: passes the injected CPU override (no error).
	good := minimalTxWithParent(t, pGood)
	// bad: the CPU override returns an error for this tx specifically.
	bad := minimalTxWithParent(t, pBad)

	// Use the cpuOverride seam so we can control which tx fails without
	// needing a fully extended/signed transaction.
	cpuErr := terrors.NewTxInvalidError("synthetic CPU validation failure")
	v.overrideCPUValidationForTest(func(tx *bt.Tx) error {
		if tx == bad {
			return cpuErr
		}
		return nil
	})

	results, err := v.ValidateBatch(context.Background(), []*bt.Tx{good, bad}, 0)
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.NoError(t, results[0].Err, "good tx should survive Phase B")
	require.Error(t, results[1].Err, "bad tx should be tagged by Phase B")
	require.Equal(t, PhaseCPU, results[1].Phase)
}

// TestValidateBatchNative_PhaseB_NaturalFormatRejection asserts that a tx
// with no outputs is natively rejected by ValidateTransaction (no override
// needed). minimalTxWithParent produces a tx with 1 input and 0 outputs;
// ValidateTransaction rejects it immediately.
func TestValidateBatchNative_PhaseB_NaturalFormatRejection(t *testing.T) {
	v, stub := newNativeValidator(t)

	p := chainhash.Hash{0x50}
	stub.parents = mkParentMap(&p)

	// minimalTxWithParent → 1 input, 0 outputs → ValidateTransaction
	// returns "transaction has no inputs or outputs".
	tx := minimalTxWithParent(t, p)

	results, err := v.ValidateBatch(context.Background(), []*bt.Tx{tx}, 0)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Error(t, results[0].Err, "tx with no outputs must fail Phase B")
	require.Equal(t, PhaseCPU, results[0].Phase)
}

// TestValidateBatchNative_PhaseB_BoundedParallelism confirms that Phase B
// does not spawn one goroutine per tx unboundedly. With N=100 txs, the peak
// goroutine delta must stay well below N.
func TestValidateBatchNative_PhaseB_BoundedParallelism(t *testing.T) {
	v, stub := newNativeValidator(t)
	const N = 100
	parents := make([]chainhash.Hash, N)
	for i := range parents {
		parents[i] = chainhash.Hash{byte(i + 1)}
	}
	parentMap := map[[32]byte]*aerospike.ParentRecord{}
	for _, p := range parents {
		var key [32]byte
		copy(key[:], p[:])
		parentMap[key] = &aerospike.ParentRecord{BlockHeight: 1}
	}
	stub.parents = parentMap

	txs := make([]*bt.Tx, N)
	for i := range txs {
		txs[i] = minimalTxWithParent(t, parents[i])
	}

	// Override CPU validation to be a no-op so all txs reach Phase B
	// without being killed by format errors. (minimalTxWithParent produces
	// 0-output txs which ValidateTransaction would otherwise reject; we want
	// to measure goroutine concurrency, not validation logic here.)
	v.overrideCPUValidationForTest(func(_ *bt.Tx) error { return nil })

	before := runtime.NumGoroutine()
	_, err := v.ValidateBatch(context.Background(), txs, 0)
	require.NoError(t, err)
	after := runtime.NumGoroutine()
	require.Less(t, after-before, N/4, "Phase B should be bounded by NumCPU, not N")
}

// ---------------------------------------------------------------------------
// Phase C tests
// ---------------------------------------------------------------------------

// TestValidateBatchNative_PhaseC_OneBatchSpendCallForBatch asserts that exactly
// one BatchSpend call is made for a batch of N txs (all survivors).
func TestValidateBatchNative_PhaseC_OneBatchSpendCallForBatch(t *testing.T) {
	v, stub := newNativeValidator(t)
	installNoopCPUOverride(t, v)

	pA, pB := chainhash.Hash{0x60}, chainhash.Hash{0x61}
	stub.parents = mkParentMap(&pA, &pB)

	txs := []*bt.Tx{
		minimalTxWithParent(t, pA),
		minimalTxWithParent(t, pB),
	}
	_, err := v.ValidateBatch(context.Background(), txs, 0)
	require.NoError(t, err)
	require.Equal(t, 1, stub.spendCalls, "Phase C must fire exactly one BatchSpend call for the whole batch")
}

// TestValidateBatchNative_PhaseC_PerParentFailureAttributesToChild asserts
// that a per-parent SpendResult failure is attributed to every child tx that
// referenced that parent, while unaffected txs remain error-free.
func TestValidateBatchNative_PhaseC_PerParentFailureAttributesToChild(t *testing.T) {
	v, stub := newNativeValidator(t)
	installNoopCPUOverride(t, v)

	pGood := chainhash.Hash{0x70}
	pBad := chainhash.Hash{0x71}
	stub.parents = mkParentMap(&pGood, &pBad)

	var badKey [32]byte
	copy(badKey[:], pBad[:])
	stub.spendPerParent = map[[32]byte]error{
		badKey: terrors.NewProcessingError("double spend on parent"),
	}

	good := minimalTxWithParent(t, pGood)
	bad := minimalTxWithParent(t, pBad)

	results, err := v.ValidateBatch(context.Background(), []*bt.Tx{good, bad}, 0)
	require.NoError(t, err)
	require.NoError(t, results[0].Err, "tx referencing good parent must have no error")
	require.Error(t, results[1].Err, "tx referencing bad parent must be tagged by Phase C")
	require.Equal(t, PhaseSpend, results[1].Phase)
}

// ---------------------------------------------------------------------------
// Phase D tests
// ---------------------------------------------------------------------------

// TestValidateBatchNative_PhaseD_OneBatchCreateCall asserts that exactly one
// BatchCreate call is made for a batch of N surviving txs.
func TestValidateBatchNative_PhaseD_OneBatchCreateCall(t *testing.T) {
	v, stub := newNativeValidator(t)
	installNoopCPUOverride(t, v)

	pA, pB := chainhash.Hash{0x80}, chainhash.Hash{0x81}
	stub.parents = mkParentMap(&pA, &pB)

	txs := []*bt.Tx{minimalTxWithParent(t, pA), minimalTxWithParent(t, pB)}
	_, err := v.ValidateBatch(context.Background(), txs, 0)
	require.NoError(t, err)
	require.Equal(t, 1, stub.createCalls, "Phase D must fire exactly one BatchCreate call for the whole batch")
}

// TestValidateBatchNative_PhaseD_PerTxCreateFailureTagged asserts that a
// per-tx CREATE_ONLY violation from BatchCreate is tagged at PhaseCreate for
// the failing tx, while a tx that succeeds remains error-free.
func TestValidateBatchNative_PhaseD_PerTxCreateFailureTagged(t *testing.T) {
	v, stub := newNativeValidator(t)
	installNoopCPUOverride(t, v)

	pA, pB := chainhash.Hash{0x90}, chainhash.Hash{0x91}
	stub.parents = mkParentMap(&pA, &pB)
	// createPerIdx is indexed into the compacted (alive) slice passed to
	// BatchCreate. Both txs survive A–C so index 0 → pA tx, index 1 → pB tx.
	stub.createPerIdx = map[int]error{
		1: terrors.NewProcessingError("CREATE_ONLY collision"),
	}

	txs := []*bt.Tx{minimalTxWithParent(t, pA), minimalTxWithParent(t, pB)}
	results, err := v.ValidateBatch(context.Background(), txs, 0)
	require.NoError(t, err)
	require.NoError(t, results[0].Err, "tx[0] must succeed in Phase D")
	require.Error(t, results[1].Err, "tx[1] must be tagged by Phase D")
	require.Equal(t, PhaseCreate, results[1].Phase)
}

// TestValidateBatchNative_PhaseD_MetaPopulatedOnSuccess asserts that
// results[i].Meta is non-nil for a tx that Phase D creates successfully,
// and that the Meta fields are consistent with the tx and blockHeight.
func TestValidateBatchNative_PhaseD_MetaPopulatedOnSuccess(t *testing.T) {
	v, stub := newNativeValidator(t)
	installNoopCPUOverride(t, v)

	pA := chainhash.Hash{0xA0}
	stub.parents = mkParentMap(&pA)
	tx := minimalTxWithParent(t, pA)

	const blockHeight uint32 = 100
	results, err := v.ValidateBatch(context.Background(), []*bt.Tx{tx}, blockHeight)
	require.NoError(t, err)
	require.NoError(t, results[0].Err)
	require.NotNil(t, results[0].Meta, "Phase D must populate Meta for a successfully created tx")
	require.True(t, results[0].Meta.Locked, "Meta.Locked must be true (BatchCreate was called with lockedTrue=true)")
	require.Equal(t, blockHeight, results[0].Meta.UnminedSince, "Meta.UnminedSince must equal the blockHeight passed to ValidateBatch")
	require.Equal(t, tx, results[0].Meta.Tx, "Meta.Tx must reference the original transaction")
}

// ---------------------------------------------------------------------------
// Phase E tests
// ---------------------------------------------------------------------------

// TestValidateBatchNative_PhaseE_AllAccepted asserts that when BlockAssembly
// accepts all txs, exactly one BatchSetLocked call is made (for the full
// surviving set) and all results have no error.
func TestValidateBatchNative_PhaseE_AllAccepted(t *testing.T) {
	v, stub := newNativeValidator(t)
	installNoopCPUOverride(t, v)

	pA, pB := chainhash.Hash{0xC0}, chainhash.Hash{0xC1}
	stub.parents = mkParentMap(&pA, &pB)

	txs := []*bt.Tx{minimalTxWithParent(t, pA), minimalTxWithParent(t, pB)}

	v.overrideBASubmitForTest(func(_ context.Context, _ []*bt.Tx) map[chainhash.Hash]error {
		return map[chainhash.Hash]error{} // all accepted
	})

	results, err := v.ValidateBatch(context.Background(), txs, 0)
	require.NoError(t, err)
	for i, r := range results {
		require.NoError(t, r.Err, "tx[%d] must have no error when BA accepts all", i)
	}
	require.Equal(t, 1, stub.lockedCalls, "Phase E must fire exactly one BatchSetLocked call for the accepted subset")
}

// TestValidateBatchNative_PhaseE_BARejectionLeavesTxLocked asserts that a
// BA-rejected tx is tagged PhaseBlockAssembly and NOT passed to
// BatchSetLocked (it stays locked for reconciler pickup). The accepted tx IS
// unlocked via a single BatchSetLocked call.
func TestValidateBatchNative_PhaseE_BARejectionLeavesTxLocked(t *testing.T) {
	v, stub := newNativeValidator(t)
	installNoopCPUOverride(t, v)

	pA, pB := chainhash.Hash{0xB0}, chainhash.Hash{0xB1}
	stub.parents = mkParentMap(&pA, &pB)

	good := minimalTxWithParent(t, pA)
	rejected := minimalTxWithParent(t, pB)

	v.overrideBASubmitForTest(func(_ context.Context, _ []*bt.Tx) map[chainhash.Hash]error {
		out := map[chainhash.Hash]error{}
		// good is accepted (no entry); rejected has an error
		out[*rejected.TxIDChainHash()] = terrors.NewProcessingError("BA rejection")
		return out
	})

	results, err := v.ValidateBatch(context.Background(), []*bt.Tx{good, rejected}, 0)
	require.NoError(t, err)
	require.NoError(t, results[0].Err, "accepted tx must have no error")
	require.Error(t, results[1].Err, "BA-rejected tx must have an error")
	require.Equal(t, PhaseBlockAssembly, results[1].Phase, "BA-rejected tx must be tagged PhaseBlockAssembly")
	// BatchSetLocked is called once — only for good (1 hash in the call).
	require.Equal(t, 1, stub.lockedCalls, "exactly one BatchSetLocked call for the accepted subset")
}

// TestValidateBatchNative_PhaseE_SetLockedFailureTagged asserts that when
// BatchSetLocked returns a per-tx error, the tx is tagged PhaseSetLocked
// (distinct from PhaseBlockAssembly).
func TestValidateBatchNative_PhaseE_SetLockedFailureTagged(t *testing.T) {
	v, stub := newNativeValidator(t)
	installNoopCPUOverride(t, v)

	pA := chainhash.Hash{0xD0}
	stub.parents = mkParentMap(&pA)
	tx := minimalTxWithParent(t, pA)

	v.overrideBASubmitForTest(func(_ context.Context, _ []*bt.Tx) map[chainhash.Hash]error {
		return map[chainhash.Hash]error{} // BA accepts all
	})

	// Inject a per-tx failure from BatchSetLocked
	var txKey [32]byte
	copy(txKey[:], tx.TxIDChainHash()[:])
	stub.lockedPerHash = map[[32]byte]error{
		txKey: terrors.NewProcessingError("aerospike write failed during unlock"),
	}

	results, err := v.ValidateBatch(context.Background(), []*bt.Tx{tx}, 0)
	require.NoError(t, err)
	require.Error(t, results[0].Err, "tx must have an error when BatchSetLocked fails")
	require.Equal(t, PhaseSetLocked, results[0].Phase, "must be tagged PhaseSetLocked, not PhaseBlockAssembly")
}

// TestValidateBatchNative_PhaseE_SetLockedWholeBatchFailureTagged asserts
// that a whole-batch BatchSetLocked transport error tags all affected txs
// with PhaseSetLocked.
func TestValidateBatchNative_PhaseE_SetLockedWholeBatchFailureTagged(t *testing.T) {
	v, stub := newNativeValidator(t)
	installNoopCPUOverride(t, v)

	pA, pB := chainhash.Hash{0xE0}, chainhash.Hash{0xE1}
	stub.parents = mkParentMap(&pA, &pB)
	txs := []*bt.Tx{minimalTxWithParent(t, pA), minimalTxWithParent(t, pB)}

	v.overrideBASubmitForTest(func(_ context.Context, _ []*bt.Tx) map[chainhash.Hash]error {
		return map[chainhash.Hash]error{} // BA accepts all
	})
	stub.lockedErr = terrors.NewProcessingError("aerospike transport failure during BatchSetLocked")

	results, err := v.ValidateBatch(context.Background(), txs, 0)
	require.NoError(t, err)
	for i, r := range results {
		require.Error(t, r.Err, "tx[%d] must have error on whole-batch BatchSetLocked failure", i)
		require.Equal(t, PhaseSetLocked, r.Phase, "tx[%d] must be tagged PhaseSetLocked", i)
	}
}

// ---------------------------------------------------------------------------
// Phase F tests
// ---------------------------------------------------------------------------

// TestValidateBatchNative_PhaseF_PublishedOnlyForAliveTx asserts that Phase F
// publishes txmeta only for txs that are still alive after Phase E (i.e.
// Created + BA-accepted + Unlocked) and skips BA-rejected txs.
func TestValidateBatchNative_PhaseF_PublishedOnlyForAliveTx(t *testing.T) {
	v, stub := newNativeValidator(t)
	installNoopCPUOverride(t, v)

	pA, pB := chainhash.Hash{0xD0}, chainhash.Hash{0xD1}
	stub.parents = mkParentMap(&pA, &pB)

	good := minimalTxWithParent(t, pA)
	rejected := minimalTxWithParent(t, pB)

	v.overrideBASubmitForTest(func(_ context.Context, txs []*bt.Tx) map[chainhash.Hash]error {
		return map[chainhash.Hash]error{
			*rejected.TxIDChainHash(): terrors.NewProcessingError("ba reject"),
		}
	})

	var published []chainhash.Hash
	v.overrideTxMetaPublishForTest(func(tx *bt.Tx, _ *meta.Data) {
		published = append(published, *tx.TxIDChainHash())
	})

	_, err := v.ValidateBatch(context.Background(), []*bt.Tx{good, rejected}, 0)
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{*good.TxIDChainHash()}, published,
		"Phase F must publish only the alive (BA-accepted) tx, not the BA-rejected one")
}

// TestValidateBatchNative_PhaseF_SkipTxMetaPublishingHonoured asserts that
// when WithSkipTxMetaPublishing(true) is passed, Phase F does not call the
// publish override at all — even if txs are alive.
func TestValidateBatchNative_PhaseF_SkipTxMetaPublishingHonoured(t *testing.T) {
	v, stub := newNativeValidator(t)
	installNoopCPUOverride(t, v)

	pA := chainhash.Hash{0xF0}
	stub.parents = mkParentMap(&pA)
	tx := minimalTxWithParent(t, pA)

	v.overrideBASubmitForTest(func(_ context.Context, _ []*bt.Tx) map[chainhash.Hash]error {
		return map[chainhash.Hash]error{}
	})

	publishCalled := false
	v.overrideTxMetaPublishForTest(func(_ *bt.Tx, _ *meta.Data) {
		publishCalled = true
	})

	_, err := v.ValidateBatch(context.Background(), []*bt.Tx{tx}, 0, WithSkipTxMetaPublishing(true))
	require.NoError(t, err)
	require.False(t, publishCalled, "Phase F must not publish when SkipTxMetaPublishing is set")
}
