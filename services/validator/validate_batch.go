package validator

import (
	"context"
	"runtime"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	terrors "github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
	"golang.org/x/sync/errgroup"
)

// batchUtxoStore is the minimal UTXO store surface ValidateBatch needs.
// The concrete *aerospike.Store satisfies it; tests use a stub.
type batchUtxoStore interface {
	BatchGetParents(ctx context.Context, parentHashes [][]byte) (map[[32]byte]*aerospike.ParentRecord, [][]byte, error)
	BatchSpend(ctx context.Context, spends []*utxo.Spend, blockHeight uint32, ignoreFlags ...utxo.IgnoreFlags) ([]aerospike.SpendResult, error)
	BatchCreate(ctx context.Context, txs []*bt.Tx, blockHeight uint32, lockedTrue bool) ([]aerospike.CreateResult, error)
	BatchSetLocked(ctx context.Context, txHashes [][]byte, locked bool) ([]aerospike.SetLockedResult, error)
}

// ValidateBatch validates a batch of transactions through the six-phase
// batched pipeline. Each phase issues a single round-trip to the UTXO store,
// amortising per-call overhead across the full batch.
//
// The returned slice is positionally aligned with txs. ValidateBatch returns
// err != nil only on a whole-batch failure; per-tx errors live in results[i].Err.
//
// The configured UTXO store must implement batchUtxoStore (i.e. be an
// *aerospike.Store or a test stub). Call Validate for per-tx validation when
// the store does not support the batch methods.
func (v *Validator) ValidateBatch(
	ctx context.Context,
	txs []*bt.Tx,
	blockHeight uint32,
	opts ...Option,
) ([]ValidationResult, error) {
	if len(txs) == 0 {
		return []ValidationResult{}, nil
	}

	// Capture the atomic block state once (matching validateInternal) so the
	// blockHeight default and the per-tx finality check below see a consistent
	// height/median-time pair, and pre-process the options once for reuse.
	blockState := v.GetBlockState()
	validationOptions := ProcessOptions(opts...)

	// Mirror Validator.validateInternal: callers may pass blockHeight=0 meaning
	// "use current chain tip + 1". Without this, BDK 1.2.4 ValidateTransaction
	// rejects bh=0 in script validation, breaking parity with the direct path.
	if blockHeight == 0 {
		blockHeight = blockState.Height + 1
	}

	store, ok := v.getBatchUtxoStore()
	if !ok {
		return nil, terrors.NewConfigurationError("configured UTXO store does not implement batch methods; use Validate for per-tx validation")
	}

	results := make([]ValidationResult, len(txs))
	for i, tx := range txs {
		results[i].TxHash = *tx.TxIDChainHash()
	}

	// Phase A: fetch all unique parent hashes in a single BatchGetParents call.
	phaseAStart := time.Now()
	parentHashes := collectUniqueParents(txs)
	parents, _, err := store.BatchGetParents(ctx, parentHashes)
	if err != nil {
		v.phaseMetrics.add(PhaseGetParents, time.Since(phaseAStart))
		return results, err
	}

	// Both an explicit "missing" hash and a hash absent from the parents map
	// are treated identically — any absent parent marks the tx as failed.
	// The missing slice is intentionally discarded here; both cases are
	// detected by the absence of the hash from the parents map below.

	alive := make([]bool, len(txs))
	for i, tx := range txs {
		parentMissing := false
		for _, in := range tx.Inputs {
			ph := in.PreviousTxIDChainHash()
			var key [32]byte
			copy(key[:], ph[:])

			if _, present := parents[key]; present {
				continue
			}

			// Parent is absent from the result map — either explicitly
			// listed as missing by Aerospike, or simply not returned.
			// Both cases map to ErrTxMissingParent.
			results[i].Err = terrors.ErrTxMissingParent
			results[i].Phase = PhaseGetParents
			parentMissing = true
			break
		}
		alive[i] = !parentMissing
	}

	// Phase A hydration: populate input.PreviousTxScript and PreviousTxSatoshis
	// from the already-fetched parent records for every alive tx whose inputs
	// are not already extended. This is pure CPU work — zero additional
	// Aerospike round-trips. ParentRecord.Outputs was decoded inside
	// BatchGetParents at fetch time, so OutputAt(idx) is an O(1) slice lookup.
	//
	// Also derives per-input UTXO heights from ParentRecord.BlockHeight for
	// Phase B (runCPUValidation / BDK script engine). This mirrors what
	// getTransactionInputBlockHeightsAndExtendTx does for the per-tx Validate
	// path, ensuring Validate(tx) and ValidateBatch([tx]) agree on the same
	// hydrated tx state.
	utxoHeightsByTx := make([][]uint32, len(txs))
	for i, tx := range txs {
		if !alive[i] {
			continue
		}
		heights := make([]uint32, len(tx.Inputs))
		hydrationFailed := false
		for j, in := range tx.Inputs {
			ph := in.PreviousTxIDChainHash()
			var key [32]byte
			copy(key[:], ph[:])

			pr := parents[key]
			heights[j] = pr.BlockHeight

			if tx.IsExtended() {
				// Script + satoshis already present; only heights needed.
				continue
			}
			out, outErr := pr.OutputAt(in.PreviousTxOutIndex)
			if outErr != nil {
				results[i].Err = outErr
				results[i].Phase = PhaseGetParents
				alive[i] = false
				hydrationFailed = true
				break
			}
			in.PreviousTxScript = out.LockingScript
			in.PreviousTxSatoshis = out.Satoshis
		}
		if !hydrationFailed && !tx.IsExtended() {
			tx.SetExtended(true)
		}
		utxoHeightsByTx[i] = heights
	}
	v.phaseMetrics.add(PhaseGetParents, time.Since(phaseAStart))

	// Phase B — per-tx CPU validation (format + scripts).
	// Runs in an errgroup bounded by NumCPU so the goroutine count does not
	// grow unboundedly with batch size.
	phaseBStart := time.Now()
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(runtime.NumCPU())
	for i := range txs {
		if !alive[i] {
			continue
		}
		i := i // capture loop var
		g.Go(func() error {
			// Contextual checks (coinbase rejection + transaction finality) that
			// validateInternal runs before script validation. The batch path must
			// run the identical checks, otherwise Validate and ValidateBatch
			// disagree and the batch path would accept non-final transactions the
			// per-tx path rejects.
			if err := v.checkContextual(txs[i], blockHeight, blockState, validationOptions); err != nil {
				results[i].Err = err
				results[i].Phase = PhaseCPU
				alive[i] = false

				return nil
			}
			if err := v.runCPUValidation(gCtx, txs[i], blockHeight, utxoHeightsByTx[i], opts...); err != nil {
				results[i].Err = err
				results[i].Phase = PhaseCPU
				alive[i] = false
			}
			return nil // never bubble per-tx errors as whole-batch errors
		})
	}
	_ = g.Wait() // errgroup never returns a non-nil error here
	v.phaseMetrics.add(PhaseCPU, time.Since(phaseBStart))

	// Phase C — BatchSpend. One BatchOperate carries all surviving inputs.
	// Per-input SpendResult.Err is attributed back to every tx that referenced
	// the failed parent.
	phaseCStart := time.Now()
	spends := buildSpendsForBatch(txs, alive, results)
	if len(spends) > 0 {
		spendResults, spendErr := store.BatchSpend(ctx, spends, blockHeight)
		if spendErr != nil {
			// Whole-batch transport failure — mark all survivors as PhaseSpend.
			for i, a := range alive {
				if a {
					results[i].Err = spendErr
					results[i].Phase = PhaseSpend
					alive[i] = false
				}
			}
		} else {
			// Build a map: failed parent hash → first error seen.
			type parentKey [32]byte
			failedParents := make(map[parentKey]error)
			for j, sr := range spendResults {
				if sr.Err == nil {
					continue
				}
				sp := spends[j]
				var key parentKey
				copy(key[:], sp.TxID[:])
				if _, already := failedParents[key]; !already {
					failedParents[key] = sr.Err
				}
			}
			// Attribute parent failures to every tx that referenced them.
			for i, tx := range txs {
				if !alive[i] {
					continue
				}
				for _, in := range tx.Inputs {
					ph := in.PreviousTxIDChainHash()
					var key parentKey
					copy(key[:], ph[:])
					if e, bad := failedParents[key]; bad {
						results[i].Err = e
						results[i].Phase = PhaseSpend
						alive[i] = false
						break
					}
				}
			}
		}
	}
	v.phaseMetrics.add(PhaseSpend, time.Since(phaseCStart))

	// Phase D — BatchCreate (locked=true). One BatchOperate carries all surviving txs.
	// Per-tx errors (CREATE_ONLY collisions, large-tx-needs-external-storage) are tagged
	// PhaseCreate. Surviving txs have results[i].Meta populated so Phase F (Kafka) and
	// single-tx callers can use it.
	phaseDStart := time.Now()
	aliveTxs, aliveIdx := compactAlive(txs, alive)
	if len(aliveTxs) > 0 {
		createResults, cErr := store.BatchCreate(ctx, aliveTxs, blockHeight, true)
		if cErr != nil {
			// Whole-batch transport failure — mark all survivors as PhaseCreate.
			for _, i := range aliveIdx {
				results[i].Err = cErr
				results[i].Phase = PhaseCreate
				alive[i] = false
			}
		} else {
			for j, cr := range createResults {
				i := aliveIdx[j]
				if cr.Err != nil {
					results[i].Err = cr.Err
					results[i].Phase = PhaseCreate
					alive[i] = false
					continue
				}
				// Populate Meta for the surviving tx. Mirrors what
				// ValidateWithOptions / CreateInUtxoStore builds via
				// util.TxMetaDataFromTx after a successful Create call,
				// with Locked set to true (matching lockedTrue=true above).
				m, metaErr := buildMetaFromTx(aliveTxs[j], blockHeight)
				if metaErr != nil {
					results[i].Err = metaErr
					results[i].Phase = PhaseCreate
					alive[i] = false
					continue
				}
				results[i].Meta = m
			}
		}
	}
	v.phaseMetrics.add(PhaseCreate, time.Since(phaseDStart))

	// Phase E — submit surviving txs to BlockAssembly, then unlock the
	// BA-acknowledged subset via a single BatchSetLocked(false) call.
	// BA-rejected txs are left locked and tagged PhaseBlockAssembly so the
	// existing locked-tx reconciler can pick them up via the normal retry path.
	aliveTxs, aliveIdx = compactAlive(txs, alive)
	if len(aliveTxs) > 0 {
		phaseEStart := time.Now()
		baErrs := v.submitToBlockAssemblyBatch(ctx, aliveTxs)
		unlockHashes := make([][]byte, 0, len(aliveTxs))
		unlockIdx := make([]int, 0, len(aliveTxs))
		for j, tx := range aliveTxs {
			i := aliveIdx[j]
			h := tx.TxIDChainHash()
			if e, rejected := baErrs[*h]; rejected {
				results[i].Err = e
				results[i].Phase = PhaseBlockAssembly
				alive[i] = false
				continue
			}
			unlockHashes = append(unlockHashes, h[:])
			unlockIdx = append(unlockIdx, i)
		}
		v.phaseMetrics.add(PhaseBlockAssembly, time.Since(phaseEStart))

		if len(unlockHashes) > 0 {
			phaseFStart := time.Now()
			ulResults, uErr := store.BatchSetLocked(ctx, unlockHashes, false)
			if uErr != nil {
				for _, i := range unlockIdx {
					results[i].Err = uErr
					results[i].Phase = PhaseSetLocked
					alive[i] = false
				}
			} else {
				for j, ur := range ulResults {
					if ur.Err != nil {
						i := unlockIdx[j]
						results[i].Err = ur.Err
						results[i].Phase = PhaseSetLocked
						alive[i] = false
					}
				}
			}
			v.phaseMetrics.add(PhaseSetLocked, time.Since(phaseFStart))
		}
	}

	// Phase F — TxMeta Kafka publish for all surviving tx. Fire-and-forget;
	// uses the existing v.txmetaKafkaBatcher, which is a go-batcher we
	// deliberately keep in place because Kafka batching is a different concern
	// from the UTXO hot path. Errors are logged but do not fail the batch —
	// mirroring the existing single-tx path (Validator.go line ~783).
	// When SkipTxMetaPublishing is set in the opts, the publish is skipped
	// entirely (e.g. legacy catchup / quickValidationMode).
	skipPublish := validationOptions.SkipTxMetaPublishing || (v.txmetaKafkaProducerClient == nil && v.txMetaPublishOverride == nil)
	if !skipPublish {
		for i, tx := range txs {
			if !alive[i] {
				continue
			}
			if v.txMetaPublishOverride != nil {
				v.txMetaPublishOverride(tx, results[i].Meta)
				continue
			}
			if txMetaErr := v.sendTxMetaToKafka(results[i].Meta, tx.TxIDChainHash()); txMetaErr != nil {
				v.logger.Errorf("[ValidateBatch][%s] failed to serialize/enqueue txmeta for kafka: %v",
					tx.TxIDChainHash().String(), txMetaErr)
			}
		}
	}

	return results, nil
}

// compactAlive returns the subset of txs where alive[i] is true, together with
// their original indexes. Used by Phase D to build the BatchCreate input slice
// and map results back to the full results slice.
func compactAlive(txs []*bt.Tx, alive []bool) ([]*bt.Tx, []int) {
	out := make([]*bt.Tx, 0, len(txs))
	idx := make([]int, 0, len(txs))
	for i, tx := range txs {
		if alive[i] {
			out = append(out, tx)
			idx = append(idx, i)
		}
	}
	return out, idx
}

// buildMetaFromTx constructs the *meta.Data returned for a tx that was
// successfully created in Phase D. It mirrors the construction performed by
// util.TxMetaDataFromTx (called by Store.Create → CreateInUtxoStore) and sets
// Locked=true to match the lockedTrue=true flag passed to BatchCreate.
func buildMetaFromTx(tx *bt.Tx, blockHeight uint32) (*meta.Data, error) {
	m, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return nil, err
	}
	m.Locked = true
	// blockHeight is used by the caller for fee/UTXO-hash computation; the
	// meta.Data struct does not have a plain BlockHeight field — unmined txs
	// use UnminedSince to record when they entered the mempool.
	m.UnminedSince = blockHeight
	return m, nil
}

// runCPUValidation runs the format + script checks for a single transaction
// without touching the UTXO store. It calls TxValidatorI methods directly
// (bypassing the v.validateTransaction wrapper which would call
// extendTransaction → UTXO store). Phase A already confirmed
// parents are present and hydrated the tx to extended form; utxoHeights holds
// one block height per input derived from the ParentRecord.BlockHeight values
// fetched in Phase A.
//
// In test code, v.cpuOverride intercepts the call so tests can inject
// controlled failures without needing a fully-extended signed transaction.
func (v *Validator) runCPUValidation(ctx context.Context, tx *bt.Tx, blockHeight uint32, utxoHeights []uint32, opts ...Option) error {
	if v.cpuOverride != nil {
		return v.cpuOverride(tx)
	}
	processedOpts := ProcessOptions(opts...)
	// Post BDK 1.2.4 (upstream b61c1f87e), TxValidator.ValidateTransaction performs
	// both Teranode-owned format checks AND BDK script validation in a single call
	// (the old separate ValidateTransactionScripts entry point was removed). The
	// merged call still uses utxoHeights for checkFees → isConsolidationTx and
	// passes them through to the BDK script engine.
	return v.txValidator.ValidateTransaction(tx, blockHeight, utxoHeights, processedOpts)
}

// checkContextual runs the per-tx contextual checks that validateInternal
// performs before script validation — coinbase rejection and transaction
// finality (nLockTime / median-time-past). The batched pipeline must run the
// identical checks so Validate(tx) and ValidateBatch([tx]) agree; without this
// the batch path would accept non-final transactions the per-tx path rejects,
// and would diverge on the "utxo store not ready" readiness gate.
func (v *Validator) checkContextual(tx *bt.Tx, blockHeight uint32, blockState utxo.BlockState, validationOptions *Options) error {
	txID := tx.TxIDChainHash().String()

	// Reject coinbase first, matching validateInternal (and bitcoin-sv
	// CheckRegularTransaction) which short-circuits before any contextual check.
	if tx.IsCoinbase() {
		return terrors.NewProcessingError("[Validate][%s] coinbase transactions are not supported", txID)
	}

	comparisonTime, skipFinality, finalityErr := selectFinalityComparisonTime(validationOptions, blockHeight, uint32(v.settings.ChainCfgParams.CSVHeight), blockState)
	if finalityErr != nil {
		return finalityErr
	}

	if !skipFinality {
		if err := util.IsTransactionFinal(tx, blockHeight, comparisonTime); err != nil {
			return terrors.NewUtxoNonFinalError("[Validate][%s] transaction is not final", txID, err)
		}
	}

	return nil
}

// getBatchUtxoStore returns the validator's UTXO store as a batchUtxoStore if
// it satisfies the interface (i.e. is an *aerospike.Store or a test stub).
// Returns false when the store is e.g. a sql.Store that does not implement
// the batch methods.
func (v *Validator) getBatchUtxoStore() (batchUtxoStore, bool) {
	if v.batchStoreOverride != nil {
		return v.batchStoreOverride, true
	}
	s, ok := v.utxoStore.(batchUtxoStore)
	return s, ok
}

// collectUniqueParents walks all tx inputs and returns the deduplicated set of
// parent tx hashes as a [][]byte (preserves first-seen order).
func collectUniqueParents(txs []*bt.Tx) [][]byte {
	seen := make(map[[32]byte]struct{})
	out := make([][]byte, 0, len(txs))
	for _, tx := range txs {
		for _, in := range tx.Inputs {
			ph := in.PreviousTxIDChainHash()
			var key [32]byte
			copy(key[:], ph[:])
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			b := make([]byte, 32)
			copy(b, ph[:])
			out = append(out, b)
		}
	}
	return out
}

// byteSliceSet converts a [][]byte into a map keyed by [32]byte for O(1) lookup.
func byteSliceSet(hashes [][]byte) map[[32]byte]struct{} {
	set := make(map[[32]byte]struct{}, len(hashes))
	for _, h := range hashes {
		var key [32]byte
		copy(key[:], h)
		set[key] = struct{}{}
	}
	return set
}

// buildSpendsForBatch flattens all surviving tx inputs into a single
// []*utxo.Spend slice suitable for one BatchSpend call. It mirrors the
// existing single-tx spend construction in utxo.GetSpends (stores/utxo/utils.go).
//
// If GetSpends fails for a tx (e.g. missing extended input data), the tx is
// marked failed at PhaseSpend via results/alive and excluded from the returned
// slice so BatchSpend never receives a nil or invalid entry.
func buildSpendsForBatch(txs []*bt.Tx, alive []bool, results []ValidationResult) []*utxo.Spend {
	// Pre-size to the total number of inputs across surviving txs.
	total := 0
	for i, tx := range txs {
		if alive[i] {
			total += len(tx.Inputs)
		}
	}
	spends := make([]*utxo.Spend, 0, total)

	for i, tx := range txs {
		if !alive[i] {
			continue
		}
		txSpends, err := utxo.GetSpends(tx)
		if err != nil {
			// Extended input data was unavailable — mark as PhaseSpend failure.
			results[i].Err = err
			results[i].Phase = PhaseSpend
			alive[i] = false
			continue
		}
		spends = append(spends, txSpends...)
	}
	return spends
}

// submitToBlockAssemblyBatch submits a batch of txs to BlockAssembly.
// Returns a map keyed by tx hash of per-tx errors (only present for rejected
// txs; an absent entry means accepted). Uses the test override if set;
// otherwise calls v.blockAssembler.StoreBatch, which dispatches the gRPC
// batch call immediately without routing through the BA client's internal
// go-batcher — eliminating the gather-timeout delay on the BA side.
//
// Service-level errors (e.g. blockAssembler is nil or StoreBatch returns a
// non-nil error) are treated as per-tx rejections tagged in the result map.
// The rationale: a transient BA unavailability should surface as
// PhaseBlockAssembly (leaving the tx locked for reconciler pickup) rather
// than as a whole-batch failure that would incorrectly un-spend and un-create
// txs that were successfully written to Aerospike in Phase D.
func (v *Validator) submitToBlockAssemblyBatch(ctx context.Context, txs []*bt.Tx) map[chainhash.Hash]error {
	if v.blockAssemblySubmitOverride != nil {
		return v.blockAssemblySubmitOverride(ctx, txs)
	}

	// If BlockAssembly is disabled (blockAssembler is nil), treat every tx as
	// accepted (no error) — mirroring the existing single-tx path which skips
	// the sendToBlockAssembler call entirely when addToBlockAssembly is false.
	if v.blockAssembler == nil {
		return map[chainhash.Hash]error{}
	}

	// Build BA batch items in parallel. TxInpoints+GetFees are CPU work
	// (serialization + arithmetic) that scales linearly with len(txs); doing
	// them serially before the single StoreBatch call would re-introduce the
	// performance regression at high concurrency. Use a per-tx slot rather
	// than appending under a mutex so the parallel section is lock-free.
	type itemOrErr struct {
		item blockassembly.BatchItem
		err  error
		skip bool
	}
	slots := make([]itemOrErr, len(txs))
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(runtime.NumCPU())
	for i, tx := range txs {
		i, tx := i, tx
		g.Go(func() error {
			_ = gCtx
			h := tx.TxIDChainHash()
			txInpoints, err := subtree.NewTxInpointsFromTx(tx)
			if err != nil {
				slots[i] = itemOrErr{err: err}
				return nil
			}
			fee, err := util.GetFees(tx)
			if err != nil {
				slots[i] = itemOrErr{err: err}
				return nil
			}
			slots[i] = itemOrErr{
				item: blockassembly.BatchItem{
					Hash:       h,
					Fee:        fee,
					SizeBytes:  uint64(tx.Size()), //nolint:gosec
					TxInpoints: txInpoints,
				},
			}
			return nil
		})
	}
	_ = g.Wait()

	items := make([]blockassembly.BatchItem, 0, len(txs))
	hashes := make([]*chainhash.Hash, 0, len(txs))
	out := make(map[chainhash.Hash]error)
	for i, slot := range slots {
		h := txs[i].TxIDChainHash()
		if slot.err != nil {
			out[*h] = slot.err
			continue
		}
		items = append(items, slot.item)
		hashes = append(hashes, h)
	}

	if len(items) == 0 {
		return out
	}

	// StoreBatch dispatches the gRPC call directly — no go-batcher coalescing.
	if batchErr := v.blockAssembler.StoreBatch(ctx, items); batchErr != nil {
		// Whole-batch transport failure — attribute to all attempted txs.
		for _, h := range hashes {
			out[*h] = batchErr
		}
	}

	return out
}
