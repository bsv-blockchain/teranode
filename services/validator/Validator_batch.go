/*
Package validator implements BSV Blockchain transaction validation functionality.

This file implements ValidateBatch, the batched form of ValidateWithOptions.
*/
package validator

import (
	"context"
	"runtime"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"go.opentelemetry.io/otel/trace"
)

// BatchValidator is implemented by validators that can validate a batch of
// transactions through the three-phase pipeline of Validator.ValidateBatch. It is
// deliberately not part of Interface: callers type-assert for it and fall back to
// one ValidateWithOptions call per transaction when it is absent (the gRPC client,
// mocks).
type BatchValidator interface {
	// ValidateBatch validates txs and returns parallel slices: results[i] and errs[i]
	// belong to txs[i]. errs[i] is the error ValidateWithOptions would have returned
	// for txs[i] on its own.
	ValidateBatch(ctx context.Context, txs []*bt.Tx, blockHeight uint32, validationOptions *Options) ([]*meta.Data, []error)
}

// batchTx carries one transaction of a ValidateBatch call through the phases.
type batchTx struct {
	idx         int
	tx          *bt.Tx
	txID        string
	ctx         context.Context
	span        trace.Span
	end         func(...error)
	blockHeight uint32
	utxoHeights []uint32
	err         error
}

// batchPhase identifies the per-transaction phase a batchJob runs.
type batchPhase uint8

const (
	batchPhasePreDecision batchPhase = iota + 1
	batchPhasePostDecision
)

// batchJob is one per-transaction unit of work for the batch worker pool. Jobs are
// recycled through batchJobPool so a warm pool allocates nothing per transaction.
type batchJob struct {
	phase             batchPhase
	ctx               context.Context
	ctxLogger         ulogger.Logger
	st                *batchTx
	blockHeight       uint32
	validationOptions *Options
	results           []*meta.Data
	errs              []error
	wg                *sync.WaitGroup
}

var batchJobPool = sync.Pool{New: func() any { return &batchJob{} }}

// batchWorkerPool is the set of persistent goroutines that run Phase 1 and Phase 3
// of ValidateBatch, replacing one goroutine per transaction per phase.
//
// mu orders submissions against stop: a job is only ever queued while the pool is
// not stopped, and workers drain whatever is queued before exiting, so a job can
// never be stranded in the channel buffer by a concurrent Close.
type batchWorkerPool struct {
	mu      sync.RWMutex
	stopped bool
	jobs    chan *batchJob
	stop    chan struct{}
}

// batchWorkers returns the validator's batch worker pool, starting it on first use
// with validator_batchPhaseWorkers workers (0 = 2x GOMAXPROCS). It returns nil when
// the validator was closed before any batch; callers then run jobs inline.
func (v *Validator) batchWorkers() *batchWorkerPool {
	v.batchPoolOnce.Do(func() {
		nWorkers := v.settings.Validator.BatchPhaseWorkers
		if nWorkers <= 0 {
			nWorkers = 2 * runtime.GOMAXPROCS(0)
		}

		pool := &batchWorkerPool{
			// The buffer lets a batch queue its jobs ahead of the workers.
			jobs: make(chan *batchJob, nWorkers*4),
			stop: make(chan struct{}),
		}

		for range nWorkers {
			go v.batchWorker(pool)
		}

		v.batchPool = pool
	})

	return v.batchPool
}

// batchWorker runs jobs until the pool is stopped, then drains what is queued.
func (v *Validator) batchWorker(pool *batchWorkerPool) {
	for {
		select {
		case job := <-pool.jobs:
			v.runBatchJob(job)
		case <-pool.stop:
			for {
				select {
				case job := <-pool.jobs:
					v.runBatchJob(job)
				default:
					return
				}
			}
		}
	}
}

// stopBatchWorkers stops the batch worker pool, if it was started, and prevents it
// from being started afterwards. Called from Close.
func (v *Validator) stopBatchWorkers() {
	// Claim the once so a later batchWorkers call does not start a pool.
	v.batchPoolOnce.Do(func() {})

	pool := v.batchPool
	if pool == nil {
		return
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()

	if !pool.stopped {
		pool.stopped = true
		close(pool.stop)
	}
}

// submitBatchJob hands job to the worker pool, or runs it on the calling goroutine
// when there is no pool or the pool has been stopped. A cancelled ctx makes the job
// fail with ctx.Err() instead of waiting for a worker.
func (v *Validator) submitBatchJob(pool *batchWorkerPool, job *batchJob) {
	if pool == nil {
		v.runBatchJob(job)
		return
	}

	pool.mu.RLock()

	if pool.stopped {
		pool.mu.RUnlock()
		v.runBatchJob(job)

		return
	}

	// The workers are running for as long as the read lock is held, so this send
	// always completes or the context ends.
	select {
	case pool.jobs <- job:
		pool.mu.RUnlock()
	case <-job.ctx.Done():
		pool.mu.RUnlock()
		v.failBatchJob(job, job.ctx.Err())
	}
}

// runBatchJob runs one job, signals its WaitGroup and recycles it.
func (v *Validator) runBatchJob(job *batchJob) {
	switch job.phase {
	case batchPhasePreDecision:
		v.validateBatchPhase1(job.ctx, job.st, job.blockHeight, job.validationOptions)
	case batchPhasePostDecision:
		st := job.st
		job.results[st.idx], job.errs[st.idx] = v.validateBatchPhase3(job.ctx, job.ctxLogger, st, job.blockHeight, job.validationOptions)
	}

	v.releaseBatchJob(job)
}

// failBatchJob completes a job that never ran with err.
func (v *Validator) failBatchJob(job *batchJob, err error) {
	switch job.phase {
	case batchPhasePreDecision:
		job.st.err = err
	case batchPhasePostDecision:
		job.errs[job.st.idx] = err

		if job.st.end != nil {
			job.st.end(err)
		}
	}

	v.releaseBatchJob(job)
}

func (v *Validator) releaseBatchJob(job *batchJob) {
	wg := job.wg
	*job = batchJob{}
	batchJobPool.Put(job)
	wg.Done()
}

// ValidateBatch validates a batch of transactions with the same options using a
// three-phase pipeline:
//
//	Phase 1 (worker pool, per tx): validateBeforeDecision and the extension /
//	        unconfirmed-height preparation of validateTransaction.
//	Phase 2 (batched):          txValidator.ValidateTransactionBatch - the
//	        Teranode-owned checks per tx, then BDK in sub-batches (one CGO call
//	        per validator_scriptBatchThreads chunk instead of one per tx).
//	Phase 3 (worker pool, per tx): BIP68 and validateAfterDecision (spend-and-create,
//	        block-assembly hand-off with shed retry / unwind / deadline handling,
//	        txmeta publish, 2PC unlock).
//
// Every phase reuses the code of the single path, so a transaction gets the same
// error, the same store and hand-off semantics and the same metrics as it would
// through ValidateWithOptions. TX_LOCKED / TX_CREATING outcomes go through the same
// retryWhileParentCommitting budget, and failures are published with the same
// publishRejection.
//
// Transactions that spend an output of another transaction in the same batch are
// validated in a later wave, after that parent has been through Phase 3; without
// this, Phase 1 would read the store before the parent is created and reject the
// child as missing its parent.
//
// Returns parallel slices: results[i] and errs[i] correspond to txs[i].
func (v *Validator) ValidateBatch(ctx context.Context, txs []*bt.Tx, blockHeight uint32, validationOptions *Options) ([]*meta.Data, []error) {
	n := len(txs)
	results := make([]*meta.Data, n)
	errs := make([]error, n)

	if n == 0 {
		return results, errs
	}

	if validationOptions == nil {
		validationOptions = NewDefaultOptions()
	}

	ctxLogger := v.logger.WithTraceContext(ctx)

	for i, tx := range txs {
		if tx == nil {
			errs[i] = errors.NewTxInvalidError("[ValidateBatch] transaction at index %d is nil", i)
		}
	}

	for _, wave := range batchWaves(txs, errs) {
		v.validateBatchWave(ctx, ctxLogger, txs, wave, blockHeight, validationOptions, results, errs)
	}

	for i, err := range errs {
		if err != nil && txs[i] != nil {
			v.publishRejection(ctx, ctxLogger, txs[i], err)
		}
	}

	return results, errs
}

// batchWaves orders the indices of txs (skipping those that already have an error)
// into waves: a transaction is placed one wave after the latest in-batch parent it
// spends from, so a parent always completes Phase 3 before a child starts Phase 1.
// It also caches each transaction's hash.
func batchWaves(txs []*bt.Tx, errs []error) [][]int {
	byHash := make(map[chainhash.Hash]int, len(txs))

	for i, tx := range txs {
		if errs[i] != nil {
			continue
		}

		tx.SetTxHash(tx.TxIDChainHash())
		byHash[*tx.TxIDChainHash()] = i
	}

	const (
		unvisited = -1
		visiting  = -2
	)

	level := make([]int, len(txs))
	for i := range level {
		level[i] = unvisited
	}

	var levelOf func(i int) int

	levelOf = func(i int) int {
		switch level[i] {
		case visiting:
			// A cycle cannot exist between valid txids; break it rather than recurse.
			return 0
		case unvisited:
		default:
			return level[i]
		}

		level[i] = visiting
		l := 0

		for _, in := range txs[i].Inputs {
			if p, ok := byHash[*in.PreviousTxIDChainHash()]; ok && p != i {
				if pl := levelOf(p) + 1; pl > l {
					l = pl
				}
			}
		}

		level[i] = l

		return l
	}

	var waves [][]int

	for i := range txs {
		if errs[i] != nil {
			continue
		}

		l := levelOf(i)
		for len(waves) <= l {
			waves = append(waves, nil)
		}

		waves[l] = append(waves[l], i)
	}

	return waves
}

// validateBatchWave runs the three phases for the transactions at idxs, which have
// no dependencies on each other.
func (v *Validator) validateBatchWave(ctx context.Context, ctxLogger ulogger.Logger, txs []*bt.Tx, idxs []int, blockHeight uint32, validationOptions *Options,
	results []*meta.Data, errs []error) {
	states := make([]batchTx, len(idxs))
	pool := v.batchWorkers()

	// Phase 1: pre-decision checks, per transaction on the worker pool.
	var wg sync.WaitGroup

	wg.Add(len(idxs))

	for k, idx := range idxs {
		st := &states[k]
		st.idx = idx
		st.tx = txs[idx]

		job := batchJobPool.Get().(*batchJob)
		job.phase = batchPhasePreDecision
		job.ctx = ctx
		job.st = st
		job.blockHeight = blockHeight
		job.validationOptions = validationOptions
		job.wg = &wg

		v.submitBatchJob(pool, job)
	}

	wg.Wait()

	// Phase 2: Teranode-owned checks and BDK, batched.
	v.validateBatchPhase2(states, validationOptions)

	// Phase 3: BIP68 and the post-decision store work, per transaction on the
	// worker pool.
	for k := range states {
		st := &states[k]
		if st.err != nil {
			errs[st.idx] = st.err
			continue
		}

		wg.Add(1)

		job := batchJobPool.Get().(*batchJob)
		job.phase = batchPhasePostDecision
		job.ctx = ctx
		job.ctxLogger = ctxLogger
		job.st = st
		job.blockHeight = blockHeight
		job.validationOptions = validationOptions
		job.results = results
		job.errs = errs
		job.wg = &wg

		v.submitBatchJob(pool, job)
	}

	wg.Wait()
}

// validateBatchPhase1 opens the per-transaction span validateInternal would open
// and runs everything up to the BDK call. On failure the span is ended and st.err
// holds the error, wrapped exactly as validateInternal wraps it.
func (v *Validator) validateBatchPhase1(ctx context.Context, st *batchTx, blockHeight uint32, validationOptions *Options) {
	if err := ctx.Err(); err != nil {
		st.err = err
		return
	}

	tx := st.tx
	tx.SetTxHash(tx.TxIDChainHash())
	st.txID = tx.TxIDChainHash().String()

	st.ctx, st.span, st.end = tracing.Tracer("validator").Start(
		ctx,
		"validateInternal",
		tracing.WithParentStat(v.stats),
		tracing.WithHistogram(prometheusTransactionValidateTotal),
		tracing.WithTag("txid", st.txID),
	)

	utxoHeights, resolvedHeight, err := v.validateBeforeDecision(st.ctx, st.span, tx, st.txID, blockHeight, validationOptions)
	if err == nil {
		if utxoHeights, err = v.prepareTransactionValidation(st.ctx, st.span, tx, resolvedHeight, utxoHeights, validationOptions); err != nil {
			err = errors.NewProcessingError("[Validate][%s] error validating transaction", st.txID, err)
			st.span.RecordError(err)
		}
	}

	if err != nil {
		st.err = err
		st.end(err)

		return
	}

	st.blockHeight = resolvedHeight
	st.utxoHeights = utxoHeights
}

// validateBatchPhase2 runs txValidator.ValidateTransactionBatch over every
// transaction that passed Phase 1. Each transaction keeps its own resolved block
// height and utxo heights.
func (v *Validator) validateBatchPhase2(states []batchTx, validationOptions *Options) {
	var (
		batchTxs     = make([]*bt.Tx, 0, len(states))
		batchHeights = make([]uint32, 0, len(states))
		batchUtxos   = make([][]uint32, 0, len(states))
		batchStates  = make([]*batchTx, 0, len(states))
	)

	for k := range states {
		st := &states[k]
		if st.err != nil {
			continue
		}

		batchTxs = append(batchTxs, st.tx)
		batchHeights = append(batchHeights, st.blockHeight)
		batchUtxos = append(batchUtxos, st.utxoHeights)
		batchStates = append(batchStates, st)
	}

	if len(batchTxs) == 0 {
		return
	}

	for j, err := range v.txValidator.ValidateTransactionBatch(batchTxs, batchHeights, batchUtxos, validationOptions) {
		if err == nil {
			continue
		}

		st := batchStates[j]
		st.err = errors.NewProcessingError("[Validate][%s] error validating transaction", st.txID, err)
		st.span.RecordError(st.err)
		st.end(st.err)
	}
}

// validateBatchPhase3 finishes one transaction that passed Phase 2: BIP68, then the
// post-decision store work, then (on TX_LOCKED / TX_CREATING) the shared retry,
// which re-runs the whole single-transaction validation like ValidateWithOptions.
func (v *Validator) validateBatchPhase3(ctx context.Context, ctxLogger ulogger.Logger, st *batchTx, blockHeight uint32, validationOptions *Options) (txMetaData *meta.Data, err error) {
	if err = v.validateTransactionBIP68(st.span, st.tx, st.blockHeight, st.utxoHeights, validationOptions); err != nil {
		err = errors.NewProcessingError("[Validate][%s] error validating transaction", st.txID, err)
		st.span.RecordError(err)
	} else {
		txMetaData, err = v.validateAfterDecision(st.ctx, st.span, st.tx, st.txID, st.blockHeight, validationOptions)
	}

	st.end(err)

	return v.retryWhileParentCommitting(ctx, ctxLogger, st.tx, txMetaData, err, func() (*meta.Data, error) {
		return v.validateInternal(ctx, st.tx, blockHeight, validationOptions)
	})
}
