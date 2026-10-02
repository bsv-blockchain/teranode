package subtreevalidation

import (
	"context"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

// parentOutputsChunkSize is how many outpoints the batch path asks the store for
// in one ParentOutputsForValidation call. Small enough that the first answers,
// and so the first script checks, come back early.
const parentOutputsChunkSize = 1024

// unconfirmedParentHeight mirrors the validator's sentinel for a parent the store
// has no block recorded for. The validator substitutes the candidate height for
// it on the block path (Options.UnconfirmedParentsAtCandidateHeight).
const unconfirmedParentHeight uint32 = 0xFFFFFFFF

// batchChecker returns the local validator's batch checker when CheckBlockSubtrees
// should take the batch path: the node is catching up (CATCHINGBLOCKS), the block
// is above the highest checkpoint, and the validator runs in this process. Every
// other case keeps processTransactionsInLevels. In RUNNING the level path also
// hands transactions to block assembly, which the batch path does not do.
func (u *Server) batchChecker(state blockchain.FSMStateType, blockHeight uint32) (validator.BlockBatchChecker, bool) {
	if state != blockchain.FSMStateCATCHINGBLOCKS {
		return nil, false
	}

	if blockHeight <= blockchain.HighestCheckpointHeight(u.settings.ChainCfgParams.Checkpoints) {
		return nil, false
	}

	checker, ok := u.validatorClient.(validator.BlockBatchChecker)

	return checker, ok
}

// checkBlockBodyBound proves, before anything is written, that the block's
// subtree list is the one its header commits to: it loads the first and last
// subtrees' node lists and checks the merkle root with the coinbase substituted.
// Every other subtree contributes only its key. A node list fetched from a peer
// is checked against its key before it is stored as FileTypeSubtreeToCheck, so
// the batch load reads it locally. A local subtree file is not re-hashed, here
// or on the level path: this node writes one only after that check, or, on
// legacy catch-up, after checking the block's merkle root.
func (u *Server) checkBlockBodyBound(ctx context.Context, request *subtreevalidation_api.CheckBlockSubtreesRequest, block *model.Block, peerID string, dah uint32) error {
	if len(block.Subtrees) == 0 {
		return nil
	}

	first, err := u.getSubtreeToCheck(ctx, request, *block.Subtrees[0], peerID, dah)
	if err != nil {
		return err
	}

	last := first
	if len(block.Subtrees) > 1 {
		if last, err = u.getSubtreeToCheck(ctx, request, *block.Subtrees[len(block.Subtrees)-1], peerID, dah); err != nil {
			return err
		}
	}

	return block.CheckBodyBoundToHeader(first, last)
}

// processTransactionsBatched validates one load batch of a block during catch-up
// above the checkpoint. It replaces processTransactionsInLevels there; the caller
// no longer works in dependency levels, the store does.
//
//  1. Resolve: clear every input's previous-output fields, then fill each one
//     from a same-batch parent held in memory (keyed by the txid this node
//     computed while reading the subtree data) or from the store's
//     ParentOutputsForValidation. A spend of an outpoint twice, or of a
//     transaction later in the batch, fails the block.
//  2. Check every transaction's scripts, fees and consensus rules on all cores.
//     With every input resolved no transaction waits on another.
//  3. Write the checked transactions, in block order, with SpendAndCreateMulti.
//  4. Send every transaction that could not take this path, and its
//     descendants, through processTransactionsInLevels, which handles missing
//     parents, conflicts and already-mined records exactly as today.
func (u *Server) processTransactionsBatched(ctx context.Context, checker validator.BlockBatchChecker, allTransactions []*bt.Tx,
	blockHash chainhash.Hash, blockHeight uint32, candidateBlockTime uint32, candidateParentMedianTime uint32, blockIds map[uint32]bool) error {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "processTransactionsBatched",
		tracing.WithParentStat(u.stats),
		tracing.WithDebugLogMessage(u.logger, "[processTransactionsBatched] Processing %d transactions at block height %d", len(allTransactions), blockHeight),
	)
	defer deferFn()

	if len(allTransactions) == 0 {
		return nil
	}

	txHashes := make([]chainhash.Hash, len(allTransactions))
	position := make(map[chainhash.Hash]int, len(allTransactions))

	for i, tx := range allTransactions {
		if tx == nil {
			return errors.NewProcessingError("[processTransactionsBatched] transaction is nil at index %d", i)
		}

		txHashes[i] = *tx.TxIDChainHash()

		// A transaction twice in the batch is a CVE-2012-2459 duplicate-last
		// mutation: it keeps the subtree root, so checkBlockBodyBound cannot see
		// it. Classify it as ValidateSubtreeInternal does, corrupt rather than
		// invalid, before its repeated spends read as a double spend and condemn
		// an honest block hash.
		if first, dup := position[txHashes[i]]; dup {
			return errors.NewBlockCorruptError("[processTransactionsBatched] duplicate transaction %s at indexes %d and %d", txHashes[i], first, i)
		}

		position[txHashes[i]] = i
	}

	// Pre-check, as the level path does: drop what is already validated.
	start := time.Now()
	txMetaSlice := make([]metaSliceItem, len(txHashes))

	missed, err := u.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, false)
	if err != nil {
		return errors.NewProcessingError("[processTransactionsBatched] Failed to check txMeta cache", err)
	}

	if missed > 0 {
		missed, err = u.processTxMetaUsingStore(ctx, txHashes, txMetaSlice, blockIds, u.settings.SubtreeValidation.BatchMissingTransactions, false, true)
		if err != nil {
			return errors.NewProcessingError("[processTransactionsBatched] Failed to check txMeta store", err)
		}
	}

	prometheusSubtreeValidationBatchStep.WithLabelValues("precheck").Observe(time.Since(start).Seconds())

	if missed == 0 {
		return nil
	}

	b := &batchState{
		txs:      allTransactions,
		hashes:   txHashes,
		position: position,
		heights:  make([][]uint32, len(allTransactions)),
		parents:  make([][]int, len(allTransactions)),
		fallback: make([]bool, len(allTransactions)),
	}

	for i, tx := range allTransactions {
		if tx.IsCoinbase() {
			// The coinbase that arrives with the subtree data is never a parent.
			delete(b.position, txHashes[i])

			continue
		}

		if !txMetaSlice[i].isSet {
			b.missing = append(b.missing, i)
		}
	}

	validatorOptions := validator.ProcessOptions(
		validator.WithSkipPolicyChecks(true),
		validator.WithInBlock(true),
		validator.WithCreateConflicting(true),
		validator.WithIgnoreLocked(true),
		validator.WithCandidateBlockTime(candidateBlockTime),
		validator.WithCandidateParentMedianTime(candidateParentMedianTime),
		validator.WithUnconfirmedParentsAtCandidateHeight(true),
		validator.WithAddTXToBlockAssembly(false),
	)

	if err = u.validatorClient.EnsureMTPLoaded(ctx, blockHeight); err != nil {
		return errors.NewProcessingError("[processTransactionsBatched] failed to pre-load MTP store", err)
	}

	if err = u.resolveAndCheckBatch(ctx, checker, b, blockHeight, validatorOptions); err != nil {
		return err
	}

	start = time.Now()

	if err = u.writeBatch(ctx, checker, b, blockHeight); err != nil {
		return err
	}

	prometheusSubtreeValidationBatchStep.WithLabelValues("write").Observe(time.Since(start).Seconds())

	// Everything that did not take the batch path goes through today's path, in
	// block order. It re-reads parents from the store, so the inputs this path
	// resolved are overwritten, never trusted.
	var fallbackTxs []*bt.Tx

	for _, i := range b.missing {
		if b.fallback[i] {
			fallbackTxs = append(fallbackTxs, allTransactions[i])
		}
	}

	prometheusSubtreeValidationBatchTxs.WithLabelValues("fallback").Add(float64(len(fallbackTxs)))

	if len(fallbackTxs) == 0 {
		return nil
	}

	u.logger.Debugf("[processTransactionsBatched] %d of %d transactions go through the per-transaction path", len(fallbackTxs), len(b.missing))

	start = time.Now()
	defer func() {
		prometheusSubtreeValidationBatchStep.WithLabelValues("fallback").Observe(time.Since(start).Seconds())
	}()

	return u.processTransactionsInLevels(ctx, fallbackTxs, blockHash, chainhash.Hash{}, blockHeight, candidateBlockTime, candidateParentMedianTime, blockIds, false)
}

// batchState is one load batch on the batch path. Indices are positions in txs.
type batchState struct {
	txs      []*bt.Tx
	hashes   []chainhash.Hash
	position map[chainhash.Hash]int // non-coinbase transactions of the batch, by locally computed txid
	missing  []int                  // transactions to validate, in block order
	heights  [][]uint32             // per missing transaction, each input's parent height
	parents  [][]int                // per missing transaction, its parents' positions in the batch
	fallback []bool                 // sent through the per-transaction path instead
}

// markDescendantsFallback sends every missing transaction with a parent in the
// batch that falls back through the per-transaction path as well. Parents come
// before children, so one pass in block order suffices.
func (b *batchState) markDescendantsFallback() {
	for _, i := range b.missing {
		if b.fallback[i] {
			continue
		}

		for _, p := range b.parents[i] {
			if b.fallback[p] {
				b.fallback[i] = true
				break
			}
		}
	}
}

type storeInputRef struct {
	tx, input int
}

// resolveAndCheckBatch extends every input of every missing transaction (step
// 1) and checks each transaction on all cores (step 2), checking a transaction
// as soon as its own inputs are filled in. A transaction whose parents are all
// in the batch is checked straight away; the rest are checked as the store reads
// covering their inputs come back. The script checks, which are CPU work, so run
// while the parent reads wait on the store, as they overlap the store waits on
// the per-transaction path.
//
// A consensus failure fails the block, as on the level path. A missing parent,
// or a check that fails for any other reason, sends the transaction and its
// descendants through the per-transaction path.
func (u *Server) resolveAndCheckBatch(ctx context.Context, checker validator.BlockBatchChecker, b *batchState, blockHeight uint32, opts *validator.Options) error {
	start := time.Now()

	refs, outpoints, err := resolveFromMemory(b, blockHeight)
	if err != nil {
		return err
	}

	// pending[i] counts transaction i's inputs still waiting on the store. The
	// goroutine that takes it to zero hands the transaction to the checkers.
	pending := make([]atomic.Int32, len(b.txs))
	for _, ref := range refs {
		pending[ref.tx].Add(1)
	}

	// sentBack marks a transaction for the per-transaction path. Store-read
	// goroutines and checkers set it concurrently; it is folded into b.fallback
	// once both are done.
	sentBack := make([]atomic.Bool, len(b.txs))

	// A failed read stops the checkers; a failed check stops the reads through
	// the errgroup's context. Whichever failed first is the error returned.
	stageCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, gCtx := errgroup.WithContext(stageCtx)

	var readFailedFirst atomic.Bool

	// Each transaction is sent at most once, so sends never block.
	ready := make(chan int, len(b.missing))

	checkStart := time.Now()

	for range runtime.GOMAXPROCS(0) {
		g.Go(func() error {
			for i := range ready {
				if err := u.checkOne(gCtx, checker, b, i, blockHeight, opts, sentBack); err != nil {
					return err
				}
			}

			return nil
		})
	}

	for _, i := range b.missing {
		if pending[i].Load() == 0 {
			ready <- i
		}
	}

	reads := errgroup.Group{}
	reads.SetLimit(max(1, u.settings.BlockValidation.ProcessTxMetaUsingStoreConcurrency))

	for chunkStart := 0; chunkStart < len(outpoints); chunkStart += parentOutputsChunkSize {
		chunkEnd := min(chunkStart+parentOutputsChunkSize, len(outpoints))

		reads.Go(func() error {
			fail := func(err error) error {
				if gCtx.Err() == nil {
					readFailedFirst.Store(true)
				}

				cancel()

				return err
			}

			answers, err := u.utxoStore.ParentOutputsForValidation(gCtx, outpoints[chunkStart:chunkEnd])
			if err != nil {
				return fail(errors.NewProcessingError("[processTransactionsBatched] failed to read parent outputs", err))
			}

			if len(answers) != chunkEnd-chunkStart {
				return fail(errors.NewProcessingError("[processTransactionsBatched] store returned %d parent outputs for %d outpoints", len(answers), chunkEnd-chunkStart))
			}

			for n, answer := range answers {
				ref := refs[chunkStart+n]
				op := outpoints[chunkStart+n]

				if err := applyParentOutput(b, ref, op, answer, sentBack); err != nil {
					return fail(err)
				}

				if pending[ref.tx].Add(-1) == 0 {
					ready <- ref.tx
				}
			}

			return nil
		})
	}

	readErr := reads.Wait()

	// The resolve and check steps overlap by design, so the two observations do
	// not add up. check_after_reads is the part of the check that no read
	// overlaps: large when the script checks, not the parent reads, set the pace.
	readsDone := time.Now()

	prometheusSubtreeValidationBatchStep.WithLabelValues("resolve").Observe(readsDone.Sub(start).Seconds())

	close(ready)

	checkErr := g.Wait()

	prometheusSubtreeValidationBatchStep.WithLabelValues("check").Observe(time.Since(checkStart).Seconds())
	prometheusSubtreeValidationBatchStep.WithLabelValues("check_after_reads").Observe(time.Since(readsDone).Seconds())

	switch {
	case readErr != nil && (checkErr == nil || readFailedFirst.Load()):
		return readErr
	case checkErr != nil:
		return errors.NewProcessingError("[processTransactionsBatched] failed to check transactions", checkErr)
	case readErr != nil:
		return readErr
	}

	for _, i := range b.missing {
		if sentBack[i].Load() {
			b.fallback[i] = true
		}
	}

	b.markDescendantsFallback()

	return nil
}

// resolveFromMemory clears every input's previous-output fields and fills each
// one whose parent is in the batch. It returns the inputs left for the store,
// in block order.
func resolveFromMemory(b *batchState, blockHeight uint32) ([]storeInputRef, []utxo.Outpoint, error) {
	spent := make(map[utxo.Outpoint]int)

	// lastChild[p] is 1 + the position of the last transaction that recorded p
	// as a parent. A transaction's inputs are walked together, so this dedups
	// its parents in O(1) each; a scan of its list would be quadratic in a
	// consolidation transaction's parent count.
	lastChild := make([]int, len(b.txs))

	var (
		refs      []storeInputRef
		outpoints []utxo.Outpoint
		fromMem   int
	)

	for _, i := range b.missing {
		tx := b.txs[i]
		b.heights[i] = make([]uint32, len(tx.Inputs))

		for k, in := range tx.Inputs {
			// Never check a transaction with previous-output fields a peer
			// supplied (GHSA-v76m-6vc7-g7c7).
			in.PreviousTxSatoshis = 0
			in.PreviousTxScript = nil

			op := utxo.Outpoint{TxID: *in.PreviousTxIDChainHash(), Vout: in.PreviousTxOutIndex}

			if first, dup := spent[op]; dup {
				return nil, nil, errors.NewBlockInvalidError("[processTransactionsBatched] transactions %s and %s both spend %s:%d", b.hashes[first], b.hashes[i], op.TxID, op.Vout)
			}

			spent[op] = i

			p, inBatch := b.position[op.TxID]
			if !inBatch {
				refs = append(refs, storeInputRef{tx: i, input: k})
				outpoints = append(outpoints, op)

				continue
			}

			if p >= i {
				return nil, nil, errors.NewBlockInvalidError("[processTransactionsBatched] transaction %s spends %s, which is not earlier in the block", b.hashes[i], op.TxID)
			}

			parent := b.txs[p]
			if int(op.Vout) >= len(parent.Outputs) || parent.Outputs[op.Vout] == nil {
				return nil, nil, errors.NewTxInvalidError("[processTransactionsBatched] transaction %s spends output %d of %s, which has %d outputs", b.hashes[i], op.Vout, op.TxID, len(parent.Outputs))
			}

			in.PreviousTxSatoshis = parent.Outputs[op.Vout].Satoshis
			in.PreviousTxScript = parent.Outputs[op.Vout].LockingScript
			// A same-block parent is mined at this block's height.
			b.heights[i][k] = blockHeight
			if lastChild[p] != i+1 {
				lastChild[p] = i + 1
				b.parents[i] = append(b.parents[i], p)
			}

			fromMem++
		}
	}

	prometheusSubtreeValidationBatchParentOutputs.WithLabelValues("memory").Add(float64(fromMem))
	prometheusSubtreeValidationBatchParentOutputs.WithLabelValues("store").Add(float64(len(outpoints)))

	return refs, outpoints, nil
}

// applyParentOutput fills one input from the store's answer. Each input belongs
// to exactly one read, so no two goroutines write the same slot.
func applyParentOutput(b *batchState, ref storeInputRef, op utxo.Outpoint, answer utxo.ParentOutput, sentBack []atomic.Bool) error {
	in := b.txs[ref.tx].Inputs[ref.input]

	switch {
	case answer.Err != nil:
		// A store fault is never a verdict on the block: retry the batch.
		return errors.NewProcessingError("[processTransactionsBatched] failed to read parent output %s:%d", op.TxID, op.Vout, answer.Err)
	case answer.Status == utxo.ParentOutputTxNotFound:
		// The per-transaction path reports the missing parent and defers it, as
		// today.
		sentBack[ref.tx].Store(true)
		return nil
	case answer.Status == utxo.ParentOutputNoSuchIndex:
		return errors.NewTxInvalidError("[processTransactionsBatched] transaction %s spends output %d of %s, which does not exist", b.hashes[ref.tx], op.Vout, op.TxID)
	case answer.Status == utxo.ParentOutputMined:
		b.heights[ref.tx][ref.input] = answer.Height
	case answer.Status == utxo.ParentOutputNotMined:
		b.heights[ref.tx][ref.input] = unconfirmedParentHeight
	default:
		return errors.NewProcessingError("[processTransactionsBatched] store gave no answer for parent output %s:%d", op.TxID, op.Vout)
	}

	in.PreviousTxSatoshis = answer.Satoshis
	in.PreviousTxScript = answer.LockingScript

	return nil
}

// checkOne checks one fully resolved transaction.
func (u *Server) checkOne(ctx context.Context, checker validator.BlockBatchChecker, b *batchState, i int, blockHeight uint32, opts *validator.Options, sentBack []atomic.Bool) error {
	if sentBack[i].Load() {
		return nil
	}

	err := checker.CheckExtendedTransaction(ctx, b.txs[i], blockHeight, b.heights[i], opts)
	if err == nil {
		return nil
	}

	if errors.Is(err, errors.ErrTxInvalid) && !errors.Is(err, errors.ErrTxPolicy) {
		u.logger.Warnf("[processTransactionsBatched] Invalid transaction detected: %s: %v", b.hashes[i], err)
		return err
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	sentBack[i].Store(true)

	return nil
}

// writeBatch writes the checked transactions with SpendAndCreateMulti, in block
// order, one list at a time, each capped by
// subtreevalidation_spendAndCreateMultiMaxTxs (step 3). The default cap covers a
// whole load batch, so a batch is normally one list, and the store writes each
// dependency level exactly as wide as subtree validation writes it today.
// Created records have their txmeta published, as the validator publishes them;
// every other outcome falls back.
func (u *Server) writeBatch(ctx context.Context, checker validator.BlockBatchChecker, b *batchState, blockHeight uint32) error {
	maxTxs := max(1, u.settings.SubtreeValidation.SpendAndCreateMultiMaxTxs)

	list := make([]int, 0, min(maxTxs, len(b.missing)))

	for _, i := range b.missing {
		if b.fallback[i] {
			continue
		}

		if len(list) >= maxTxs {
			if err := u.writeList(ctx, checker, b, list, blockHeight); err != nil {
				return err
			}

			list = list[:0]
		}

		list = append(list, i)
	}

	if len(list) == 0 {
		return nil
	}

	return u.writeList(ctx, checker, b, list, blockHeight)
}

// writeList writes one list. Every earlier list has finished, so their outcomes
// are settled: a transaction whose parent did not end up created is sent back
// with it, and parents in this list are the store's to handle
// (MultiTxParentFailed).
func (u *Server) writeList(ctx context.Context, checker validator.BlockBatchChecker, b *batchState, members []int, blockHeight uint32) error {
	list := make([]int, 0, len(members))

	for _, i := range members {
		for _, p := range b.parents[i] {
			if b.fallback[p] {
				b.fallback[i] = true
				break
			}
		}

		if !b.fallback[i] {
			list = append(list, i)
		}
	}

	if len(list) == 0 {
		return nil
	}

	txs := make([]*bt.Tx, len(list))
	txids := make([]chainhash.Hash, len(list))

	for n, i := range list {
		txs[n] = b.txs[i]
		txids[n] = b.hashes[i]
	}

	prometheusSubtreeValidationBatchLists.Inc()

	results, err := u.utxoStore.SpendAndCreateMulti(ctx, txs, blockHeight, utxo.WithTXIDs(txids), utxo.WithIgnoreLocked(true))

	switch {
	case utxo.IsSpendAndCreateMultiRefused(err):
		// The caller already checked what the store refuses, so this is a bug
		// here. Nothing was written; send the list through today's path.
		u.logger.Errorf("[processTransactionsBatched] store refused a list of %d transactions, sending them through the per-transaction path: %v", len(list), err)

		for _, i := range list {
			b.fallback[i] = true
		}

		return nil
	case err != nil:
		// Retrying the batch is safe: existing records are recognised one by one.
		return errors.NewProcessingError("[processTransactionsBatched] SpendAndCreateMulti failed", err)
	case len(results) != len(list):
		return errors.NewProcessingError("[processTransactionsBatched] SpendAndCreateMulti returned %d results for %d transactions", len(results), len(list))
	}

	created := 0

	for n, r := range results {
		if r.Status == utxo.MultiTxCreated {
			created++

			checker.PublishTxMeta(r.Meta, &txids[n], true)

			continue
		}

		// Existed, failed or parent failed: today's path decides, including the
		// already-mined-on-our-chain and conflicting checks.
		b.fallback[list[n]] = true
	}

	prometheusSubtreeValidationBatchTxs.WithLabelValues("created").Add(float64(created))

	return nil
}
