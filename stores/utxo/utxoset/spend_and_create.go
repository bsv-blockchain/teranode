package utxoset

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/jackc/pgx/v5"
)

// SpendAndCreate spends the transaction's inputs and creates its outputs in ONE
// PostgreSQL transaction.
//
// This is the atomic implementation that utxo.SequentialSpendAndCreate exists to stand
// in for. The sequential helper must spend, then create, and on a create failure roll
// the spends back by calling Unspend with retries and exponential backoff — compensating
// logic that exists purely because the two halves cannot be made atomic in the stores it
// was written for.
//
// Here they can be, and the compensation disappears entirely. A spend is a DELETE and a
// create is an INSERT; issue both inside one transaction and a failure is a ROLLBACK, so
// the deletes simply never happened. There is no window in which the inputs are spent
// and the outputs are missing, no retry loop, and no dependence on Unspend — which
// matters for this store in particular, because its Unspend needs the spend journal.
//
// On ErrTxExists this is still all-or-nothing, and that is not a contract violation.
// The interface says the error is returned "with the spends left in place", which reads
// like a required partial commit but is not: if the transaction already exists then it
// was already processed, its inputs were already spent BY IT, and the spend phase wrote
// nothing. The queue store of PR 684 makes that explicit -- its spend is
// ON CONFLICT (prev_tx_hash, prev_output_idx) DO NOTHING, so a re-spend is a no-op. So
// there is nothing to leave in place and nothing to undo, and a ROLLBACK reaches the
// same state a COMMIT would. Every path here is genuinely atomic.
//
// The store reaches that branch through tx_ident's primary key. Delete-on-spend has no
// spends row carrying spending_data, so a duplicate arrives as per-input ErrSpent from a
// DELETE affecting zero rows, which is indistinguishable from a genuine double spend.
// Identity answers it instead: the claim in createIn either inserts the row or reports
// that someone already holds this txid, and it does so without writing a UTXO row. One
// mechanism covers both arrival paths, the re-applied block and the duplicate mempool
// submission, which is what retires the applied_block ledger.
func (s *Store) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32,
	opts ...utxo.CreateOption) (_ *meta.Data, spends []*utxo.Spend, err error) {
	// Set from the batch's answer; see spendAndCreateResult. notSubmitted is set when the caller had
	// already given up before anything was queued or sent, so nothing was written.
	var rerunAfterCommitError, notSubmitted bool

	options := &utxo.CreateOptions{}
	for _, opt := range opts {
		opt(options)
	}

	if options.CreateOnly && options.SpendOnly {
		return nil, nil, errors.NewInvalidArgumentError("[utxoset][SpendAndCreate] WithCreateOnly and WithSpendOnly are mutually exclusive")
	}

	// No transaction is refused rather than read as one with nothing to spend. It used to
	// return no spends and no error, which a caller takes as every input spent: conflict
	// promotion did exactly that with a winner whose body had aged out.
	if tx == nil {
		return nil, nil, errors.NewInvalidArgumentError("[utxoset][SpendAndCreate] no transaction")
	}

	// Before the batcher and before any transaction, so a wait for a position holds nothing, and a
	// call that cannot get one fails having spent and written nothing. A call that creates nothing
	// takes no position. createIn parses opts again, so the position goes there too.
	if !options.SpendOnly {
		position, takeErr := s.Take(ctx, options)
		if takeErr != nil {
			return nil, nil, takeErr
		}

		// A position this call took from the source is its own to record if the call fails. The
		// batched path waits for its batch's answer whatever the caller's context does, and a batch
		// that rolls back runs the item again with the same position, so the answer returned here
		// is the final one.
		//
		// It also goes back for reuse when the final answer proves nothing carries it:
		// refusedUnwritten for exists, utxo.RefusedSpend for a refused spend. A rerun after a failed
		// batch never returns it: that batch's COMMIT may have landed with it.
		if utxo.TookLanePosition(options.LanePosition, position) {
			defer func() {
				if err == nil {
					return
				}

				switch {
				case notSubmitted:
					s.BurnLanePosition(options.Caller, utxo.LaneBurnError)
				case rerunAfterCommitError:
					s.burnUnlessStored(ctx, tx, options, position, err, spends)
				default:
					s.BurnCreateLanePosition(options, position, err, spends)
				}

				if rerunAfterCommitError {
					return
				}

				if refusedUnwritten(err, options) {
					s.ReturnLanePosition(position, utxo.LaneReturnExists, true)
				} else if !options.CreateOnly && utxo.RefusedSpend(err, spends) {
					s.ReturnLanePosition(position, utxo.LaneReturnSpendRejected, true)
				}
			}()
		}

		options.LanePosition = position
		opts = utxo.WithTakenLanePosition(opts, position)
	}

	// Batched when configured, which is the production path: see spend_and_create_batch.go.
	// Callers arriving together share one transaction, one spend statement and one create
	// statement, and each still gets the answer this function alone would have given it.
	//
	// The conflicting case takes the single path. It writes to the PARENTS of the incoming
	// transaction rather than only to the transaction itself, so two items in one batch can
	// touch the same row, which is exactly the overlap the batched path assumes away. A store
	// that is closing takes it too, so the answer is the pool's closed error and not a panic on
	// the batcher's closed channel; enqueue makes that hold when Close lands mid-call.
	if s.spendAndCreateBatcher != nil && !options.Conflicting && !s.closed.Load() {
		// A caller that has already given up gets its answer before anything is queued.
		// Once queued the item is applied whether or not anyone is waiting.
		if ctxErr := ctx.Err(); ctxErr != nil {
			// Nothing was queued, so the position certainly burned.
			notSubmitted = true

			return nil, nil, ctxErr
		}

		item := newSpendAndCreateItem(tx, blockHeight, options, opts)

		if !s.enqueue(func() { s.spendAndCreateBatcher.PutCtx(ctx, item) }) {
			return s.spendAndCreateOne(ctx, tx, blockHeight, options, opts...)
		}

		// Waited for unconditionally, and that is deliberate. The batch runs under its own
		// context and is bounded by the database exactly as the single path's COMMIT is, so
		// the wait is bounded too. Returning the caller's context error instead would tell it
		// "not applied" about a transaction that is about to be applied, and the validator
		// acts on that: it would neither hand the transaction to block assembly nor accept a
		// resubmission, which meets ErrTxExists. The only truthful answer is the batch's.
		res := <-item.done
		rerunAfterCommitError = res.rerunAfterCommitError

		return res.data, res.spends, res.err
	}

	// A call whose context has already ended fails before it opens a database transaction.
	if ctx.Err() != nil {
		notSubmitted = true
	}

	return s.spendAndCreateOne(ctx, tx, blockHeight, options, opts...)
}

// spendAndCreateOne is SpendAndCreate for one transaction in its own database transaction.
//
// It is the reference semantics: the batched path's contract is to give every caller the
// answer this would, and it hands an item here whenever it cannot settle that inside the batch.
func (s *Store) spendAndCreateOne(ctx context.Context, tx *bt.Tx, blockHeight uint32,
	options *utxo.CreateOptions, opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	// BEFORE the transaction is opened, never inside it. See
	// ensureSpendJournalPartition: the DDL needs its own pool connection, and taking one
	// while holding a transaction from the same pool deadlocks the pool under
	// concurrency.
	if !options.CreateOnly {
		if err := s.ensureSpendJournalPartition(ctx, blockHeight); err != nil {
			return nil, nil, err
		}
	}

	notedHeight := s.GetBlockHeight()

	if !options.SpendOnly && !s.seedingCreate(options) {
		if err := s.ensureTxBodyPartition(ctx, blockHeight); err != nil {
			return nil, nil, err
		}

		if mi, mined := minedBlock(options.MinedBlockInfos); mined {
			if err := s.ensureTxMinedPartition(ctx, mi.BlockHeight); err != nil {
				return nil, nil, err
			}
		}

		// The conflict note lands at the store's CURRENT height, which need not be in the
		// same partition as blockHeight: a block being applied is behind the tip, and the ensure
		// above is for the block's height. Asked for explicitly so the note cannot fail on a
		// partition nothing created, and the height read here is the one createIn is given.
		if options.Conflicting {
			if err := s.ensureSpendJournalPartition(ctx, notedHeight); err != nil {
				return nil, nil, err
			}
		}
	}

	dbTx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, errors.NewStorageError("[utxoset][SpendAndCreate] begin", err)
	}

	committed := false

	defer func() {
		if !committed {
			// Rollback IS the compensation. Nothing to undo by hand.
			_ = dbTx.Rollback(ctx)
		}
	}()

	var spends []*utxo.Spend

	if !options.CreateOnly && !options.SpendOnly {
		// The transaction's lock is taken BEFORE its spends, not only before the create's claim.
		// A SpendAndCreateMulti chunk holds the lock of every transaction it writes from before
		// its spends to its commit, so a writer of the same transaction that arrives here waits
		// for the chunk and then finds the transaction stored, or finds the coins free if the
		// chunk rolled back. Taken after the spends, the lock would let this writer meet a
		// spend the chunk had made but not yet committed.
		id := createTxID(tx, options)
		if err = s.lockTxids(ctx, dbTx, [][]byte{id[:]}); err != nil {
			return nil, nil, err
		}
	}

	if !options.CreateOnly {
		spends, err = s.spendIn(ctx, dbTx, tx, blockHeight, options.IgnoreFlags)
		if err != nil {
			return nil, spends, err
		}

		// Per-input failures are reported on the Spend records, not as a returned
		// error, so they must be inspected here: committing a partial spend would
		// leave the transaction half-applied.
		// Per-input failures are reported on the Spend records rather than as a returned
		// error, so they must be inspected here -- committing a partial spend would leave
		// the transaction half-applied. The aggregate is a UtxoError because that is what
		// callers match on; each input's specific cause (ErrFrozen, ErrSpent, immaturity)
		// stays on its own Spend record for conflict detection.
		var spendErrors []error

		for _, sp := range spends {
			if sp != nil && sp.Err != nil {
				spendErrors = append(spendErrors, sp.Err)
			}
		}

		if len(spendErrors) > 0 {
			return nil, spends, errors.NewUtxoError("[utxoset][SpendAndCreate] %d of %d inputs could not be spent", len(spendErrors), len(spends), spendErrors[0])
		}

		if options.SpendOnly {
			if err = dbTx.Commit(ctx); err != nil {
				return nil, spends, errors.NewStorageError("[utxoset][SpendAndCreate] commit (spend-only)", err)
			}

			committed = true

			return nil, spends, nil
		}
	}

	data, err := s.createIn(ctx, dbTx, tx, blockHeight, notedHeight, opts...)

	// ErrTxExists is NOT a failure to roll back, and treating it as one is the defect this
	// ordering exists to avoid.
	//
	// The interface says the error is returned "with the spends left in place"
	// (Interface.go:433-435), and the returned slice is the signal (:441-443). Both block
	// application paths create every transaction in one pass and spend the inputs in a
	// separate pass, so a transaction can genuinely be present while its own inputs are
	// still unspent. Rolling back here would tell the caller "already have it, nothing to
	// do" while the parent UTXOs stayed live and spendable by anyone else, which makes a
	// double spend mineable by this node. The claim itself wrote nothing, so committing
	// keeps the spends and nothing else.
	txExists := errors.Is(err, errors.ErrTxExists)

	if err != nil && !txExists {
		return nil, spends, err
	}

	if cerr := commitCreates(ctx, dbTx); cerr != nil {
		// The commit may have landed, as the SQL store marks the same failure.
		return nil, spends, utxo.MarkCreateOutcomeUnknown(errors.NewStorageError("[utxoset][SpendAndCreate] commit", cerr))
	}

	committed = true

	if txExists {
		return nil, spends, err
	}

	return data, spends, nil
}

// laneBurnProbeTimeout bounds burnUnlessStored's read. The read only labels a metric, and the
// caller waits for it, so it must never hold the caller, or the validator's lock on the txid, long.
var laneBurnProbeTimeout = 5 * time.Second

// burnUnlessStored records the burn of position for a create whose rerun, after its batch's COMMIT
// reported failure, failed with err. That COMMIT may have landed with this very position, whatever
// the rerun then met, so the stored record is read. A batch that failed before sending its COMMIT
// rolled back whole, and its reruns burn like any single call. Carrying the position, nothing burned. Carrying
// another position is taken as a burn for the rerun's own reason, exists for ErrTxExists, though
// it does not prove one: block assembly can delete the record and re-create it with a new
// position between the rerun and this read, and this read holds no lock against that. Missing, or
// unreadable within laneBurnProbeTimeout, or read without a position (the read can answer from a
// row that does not carry one, such as a live UTXO below the lookup floor), the outcome is unknown.
func (s *Store) burnUnlessStored(ctx context.Context, tx *bt.Tx, options *utxo.CreateOptions, position uint64,
	err error, spends []*utxo.Spend) {
	txid := utxo.CreatedTxID(tx, options)

	probeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), laneBurnProbeTimeout)
	defer cancel()

	// Read directly, not through the get batcher: queueing on it takes the store's close lock,
	// which a closing store with a writer stuck on a full queue can hold past any deadline.
	md, getErr := s.getDirect(probeCtx, &txid, false, false, true)

	switch {
	case getErr == nil && md != nil && md.LanePosition == position:
		return
	case getErr == nil && md != nil && md.LanePosition != lane.None:
		s.BurnLanePosition(options.Caller, utxo.LaneBurnReason(err, spends))
	default:
		// No record proves nothing: the batch's failed COMMIT may still be completing on its own
		// connection, and this read does not wait for it.
		s.BurnLanePosition(options.Caller, utxo.LaneBurnOutcomeUnknown)
	}
}

// createCommitFault is a test hook called after a transaction that writes creates commits. An error
// from it is returned as the commit's, which is the shape of a commit the server made and the client
// did not hear about. nil in production.
var createCommitFault func() error

// commitCreates commits a transaction that writes creates. A failure may have landed: the server
// can commit and the client not hear, so a caller counts a lane position it carried as
// outcome_unknown.
func commitCreates(ctx context.Context, dbTx pgx.Tx) error {
	if err := dbTx.Commit(ctx); err != nil {
		return err
	}

	if createCommitFault != nil {
		return createCommitFault()
	}

	return nil
}
