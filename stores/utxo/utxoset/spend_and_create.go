package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
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
// nothing. The postgres store makes that explicit -- its spend is
// ON CONFLICT (prev_tx_hash, prev_output_idx) DO NOTHING, so a re-spend is a no-op. So
// there is nothing to leave in place and nothing to undo, and a ROLLBACK reaches the
// same state a COMMIT would. Every path here is genuinely atomic.
//
// The store reaches that branch through tx_ident's primary key. Delete-on-spend has no
// spends row carrying spending_data, so a duplicate arrives as per-input ErrSpent from a
// DELETE affecting zero rows, which is indistinguishable from a genuine double spend.
// Identity answers it instead: the claim in createIn either inserts the row or reports
// that someone already holds this txid, and it does so without writing a coin row. One
// mechanism covers both arrival paths, the re-applied block and the duplicate mempool
// submission, which is what retires the applied_block ledger.
func (s *Store) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32,
	opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	options := &utxo.CreateOptions{}
	for _, opt := range opts {
		opt(options)
	}

	if options.CreateOnly && options.SpendOnly {
		return nil, nil, errors.NewInvalidArgumentError("[utxoset][SpendAndCreate] WithCreateOnly and WithSpendOnly are mutually exclusive")
	}

	// Batched when configured, which is the production path: see spend_and_create_batch.go.
	// Callers arriving together share one transaction, one spend statement and one create
	// statement, and each still gets the answer this function alone would have given it.
	//
	// The conflicting case takes the single path. It writes to the PARENTS of the incoming
	// transaction rather than only to the transaction itself, so two items in one batch can
	// touch the same row, which is exactly the overlap the batched path assumes away. A store
	// that is closing takes it too, so the answer is the pool's closed error and not a panic on
	// the batcher's closed channel.
	if s.spendAndCreateBatcher != nil && !options.Conflicting && !s.closed.Load() {
		// A caller that has already given up gets its answer before anything is queued.
		// Once queued the item is applied whether or not anyone is waiting.
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}

		item := newSpendAndCreateItem(tx, blockHeight, options, opts)

		s.spendAndCreateBatcher.PutCtx(ctx, item)

		// Waited for unconditionally, and that is deliberate. The batch runs under its own
		// context and is bounded by the database exactly as the single path's COMMIT is, so
		// the wait is bounded too. Returning the caller's context error instead would tell it
		// "not applied" about a transaction that is about to be applied, and the validator
		// acts on that: it would neither hand the transaction to block assembly nor accept a
		// resubmission, which meets ErrTxExists. The only truthful answer is the batch's.
		res := <-item.done

		return res.data, res.spends, res.err
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

	if !options.SpendOnly {
		if err := s.ensureTxBodyPartition(ctx, blockHeight); err != nil {
			return nil, nil, err
		}

		if mi, mined := minedBlock(options.MinedBlockInfos); mined {
			if err := s.ensureTxMinedPartition(ctx, mi.BlockHeight); err != nil {
				return nil, nil, err
			}
		}

		// The conflict note lands at the store's CURRENT height, which need not be in the
		// same window as blockHeight: a block being applied is behind the tip, and the ensure
		// above is for the block's height. Asked for explicitly so the note cannot fail on a
		// window nothing created, and the height read here is the one createIn is given.
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
	// do" while the parent coins stayed live and spendable by anyone else, which makes a
	// double spend mineable by this node. The claim itself wrote nothing, so committing
	// keeps the spends and nothing else.
	txExists := errors.Is(err, errors.ErrTxExists)

	if err != nil && !txExists {
		return nil, spends, err
	}

	if cerr := dbTx.Commit(ctx); cerr != nil {
		return nil, spends, errors.NewStorageError("[utxoset][SpendAndCreate] commit", cerr)
	}

	committed = true

	if txExists {
		return nil, spends, err
	}

	return data, spends, nil
}
