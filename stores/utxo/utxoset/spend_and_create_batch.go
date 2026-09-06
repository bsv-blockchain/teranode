package utxoset

import (
	"context"
	"runtime/debug"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util/batchermetrics"
	"github.com/jackc/pgx/v5"
)

// The batched form of SpendAndCreate.
//
// Every production caller of this store arrives through SpendAndCreate, and until this file
// existed each call opened its own PostgreSQL transaction: one BEGIN, a spend statement with a
// plan of one, a create statement with a plan of one, one COMMIT. On the mainnet box that was
// 15,000 commits a second, and postgres spent its time on the locks every commit must take,
// assigning a transaction id and reserving and flushing WAL, rather than on the rows. Both
// statements were written to take arrays. Nothing fed them more than one.
//
// This batcher does. Callers arriving from many goroutines are applied together in ONE
// transaction: one spend statement carrying every input of every item, one create statement
// carrying every item, one commit. Per-commit cost is divided by the batch width and
// durability is untouched, which is the whole point: it is the same win that turning fsync off
// would buy, obtained without turning anything off.
//
// The contract is that each caller receives exactly the answer the single-transaction path
// would have given it. Where that cannot be arranged inside the batch, the item is handed to
// the single path rather than approximated, and it is handed over in the state it arrived in.

// spendAndCreateBatchByteBudget bounds the serialized bytes one batch transaction carries.
//
// The batcher counts items, not bytes, and a PostgreSQL parameter cannot exceed a gigabyte.
// A batch of large transactions would fail its statement deterministically and be redone as
// singles at twice the cost, and long before that it would hold every body in memory twice.
// So a flushed batch is cut into chunks under this budget, each its own transaction. At the
// usual few hundred bytes per transaction the budget is never reached and a flush is one chunk.
// A variable rather than a constant so a test can force the split.
var spendAndCreateBatchByteBudget = 16 << 20

// spendAndCreateResult is what one queued SpendAndCreate gets back.
type spendAndCreateResult struct {
	data   *meta.Data
	spends []*utxo.Spend
	err    error
}

// inputClaim is what an input said about the coin it spends when the call arrived.
//
// The spend statement is also the decorate fetch: for every input it takes it overwrites the
// caller's claimed satoshis and script with the store's, after comparing the two. A transaction
// that is run a second time therefore no longer carries its own claim, and a forged one would
// pass the comparison it just failed. Every re-run in this file first puts the claims back, so a
// re-run is a fresh run and nothing has to reason about which inputs were overwritten when.
type inputClaim struct {
	satoshis uint64
	script   *bscript.Script
}

// spendAndCreateItem is a single SpendAndCreate waiting for its batch to flush.
type spendAndCreateItem struct {
	tx          *bt.Tx
	blockHeight uint32
	options     *utxo.CreateOptions
	// opts is kept alongside the parsed options because the single-item path re-parses it.
	opts   []utxo.CreateOption
	claims []inputClaim
	done   chan spendAndCreateResult
}

// newSpendAndCreateItem is the only way to build an item, so nothing can be queued without its
// claims captured. An item without them would be re-run carrying the store's answer to an
// earlier run, which is the hole restoreClaims closes.
func newSpendAndCreateItem(tx *bt.Tx, blockHeight uint32, options *utxo.CreateOptions,
	opts []utxo.CreateOption) *spendAndCreateItem {
	return &spendAndCreateItem{
		tx:          tx,
		blockHeight: blockHeight,
		options:     options,
		opts:        opts,
		claims:      captureClaims(tx),
		done:        make(chan spendAndCreateResult, 1),
	}
}

// captureClaims records what every input of tx claims about its coin.
func captureClaims(tx *bt.Tx) []inputClaim {
	if tx == nil {
		return nil
	}

	claims := make([]inputClaim, len(tx.Inputs))

	for i, in := range tx.Inputs {
		if in != nil {
			claims[i] = inputClaim{satoshis: in.PreviousTxSatoshis, script: in.PreviousTxScript}
		}
	}

	return claims
}

// restoreClaims puts the item's transaction back exactly as it arrived, so the next run of it
// sees the caller's claims and not the store's answer to an earlier run.
func (it *spendAndCreateItem) restoreClaims() {
	if it.tx == nil {
		return
	}

	for i, in := range it.tx.Inputs {
		if in != nil && i < len(it.claims) {
			in.PreviousTxSatoshis = it.claims[i].satoshis
			in.PreviousTxScript = it.claims[i].script
		}
	}
}

// newSpendAndCreateBatcher wires SpendAndCreate through the shared batcher.
//
// Dispatch is backgrounded and bounded, unlike the create batcher's, because this is the
// production write path and one batch per round trip on one goroutine would trade the callers'
// parallelism, which is in the hundreds, for one. The concurrency bound is the same setting
// the aerospike store applies to all of its batchers. Each in-flight batch holds exactly one
// pool connection and takes no second one while it holds it, so the bound need only stay
// below the pool size to be safe.
//
// Drain mode and greedy accumulate used to be a decision made here: drain forced on, because
// under concurrent load it adapts the batch to whatever has queued rather than waiting on a
// timer, and against the single-transaction path on a local instance that won at every
// concurrency below the batch width. That result is real but partial: it is a tip-time trade,
// made under bursts wide enough to fill a worker's queue between flushes. Mainnet at height
// 430,000 is not that load. There the batcher fired 26.5 million times, every flush adaptive,
// averaging 11.8 items for a fixed 9.3 ms commit cost each -- paying the per-commit cost of a
// twelve-row batch when the queue depth would have supported far more had the flush waited.
// Greedy accumulate never fires early; it only pulls what is already queued into the batch
// faster, so it does not pay that cost and does not lose the drain columns' win under real
// bursts either. Which one is right depends on the load a given deployment actually sees, so
// the choice is now the operator's, through the same StoreBatcherDrainMode and
// StoreBatcherGreedyAccumulate keys every store reads, not a constant fixed here.
func newSpendAndCreateBatcher(s *Store, size int, duration time.Duration, background bool,
	maxConcurrent int, drainMode, greedyAccumulate bool) *batcher.Batcher[spendAndCreateItem] {
	b := batcher.NewWithPool(size, duration, s.sendSpendAndCreateBatch, background,
		batcher.WithName("utxoset_spend_and_create"),
		batcher.WithLogger(s.logger),
		batcher.WithMetrics(batchermetrics.Provider()),
		batcher.WithGreedyAccumulate(greedyAccumulate))

	if background && maxConcurrent > 0 {
		b.SetMaxConcurrent(maxConcurrent)
	}

	if drainMode {
		b.SetDrainMode(true)
	}

	return b
}

// sendSpendAndCreateBatch applies one flushed batch and answers every caller in it.
func (s *Store) sendSpendAndCreateBatch(batch []*spendAndCreateItem) {
	// Marks this batch as in flight so Close waits for the database work, not merely for the
	// hand-off. See createInFlight on Store.
	s.spendAndCreateInFlight.Add(1)
	defer s.spendAndCreateInFlight.Done()

	delivered := make([]bool, len(batch))

	// A panic in here would be caught by the batcher and logged, and every caller in the
	// batch would then wait forever on a channel nobody writes to. They get an error instead.
	defer func() {
		if r := recover(); r != nil {
			s.logger.Errorf("[utxoset][SpendAndCreate] batch of %d panicked: %v\n%s", len(batch), r, debug.Stack())

			err := errors.NewStorageError("[utxoset][SpendAndCreate] batch panicked: %v", r)

			for i, item := range batch {
				if !delivered[i] {
					delivered[i] = true
					item.done <- spendAndCreateResult{err: err}
				}
			}
		}
	}()

	// Cut by bytes, then apply each chunk on its own. Items keep their position in batch so
	// the delivery bookkeeping above still lines up.
	start := 0
	bytes := 0

	for i, item := range batch {
		size := 0
		if item.tx != nil {
			size = item.tx.Size()
		}

		if i > start && bytes+size > spendAndCreateBatchByteBudget {
			s.applyChunk(batch[start:i], delivered[start:i])
			start, bytes = i, 0
		}

		bytes += size
	}

	s.applyChunk(batch[start:], delivered[start:])
}

// applyChunk applies one chunk in one transaction and answers its callers.
func (s *Store) applyChunk(chunk []*spendAndCreateItem, delivered []bool) {
	ctx := context.Background()

	deliver := func(i int, r spendAndCreateResult) {
		delivered[i] = true
		chunk[i].done <- r
	}

	// Partitions BEFORE any transaction is opened. The DDL needs its own pool connection, and
	// taking one while holding a transaction from the same pool deadlocks the pool under
	// concurrency, with no timeout. A chunk can span journal leaves and body windows, so every
	// distinct one is prepared. A failure here is reported to every item, as the create batcher
	// does: it is a catalog failure that a retry through the single path would only repeat.
	if err := s.ensurePartitionsFor(ctx, chunk); err != nil {
		for i := range chunk {
			deliver(i, spendAndCreateResult{err: err})
		}

		return
	}

	results, final, err := s.runSpendAndCreateBatch(ctx, chunk)
	if err != nil {
		// A batch-level failure: BEGIN, a statement, or COMMIT failed. Everything the batch
		// did was rolled back, or on an ambiguous COMMIT may have landed, and the single path
		// is idempotent under both: a landed create answers ErrTxExists and a landed spend is
		// recognised as a replay from the journal. So every item the batch had not already
		// settled is redone alone, and gets precisely the answer it would have got had the
		// batcher not existed. One bad batch therefore never yields a worse answer than the
		// same calls made singly.
		s.logger.Warnf("[utxoset][SpendAndCreate] batch of %d fell back to single transactions: %v", len(chunk), err)
	}

	// Settled answers go out first, so a caller that has its answer is not held behind the
	// single transactions below.
	for i := range chunk {
		if final[i] {
			deliver(i, results[i])
		}
	}

	// Whatever the batch could not settle is settled by the single path. After a successful
	// commit that is only the items that lost a coin to a sibling, whose verdict had to wait
	// for the sibling's fate. After a failure it is everything except the items the batch had
	// already rejected outright.
	//
	// A rejected item is never re-run and its verdict stands in both cases. It was reached
	// against committed state: a claim that did not match the coin, a frozen or conflicting or
	// immature coin, or a coin taken by a transaction outside this batch. Whatever ended the
	// batch, that answer is the one the single path would give.
	//
	// Every item that IS re-run goes back with the claims it arrived with. That, and not any
	// argument about which inputs an earlier round happened to overwrite, is what makes a
	// re-run safe: it is a fresh run. An earlier statement may have been interrupted with half
	// its rows already returned, and the inputs those rows decorated cannot be told apart from
	// the rest without this.
	for i, item := range chunk {
		if final[i] {
			continue
		}

		item.restoreClaims()

		data, spends, ierr := s.spendAndCreateOne(ctx, item.tx, item.blockHeight, item.options, item.opts...)
		deliver(i, spendAndCreateResult{data: data, spends: spends, err: ierr})
	}
}

// ensurePartitionsFor creates every journal leaf, body window and membership window the batch
// will write into.
func (s *Store) ensurePartitionsFor(ctx context.Context, batch []*spendAndCreateItem) error {
	leaves := make(map[uint32]struct{}, 2)
	windows := make(map[uint32]struct{}, 2)
	mined := make(map[uint32]struct{}, 2)

	for _, item := range batch {
		if !item.options.CreateOnly {
			if leaf := item.blockHeight / SpendJournalPartitionBlocks; !has(leaves, leaf) {
				leaves[leaf] = struct{}{}

				if err := s.ensureSpendJournalPartition(ctx, item.blockHeight); err != nil {
					return err
				}
			}
		}

		if !item.options.SpendOnly {
			if win := item.blockHeight / TxBodyPartitionBlocks; !has(windows, win) {
				windows[win] = struct{}{}

				if err := s.ensureTxBodyPartition(ctx, item.blockHeight); err != nil {
					return err
				}
			}

			if mi, isMined := minedBlock(item.options.MinedBlockInfos); isMined {
				if win := mi.BlockHeight / TxMinedPartitionBlocks; !has(mined, win) {
					mined[win] = struct{}{}

					if err := s.ensureTxMinedPartition(ctx, mi.BlockHeight); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func has(m map[uint32]struct{}, k uint32) bool {
	_, ok := m[k]
	return ok
}

// runSpendAndCreateBatch applies the batch in one transaction.
//
// It returns one result per item and, alongside, which of those results are final. After a
// commit every result is final except those of items that lost a coin to a sibling, which the
// caller must judge again once the batch is durable. After an error the only final results are
// outright rejections reached in an earlier round; nothing else was answered, and the caller
// must redo those items some other way.
//
// The spend phase runs as a loop of rounds. A round issues the spend statement for every item
// still active and inspects the per-input records it fills in. If no item failed, the
// transaction stays open and the round is the last. Otherwise the whole transaction is rolled
// back, the failed items are removed, and the survivors are run again in a fresh one, with
// their claims restored first.
//
// Rolling back the whole transaction rather than compensating inside it is a correctness
// requirement, not a shortcut. Deleting the failed item's journal rows and re-inserting its
// coins would look equivalent, and under READ COMMITTED it is not: a competing spend that
// blocked on our deleted row would, once we committed, find that row version deleted and never
// see the re-inserted one, and would report a live coin as spent. A rollback makes it re-read
// the row that never went away. The spend phase is the first thing the transaction does, so a
// whole rollback loses nothing over a savepoint and costs nothing on the round that succeeds.
//
// The loop ends because every extra round removes at least one item. In practice the first
// round collects every failure at once, and a second round fails only if a competitor committed
// in between, which it may well have, since the rollback is what released the rows it was
// waiting on.
func (s *Store) runSpendAndCreateBatch(ctx context.Context, batch []*spendAndCreateItem) (
	[]spendAndCreateResult, []bool, error) {
	results := make([]spendAndCreateResult, len(batch))

	// rejected marks an item whose verdict is fixed: it failed the spend phase and nothing it
	// failed on depends on a sibling. deferred marks one that lost at least one coin to a
	// sibling, whose verdict waits for the commit. Both are out of every later phase; only
	// rejected survives an error as a final answer.
	rejected := make([]bool, len(batch))
	deferred := make([]bool, len(batch))

	siblings := make(map[chainhash.Hash]struct{}, len(batch))

	for _, item := range batch {
		if item.tx != nil {
			siblings[*item.tx.TxIDChainHash()] = struct{}{}
		}
	}

	active := make([]int, 0, len(batch))

	for i, item := range batch {
		if !item.options.CreateOnly && item.tx != nil && !item.tx.IsCoinbase() {
			active = append(active, i)
		}
	}

	var (
		dbTx pgx.Tx
		err  error
	)

	rollback := func() {
		if dbTx != nil {
			_ = dbTx.Rollback(ctx)
			dbTx = nil
		}
	}

	for round := 0; len(active) > 0; round++ {
		if round > 0 {
			for _, i := range active {
				batch[i].restoreClaims()
			}
		}

		dbTx, err = s.pool.Begin(ctx)
		if err != nil {
			return results, rejected, errors.NewStorageError("[utxoset][SpendAndCreate] begin batch", err)
		}

		items := make([]*spendItem, len(active))
		for k, i := range active {
			items[k] = &spendItem{tx: batch[i].tx, blockHeight: batch[i].blockHeight,
				ignoreFlags: batch[i].options.IgnoreFlags}
		}

		plan := planSpends(items)

		if err = s.runSpendPlan(ctx, dbTx, plan); err != nil {
			rollback()
			return results, rejected, err
		}

		survivors := active[:0]

		for k, i := range active {
			spends := plan.perItem[k]

			var (
				failures int
				first    error
			)

			for _, sp := range spends {
				if sp != nil && sp.Err != nil {
					failures++

					if first == nil {
						first = sp.Err
					}
				}
			}

			if failures == 0 {
				survivors = append(survivors, i)
				results[i].spends = spends

				continue
			}

			if lostToASibling(batch[i].tx, spends, siblings) {
				deferred[i] = true
				continue
			}

			rejected[i] = true

			// The aggregate is a UtxoError because that is what callers match on; each
			// input's specific cause stays on its own record for conflict detection. This is
			// the single path's wording, and it must stay identical: callers read it.
			results[i] = spendAndCreateResult{spends: spends,
				err: errors.NewUtxoError("[utxoset][SpendAndCreate] %d of %d inputs could not be spent", failures, len(spends), first)}
		}

		if len(survivors) == len(active) {
			break
		}

		rollback()

		active = survivors
	}

	creators := make([]*createItem, 0, len(batch))
	owners := make([]int, 0, len(batch))

	for i, item := range batch {
		if item.options.SpendOnly || rejected[i] || deferred[i] {
			continue
		}

		creators = append(creators, &createItem{tx: item.tx, blockHeight: item.blockHeight, options: item.options})
		owners = append(owners, i)
	}

	if len(creators) > 0 {
		plan := s.planCreates(creators)

		// A create-only batch has no spend phase and so no transaction yet, and it needs one:
		// the claim's advisory lock is transaction-scoped, so taken on the pool it would be
		// released at the end of its own statement and would guard nothing. This used to run
		// on the pool to save the BEGIN and COMMIT round trips, which is a real cost on the
		// block-application path and a smaller one than two creates of the same transaction
		// each writing a full set of coins.
		if dbTx == nil {
			dbTx, err = s.pool.Begin(ctx)
			if err != nil {
				return results, rejected, errors.NewStorageError("[utxoset][SpendAndCreate] begin creates", err)
			}
		}

		if err = s.lockTxids(ctx, dbTx, plan.txids); err != nil {
			rollback()
			return results, rejected, err
		}

		if err = s.runCreatePlan(ctx, dbTx, plan); err != nil {
			rollback()
			return results, rejected, err
		}

		// ErrTxExists lands here, per item, with that item's spends still in the transaction
		// about to commit. That is the contract: the interface says the error is returned with
		// the spends left in place, and spend_and_create.go explains why rolling them back
		// would make a double spend mineable.
		for k, i := range owners {
			results[i].data = plan.perItem[k]
			results[i].err = plan.errs[k]
		}
	}

	if dbTx != nil {
		if err = dbTx.Commit(ctx); err != nil {
			rollback()
			return results, rejected, errors.NewStorageError("[utxoset][SpendAndCreate] commit batch", err)
		}
	}

	// Durable. Everything is final now except the items still waiting on a sibling's fate.
	final := make([]bool, len(batch))
	for i := range batch {
		final[i] = !deferred[i]
	}

	return results, final, nil
}

// lostToASibling reports whether any failure on an item is a coin taken by another item of the
// same batch.
//
// Such an item's verdict was reached against its sibling's UNCOMMITTED delete, in the same
// statement, and the sibling may yet not commit: it may itself fail on another input this round,
// or lose to an outside competitor in the next. The verdict could then be wrong in both
// directions, naming a spender that never spent, or rejecting a transaction that should have
// won. The single path never has this problem, because two competing spends serialise on the
// row lock and the loser reads committed state. So the item is judged again by the single path
// once the batch has committed, with its claims restored, which reproduces exactly that.
//
// Any sibling loss is enough, whatever else the item failed on. An item that lost a coin to a
// sibling AND made a false claim about another coin is still rejected on the re-run, for the
// false claim, and reports only the failures that are true against committed state. Settling it
// in-round would hand the caller a spend record naming a sibling that may never have spent.
//
// A missing coin the journal cannot attribute to anyone, or attributes to a transaction outside
// this batch, is a verdict about committed state and stands. So is every other error class.
func lostToASibling(tx *bt.Tx, spends []*utxo.Spend, siblings map[chainhash.Hash]struct{}) bool {
	own := tx.TxIDChainHash()

	for _, sp := range spends {
		if sp == nil || sp.Err == nil || sp.ConflictingTxID == nil {
			continue
		}

		if !errors.Is(sp.Err, errors.ErrSpent) || *sp.ConflictingTxID == *own {
			continue
		}

		if _, ok := siblings[*sp.ConflictingTxID]; ok {
			return true
		}
	}

	return false
}
