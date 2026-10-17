package utxoset

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

// SpendAndCreateMulti writes the NET effect of a list of transactions.
//
// The list is ordered parents first, and a block's transactions mostly spend each other: on
// mainnet above the checkpoint nearly every input of a chained block names a parent in the same
// block. Written one transaction at a time, every such output is inserted into the UTXO table,
// then deleted from it by its child moments later, and the delete copies it into the spend
// journal. The insert and the delete each touch the UTXO table's index, and that index is the
// working set that does not fit in memory. This method skips both: an output created and spent
// inside the list never becomes a UTXO row. Its child's spend of it is written straight to the
// spend journal, which is the one row a spend leaves behind anyway.
//
// It runs in two steps, and no database transaction spans the list.
//
//  1. Spend every input whose parent is NOT in the list, in chunks, each its own database
//     transaction. A transaction whose spend fails is rolled out of its chunk before the chunk
//     commits, so a failed transaction never leaves a spend behind. Its descendants in the list
//     are then failed too, and any of their spends a parallel chunk already committed are
//     restored through Unspend. That is the rare path: it needs a double spend in a block.
//  2. Create the survivors in dependency-level order. A parent's chunk writes its identity, its
//     body, the UTXOs nobody in the list spends, and one spend-journal row for each output a
//     child in the list spends. The child's chunk writes only the child. Consecutive narrow
//     levels share one chunk transaction, so a deep chain does not pay a commit per level; a
//     level too wide for one chunk is split and its chunks run in parallel. See createLevels.
//
// That order is what makes a crash at any point safe to repeat. Every transaction that exists
// has made all of its spends: its outside spends committed in step 1, before any create, and
// its spends of list parents committed with those parents, which are at an earlier level and so
// in an earlier commit or the same one. On the repeat the caller's pre-check drops every
// transaction that exists, and every remaining input is either an outside spend repeated by the
// same spender, or a spend-journal row naming the same spender, and the spend path accepts both
// as the same spend.
//
// It does NOT net, and hands the list to the per-transaction default instead, for a list whose
// options the netted write does not model: create-only or spend-only, and frozen, conflicting or
// locked creates.
//
// Below the checkpoint, where quick validation applies blocks with their block carried from
// birth and outpoint-only spends, it nets too. A create there takes the block-path claim, which
// writes a containment row for every transaction whether or not any of its outputs survive as
// UTXOs, so a transaction whose outputs were all netted is still found on a repeat and is not
// written twice. An outpoint-only child presents no claim to check against its parent's output,
// so its netted inputs are excused the check exactly as its outside spends are.
//
// A transaction in the list that turns out to exist already, which the caller's pre-check makes
// a race with another writer, is reported MultiTxExisted when nothing in the list spends it.
// When something does, its outputs are real UTXOs and its children must spend them for real, so
// the write stops at that level and the rest of the list goes to the per-transaction default,
// which spends them; every spend already made repeats there as the same spend.
func (s *Store) SpendAndCreateMulti(ctx context.Context, txs []*bt.Tx, blockHeight uint32,
	opts ...utxo.CreateOption) ([]utxo.SpendAndCreateMultiResult, error) {
	list, err := utxo.PrepareSpendAndCreateMulti(txs, opts...)
	if err != nil {
		return nil, err
	}

	if len(txs) == 0 {
		return []utxo.SpendAndCreateMultiResult{}, nil
	}

	if !s.canNet(list.Options) {
		return utxo.DefaultSpendAndCreateMulti(ctx, s, utxo.SpendAndCreateMultiConcurrency(s.settings), txs, blockHeight, opts...)
	}

	w := s.newNettedWrite(txs, list, blockHeight, opts)

	return w.run(ctx)
}

// canNet reports whether the netted write models every option of the list.
func (s *Store) canNet(o *utxo.CreateOptions) bool {
	return !o.SpendOnly && !o.CreateOnly && !o.Frozen && !o.Conflicting && !o.Locked
}

// multiSpendChunkInputs bounds the inputs one step-1 transaction spends. A variable so a test
// can force several chunks.
var multiSpendChunkInputs = 8192

// multiCreateChunkTxs bounds the transactions one step-2 transaction creates. The create takes
// one advisory lock per transaction (lockTxids), held to commit, and Postgres keeps those in a
// shared table sized max_locks_per_transaction x max_connections, 6,400 slots at the defaults
// mainnet runs. A chunk cut by bytes alone put 30,000 locks in one transaction and failed with
// "out of shared memory". At this bound and multiConcurrency chunks in flight the list holds at
// most about 2,000. A variable so a test can force one transaction per chunk.
var multiCreateChunkTxs = 256

// multiFault is a test hook called after step 1, as "spent", and after each commit unit of step
// 2, as "created group N" with N counting from 0: a group of merged levels, or one level split
// into parallel chunks. An error from it stops the write there. errMultiCrash stops it as a crash
// would, with no cleanup; any other error is an ordinary failure, and the write gives back the
// spends of the transactions it did not write. nil in production.
var multiFault func(stage string) error

// multiCrash is the error a multiFault hook returns to stop the write as a crash would: the
// process is gone, so nothing gives the step-1 spends back. It is its own type, matched by
// identity, for the reason nettedParentExists gives.
type multiCrash struct{}

func (multiCrash) Error() string {
	return "[utxoset][SpendAndCreateMulti] simulated crash"
}

var errMultiCrash error = multiCrash{}

// multiGiveBackTimeout bounds the Unspend that gives back the spends of a write that stopped.
// That Unspend runs on a context detached from the caller's, because the commonest stop is the
// caller's context being cancelled, and an Unspend on it would fail at once.
const multiGiveBackTimeout = time.Minute

// multiCreateCommitted is a test hook called with the transaction count of each step-2 chunk
// transaction that commits. nil in production.
var multiCreateCommitted func(txs int)

// multiCreateBeforeCommit is a test hook called just before each step-2 chunk transaction
// commits, with the context the commit will run under and the chunk's txids. nil in production.
var multiCreateBeforeCommit func(ctx context.Context, txids []chainhash.Hash)

// multiCreateAfterCommit is a test hook called after each step-2 chunk transaction commits and
// before its results are set. An error from it is returned as the commit's, which is the shape
// of a commit that the server made and the client did not hear about. nil in production.
var multiCreateAfterCommit func() error

// multiConcurrency is how many chunk transactions run at once. Each holds one pool connection
// and takes no second one, so at most half the pool, leaving the rest of the node its share,
// and at most 8, which with multiCreateChunkTxs bounds the advisory locks held at once.
func (s *Store) multiConcurrency() int {
	return min(max(int(s.pool.Config().MaxConns)/2, 1), 8)
}

// multiTx is one transaction of the list as the netted write tracks it.
type multiTx struct {
	pos     int
	tx      *bt.Tx
	txid    chainhash.Hash
	parents []int
	// netted[vin] is true when input vin's parent is in the list. nil when none is.
	netted []bool
	// nettedOuts is the number of this transaction's outputs a child in the list spends.
	nettedOuts int
	claims     []inputClaim
	spends     []*utxo.Spend
	// replayed[vin] is true when step 1 found input vin's coin already spent by this
	// transaction: another writer of it took the coin, or an earlier attempt that crashed. This
	// write took nothing there, so the spend is not its to give back, and it stands only while
	// that other writer keeps it. nil when no input was a repeat.
	replayed []bool
	// spent is true once its step-1 chunk committed.
	spent bool
	// journaled is true once its step-2 chunk committed, and with it a spend-journal row for
	// every output of it a child in the list spends, naming that child. Unlike result, which
	// the per-transaction default overwrites, it says the netted write itself made those rows.
	journaled bool
	result    utxo.SpendAndCreateMultiResult
}

func (it *multiTx) dead() bool {
	return it.result.Status == utxo.MultiTxFailed || it.result.Status == utxo.MultiTxParentFailed
}

func (it *multiTx) restoreClaims() {
	for i, in := range it.tx.Inputs {
		if in != nil && i < len(it.claims) {
			in.PreviousTxSatoshis = it.claims[i].satoshis
			in.PreviousTxScript = it.claims[i].script
		}
	}
}

// outsideSpends is the records of the inputs step 1 spent.
func (it *multiTx) outsideSpends() []*utxo.Spend {
	out := make([]*utxo.Spend, 0, len(it.spends))

	for vin, sp := range it.spends {
		if sp != nil && (it.netted == nil || !it.netted[vin]) {
			out = append(out, sp)
		}
	}

	return out
}

// takenOutsideSpends is the outside spends this write took itself: settled without an error and
// not repeats of a spend someone else made. Only these are the write's to give back. A repeat is
// the spend of another writer of the same transaction, which may yet write it.
func (it *multiTx) takenOutsideSpends() []*utxo.Spend {
	out := make([]*utxo.Spend, 0, len(it.spends))

	for vin, sp := range it.spends {
		if sp == nil || sp.Err != nil || (it.netted != nil && it.netted[vin]) || (it.replayed != nil && it.replayed[vin]) {
			continue
		}

		out = append(out, sp)
	}

	return out
}

// nettedSpendsJournaled is the spends of this transaction's netted inputs whose parent a
// committed step-2 group of this write created, with a journal row naming this transaction for
// each. This write made those rows, so they are its to give back.
func (w *nettedWrite) nettedSpendsJournaled(it *multiTx) []*utxo.Spend {
	var out []*utxo.Spend

	for vin, in := range it.tx.Inputs {
		if it.netted != nil && it.netted[vin] && it.spends[vin] != nil &&
			w.items[w.position[*in.PreviousTxIDChainHash()]].journaled {
			out = append(out, it.spends[vin])
		}
	}

	return out
}

// replayedOutsideSpends is the outside spends step 1 settled as repeats of another writer's.
func (it *multiTx) replayedOutsideSpends() []*utxo.Spend {
	var out []*utxo.Spend

	for vin, sp := range it.spends {
		if sp != nil && sp.Err == nil && it.replayed != nil && it.replayed[vin] && (it.netted == nil || !it.netted[vin]) {
			out = append(out, sp)
		}
	}

	return out
}

type nettedWrite struct {
	s           *Store
	txs         []*bt.Tx
	list        *utxo.SpendAndCreateMultiList
	blockHeight uint32
	opts        []utxo.CreateOption
	items       []*multiTx
	// position is each transaction's index in items, by txid.
	position map[chainhash.Hash]int

	// What the write did and where its time went, for the slow-list log line. The chunk
	// timings are summed across chunks that run in parallel, so they can exceed wall time.
	spendChunks  int
	levels       int
	createChunks atomic.Int64
	lockNs       atomic.Int64
	createNs     atomic.Int64
	journalNs    atomic.Int64
	commitNs     atomic.Int64
	bodyBytes    atomic.Int64
}

// multiSlowListLog is the step-1-plus-step-2 time above which a list gets an info log line
// saying where the time went.
const multiSlowListLog = time.Second

func (s *Store) newNettedWrite(txs []*bt.Tx, list *utxo.SpendAndCreateMultiList, blockHeight uint32,
	opts []utxo.CreateOption) *nettedWrite {
	position := make(map[chainhash.Hash]int, len(txs))
	for i := range txs {
		position[list.TxIDs[i]] = i
	}

	w := &nettedWrite{s: s, txs: txs, list: list, blockHeight: blockHeight, opts: opts, items: make([]*multiTx, len(txs)),
		position: position}

	for i, tx := range txs {
		it := &multiTx{pos: i, tx: tx, txid: list.TxIDs[i], parents: list.ParentsInList[i], claims: captureClaims(tx),
			spends: make([]*utxo.Spend, len(tx.Inputs))}

		if len(it.parents) > 0 {
			it.netted = make([]bool, len(tx.Inputs))

			for vin, in := range tx.Inputs {
				if _, ok := position[*in.PreviousTxIDChainHash()]; ok {
					it.netted[vin] = true
				}
			}
		}

		w.items[i] = it
	}

	return w
}

func (w *nettedWrite) run(ctx context.Context) (_ []utxo.SpendAndCreateMultiResult, err error) {
	start := time.Now()

	defer func() {
		if err != nil {
			err = w.giveBackUnwritten(ctx, err)
		}
	}()

	var spent time.Time

	defer func() {
		if spent.IsZero() {
			spent = time.Now()
		}

		w.logIfSlow(start, spent, time.Now())
	}()

	w.checkNettedInputs()
	w.failDescendants()

	if err := w.spendOutside(ctx); err != nil {
		return nil, err
	}

	spent = time.Now()

	if err := w.undoDescendantsOfFailures(ctx); err != nil {
		return nil, err
	}

	if multiFault != nil {
		if err := multiFault("spent"); err != nil {
			return nil, err
		}
	}

	if err := w.createLevels(ctx); err != nil {
		if err != errNettedParentExists { //nolint:errorlint // matched by identity; see nettedParentExists
			return nil, err
		}

		if err := w.finishPerTransaction(ctx); err != nil {
			return nil, err
		}
	}

	return w.results(), nil
}

// giveBackUnwritten restores the spends of every transaction the write did not write, when the
// write stops with an error: a step-1 chunk or a step-2 group that failed, a cancelled context,
// or the per-transaction default giving up. Step 1 committed its spends before step 2 began, so
// without this the inputs of a transaction with no record stay spent by it. A retry of the same
// block repeats them as the same spends and completes the write, but a retry does not always
// come: the block can lose to a fork, or nobody asks for it again. A valid spend of those coins
// then gets ErrSpent naming a spender that does not exist, and SpendsMadeBy has no record to give
// them back from. The sql and aerospike stores do the same through SequentialSpendAndCreate,
// which unspends when the create fails.
//
// A transaction that was created, or found to exist, keeps its spends: they are its own. Each
// other transaction gives back the outside spends this write took, and its spends of outputs of
// a parent a committed step-2 group journaled for it, as undoSpendsOfFailures does. A repeat of a
// spend another writer of the same transaction made is not given back: it is that writer's, and
// it may yet write the transaction. Nor is any spend whose transaction has a record by the time
// the give-back runs, which another writer can have written, or a step-2 commit this write saw
// fail can have made on the server: giveBackUnlessWritten asks the store under the transactions'
// locks. The restore matches only a UTXO whose journal row names the transaction as the spender,
// so an outside spend that did not commit, or one restored already, is a no-op.
//
// A crash between the two steps is not covered: nothing runs, and the spends stay until a retry
// of the block. sql and aerospike have the same gap.
func (w *nettedWrite) giveBackUnwritten(ctx context.Context, cause error) error {
	if _, crashed := cause.(multiCrash); crashed { //nolint:errorlint // matched by identity; see multiCrash
		return cause
	}

	var undo []*utxo.Spend

	for _, it := range w.items {
		if st := it.result.Status; st == utxo.MultiTxCreated || st == utxo.MultiTxExisted {
			continue
		}

		undo = append(undo, it.takenOutsideSpends()...)
		undo = append(undo, w.nettedSpendsJournaled(it)...)
	}

	if len(undo) == 0 {
		return cause
	}

	giveBackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), multiGiveBackTimeout)
	defer cancel()

	if err := w.s.giveBackUnlessWritten(giveBackCtx, undo); err != nil {
		w.s.logger.Errorf("[utxoset][SpendAndCreateMulti] the write at height %d stopped (%v) and %d of its spends could not be given back: %v",
			w.blockHeight, cause, len(undo), err)

		return errors.NewStorageError("[utxoset][SpendAndCreateMulti] the write stopped (%v) and its spends could not be given back", cause, err)
	}

	return cause
}

func (w *nettedWrite) results() []utxo.SpendAndCreateMultiResult {
	out := make([]utxo.SpendAndCreateMultiResult, len(w.items))
	for i, it := range w.items {
		out[i] = it.result
	}

	return out
}

// checkNettedInputs does in memory what the spend statement does for an input whose parent is
// in the list: the output must be one the create would store as a UTXO, and the input's claimed
// value and script must match it. A parent created by this list carries no flags and no
// maturity, because canNet admits no flagged create and a coinbase is never in a list, so there
// is nothing else the statement would refuse. The input is then decorated, as the statement's
// RETURNING decorates a spent input.
func (w *nettedWrite) checkNettedInputs() {
	genesis := w.s.settings.ChainCfgParams.GenesisActivationHeight

	for _, it := range w.items {
		if it.netted == nil {
			continue
		}

		failures := 0

		var first error

		for vin, in := range it.tx.Inputs {
			if !it.netted[vin] {
				continue
			}

			parent := w.items[w.position[*in.PreviousTxIDChainHash()]]
			rec := &utxo.Spend{TxID: &parent.txid, Vout: in.PreviousTxOutIndex,
				SpendingData: &spendpkg.SpendingData{TxID: &it.txid, Vin: vin}}
			it.spends[vin] = rec

			out := parent.tx.Outputs[in.PreviousTxOutIndex]
			if out == nil || out.LockingScript == nil || !utxo.ShouldStoreOutputAsUTXO(out, w.blockHeight, genesis) {
				rec.Err = errors.NewTxNotFoundError("[utxoset][SpendAndCreateMulti] output %d of %s is not a spendable output", in.PreviousTxOutIndex, parent.txid.String())
			} else if err := claimMismatch(in, rec, int64(out.Satoshis), *out.LockingScript, nil, w.list.Options.IgnoreFlags.SkipUTXOHashCheck); err != nil { //nolint:gosec // satoshis fit int64
				rec.Err = err
			} else {
				decorateInput(in, int64(out.Satoshis), *out.LockingScript, nil) //nolint:gosec // satoshis fit int64
			}

			if rec.Err != nil {
				failures++

				if first == nil {
					first = rec.Err
				}
			}
		}

		if failures > 0 {
			it.result = utxo.SpendAndCreateMultiResult{Status: utxo.MultiTxFailed, Spends: it.spends,
				Err: errors.NewUtxoError("[utxoset][SpendAndCreate] %d of %d inputs could not be spent", failures, len(it.spends), first)}
		}
	}
}

// failDescendants marks every transaction with a dead parent as ParentFailed, in list order, so
// a whole subtree goes with its root. It returns the ones it newly marked.
func (w *nettedWrite) failDescendants() []*multiTx {
	var marked []*multiTx

	for _, it := range w.items {
		if it.dead() {
			continue
		}

		for _, p := range it.parents {
			if w.items[p].dead() {
				it.result = utxo.SpendAndCreateMultiResult{Status: utxo.MultiTxParentFailed, Spends: it.spends,
					Err: errors.NewProcessingError("SpendAndCreateMulti: a parent of %s earlier in the list failed", it.txid.String())}
				marked = append(marked, it)

				break
			}
		}
	}

	return marked
}

// spendOutside is step 1. Chunks touch disjoint UTXOs, because no outpoint is spent twice in a
// list, so they run in parallel without waiting on each other's row locks.
func (w *nettedWrite) spendOutside(ctx context.Context) error {
	var (
		chunks [][]*multiTx
		cur    []*multiTx
		inputs int
	)

	for _, it := range w.items {
		if it.dead() {
			continue
		}

		outside := len(it.tx.Inputs)

		for _, n := range it.netted {
			if n {
				outside--
			}
		}

		if outside == 0 {
			continue
		}

		if len(cur) > 0 && inputs+outside > multiSpendChunkInputs {
			chunks = append(chunks, cur)
			cur, inputs = nil, 0
		}

		cur = append(cur, it)
		inputs += outside
	}

	if len(cur) > 0 {
		chunks = append(chunks, cur)
	}

	w.spendChunks = len(chunks)

	if len(chunks) == 0 {
		return nil
	}

	if err := w.s.ensureSpendJournalPartition(ctx, w.blockHeight); err != nil {
		return err
	}

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(w.s.multiConcurrency())

	for _, chunk := range chunks {
		g.Go(func() error { return w.spendChunk(gCtx, chunk) })
	}

	return g.Wait()
}

// spendChunk spends one chunk's outside inputs in one database transaction. A transaction with
// any failed input is taken out and the rest run again in a fresh database transaction, for the
// reason runSpendAndCreateBatch gives: only a whole rollback lets a competing spend that waited
// on our delete see the row again.
func (w *nettedWrite) spendChunk(ctx context.Context, chunk []*multiTx) error {
	flags := w.list.Options.IgnoreFlags
	active := chunk

	for round := 0; len(active) > 0; round++ {
		if round > 0 {
			for _, it := range active {
				it.restoreClaims()
			}
		}

		dbTx, err := w.s.pool.Begin(ctx)
		if err != nil {
			return errors.NewStorageError("[utxoset][SpendAndCreateMulti] begin spend chunk", err)
		}

		items := make([]*spendItem, len(active))
		for k, it := range active {
			items[k] = &spendItem{tx: it.tx, blockHeight: w.blockHeight, ignoreFlags: flags, skip: it.netted}
		}

		plan := planSpends(items)

		if err = w.s.runSpendPlan(ctx, dbTx, plan); err != nil {
			_ = dbTx.Rollback(ctx)
			return err
		}

		survivors := make([]*multiTx, 0, len(active))

		for k, it := range active {
			recs := plan.perItem[k]

			var (
				failures int
				first    error
			)

			for vin, sp := range recs {
				if it.netted != nil && it.netted[vin] {
					continue
				}

				it.spends[vin] = sp

				if sp != nil && sp.Err != nil {
					failures++

					if first == nil {
						first = sp.Err
					}
				}
			}

			if failures == 0 {
				survivors = append(survivors, it)
				continue
			}

			it.result = utxo.SpendAndCreateMultiResult{Status: utxo.MultiTxFailed, Spends: it.spends,
				Err: errors.NewUtxoError("[utxoset][SpendAndCreate] %d of %d inputs could not be spent", failures, len(recs), first)}
		}

		if len(survivors) == len(active) {
			for k, replayed := range plan.replayed {
				if !replayed {
					continue
				}

				it := active[plan.owner[k]]
				if it.replayed == nil {
					it.replayed = make([]bool, len(it.tx.Inputs))
				}

				it.replayed[plan.ownerVin[k]] = true
			}

			if err = dbTx.Commit(ctx); err != nil {
				_ = dbTx.Rollback(ctx)
				return errors.NewStorageError("[utxoset][SpendAndCreateMulti] commit spend chunk", err)
			}

			for _, it := range active {
				it.spent = true
			}

			return nil
		}

		_ = dbTx.Rollback(ctx)
		active = survivors
	}

	return nil
}

// undoDescendantsOfFailures fails the descendants of step-1 failures and restores whatever
// outside spends of theirs a parallel chunk already committed. A failed transaction itself left
// nothing behind: its chunk rolled back before committing without it.
func (w *nettedWrite) undoDescendantsOfFailures(ctx context.Context) error {
	var undo []*utxo.Spend

	for _, it := range w.failDescendants() {
		if it.spent {
			undo = append(undo, it.takenOutsideSpends()...)
		}
	}

	if len(undo) == 0 {
		return nil
	}

	if err := w.s.giveBackUnlessWritten(ctx, undo); err != nil {
		return errors.NewStorageError("[utxoset][SpendAndCreateMulti] restoring the spends of descendants of a failed transaction", err)
	}

	return nil
}

// nettedParentExists stops step 2 when a transaction whose outputs the list nets turns out to
// exist already. It is its own type, matched by identity, because the errors package matches
// its own errors by code: a sentinel built from it would also match every unrelated processing
// error, and a real failure would be mistaken for this one.
type nettedParentExists struct{}

func (nettedParentExists) Error() string {
	return "[utxoset][SpendAndCreateMulti] a parent in the list already exists"
}

var errNettedParentExists error = nettedParentExists{}

// nettedKey names one output by its parent's txid and its packed key.
type nettedKey [48]byte

// nettedSpender is the transaction in the list that spends a netted output, and which of its
// inputs does it. The input index goes into the journal row, so GetSpend names it.
type nettedSpender struct {
	txid []byte
	vin  int32
}

func newNettedKey(txid []byte, ukey [16]byte) nettedKey {
	var k nettedKey

	copy(k[:32], txid)
	copy(k[32:], ukey[:])

	return k
}

// createLevels is step 2.
//
// Levels are written in order, and the cost it is shaped around is the commit. A deep chain is
// one transaction per level, and a commit per level is ~3 ms; mainnet block 955958 has 1,055
// transactions in 592 levels and spent 4.4 s here when every level committed alone, against
// ~0.6 s for a flat block of the same size. So consecutive levels are gathered into one pending
// group while it stays within multiCreateChunkTxs transactions and spendAndCreateBatchByteBudget
// bytes, the same bounds one chunk has, and the group is written as one chunk transaction. A
// level that would push the group past either bound flushes the group first and starts the
// next. A level that alone exceeds a bound is written as before: split into chunks that run
// multiConcurrency at a time, all of them committed before the next level starts.
//
// Merging keeps the crash argument intact. A merged group commits a parent's netted journal rows
// and the identity of the child that spends them in the same database transaction, and groups
// commit in level order, so every transaction that exists still has all of its spends. A parent
// in a group that turns out to exist already (errNettedParentExists) rolls the whole group back:
// none of its members gets a result, so finishPerTransaction, which takes every live transaction
// still MultiTxNotAttempted, hands the group and everything after it to the per-transaction
// default. Results are only set after a commit, so a rolled-back group leaves none behind.
func (w *nettedWrite) createLevels(ctx context.Context) error {
	spender := make(map[nettedKey]nettedSpender)
	level := make([]int, len(w.items))
	maxLevel := 0

	for i, it := range w.items {
		if it.dead() {
			continue
		}

		for vin, in := range it.tx.Inputs {
			if it.netted == nil || !it.netted[vin] {
				continue
			}

			parent := in.PreviousTxIDChainHash()
			spender[newNettedKey(parent[:], Pack(parent[:], in.PreviousTxOutIndex))] =
				nettedSpender{txid: it.txid[:], vin: int32(vin)} //nolint:gosec // an input index fits int32
		}

		for _, p := range it.parents {
			level[i] = max(level[i], level[p]+1)
			w.items[p].nettedOuts++
		}

		maxLevel = max(maxLevel, level[i])
	}

	w.levels = maxLevel + 1
	byLevel := make([][]*multiTx, maxLevel+1)
	sizes := make(map[*multiTx]int, len(w.items))

	for i, it := range w.items {
		if !it.dead() {
			byLevel[level[i]] = append(byLevel[level[i]], it)
			sizes[it] = it.tx.Size()
		}
	}

	if err := w.s.ensureTxBodyPartition(ctx, w.blockHeight); err != nil {
		return err
	}

	if mi, mined := minedBlock(w.list.Options.MinedBlockInfos); mined {
		if err := w.s.ensureTxMinedPartition(ctx, mi.BlockHeight); err != nil {
			return err
		}
	}

	if len(spender) > 0 {
		if err := w.s.ensureSpendJournalPartition(ctx, w.blockHeight); err != nil {
			return err
		}
	}

	var (
		pending      []*multiTx
		pendingBytes int
		group        int
	)

	// committed runs the fault hook after one commit unit and counts it.
	committed := func() error {
		stage := "created group " + strconv.Itoa(group)
		group++

		if multiFault != nil {
			return multiFault(stage)
		}

		return nil
	}

	flush := func() error {
		if len(pending) == 0 {
			return nil
		}

		if err := w.createChunk(ctx, pending, spender); err != nil {
			return err
		}

		pending, pendingBytes = nil, 0

		return committed()
	}

	for _, members := range byLevel {
		if len(members) == 0 {
			continue
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		levelBytes := 0
		for _, it := range members {
			levelBytes += sizes[it]
		}

		if len(members) > multiCreateChunkTxs || levelBytes > spendAndCreateBatchByteBudget {
			if err := flush(); err != nil {
				return err
			}

			if err := w.createSplitLevel(ctx, members, sizes, spender); err != nil {
				return err
			}

			if err := committed(); err != nil {
				return err
			}

			continue
		}

		if len(pending)+len(members) > multiCreateChunkTxs || pendingBytes+levelBytes > spendAndCreateBatchByteBudget {
			if err := flush(); err != nil {
				return err
			}
		}

		pending = append(pending, members...)
		pendingBytes += levelBytes
	}

	return flush()
}

// createSplitLevel writes one level too wide for a single chunk: cut into chunks by the chunk
// bounds, multiConcurrency in flight, and every one committed before it returns.
//
// The chunks share the caller's context, not one derived from the group, so a chunk that fails
// does not cancel its siblings. They are independent, each safe to commit on its own by the crash
// argument above, and the commonest failure, errNettedParentExists, is not a fault at all. A
// cancelled sibling caught mid-commit could be committed on the server while its client saw only
// the cancellation; it then never got a result, and the per-transaction hand-off found the record
// this very call had written and reported it MultiTxExisted. Wait still returns the first error.
func (w *nettedWrite) createSplitLevel(ctx context.Context, members []*multiTx, sizes map[*multiTx]int,
	spender map[nettedKey]nettedSpender) error {
	var g errgroup.Group

	g.SetLimit(w.s.multiConcurrency())

	start, size := 0, 0

	for i, it := range members {
		txSize := sizes[it]

		if i > start && (size+txSize > spendAndCreateBatchByteBudget || i-start >= multiCreateChunkTxs) {
			chunk := members[start:i]
			g.Go(func() error { return w.createChunk(ctx, chunk, spender) })
			start, size = i, 0
		}

		size += txSize
	}

	if start < len(members) {
		chunk := members[start:]
		g.Go(func() error { return w.createChunk(ctx, chunk, spender) })
	}

	return g.Wait()
}

// nettedRows is a plan's spend-journal rows for the outputs the list spends.
type nettedRows struct {
	sats      []int64
	created   []int32
	spendable []int32
	flags     []int16
	mined     []int32
	blockIDs  []int32
	ukeys     [][16]byte
	txids     [][]byte
	spenders  [][]byte
	vins      []int32
	scripts   [][]byte
}

// takeNetted removes from the plan every UTXO a child in the list spends and returns the
// spend-journal row each becomes: the UTXO as the create would have written it, spent at the
// list's height by that child.
func (p *createPlan) takeNetted(spender map[nettedKey]nettedSpender) *nettedRows {
	j := &nettedRows{}
	keep := 0

	for c := range p.utxoTxids {
		by, ok := spender[newNettedKey(p.utxoTxids[c], p.utxoUkeys[c])]
		if !ok {
			p.utxoSats[keep] = p.utxoSats[c]
			p.utxoHeights[keep] = p.utxoHeights[c]
			p.utxoSpendable[keep] = p.utxoSpendable[c]
			p.utxoLeaves[keep] = p.utxoLeaves[c]
			p.utxoFlags[keep] = p.utxoFlags[c]
			p.utxoUkeys[keep] = p.utxoUkeys[c]
			p.utxoTxids[keep] = p.utxoTxids[c]
			p.utxoScripts[keep] = p.utxoScripts[c]
			p.utxoMined[keep] = p.utxoMined[c]
			p.utxoBlockIDs[keep] = p.utxoBlockIDs[c]
			keep++

			continue
		}

		j.sats = append(j.sats, p.utxoSats[c])
		j.created = append(j.created, p.utxoHeights[c])
		j.spendable = append(j.spendable, p.utxoSpendable[c])
		j.flags = append(j.flags, p.utxoFlags[c])
		j.mined = append(j.mined, p.utxoMined[c])
		j.blockIDs = append(j.blockIDs, p.utxoBlockIDs[c])
		j.ukeys = append(j.ukeys, p.utxoUkeys[c])
		j.txids = append(j.txids, p.utxoTxids[c])
		j.spenders = append(j.spenders, by.txid)
		j.vins = append(j.vins, by.vin)
		j.scripts = append(j.scripts, p.utxoScripts[c])
	}

	p.utxoSats = p.utxoSats[:keep]
	p.utxoHeights = p.utxoHeights[:keep]
	p.utxoSpendable = p.utxoSpendable[:keep]
	p.utxoLeaves = p.utxoLeaves[:keep]
	p.utxoFlags = p.utxoFlags[:keep]
	p.utxoUkeys = p.utxoUkeys[:keep]
	p.utxoTxids = p.utxoTxids[:keep]
	p.utxoScripts = p.utxoScripts[:keep]
	p.utxoMined = p.utxoMined[:keep]
	p.utxoBlockIDs = p.utxoBlockIDs[:keep]

	return j
}

// nettedJournalSQL writes the spend-journal rows of outputs created and spent inside one list.
// The columns are the ones spendJournalSQL copies off a deleted UTXO, with no hash_override: a
// UTXO this list creates has never been reassigned.
const nettedJournalSQL = `
INSERT INTO spend_journal (spent_height, spending_vin, satoshis, created_height, spendable_from, flags,
                           mined_height, block_id, ukey, txid, spending_txid, script)
SELECT $1::int, j.spending_vin, j.satoshis, j.created_height, j.spendable_from, j.flags,
       j.mined_height, j.block_id, j.ukey, j.txid, j.spending_txid, j.script
  FROM unnest($2::bigint[], $3::int[], $4::int[], $5::smallint[], $6::int[], $7::int[],
              $8::uuid[], $9::bytea[], $10::bytea[], $11::bytea[], $12::int[])
    AS j(satoshis, created_height, spendable_from, flags, mined_height, block_id,
         ukey, txid, spending_txid, script, spending_vin)`

// createChunk creates one chunk in one database transaction: the claims, the bodies and
// surviving UTXOs, and the spend-journal rows of the netted ones. The chunk is part of one level
// or several whole consecutive levels, parents before children. Results are set only after the
// commit.
func (w *nettedWrite) createChunk(ctx context.Context, chunk []*multiTx, spender map[nettedKey]nettedSpender) error {
	items := make([]*createItem, len(chunk))
	for k, it := range chunk {
		items[k] = &createItem{tx: it.tx, blockHeight: w.blockHeight, options: w.list.Options.ItemOptions(it.pos)}
	}

	plan := w.s.planCreates(items)

	for k := range chunk {
		if plan.errs[k] != nil {
			return plan.errs[k]
		}
	}

	journal := plan.takeNetted(spender)

	dbTx, err := w.s.pool.Begin(ctx)
	if err != nil {
		return errors.NewStorageError("[utxoset][SpendAndCreateMulti] begin create chunk", err)
	}

	committed := false

	defer func() {
		if !committed {
			_ = dbTx.Rollback(ctx)
		}
	}()

	t0 := time.Now()

	for _, b := range plan.bodies {
		w.bodyBytes.Add(int64(len(b)))
	}

	if err = w.s.lockTxids(ctx, dbTx, plan.txids); err != nil {
		return err
	}

	if err = w.requireReplayedSpendsStand(ctx, dbTx, chunk); err != nil {
		return err
	}

	t1 := time.Now()
	w.lockNs.Add(int64(t1.Sub(t0)))

	if err = w.s.runCreatePlan(ctx, dbTx, plan); err != nil {
		return err
	}

	existed := make([]bool, len(chunk))

	for k, it := range chunk {
		switch err := plan.errs[k]; {
		case err == nil:
		case errors.Is(err, errors.ErrTxExists):
			if it.nettedOuts > 0 {
				return errNettedParentExists
			}

			existed[k] = true
		default:
			return err
		}
	}

	t2 := time.Now()
	w.createNs.Add(int64(t2.Sub(t1)))

	if err = w.writeNettedJournal(ctx, dbTx, journal); err != nil {
		return err
	}

	t3 := time.Now()
	w.journalNs.Add(int64(t3.Sub(t2)))

	if multiCreateBeforeCommit != nil {
		txids := make([]chainhash.Hash, len(chunk))
		for k, it := range chunk {
			txids[k] = it.txid
		}

		multiCreateBeforeCommit(ctx, txids)
	}

	// The commit runs on a context the caller cannot cancel. A cancel that lands while the commit
	// is in flight can leave it committed on the server and failed on the client, and the
	// give-back then has to find that out from the store. Detached, the commit gives a true
	// answer unless the connection itself fails.
	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), multiGiveBackTimeout)
	err = dbTx.Commit(commitCtx)

	cancel()

	if err != nil {
		return errors.NewStorageError("[utxoset][SpendAndCreateMulti] commit create chunk", err)
	}

	w.commitNs.Add(int64(time.Since(t3)))
	w.createChunks.Add(1)

	committed = true

	if multiCreateAfterCommit != nil {
		if err = multiCreateAfterCommit(); err != nil {
			return err
		}
	}

	if multiCreateCommitted != nil {
		multiCreateCommitted(len(chunk))
	}

	for k, it := range chunk {
		if existed[k] {
			it.result = utxo.SpendAndCreateMultiResult{Status: utxo.MultiTxExisted, Spends: it.spends}
			continue
		}

		it.journaled = true
		it.result = utxo.SpendAndCreateMultiResult{Status: utxo.MultiTxCreated, Meta: plan.perItem[k], Spends: it.spends}
	}

	return nil
}

// replayedSpendGoneSQL counts the spends of $1-$3 (parent output, spender) that no longer have
// a journal row naming the spender.
const replayedSpendGoneSQL = `
SELECT count(*)
  FROM unnest($1::uuid[], $2::bytea[], $3::bytea[]) AS k(ukey, txid, spender)
 WHERE NOT EXISTS (SELECT 1 FROM spend_journal j
                    WHERE j.ukey = k.ukey
                      AND j.txid = k.txid
                      AND j.spending_txid = k.spender)`

// requireReplayedSpendsStand fails the chunk when a spend step 1 settled as a repeat has been
// given back since.
//
// A repeat is a spend another writer of the same transaction made, and step 1 took nothing for
// it. That writer can stop before writing the transaction and give the spend back, in which
// case the coin is live again and this write would create the transaction with an input that is
// not spent. The check runs under the chunk's advisory locks, which the give-back takes too
// (giveBackUnlessWritten), so a give-back either finished before it, and the row is gone, or
// waits until this chunk has committed and then finds the transaction written and keeps the
// spend. A spend this write took itself needs no check: only the writer that took a spend gives
// it back. Repeats are rare, so on the common path this is no statement at all.
//
// The write fails with a storage error, as for any other failure, and its caller retries the
// list, which spends the coin for real this time.
func (w *nettedWrite) requireReplayedSpendsStand(ctx context.Context, dbTx pgx.Tx, chunk []*multiTx) error {
	var spends []*utxo.Spend

	for _, it := range chunk {
		spends = append(spends, it.replayedOutsideSpends()...)
	}

	if len(spends) == 0 {
		return nil
	}

	keys, err := unspendKeysOf(spends)
	if err != nil {
		return err
	}

	var gone int

	if err := dbTx.QueryRow(ctx, replayedSpendGoneSQL, keys.ukeys, keys.ptxids, keys.stxids).Scan(&gone); err != nil {
		return errors.NewStorageError("[utxoset][SpendAndCreateMulti] check repeated spends", err)
	}

	if gone > 0 {
		return errors.NewStorageError("[utxoset][SpendAndCreateMulti] %d of %d spends this write found already made by another writer of the same transaction were given back before the create; retry the list",
			gone, len(spends))
	}

	return nil
}

func (w *nettedWrite) writeNettedJournal(ctx context.Context, dbTx pgx.Tx, j *nettedRows) error {
	if len(j.ukeys) == 0 {
		return nil
	}

	if _, err := dbTx.Exec(ctx, nettedJournalSQL, int32(w.blockHeight), //nolint:gosec // a height fits int32
		j.sats, j.created, j.spendable, j.flags, j.mined, j.blockIDs, j.ukeys, j.txids, j.spenders, j.scripts, j.vins); err != nil {
		return errors.NewStorageError("[utxoset][SpendAndCreateMulti] journal netted spends", err)
	}

	return nil
}

// finishPerTransaction hands every transaction step 2 has not settled to the per-transaction
// default, in list order. Their outside spends repeat there as the same spends, and a spend of a
// parent this write already created finds the journal row naming it.
func (w *nettedWrite) finishPerTransaction(ctx context.Context) error {
	var (
		rest  []*multiTx
		txs   []*bt.Tx
		txids []chainhash.Hash
		idxs  []int
	)

	listIdxs := w.list.Options.SubtreeIdxs

	for _, it := range w.items {
		if it.dead() || it.result.Status != utxo.MultiTxNotAttempted {
			continue
		}

		it.restoreClaims()

		rest = append(rest, it)
		txs = append(txs, it.tx)
		txids = append(txids, it.txid)

		if listIdxs != nil {
			idxs = append(idxs, listIdxs[it.pos])
		}
	}

	if len(rest) == 0 {
		return nil
	}

	// The shorter list carries its own txids and, from each transaction's place in the original
	// list, its own subtree indexes; both replace the original list's.
	opts := append(append([]utxo.CreateOption{}, w.opts...), utxo.WithTXIDs(txids))
	if listIdxs != nil {
		opts = append(opts, utxo.WithSubtreeIdxs(idxs))
	}

	results, err := utxo.DefaultSpendAndCreateMulti(ctx, w.s, utxo.SpendAndCreateMultiConcurrency(w.s.settings), txs, w.blockHeight, opts...)
	for k, r := range results {
		rest[k].result = r
	}

	if err != nil {
		return err
	}

	return w.undoSpendsOfFailures(ctx, rest)
}

// undoSpendsOfFailures restores every spend the netted write committed for a
// transaction the per-transaction default then failed or skipped: its outside spends from step
// 1, and its spends of parents an earlier step-2 chunk created.
//
// The default runs each transaction's spends in its own database transaction and rolls that
// back when the transaction fails, but neither kind was made there. Step 1 committed the outside
// spends before step 2 began. A parent's step-2 chunk committed, with the parent, a
// spend-journal row naming the child for each output the child nets, and no UTXO row for it at
// all. The default repeats both as the same spend, which writes nothing. Without this a child
// rejected by the default, for instance because a parent in the list already existed and its
// output had been taken, kept every independent input it spent in step 1 marked spent by a
// transaction that has no record, and every output it netted from a parent created in an
// earlier chunk was gone from the UTXO set: in the journal under a spender that never existed,
// unspendable by anyone. Unspend restores both kinds the same way, from the journal row that
// names the spender. A netted spend of a parent the default itself created needs nothing: that
// parent's outputs are real UTXOs and the child's spend of them rolled back with the child. The
// rest of the netted write already holds the rule this restores: a failed transaction never
// leaves a spend behind.
func (w *nettedWrite) undoSpendsOfFailures(ctx context.Context, items []*multiTx) error {
	var undo []*utxo.Spend

	for _, it := range items {
		if !it.dead() {
			continue
		}

		if it.spent {
			undo = append(undo, it.takenOutsideSpends()...)
		}

		undo = append(undo, w.nettedSpendsJournaled(it)...)
	}

	if len(undo) == 0 {
		return nil
	}

	if err := w.s.giveBackUnlessWritten(ctx, undo); err != nil {
		return errors.NewStorageError("[utxoset][SpendAndCreateMulti] restoring the spends of transactions the per-transaction default rejected", err)
	}

	return nil
}

// logIfSlow writes one line for a list that took longer than multiSlowListLog, saying how big
// and how deep it was and where the time went. Step 1 is the outside spends, step 2 the
// creates; the chunk timings inside step 2 are summed across parallel chunks.
func (w *nettedWrite) logIfSlow(start, spent, end time.Time) {
	if end.Sub(start) < multiSlowListLog {
		return
	}

	w.s.logger.Infof("[utxoset][SpendAndCreateMulti] slow list at height %d: %d txs, %d levels, %.1f MB of bodies; step 1 %s in %d chunks; step 2 %s in %d chunks (lock %s, create %s, journal %s, commit %s)",
		w.blockHeight, len(w.items), w.levels, float64(w.bodyBytes.Load())/1e6,
		spent.Sub(start).Round(time.Millisecond), w.spendChunks,
		end.Sub(spent).Round(time.Millisecond), w.createChunks.Load(),
		time.Duration(w.lockNs.Load()).Round(time.Millisecond), time.Duration(w.createNs.Load()).Round(time.Millisecond),
		time.Duration(w.journalNs.Load()).Round(time.Millisecond), time.Duration(w.commitNs.Load()).Round(time.Millisecond))
}
