package utxo

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"golang.org/x/sync/errgroup"
)

// SpendAndCreateMultiStatus says what SpendAndCreateMulti did with one
// transaction of its list.
type SpendAndCreateMultiStatus uint8

const (
	// MultiTxNotAttempted is the zero value and never a final answer: the call
	// stopped (a cancelled context) before it reached this transaction.
	MultiTxNotAttempted SpendAndCreateMultiStatus = iota
	// MultiTxCreated means this call spent the transaction's inputs and created
	// its record.
	MultiTxCreated
	// MultiTxExisted means a record for the transaction already existed. Nothing
	// was written to it, and its children in the list spend it as an outside
	// parent. For a WithBelowCheckpoint list its inputs were spent again, or,
	// with no parent in the list, by its own combined call; see
	// writeCreateThenSpend.
	MultiTxExisted
	// MultiTxFailed means the transaction's SpendAndCreate failed; Err says why,
	// and Spends is what SpendAndCreate returned alongside the error.
	MultiTxFailed
	// MultiTxParentFailed means the transaction was not written, because a parent
	// earlier in the list failed. For a WithBelowCheckpoint list its record may
	// exist, though its inputs were not spent; see writeCreateThenSpend.
	MultiTxParentFailed
)

// SpendAndCreateMultiResult is SpendAndCreateMulti's answer for one transaction.
type SpendAndCreateMultiResult struct {
	Status SpendAndCreateMultiStatus
	// Meta is the metadata SpendAndCreate returned for a MultiTxCreated
	// transaction, as Create returns it: no BlockIDs.
	Meta *meta.Data
	// Spends is the []*Spend SpendAndCreate returned for this transaction, with
	// the same meaning: for MultiTxExisted the spends are in effect, and for
	// MultiTxFailed see SpendAndCreate's contract.
	Spends []*Spend
	Err    error
}

const spendAndCreateMultiRefusedKey = "spend_and_create_multi_refused"

// newSpendAndCreateMultiRefusedError builds the error SpendAndCreateMulti returns,
// with nothing written, for a list that breaks the caller's guarantees.
func newSpendAndCreateMultiRefusedError(message string, params ...interface{}) error {
	e := errors.NewInvalidArgumentError("SpendAndCreateMulti refused: "+message, params...)
	e.SetData(spendAndCreateMultiRefusedKey, true)

	return e
}

// IsSpendAndCreateMultiRefused reports whether err is SpendAndCreateMulti
// refusing its list. A refusal means a caller bug and is never retryable with the
// same arguments; nothing was written.
func IsSpendAndCreateMultiRefused(err error) bool {
	for cur := err; cur != nil; {
		var e *errors.Error
		if !errors.As(cur, &e) {
			return false
		}

		if v, ok := e.GetData(spendAndCreateMultiRefusedKey).(bool); ok && v {
			return true
		}

		cur = e.Unwrap()
	}

	return false
}

// SpendAndCreateMultiWidths bounds how many SpendAndCreate calls
// DefaultSpendAndCreateMulti keeps in flight. Level bounds one dependency level
// of the default order. Create and Spend bound the first wave and the spend
// wave of a WithBelowCheckpoint list. A width below 1 is taken as 1.
//
// CallBound is how long one call of a WithBelowCheckpoint list may run once it
// no longer follows the caller's cancellation; see writeCreateThenSpend. Zero
// means defaultSpendAndCreateMultiCallBound.
type SpendAndCreateMultiWidths struct {
	Level     int
	Create    int
	Spend     int
	CallBound time.Duration
}

// defaultSpendAndCreateMultiCallBound is CallBound when none is set: the
// settings defaults of utxostore_spendWaitTimeout (30s) and
// utxostore_spendRollbackTimeout (120s).
const defaultSpendAndCreateMultiCallBound = 150 * time.Second

// UniformSpendAndCreateMultiWidths returns widths that bound every level and
// every wave to n calls.
func UniformSpendAndCreateMultiWidths(n int) SpendAndCreateMultiWidths {
	return SpendAndCreateMultiWidths{Level: n, Create: n, Spend: n}
}

// SpendAndCreateMultiWidthsFor returns the widths Aerospike and SQL pass to
// DefaultSpendAndCreateMulti. A level is twice the subtree-validation spend
// batcher size, the bound subtree validation applies to a level, so a level
// reaches the store exactly as wide as it does there. The first wave of a
// WithBelowCheckpoint list is as wide as belowCheckpointCreateWidth says, and
// its spend wave is two rounds of every spend batcher, as quick validation's
// spend phase was before it wrote a list. A call's bound is the spend's own budget plus
// the rollback budget. It does not bound the rollback of a failed create, which
// SequentialSpendAndCreate runs on a context of its own; a call that reaches the
// bound mid-create keeps its spends, since its create may yet commit.
func SpendAndCreateMultiWidthsFor(tSettings *settings.Settings) SpendAndCreateMultiWidths {
	if tSettings == nil {
		return UniformSpendAndCreateMultiWidths(1)
	}

	return SpendAndCreateMultiWidths{
		Level:     tSettings.SubtreeValidation.SpendBatcherSize * 2,
		Create:    belowCheckpointCreateWidth(&tSettings.BlockValidation, &tSettings.UtxoStore),
		Spend:     tSettings.UtxoStore.SpendBatcherSize * tSettings.UtxoStore.SpendBatcherConcurrency * 2,
		CallBound: tSettings.UtxoStore.SpendWaitTimeout + tSettings.UtxoStore.SpendRollbackTimeout,
	}
}

// belowCheckpointCreateWidth is how many calls the first wave of a
// WithBelowCheckpoint list may have in flight at once. That wave creates every
// record of the list, and for a transaction with no parent in the list it also
// spends that transaction's inputs in the same call.
//
// It bounds callers, not store statements: each caller blocks until the batch
// it lands in commits, so the statements actually in flight to the store are
// (callers / batch size).
//
// blockvalidation_quick_validate_create_concurrency, when set above 0, is used
// directly. Otherwise the width is derived from the store's own batcher
// settings: StoreBatcherSize * BatcherMaxConcurrent, so every batcher worker
// can have a full batch of callers queued behind it, floored at
// StoreBatcherSize * 8 for a store whose BatcherMaxConcurrent is 0 (unbounded
// workers), the flat multiplier the derivation replaced.
//
// The derived value is capped at belowCheckpointDerivedCreateCeiling unless the
// floor is higher. Each caller is a goroutine holding a transaction, and at the
// default batch size of 2048 and 64 batcher workers the product is 131,072. The
// cap keeps the gain the derivation was for, more callers on a store with a
// small batch size, and never goes below the floor.
func belowCheckpointCreateWidth(bv *settings.BlockValidationSettings, store *settings.UtxoStoreSettings) int {
	if bv.QuickValidateCreateConcurrency > 0 {
		return bv.QuickValidateCreateConcurrency
	}

	derived := min(store.StoreBatcherSize*store.BatcherMaxConcurrent, belowCheckpointDerivedCreateCeiling)
	floor := store.StoreBatcherSize * 8

	if derived > floor {
		return derived
	}

	return floor
}

// belowCheckpointDerivedCreateCeiling bounds the derived first-wave width: the
// flat 8x multiplier's value at the default batch size of 2048.
const belowCheckpointDerivedCreateCeiling = 16384

// SpendAndCreateMultiStore is the subset of a store's methods that
// DefaultSpendAndCreateMulti needs.
type SpendAndCreateMultiStore interface {
	SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...CreateOption) (*meta.Data, []*Spend, error)
}

// DefaultSpendAndCreateMulti implements the Store.SpendAndCreateMulti contract by
// writing one record per transaction through s.SpendAndCreate. It writes each
// dependency level's transactions in parallel, at most widths.Level at once, and
// starts a level only after every earlier level has finished, so a parent in the
// list is created before its child spends it. It is never a plain loop: a store's
// batchers group a level's concurrent calls into one spend and one create round
// trip, as they group today's concurrent per-transaction validations.
//
// In order, it:
//  1. refuses, with nothing written, a list that spends a later or the same
//     transaction, spends one outpoint twice, names an output index past the end
//     of a parent in the list, holds a nil transaction, a transaction twice or a
//     coinbase, or passes WithTXID, WithSetCoinbase, WithCreateOnly,
//     WithSpendOnly or a WithTXIDs of the wrong length;
//  2. assigns each transaction a level in memory;
//  3. writes level by level, skipping (MultiTxParentFailed) any transaction whose
//     parent in the list failed.
//
// It makes no read of its own. As on the per-transaction path, the caller drops
// transactions that already have a record before calling, and a record that
// appears afterwards (propagation, a sibling block) comes back from
// SpendAndCreate as ErrTxExists with its spends in place and is reported
// MultiTxExisted. A repeat after a crash is safe for the same reasons it is on
// that path: a spend repeated by the same spender is accepted as the same
// spend, and SpendAndCreate spends before it creates, so a record that exists
// has made its spends.
//
// The returned error is non-nil when nothing was written (a refusal), or when
// ctx was cancelled between levels; in that case the results are returned too
// and transactions never reached are MultiTxNotAttempted.
//
// With WithBelowCheckpoint it writes in two waves instead; see
// writeCreateThenSpend.
func DefaultSpendAndCreateMulti(ctx context.Context, s SpendAndCreateMultiStore, widths SpendAndCreateMultiWidths, txs []*bt.Tx,
	blockHeight uint32, opts ...CreateOption) ([]SpendAndCreateMultiResult, error) {
	list, err := PrepareSpendAndCreateMulti(txs, opts...)
	if err != nil {
		return nil, err
	}

	if len(txs) == 0 {
		return []SpendAndCreateMultiResult{}, nil
	}

	items := listItemOptions(list)

	if list.Options.BelowCheckpoint {
		return writeCreateThenSpend(ctx, s, widths, txs, list, blockHeight, items)
	}

	txids, parentsInList := list.TxIDs, list.ParentsInList

	results := make([]SpendAndCreateMultiResult, len(txs))

	// Levels. Parents come before children, so one pass suffices.
	level := make([]int, len(txs))
	maxLevel := 0

	for i := range txs {
		for _, p := range parentsInList[i] {
			level[i] = max(level[i], level[p]+1)
		}

		maxLevel = max(maxLevel, level[i])
	}

	byLevel := make([][]int, maxLevel+1)

	for i := range txs {
		byLevel[level[i]] = append(byLevel[level[i]], i)
	}

	for _, members := range byLevel {
		if err := ctx.Err(); err != nil {
			return results, err
		}

		g := errgroup.Group{}
		g.SetLimit(max(widths.Level, 1))

		for _, i := range members {
			if failedParent(results, parentsInList[i]) {
				results[i] = SpendAndCreateMultiResult{
					Status: MultiTxParentFailed,
					Err:    errors.NewProcessingError("SpendAndCreateMulti: a parent of %s earlier in the list failed", txids[i]),
				}

				continue
			}

			g.Go(func() error {
				md, spends, err := s.SpendAndCreate(ctx, txs[i], blockHeight, items[i:i+1:i+1]...)

				switch {
				case err == nil:
					results[i] = SpendAndCreateMultiResult{Status: MultiTxCreated, Meta: md, Spends: spends}
				case errors.Is(err, errors.ErrTxExists):
					// The record already exists (propagation, a sibling block, or
					// an earlier attempt). SpendAndCreate leaves the spends in place.
					results[i] = SpendAndCreateMultiResult{Status: MultiTxExisted, Spends: spends}
				default:
					results[i] = SpendAndCreateMultiResult{Status: MultiTxFailed, Spends: spends, Err: err}
				}

				return nil
			})
		}

		_ = g.Wait()
	}

	return results, nil
}

// writeCreateThenSpend writes a WithBelowCheckpoint list in two waves. The
// first wave writes every transaction: one with no parent in the list gets one
// combined SpendAndCreate, which spends its inputs and creates its record, and
// one with a parent in the list gets a create-only call. The second wave spends
// the inputs of the transactions that only created. Every parent in the list
// exists before any child spends it, so the list costs two rounds of store
// calls however deep its chains go, and a transaction with no parent in the
// list costs one call rather than two. It is only correct for a block the
// caller already knows is valid: a transaction whose spends fail in the second
// wave has its record written anyway.
//
// The results keep their meaning, with three differences that follow from the
// order:
//   - a transaction whose first-wave call fails is MultiTxFailed, and its
//     descendants in the list are MultiTxParentFailed and not spent, but their
//     records may have been created;
//   - a transaction whose second-wave spend fails is MultiTxFailed with its
//     record created; its children in the list were spent in the same wave and
//     keep their own result;
//   - a MultiTxExisted transaction with a parent in the list has its inputs
//     spent again in the second wave, because a record found here may have been
//     created by a call that never reached its spends. A spend repeated by the
//     same spender is accepted as the same spend. One with no parent in the list
//     made its spends in its combined call, which spends before it creates and
//     leaves the spends in place when the record exists.
//
// Every spend of the second wave sets IgnoreFlags.RequireParentOutputs, so a
// missing parent output fails the spend. Without it the store would read the
// spender's own record, created moments earlier by the first wave, as proof
// that the spend happened in an earlier attempt and the parent has been pruned
// since, and a list spending an output that exists nowhere would pass. Turning
// the fallback off cannot fail a legitimate repeat: a parent is pruned only
// after its retention has passed since it was fully spent, and the chain does
// not advance past the block being applied, so a repeat of that block's list
// runs before any parent its spends emptied can be pruned.
//
// A first-wave combined call keeps the fallback, as the per-transaction path
// does. It is made only for a transaction with no parent in the list, which a
// repeat of the list writes the same way, spending before it creates, so a
// record it finds was written after its spends. A caller that repeats part of
// a list keeps that true by keeping each transaction's parents in it (below).
// On a store left ahead of the chain by a reset, a transaction whose parent
// has since been pruned is then found as existing rather than failing the
// block.
//
// A call, once started, runs to its end. A combined SpendAndCreate is not
// atomic on a store that spends and then creates (SequentialSpendAndCreate): a
// context that ends between the two leaves the create's outcome unknown, so the
// spends stay and the transaction is MultiTxFailed until a repeat of the list
// finishes it. So each call runs under context.WithoutCancel(ctx), bounded by
// widths.CallBound, and a call that has
// not started when ctx is cancelled never starts: it is MultiTxNotAttempted and
// the error is returned.
//
// A caller treats anything but MultiTxCreated and MultiTxExisted as a failed
// transaction. It may repeat the whole list, or only the failed transactions
// with their parents in the list, in list order, as quick validation does. The
// parents keep a failed transaction's shape: one that had a parent in the list
// still gets a create-only call and a second-wave spend, so a create-only
// record from the earlier attempt never meets a combined call, which would
// read it as proof its spends were made.
func writeCreateThenSpend(ctx context.Context, s SpendAndCreateMultiStore, widths SpendAndCreateMultiWidths, txs []*bt.Tx,
	list *SpendAndCreateMultiList, blockHeight uint32, items []CreateOption) ([]SpendAndCreateMultiResult, error) {
	txids, parentsInList := list.TxIDs, list.ParentsInList
	results := make([]SpendAndCreateMultiResult, len(txs))

	bound := widths.CallBound
	if bound <= 0 {
		bound = defaultSpendAndCreateMultiCallBound
	}

	// Each call's options are the transaction's own, followed by the wave's: nothing for a
	// combined call and create-only for the rest in the first wave, spend-only and
	// RequireParentOutputs in the second. Each wave's are cut from one array, so a call
	// allocates nothing for its options.
	firstWave := make([]CreateOption, 0, 2*len(txs))

	for i := range txs {
		firstWave = append(firstWave, items[i], WithCreateOnly())
	}

	all := make([]int, len(txs))
	for i := range all {
		all[i] = i
	}

	notStarted := runDetachedWave(ctx, bound, widths.Create, all, func(callCtx context.Context, i int) {
		var (
			md     *meta.Data
			spends []*Spend
			err    error
		)

		if len(parentsInList[i]) == 0 {
			md, spends, err = s.SpendAndCreate(callCtx, txs[i], blockHeight, firstWave[2*i:2*i+1:2*i+1]...)
		} else {
			md, _, err = s.SpendAndCreate(callCtx, txs[i], blockHeight, firstWave[2*i:2*i+2:2*i+2]...)
		}

		switch {
		case err == nil:
			results[i] = SpendAndCreateMultiResult{Status: MultiTxCreated, Meta: md, Spends: spends}
		case errors.Is(err, errors.ErrTxExists):
			results[i] = SpendAndCreateMultiResult{Status: MultiTxExisted, Spends: spends}
		default:
			results[i] = SpendAndCreateMultiResult{Status: MultiTxFailed, Spends: spends, Err: err}
		}
	})

	if notStarted {
		// A create-only record whose spends never ran is not a written transaction.
		for i := range txs {
			if len(parentsInList[i]) > 0 && results[i].Status != MultiTxFailed {
				results[i] = SpendAndCreateMultiResult{}
			}
		}

		return results, ctx.Err()
	}

	// A failed first-wave call leaves its outputs missing, so its descendants'
	// spends would fail too. Mark them in list order, which reaches a grandchild
	// after its parent. A transaction with no parent in the list has made its
	// spends already.
	toSpend := make([]int, 0, len(txs))

	for i := range txs {
		if results[i].Status == MultiTxFailed {
			continue
		}

		if failedParent(results, parentsInList[i]) {
			results[i] = SpendAndCreateMultiResult{
				Status: MultiTxParentFailed,
				Err:    errors.NewProcessingError("SpendAndCreateMulti: the write of a parent of %s earlier in the list failed", txids[i]),
			}

			continue
		}

		if len(parentsInList[i]) > 0 {
			toSpend = append(toSpend, i)
		}
	}

	if err := ctx.Err(); err != nil {
		for _, i := range toSpend {
			results[i] = SpendAndCreateMultiResult{}
		}

		return results, err
	}

	spent := make([]bool, len(txs))

	secondWave := make([]CreateOption, 0, 3*len(txs))
	for i := range txs {
		secondWave = append(secondWave, items[i], WithSpendOnly(), withRequireParentOutputs())
	}

	notStarted = runDetachedWave(ctx, bound, widths.Spend, toSpend, func(callCtx context.Context, i int) {
		spent[i] = true

		_, spends, err := s.SpendAndCreate(callCtx, txs[i], blockHeight, secondWave[3*i:3*i+3:3*i+3]...)
		if err != nil {
			results[i] = SpendAndCreateMultiResult{Status: MultiTxFailed, Meta: results[i].Meta, Spends: spends, Err: err}

			return
		}

		results[i].Spends = spends
	})

	if notStarted {
		// A record whose spends never ran is not a written transaction.
		for _, i := range toSpend {
			if !spent[i] {
				results[i] = SpendAndCreateMultiResult{}
			}
		}

		return results, ctx.Err()
	}

	return results, nil
}

// listItemOptions returns, for each transaction of list, one CreateOption that sets every option
// the transaction is written with: ItemOptions(i), with its txid from the list. It works them out
// once per list. Applying the list's own options again on every call, and then ItemOptions on top,
// allocated on every parse of a call's options: the list's mined-block info grows a fresh slice,
// and ItemOptions copies the struct and that slice to set one subtree index. Here every
// transaction's mined-block info is cut from one array for the whole list, with its capacity
// capped at its length, so an option appended later copies rather than writing into a neighbour.
func listItemOptions(list *SpendAndCreateMultiList) []CreateOption {
	base := list.Options
	n := len(list.TxIDs)
	k := len(base.MinedBlockInfos)
	perItem := base.SubtreeIdxs != nil && k > 0

	var infos []MinedBlockInfo
	if perItem {
		infos = make([]MinedBlockInfo, n*k)
	}

	items := make([]CreateOptions, n)
	fns := make([]CreateOption, n)

	for i := range items {
		item := *base
		item.TxIDs = nil
		item.SubtreeIdxs = nil
		item.BelowCheckpoint = false
		item.MinedBlockInfos = base.MinedBlockInfos[:k:k]

		if perItem && i < len(base.SubtreeIdxs) {
			own := infos[i*k : (i+1)*k : (i+1)*k]
			copy(own, base.MinedBlockInfos)

			for j := range own {
				own[j].SubtreeIdx = base.SubtreeIdxs[i]
			}

			item.MinedBlockInfos = own
		}

		item.TxID = &list.TxIDs[i]
		items[i] = item

		p := &items[i]
		fns[i] = func(o *CreateOptions) { *o = *p }
	}

	return fns
}

// runDetachedWave runs call for every member, at most width at once, and waits
// for all of them. Each call gets context.WithoutCancel(ctx) bounded by bound,
// so a cancel of ctx never lands inside a call; a member whose call has not
// started when ctx is cancelled is skipped. It reports whether any was skipped.
func runDetachedWave(ctx context.Context, bound time.Duration, width int, members []int, call func(callCtx context.Context, i int)) bool {
	g := errgroup.Group{}
	g.SetLimit(max(width, 1))

	var (
		mu      sync.Mutex
		skipped bool
	)

	for _, i := range members {
		g.Go(func() error {
			if ctx.Err() != nil {
				mu.Lock()
				skipped = true
				mu.Unlock()

				return nil
			}

			callCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bound)
			defer cancel()

			call(callCtx, i)

			return nil
		})
	}

	_ = g.Wait()

	return skipped
}

// SpendAndCreateMultiList is a list SpendAndCreateMulti has accepted: its
// parsed options, every transaction's txid, and for each transaction the
// positions of its parents earlier in the list.
type SpendAndCreateMultiList struct {
	Options       *CreateOptions
	TxIDs         []chainhash.Hash
	ParentsInList [][]int
}

// PrepareSpendAndCreateMulti runs SpendAndCreateMulti's refusal checks with
// nothing written, and returns what a store needs to write the list. A store
// with its own SpendAndCreateMulti calls it first so it refuses exactly the lists
// DefaultSpendAndCreateMulti refuses. An empty list is accepted and gives an
// empty SpendAndCreateMultiList.
func PrepareSpendAndCreateMulti(txs []*bt.Tx, opts ...CreateOption) (*SpendAndCreateMultiList, error) {
	options, err := ParseCreateOptions(opts...)
	if err != nil {
		return nil, newSpendAndCreateMultiRefusedError("invalid options", err)
	}

	if options.TxID != nil {
		return nil, newSpendAndCreateMultiRefusedError("WithTXID describes one transaction; use WithTXIDs")
	}

	if options.IsCoinbase != nil {
		return nil, newSpendAndCreateMultiRefusedError("WithSetCoinbase describes one transaction; a coinbase never belongs in a list")
	}

	// The results report a record created or existing, with its spends made;
	// neither half on its own fits that, and a child level would read a parent
	// as created that has no record.
	if options.CreateOnly || options.SpendOnly {
		return nil, newSpendAndCreateMultiRefusedError("WithCreateOnly and WithSpendOnly describe half a write; a list writes both halves")
	}

	if options.TxIDs != nil && len(options.TxIDs) != len(txs) {
		return nil, newSpendAndCreateMultiRefusedError("WithTXIDs has %d txids for %d transactions", len(options.TxIDs), len(txs))
	}

	if options.SubtreeIdxs != nil {
		if len(options.MinedBlockInfos) == 0 {
			return nil, newSpendAndCreateMultiRefusedError("WithSubtreeIdxs needs WithMinedBlockInfo: the indexes are positions in that block")
		}

		if len(options.SubtreeIdxs) != len(txs) {
			return nil, newSpendAndCreateMultiRefusedError("WithSubtreeIdxs has %d indexes for %d transactions", len(options.SubtreeIdxs), len(txs))
		}
	}

	if len(txs) == 0 {
		return &SpendAndCreateMultiList{Options: options}, nil
	}

	txids := options.TxIDs
	if txids == nil {
		txids = make([]chainhash.Hash, len(txs))
		for i, tx := range txs {
			if tx == nil {
				return nil, newSpendAndCreateMultiRefusedError("transaction %d is nil", i)
			}

			txids[i] = *tx.TxIDChainHash()
		}
	}

	parentsInList, err := checkSpendAndCreateMultiList(txs, txids)
	if err != nil {
		return nil, err
	}

	return &SpendAndCreateMultiList{Options: options, TxIDs: txids, ParentsInList: parentsInList}, nil
}

func failedParent(results []SpendAndCreateMultiResult, parents []int) bool {
	for _, p := range parents {
		if results[p].Status == MultiTxFailed || results[p].Status == MultiTxParentFailed {
			return true
		}
	}

	return false
}

// checkSpendAndCreateMultiList runs the in-memory refusal checks and returns, for
// each transaction, the positions of its parents in the list.
func checkSpendAndCreateMultiList(txs []*bt.Tx, txids []chainhash.Hash) ([][]int, error) {
	position := make(map[chainhash.Hash]int, len(txs))

	for i, tx := range txs {
		if tx == nil {
			return nil, newSpendAndCreateMultiRefusedError("transaction %d is nil", i)
		}

		if tx.IsCoinbase() {
			return nil, newSpendAndCreateMultiRefusedError("transaction %s is a coinbase", txids[i])
		}

		if _, dup := position[txids[i]]; dup {
			return nil, newSpendAndCreateMultiRefusedError("transaction %s appears twice", txids[i])
		}

		position[txids[i]] = i
	}

	parentsInList := make([][]int, len(txs))
	spent := make(map[Outpoint]struct{})

	// lastChild[p] is 1 + the position of the last transaction that recorded p
	// as a parent. A transaction's inputs are walked together, so this dedups
	// its parents in O(1) each; a scan of its list would be quadratic in a
	// consolidation transaction's parent count.
	lastChild := make([]int, len(txs))

	for i, tx := range txs {
		for _, in := range tx.Inputs {
			op := Outpoint{TxID: *in.PreviousTxIDChainHash(), Vout: in.PreviousTxOutIndex}

			if _, dup := spent[op]; dup {
				return nil, newSpendAndCreateMultiRefusedError("outpoint %s:%d is spent twice", op.TxID, op.Vout)
			}

			spent[op] = struct{}{}

			p, inList := position[op.TxID]
			if !inList {
				continue
			}

			if p >= i {
				return nil, newSpendAndCreateMultiRefusedError("transaction %s spends %s, which is not earlier in the list", txids[i], op.TxID)
			}

			if int(op.Vout) >= len(txs[p].Outputs) {
				return nil, newSpendAndCreateMultiRefusedError("transaction %s spends output %d of %s, which has %d outputs", txids[i], op.Vout, op.TxID, len(txs[p].Outputs))
			}

			if lastChild[p] != i+1 {
				lastChild[p] = i + 1
				parentsInList[i] = append(parentsInList[i], p)
			}
		}
	}

	return parentsInList, nil
}

// BelowCheckpointNetter is implemented by a store whose SpendAndCreateMulti nets a list below
// the highest hardcoded checkpoint itself, with outpoint-only spends and no spend-journal rows,
// as utxoset does. Quick validation then hands such a store fixed ranges of transaction
// positions, keeps unspendable transactions in the list, and never spends an existing
// transaction's inputs a second time, because no journal row would accept the repeat.
type BelowCheckpointNetter interface {
	NetsBelowCheckpoint() bool
}

// NetsBelowCheckpoint reports whether s, or the store it wraps, is a BelowCheckpointNetter that
// says yes.
func NetsBelowCheckpoint(s any) bool {
	n, ok := s.(BelowCheckpointNetter)

	return ok && n.NetsBelowCheckpoint()
}
