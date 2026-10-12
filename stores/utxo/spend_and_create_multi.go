package utxo

import (
	"context"

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
	// parent.
	MultiTxExisted
	// MultiTxFailed means the transaction's SpendAndCreate failed; Err says why,
	// and Spends is what SpendAndCreate returned alongside the error.
	MultiTxFailed
	// MultiTxParentFailed means the transaction was not written, because a parent
	// earlier in the list failed.
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

// SpendAndCreateMultiConcurrency is the per-level concurrency Aerospike and SQL
// pass to DefaultSpendAndCreateMulti: twice the subtree-validation spend batcher
// size, the same bound subtree validation applies to a level today, so a level
// reaches the store exactly as wide as it does now.
func SpendAndCreateMultiConcurrency(tSettings *settings.Settings) int {
	if tSettings == nil || tSettings.SubtreeValidation.SpendBatcherSize <= 0 {
		return 1
	}

	return tSettings.SubtreeValidation.SpendBatcherSize * 2
}

// SpendAndCreateMultiStore is the subset of a store's methods that
// DefaultSpendAndCreateMulti needs.
type SpendAndCreateMultiStore interface {
	SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...CreateOption) (*meta.Data, []*Spend, error)
}

// DefaultSpendAndCreateMulti implements the Store.SpendAndCreateMulti contract by
// writing one record per transaction through s.SpendAndCreate. It writes each
// dependency level's transactions in parallel, at most concurrency at once, and
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
func DefaultSpendAndCreateMulti(ctx context.Context, s SpendAndCreateMultiStore, concurrency int, txs []*bt.Tx,
	blockHeight uint32, opts ...CreateOption) ([]SpendAndCreateMultiResult, error) {
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

	if len(txs) == 0 {
		return []SpendAndCreateMultiResult{}, nil
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

	// parentsInList[i] holds the positions of tx i's parents that are in the list.
	parentsInList, err := checkSpendAndCreateMultiList(txs, txids)
	if err != nil {
		return nil, err
	}

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

	if concurrency < 1 {
		concurrency = 1
	}

	for _, members := range byLevel {
		if err := ctx.Err(); err != nil {
			return results, err
		}

		g := errgroup.Group{}
		g.SetLimit(concurrency)

		for _, i := range members {
			if failedParent(results, parentsInList[i]) {
				results[i] = SpendAndCreateMultiResult{
					Status: MultiTxParentFailed,
					Err:    errors.NewProcessingError("SpendAndCreateMulti: a parent of %s earlier in the list failed", txids[i]),
				}

				continue
			}

			g.Go(func() error {
				callOpts := make([]CreateOption, 0, len(opts)+1)
				callOpts = append(callOpts, opts...)
				callOpts = append(callOpts, WithTXID(&txids[i]))

				md, spends, err := s.SpendAndCreate(ctx, txs[i], blockHeight, callOpts...)

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
