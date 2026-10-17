package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// parentCoinSQL reads an output's value and script from wherever this store still holds it:
// the UTXO table while it is unspent, the spend journal once it is spent. One of the two holds
// it for as long as the journal keeps the spend, which is what lets a transaction be checked
// again after a crash once its parents' outputs have been spent by it, including outputs
// SpendAndCreateMulti netted and never wrote as UTXOs.
//
// A reassigned UTXO comes back with its hash_override, because its satoshis and script are the
// old owner's and must not be handed out.
const parentCoinSQL = `
SELECT k.ref, u.satoshis, u.script, u.hash_override
  FROM unnest($1::smallint[], $2::uuid[], $3::bytea[], $4::int[]) AS k(leaf, ukey, txid, ref)
  JOIN utxo u ON u.leaf = k.leaf AND u.ukey = k.ukey AND u.txid = k.txid
UNION ALL
SELECT k.ref, j.satoshis, j.script, j.hash_override
  FROM unnest($1::smallint[], $2::uuid[], $3::bytea[], $4::int[]) AS k(leaf, ukey, txid, ref)
  JOIN spend_journal j ON j.ukey = k.ukey AND j.txid = k.txid`

// ParentOutputsForValidation implements utxo.Store.
//
// The transaction-level answer, whether the parent exists and the lowest height of a block it
// is recorded in, comes from the same read order Get uses, with the body left out. Each output
// comes from the UTXO table while it is unspent, and from the spend journal once it is spent.
// Only an output in neither is looked for in the parent's body (readMissedFromBodies). An output
// of a known parent in none of the three is reported NoSuchIndex, as the Aerospike and SQL stores
// report a parent with no output at that index. It covers an output the seeder never held (spent
// before the snapshot), one that never existed, one the store never kept because it is provably
// unspendable, and one spent before the journal's retention; none of them can be spent, so all
// are a verdict on the spender rather than a missing parent to go and fetch. Only an unknown
// parent is TxNotFound.
//
// The body is not the source of an ordinary answer. Subtree validation asks for about 131,000
// parent outputs at once on mainnet, 1,024 to a call with up to 128 calls in flight. Reading and
// parsing each parent's whole body to keep one output held gigabytes live, none of it garbage,
// and on 2026-10-15 it took mainnet past its memory limit. The fix is to read less; a cache of
// parent outputs is ruled out on this path.
func (s *Store) ParentOutputsForValidation(ctx context.Context, outpoints []utxo.Outpoint,
	_ ...utxo.ParentOutputOption) ([]utxo.ParentOutput, error) {
	answers := make([]utxo.ParentOutput, len(outpoints))
	if len(outpoints) == 0 {
		return answers, nil
	}

	hashes := make([]chainhash.Hash, len(outpoints))
	for i, op := range outpoints {
		hashes[i] = op.TxID
	}

	res, err := s.lookupMany(ctx, hashes, false, true)
	if err != nil {
		fault := errors.NewStorageError("[utxoset][ParentOutputsForValidation] lookup", err)
		for i := range answers {
			answers[i].Err = fault
		}

		return answers, nil
	}

	fromCoins := make([]int, 0, len(outpoints))

	for i, op := range outpoints {
		if derr, bad := res.failed[op.TxID]; bad {
			answers[i].Err = derr
			continue
		}

		data, ok := res.found[op.TxID]
		if !ok {
			answers[i].Status = utxo.ParentOutputTxNotFound
			continue
		}

		answers[i].Status, answers[i].Height = parentStatus(data)
		fromCoins = append(fromCoins, i)
	}

	if len(fromCoins) > 0 {
		s.readParentCoins(ctx, outpoints, fromCoins, answers)
	}

	return answers, nil
}

// parentStatus is a parent's mined state and, when mined, the lowest height it is recorded at.
func parentStatus(data *meta.Data) (utxo.ParentOutputStatus, uint32) {
	if len(data.BlockHeights) == 0 {
		return utxo.ParentOutputNotMined, 0
	}

	lowest := data.BlockHeights[0]
	for _, h := range data.BlockHeights[1:] {
		lowest = min(lowest, h)
	}

	return utxo.ParentOutputMined, lowest
}

// readParentCoins fills the outputs of known parents from the UTXO table or the journal.
// A fault fails only the entries it was asked about.
func (s *Store) readParentCoins(ctx context.Context, outpoints []utxo.Outpoint, idx []int, answers []utxo.ParentOutput) {
	leaves := make([]int16, len(idx))
	ukeys := make([][16]byte, len(idx))
	txids := make([][]byte, len(idx))
	refs := make([]int32, len(idx))

	for k, i := range idx {
		op := outpoints[i]
		leaves[k] = LeafFor(op.TxID[:])
		ukeys[k] = Pack(op.TxID[:], op.Vout)
		txids[k] = append([]byte(nil), op.TxID[:]...)
		refs[k] = int32(k) //nolint:gosec // bounded by the call's size
	}

	fail := func(err error) {
		fault := errors.NewStorageError("[utxoset][ParentOutputsForValidation] read outputs", err)
		for _, i := range idx {
			answers[i] = utxo.ParentOutput{Err: fault}
		}
	}

	rows, err := s.pool.Query(ctx, parentCoinSQL, leaves, ukeys, txids, refs)
	if err != nil {
		fail(err)
		return
	}

	found := make([]bool, len(idx))

	for rows.Next() {
		var (
			ref          int32
			satoshis     int64
			script       []byte
			hashOverride []byte
		)

		if err := rows.Scan(&ref, &satoshis, &script, &hashOverride); err != nil {
			rows.Close()
			fail(err)

			return
		}

		if found[ref] {
			continue
		}

		found[ref] = true
		i := idx[ref]

		if hashOverride != nil {
			answers[i] = utxo.ParentOutput{Err: errors.NewProcessingError("[utxoset][ParentOutputsForValidation] output %d of %s has been reassigned; its stored value and script belong to the old owner",
				outpoints[i].Vout, outpoints[i].TxID.String())}

			continue
		}

		sc := bscript.Script(script)
		answers[i].Satoshis = uint64(satoshis) //nolint:gosec // satoshis are never negative
		answers[i].LockingScript = &sc
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		fail(err)
		return
	}

	var missed []int

	for k, i := range idx {
		if !found[k] && answers[i].Err == nil {
			missed = append(missed, i)
		}
	}

	if len(missed) > 0 {
		s.readMissedFromBodies(ctx, outpoints, missed, answers)
	}
}

// missedBodyChunk is how many parent bodies readMissedFromBodies holds at one time.
const missedBodyChunk = 16

// missedBodyChunkRead is a test hook called with the parent count of each body read
// readMissedFromBodies makes. nil in production.
var missedBodyChunkRead func(parents int)

// readMissedFromBodies answers, from the parent's body, an output the coin rows do not hold. That
// is a spent output whose journal row has dropped, or an output that never was a UTXO. The body
// still answers the first while the store keeps it, and a mined transaction submitted again
// after its journal row dropped depends on that: without the output, the validator rejects it
// before it can recognise its own resubmission. Only these misses read a body, so the read
// path for an ordinary spend stays body-free. An output in neither place is NoSuchIndex.
//
// The sender of a transaction chooses its outpoints, so a hostile one can name outputs that miss
// on purpose: out-of-range indexes, outputs never stored, outputs spent long ago. The bodies are
// therefore read missedBodyChunk parents at a time, each chunk answered and let go before the
// next, so the memory this holds has a fixed bound however many misses a call carries.
func (s *Store) readMissedFromBodies(ctx context.Context, outpoints []utxo.Outpoint, idx []int, answers []utxo.ParentOutput) {
	byParent := make(map[chainhash.Hash][]int, len(idx))
	parents := make([]chainhash.Hash, 0, len(idx))

	for _, i := range idx {
		h := outpoints[i].TxID
		if _, seen := byParent[h]; !seen {
			parents = append(parents, h)
		}

		byParent[h] = append(byParent[h], i)
	}

	for lo := 0; lo < len(parents); lo += missedBodyChunk {
		chunk := parents[lo:min(lo+missedBodyChunk, len(parents))]

		if missedBodyChunkRead != nil {
			missedBodyChunkRead(len(chunk))
		}

		res, err := s.lookupMany(ctx, chunk, false, false)
		if err != nil {
			fault := errors.NewStorageError("[utxoset][ParentOutputsForValidation] read bodies", err)

			for _, h := range chunk {
				for _, i := range byParent[h] {
					answers[i] = utxo.ParentOutput{Err: fault}
				}
			}

			continue
		}

		for _, h := range chunk {
			for _, i := range byParent[h] {
				answerFromBody(res, outpoints[i], &answers[i])
			}
		}
	}
}

// answerFromBody sets one output's value and script from its parent's body, or NoSuchIndex when
// the body is gone or has no such output.
func answerFromBody(res lookupResult, op utxo.Outpoint, answer *utxo.ParentOutput) {
	if derr, bad := res.failed[op.TxID]; bad {
		*answer = utxo.ParentOutput{Err: derr}
		return
	}

	data, ok := res.found[op.TxID]
	if !ok || data.Tx == nil || int(op.Vout) >= len(data.Tx.Outputs) ||
		data.Tx.Outputs[op.Vout] == nil || data.Tx.Outputs[op.Vout].LockingScript == nil {
		*answer = utxo.ParentOutput{Status: utxo.ParentOutputNoSuchIndex}
		return
	}

	out := data.Tx.Outputs[op.Vout]
	script := bscript.Script(append([]byte(nil), *out.LockingScript...))
	answer.Satoshis = out.Satoshis
	answer.LockingScript = &script
}
