package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
)

// decorateSQL reads the two fields an input actually needs from the parent's UTXO row.
//
// Today's store answers this by fetching the parent's raw_tx -- ~1.7 KB -- and running
// bt.NewTxFromBytes over it to extract LockingScript and Satoshis, discarding the rest
// (get.go:738; the parsed tx never escapes the loop). Measured at 1,708 ns and 114
// allocations per parent, in a process that is 64% GC under a hard 6 GiB cap.
//
// Here those two fields ARE the row, so this is an index probe and two column reads.
// Decorate runs BEFORE the spend, so the parent is still unspent and still present --
// and the page it lands on is the same one the imminent DELETE will touch, so the read
// warms it rather than competing with it.
//
// hash_override comes with them because on a REASSIGNED coin those two fields are stale and
// must not be handed out. ReAssignUTXO is given the new output's hash and nothing else, so the
// row keeps the confiscated owner's satoshis and script; decorating from them would write the
// old output onto the new owner's input, and the spend would then hash exactly those stale
// values against the override and refuse the transaction. It costs one more column on a probe
// that already reads the row.
const decorateSQL = `
SELECT k.ref, u.satoshis, u.script, u.hash_override
  FROM unnest($1::smallint[], $2::uuid[], $3::bytea[], $4::int[]) AS k(leaf, ukey, txid, ref)
  JOIN utxo u
    ON u.leaf = k.leaf
   AND u.ukey = k.ukey
   AND u.txid = k.txid`

// inputRef identifies one input across a batch of transactions.
type inputRef struct {
	txIdx  int
	inpIdx int
}

// PreviousOutputsDecorate populates tx's inputs with their parents' satoshis and
// locking scripts.
func (s *Store) PreviousOutputsDecorate(ctx context.Context, tx *bt.Tx) error {
	if tx == nil {
		return nil
	}

	return s.BatchPreviousOutputsDecorate(ctx, []*bt.Tx{tx})
}

// BatchPreviousOutputsDecorate decorates many transactions in one round trip.
//
// Inputs that already carry a script are skipped, matching the postgres store: the
// spend path populates them via RETURNING, so a transaction that has already been
// spent through this store arrives here needing nothing at all.
func (s *Store) BatchPreviousOutputsDecorate(ctx context.Context, txs []*bt.Tx) error {
	var (
		leaves []int16
		ukeys  [][16]byte
		txids  [][]byte
		refIdx []int32
		refs   []inputRef
	)

	for txIdx, tx := range txs {
		if tx == nil || tx.IsCoinbase() {
			continue
		}

		for inpIdx, in := range tx.Inputs {
			if in == nil || in.PreviousTxScript != nil {
				continue // already decorated, by Spend's RETURNING or a prior call
			}

			parent := in.PreviousTxIDChainHash()

			leaves = append(leaves, LeafFor(parent[:]))
			ukeys = append(ukeys, Pack(parent[:], in.PreviousTxOutIndex))
			txids = append(txids, parent[:])
			refIdx = append(refIdx, int32(len(refs)))
			refs = append(refs, inputRef{txIdx: txIdx, inpIdx: inpIdx})
		}
	}

	if len(refs) == 0 {
		return nil
	}

	rows, err := s.pool.Query(ctx, decorateSQL, leaves, ukeys, txids, refIdx)
	if err != nil {
		return errors.NewStorageError("[utxoset][BatchPreviousOutputsDecorate] query", err)
	}
	defer rows.Close()

	resolved := 0
	reassigned := 0

	var firstReassigned *bt.Input

	for rows.Next() {
		var (
			ref          int32
			satoshis     int64
			script       []byte
			hashOverride []byte
		)

		if err := rows.Scan(&ref, &satoshis, &script, &hashOverride); err != nil {
			return errors.NewStorageError("[utxoset][BatchPreviousOutputsDecorate] scan", err)
		}

		if int(ref) >= len(refs) {
			continue
		}

		r := refs[ref]
		in := txs[r.txIdx].Inputs[r.inpIdx]

		// The coin was found, so it counts as resolved -- it is neither missing nor spent, and
		// reporting it as a missing parent would send the caller looking for a transaction that
		// is right there. But it is left UNDECORATED, because this store does not hold the
		// output it was reassigned to. Only the new owner has that script, so only they can
		// present it, and the transaction has to arrive already extended.
		if len(hashOverride) > 0 {
			resolved++
			reassigned++

			if firstReassigned == nil {
				firstReassigned = in
			}

			continue
		}

		in.PreviousTxSatoshis = uint64(satoshis)
		in.PreviousTxScript = bscript.NewFromBytes(script)
		resolved++
	}

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][BatchPreviousOutputsDecorate] rows", err)
	}

	// Reported before the missing-parent tally, and as a processing error rather than a
	// not-found one, because the two need opposite handling. A missing parent is something the
	// caller can go and fetch; a reassigned coin never becomes decoratable, however long anyone
	// waits, so the validator must turn it into "can't extend" rather than into a parent hunt.
	//
	// Silence is the one thing this must not be. The validator marks a transaction extended the
	// moment this returns nil (Validator.go:1631-1645), so leaving an input with a nil script
	// and no error would declare it validated against no script at all.
	if reassigned > 0 {
		parent := firstReassigned.PreviousTxIDChainHash()

		return errors.NewProcessingError("[utxoset][BatchPreviousOutputsDecorate] %d of %d parent outputs were reassigned (%s:%d among them); this store holds only the hash of the new output, so the spending transaction must arrive extended",
			reassigned, len(refs), parent, firstReassigned.PreviousTxOutIndex)
	}

	if resolved < len(refs) {
		// A parent that is absent from the UTXO table is spent or was never created. Note
		// the behavioural difference from the postgres store, which can still decorate
		// from a spent parent's surviving txs row until the pruner removes it: here the
		// row is gone the moment it is spent. That is the correct answer for a
		// validation path -- a transaction spending an already-spent output is invalid
		// either way -- but it surfaces as a missing parent rather than as a
		// double-spend, so callers that distinguish those must not treat this as
		// merely "not found yet".
		return errors.NewTxNotFoundError("[utxoset][BatchPreviousOutputsDecorate] %d of %d parent outputs not in the utxo set (spent or never created)",
			len(refs)-resolved, len(refs))
	}

	return nil
}
