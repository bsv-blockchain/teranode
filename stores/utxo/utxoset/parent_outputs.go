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
// is recorded in, comes from the same read order Get uses. The output itself comes from the
// parent's body while the store keeps it. Without a body, which is the steady state below the
// checkpoint and 288 blocks behind the tip, it comes from the UTXO table or the spend journal.
// An output of a known parent in neither is reported NoSuchIndex, as the Aerospike and SQL
// stores report a parent with no output at that index. It covers an output the seeder never
// held (spent before the snapshot), one that never existed, and one spent before the journal's
// retention; none of them can be spent, so all three are a verdict on the spender rather than
// a missing parent to go and fetch. Only an unknown parent is TxNotFound.
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

	res, err := s.lookupMany(ctx, hashes, false)
	if err != nil {
		fault := errors.NewStorageError("[utxoset][ParentOutputsForValidation] lookup", err)
		for i := range answers {
			answers[i].Err = fault
		}

		return answers, nil
	}

	var fromCoins []int

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

		status, height := parentStatus(data)

		if data.Tx == nil {
			answers[i].Status, answers[i].Height = status, height
			fromCoins = append(fromCoins, i)

			continue
		}

		if int(op.Vout) >= len(data.Tx.Outputs) || data.Tx.Outputs[op.Vout] == nil || data.Tx.Outputs[op.Vout].LockingScript == nil {
			answers[i].Status = utxo.ParentOutputNoSuchIndex
			continue
		}

		out := data.Tx.Outputs[op.Vout]
		script := bscript.Script(append([]byte(nil), *out.LockingScript...))
		answers[i] = utxo.ParentOutput{Status: status, Satoshis: out.Satoshis, LockingScript: &script, Height: height}
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

// readParentCoins fills the outputs of body-less parents from the UTXO table or the journal.
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

	for k, i := range idx {
		if !found[k] && answers[i].Err == nil {
			answers[i] = utxo.ParentOutput{Status: utxo.ParentOutputNoSuchIndex}
		}
	}
}
