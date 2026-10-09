package utxoset

import (
	"context"
	"encoding/binary"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
)

// spendingDataSQL reports, for one transaction, which of its outputs still exist and which were
// taken and by whom.
//
// Both halves are needed, and neither alone would do. A live UTXO proves an output exists and
// is unspent. A journal row proves one existed and names the transaction that destroyed it. An
// output that appears in neither is one this store cannot speak for: either it never existed,
// because a provably unspendable output never gets a row, or its spend is older than the
// journal's retention.
//
// Located by a key RANGE and authorised by the full 32-byte transaction id. The packed key
// leads with the id prefix so that "every output of this transaction" is an index range scan,
// but that prefix is 96 bits and non-unique by design. Without the recheck a prefix collision
// would name a stranger as the spender of this transaction's UTXO, and the conflict walk would
// then mark that stranger conflicting along with everything descended from it.
//
// The live arm carries the UTXO's flags because a FROZEN output is not answered as unspent: see
// decorateSpendingData. The journal arm's flags are the spent UTXO's last state and mean nothing
// here, so it reports 0.
const spendingDataSQL = `
SELECT 'live'::text AS kind, u.ukey, NULL::bytea AS spender, 0::int AS spender_vin, u.flags
  FROM utxo u
 WHERE u.leaf = $1 AND u.ukey BETWEEN $2 AND $3 AND u.txid = $4
UNION ALL
SELECT 'spent'::text, j.ukey, j.spending_txid, j.spending_vin, 0::smallint
  FROM spend_journal j
 WHERE j.ukey BETWEEN $2 AND $3 AND j.txid = $4`

// wantsSpendingData reports whether the caller asked for the per-output spend state.
//
// It is the one field this store does not answer for free. Everything else on a metadata read
// arrives on a single row from a single statement, so narrowing the projection would save
// nothing, but naming the spender of every output costs a second query across two tables. The
// validator resolves parents constantly and never needs it, so it stays off that path unless
// asked for.
func wantsSpendingData(fieldNames []fields.FieldName) bool {
	for _, f := range fieldNames {
		if f == fields.Utxos || f == fields.SpentUtxos {
			return true
		}
	}

	return false
}

// wantsConflictingChildren reports whether the caller asked which losing transactions contest
// this one's UTXOs.
//
// It is the second field this store does not answer for free, and for a different reason than
// the first: the answer is in conflict_children, keyed on the txid rather than carried on
// whichever row answered the read, so naming it costs a statement. The shared conflict walks
// ask for it explicitly; the validator's parent resolution never does, and that is the path
// this keeps it off.
func wantsConflictingChildren(fieldNames []fields.FieldName) bool {
	for _, f := range fieldNames {
		if f == fields.ConflictingChildren {
			return true
		}
	}

	return false
}

// decorateSpendingData fills in who took each of a transaction's outputs.
//
// The shared conflict walks ask this question of every parent they reach and act on the answer,
// and they ask it through the metadata read rather than through any store method. This store
// deletes the UTXO row on spend, so the answer is not in the UTXO table: it is in the journal,
// which recorded the spender at the moment of the delete. Until this existed the walks saw an
// empty answer for every parent, failed on the first input, and conflict handling could not run
// here at all.
//
// The slice is indexed by output number and sized to the highest output the store knows about.
// An input naming an output beyond that is one this store genuinely cannot speak for, and the
// caller's own range check reports it rather than this silently reporting the output as
// unspent, which is the answer that would let a double spend through.
//
// The input index on each entry is the journal's spending_vin, which input of the spending
// transaction consumed the UTXO.
//
// A FROZEN output gets the frozen sentinel, subtree.FrozenBytesTxHash, as its spender, with the
// output number as the input index, which is what both reference stores put there. It is not
// spent, but the shared conflict walk recognises a frozen coin by that sentinel and nothing
// else: GetConflictingChildren keeps it in the walk's result, and GetCounterConflictingTxHashes
// refuses to demote a transaction with a frozen descendant on seeing it. Reporting a frozen
// output as nil, like an unspent one, hid it from both.
//
// Bounded by the journal's retention, like every other spender-identity answer in this store.
// Beyond it a spent output looks the same as one that never existed.
func (s *Store) decorateSpendingData(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	rows, err := s.pool.Query(ctx, spendingDataSQL,
		LeafFor(hash[:]), Pack(hash[:], 0), Pack(hash[:], ^uint32(0)), hash[:])
	if err != nil {
		return errors.NewStorageError("[utxoset][Get] spending data %s", hash.String(), err)
	}

	type entry struct {
		vout       uint32
		spender    []byte
		spenderVin int32
		frozen     bool
	}

	found := make([]entry, 0, 8)
	maxVout := -1

	for rows.Next() {
		var (
			kind       string
			ukey       [16]byte
			spender    []byte
			spenderVin int32
			flags      int16
		)

		if err := rows.Scan(&kind, &ukey, &spender, &spenderVin, &flags); err != nil {
			rows.Close()
			return errors.NewStorageError("[utxoset][Get] spending data scan %s", hash.String(), err)
		}

		vout := binary.BigEndian.Uint32(ukey[12:16])
		if int(vout) > maxVout {
			maxVout = int(vout)
		}

		found = append(found, entry{vout: vout, spender: spender, spenderVin: spenderVin,
			frozen: len(spender) == 0 && flags&FlagFrozen != 0})
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][Get] spending data rows %s", hash.String(), err)
	}

	if maxVout < 0 {
		return nil
	}

	out := make([]*spendpkg.SpendingData, maxVout+1)

	for _, e := range found {
		if e.frozen {
			out[e.vout] = spendpkg.NewSpendingData(&subtree.FrozenBytesTxHash, int(e.vout))
			continue
		}

		if len(e.spender) == 0 {
			continue // a live UTXO: still unspent, so no entry
		}

		h, herr := chainhash.NewHash(e.spender)
		if herr != nil {
			return errors.NewStorageError("[utxoset][Get] spender hash %s", hash.String(), herr)
		}

		out[e.vout] = spendpkg.NewSpendingData(h, int(e.spenderVin))
	}

	data.SpendingDatas = out

	return nil
}
