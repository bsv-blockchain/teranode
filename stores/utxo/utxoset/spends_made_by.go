package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
)

// spendsMadeBySQL finds the coins one transaction took, and what each was worth.
//
// The caller supplies the outpoints, read from the permanent record of what the transaction
// spends. This asks the journal what it captured when each of those coins was destroyed, and
// requires the destroyer to be the transaction being asked about. A coin that some OTHER
// transaction took is not this one's to give back.
//
// Located by a key range and authorised by the full 32-byte transaction id, like every other
// coin lookup here, because the packed key is a non-unique 96-bit prefix that can find a row
// but must never justify acting on one. Getting that wrong would hand back a stranger's coin.
//
// A coin this transaction never took returns no row, and one whose record has aged out of the
// journal returns no row either. Both are omitted rather than guessed at. That is deliberate:
// Unspend fails the entire restore if a single record it is given cannot be restored, so
// padding the answer would break the undo rather than complete it.
const spendsMadeBySQL = `
SELECT k.vin, j.satoshis, j.script, j.hash_override
  FROM unnest($1::uuid[], $2::bytea[], $3::int[]) AS k(ukey, txid, vin)
  JOIN spend_journal j
    ON j.ukey = k.ukey AND j.txid = k.txid AND j.spending_txid = $4::bytea`

// SpendsMadeBy returns the coins this transaction consumed, as records this store's own Unspend
// can restore.
//
// It deliberately does NOT read the transaction. A coin's identity is computed partly from the
// amount and locking script of the output being spent, and a transaction only carries those when
// it is stored in extended form, which this store does not do. More importantly the transaction
// itself is not permanent here: its bytes are dropped once past their retention window, while a
// transaction that lost a double-spend is kept indefinitely because it may still need promoting.
// Anything answering this from the transaction would therefore stop working after a couple of
// days, silently, on exactly the transactions this is for.
//
// The two sources it uses instead both outlive that. The list of what a transaction spends is on
// the identity row and lasts as long as the transaction does. The journal copied down each
// coin's amount and locking script at the moment it was destroyed, and keeps them for the
// resubmission window the rest of the system promises.
//
// Called only when undoing a conflict resolution, so a chain reorganisation or a crash replay,
// never on the path an ordinary transaction takes.
func (s *Store) SpendsMadeBy(ctx context.Context, txHash chainhash.Hash) ([]*utxo.Spend, error) {
	inpoints, err := s.readConflictingInputs(ctx, []chainhash.Hash{txHash})
	if err != nil {
		return nil, err
	}

	ip := inpoints[txHash]
	flat := ip.GetTxInpoints()

	ukeys := make([][16]byte, 0, len(flat))
	ptxids := make([][]byte, 0, len(flat))
	vins := make([]int32, 0, len(flat))
	parents := make([]chainhash.Hash, 0, len(flat))
	vouts := make([]uint32, 0, len(flat))

	for _, in := range flat {
		// The coinbase placeholder is not a real parent and took no coin.
		if in.Hash == subtree.CoinbasePlaceholderHashValue {
			continue
		}

		parent := in.Hash

		ukeys = append(ukeys, Pack(parent[:], in.Index))
		ptxids = append(ptxids, parent[:])
		vins = append(vins, int32(len(parents))) //nolint:gosec // bounded by input count
		parents = append(parents, parent)
		vouts = append(vouts, in.Index)
	}

	if len(ukeys) == 0 {
		return nil, nil
	}

	rows, err := s.pool.Query(ctx, spendsMadeBySQL, ukeys, ptxids, vins, txHash[:])
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][SpendsMadeBy] %s", txHash.String(), err)
	}

	defer rows.Close()

	out := make([]*utxo.Spend, 0, len(ukeys))

	for rows.Next() {
		var (
			vin          int32
			satoshis     int64
			script       []byte
			hashOverride []byte
		)

		if err := rows.Scan(&vin, &satoshis, &script, &hashOverride); err != nil {
			return nil, errors.NewStorageError("[utxoset][SpendsMadeBy] scan %s", txHash.String(), err)
		}

		parent := parents[vin]
		spender := txHash

		sp := &utxo.Spend{
			TxID: &parent,
			Vout: vouts[vin],
			// The spender is the ownership token Unspend restores on, so a record without it
			// is refused outright.
			SpendingData: spendpkg.NewSpendingData(&spender, int(vin)),
		}

		// A reassigned coin carries its new identity, which wins over the computed one.
		if len(hashOverride) > 0 {
			if h, herr := chainhash.NewHash(hashOverride); herr == nil {
				sp.UTXOHash = h
				out = append(out, sp)

				continue
			}
		}

		// The other two stores put the coin's computed identity on these records, so this one
		// does too. Nothing in this store reads it, since Unspend restores on the outpoint and
		// the spender, but a record that travels to shared code should not be the odd one out.
		if h, herr := util.UTXOHash(&parent, vouts[vin], bscript.NewFromBytes(script), uint64(satoshis)); herr == nil { //nolint:gosec // satoshis are never negative
			sp.UTXOHash = h
		}

		out = append(out, sp)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[utxoset][SpendsMadeBy] rows %s", txHash.String(), err)
	}

	return out, nil
}
