package utxoset

import (
	"bytes"
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
)

// setFlagsSQL ORs or ANDs a flag mask onto matching UTXO rows.
//
// $4 is OR-ed in, $5 is AND-ed, so one statement serves both freeze and unfreeze. The
// full 32-byte txid recheck is present for the same reason it is everywhere else: the
// ukey is a non-unique 96-bit prefix and can locate a row but never authorise acting on
// it.
const setFlagsSQL = `
UPDATE utxo u
   SET flags = (u.flags | $4::smallint) & $5::smallint
  FROM unnest($1::smallint[], $2::uuid[], $3::bytea[]) AS k(leaf, ukey, txid)
 WHERE u.leaf = k.leaf AND u.ukey = k.ukey AND u.txid = k.txid`

// FreezeUTXOs marks outputs unspendable.
//
// The flag lives on the UTXO row, so the spend path enforces it directly: a frozen
// coin fails the DELETE's own predicate rather than being caught by a separate lookup
// that could race the spend. There is no window in which a freeze has been recorded but
// a concurrent spend still succeeds.
func (s *Store) FreezeUTXOs(ctx context.Context, spends []*utxo.Spend, _ *settings.Settings) error {
	return s.setFlags(ctx, spends, FlagFrozen, ^int16(0), "FreezeUTXOs")
}

// UnFreezeUTXOs clears the frozen flag.
func (s *Store) UnFreezeUTXOs(ctx context.Context, spends []*utxo.Spend, _ *settings.Settings) error {
	return s.setFlags(ctx, spends, 0, ^FlagFrozen, "UnFreezeUTXOs")
}

func (s *Store) setFlags(ctx context.Context, spends []*utxo.Spend, orMask, andMask int16, op string) error {
	if len(spends) == 0 {
		return nil
	}

	leaves := make([]int16, 0, len(spends))
	ukeys := make([][16]byte, 0, len(spends))
	txids := make([][]byte, 0, len(spends))

	for _, sp := range spends {
		if sp == nil || sp.TxID == nil {
			continue
		}

		leaves = append(leaves, LeafFor(sp.TxID[:]))
		ukeys = append(ukeys, Pack(sp.TxID[:], sp.Vout))
		txids = append(txids, sp.TxID[:])
	}

	if len(ukeys) == 0 {
		return nil
	}

	tag, err := s.pool.Exec(ctx, setFlagsSQL, leaves, ukeys, txids, orMask, andMask)
	if err != nil {
		return errors.NewStorageError("[utxoset][%s]", op, err)
	}

	if tag.RowsAffected() != int64(len(ukeys)) {
		// A freeze that silently missed is a freeze that did not happen, and the caller
		// would carry on believing the coin is immobilised. The usual cause is that the
		// output is already spent -- delete-on-spend leaves no row to flag.
		return errors.NewProcessingError("[utxoset][%s] affected %d of %d outputs; the rest are already spent or were never created",
			op, tag.RowsAffected(), len(ukeys))
	}

	return nil
}

// getSpendSQL answers "what happened to this outpoint" from the UTXO table first and the
// journal second.
//
// The journal is doing double duty here. It exists so a reorg can restore a coin, but
// because it records spending_txid it is also the ONLY place this store can recover
// SPENDER IDENTITY -- the UTXO row is gone, and absence alone cannot distinguish
// "spent by X" from "never existed". That identity is therefore available exactly as far
// back as journal retention, and no further: beyond it the honest answer is NOT_FOUND.
//
// It carries hash_override and spendable_from out with the flags because both change the
// answer. hash_override is non-NULL only on a coin ReAssignUTXO has moved, and the caller's
// UTXOHash must match it: the OLD owner asking about the outpoint they no longer own must be
// refused rather than shown the coin. spendable_from is what the reassignment delay is written
// into, and the same column carries coinbase maturity, so reading it here is what makes this
// report IMMATURE for both without a second rule.
const getSpendSQL = `
SELECT 'live'::text, u.flags, NULL::bytea, u.hash_override, u.spendable_from
  FROM utxo u
 WHERE u.leaf = $1 AND u.ukey = $2 AND u.txid = $3
UNION ALL
SELECT 'spent'::text, j.flags, j.spending_txid, j.hash_override, j.spendable_from
  FROM spend_journal j
 WHERE j.ukey = $2 AND j.txid = $3
 LIMIT 1`

// GetSpend reports whether an outpoint is unspent, frozen, or spent.
func (s *Store) GetSpend(ctx context.Context, sp *utxo.Spend) (*utxo.SpendResponse, error) {
	if sp == nil || sp.TxID == nil {
		return nil, errors.NewProcessingError("[utxoset][GetSpend] nil spend")
	}

	ukey := Pack(sp.TxID[:], sp.Vout)

	rows, err := s.pool.Query(ctx, getSpendSQL, LeafFor(sp.TxID[:]), ukey, sp.TxID[:])
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][GetSpend]", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			where         string
			flags         int16
			spender       []byte
			hashOverride  []byte
			spendableFrom int32
		)

		if err := rows.Scan(&where, &flags, &spender, &hashOverride, &spendableFrom); err != nil {
			return nil, errors.NewStorageError("[utxoset][GetSpend] scan", err)
		}

		// A reassigned coin answers only to the hash of the output it was reassigned TO. The
		// old owner names the same outpoint with the old hash, and telling them the coin is
		// there and unspent would be telling them it is still theirs.
		if err := reassignedHashMismatch(sp, hashOverride); err != nil {
			return nil, err
		}

		if where == "live" {
			return liveSpendResponse(flags, spendableFrom, s.GetBlockHeight()), nil
		}

		hash, hErr := chainhash.NewHash(spender)
		if hErr != nil {
			return nil, errors.NewProcessingError("[utxoset][GetSpend] malformed spender in journal", hErr)
		}

		return &utxo.SpendResponse{
			Status:       int(utxo.Status_SPENT),
			SpendingData: spendpkg.NewSpendingData(hash, 0),
		}, nil
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[utxoset][GetSpend] rows", err)
	}

	// Absent from both. Either it never existed, or it was spent longer ago than the
	// journal retains. This store cannot tell those apart, and says NOT_FOUND rather
	// than guessing.
	return &utxo.SpendResponse{Status: int(utxo.Status_NOT_FOUND)}, nil
}

// reassignedHashMismatch refuses a lookup that names a reassigned coin by the wrong hash.
//
// Only reassigned coins have a stored hash to compare against: this store keeps the satoshis
// and the locking script themselves, not a digest, so a normal coin has nothing here and any
// UTXOHash the caller supplies goes unchecked, exactly as before. That is the same position
// the spend path takes, and the same one the sql store takes when the caller passes nil.
//
// A caller that supplies no hash is trusted, because the row was located by the outpoint and
// the full txid. The bulk UTXO endpoints do this deliberately rather than fetch whole
// transactions to recompute a digest they already have the outpoint for.
func reassignedHashMismatch(sp *utxo.Spend, hashOverride []byte) error {
	if len(hashOverride) == 0 || sp.UTXOHash == nil {
		return nil
	}

	if bytes.Equal(sp.UTXOHash[:], hashOverride) {
		return nil
	}

	return errors.NewUtxoHashMismatchError("[utxoset][GetSpend] %s:%d was reassigned; %s is no longer its utxo hash",
		sp.TxID, sp.Vout, sp.UTXOHash)
}

// liveSpendResponse turns one live coin's flags into the answer the interface reports.
//
// The tests are written as successive overrides rather than as an if/else chain, and the ORDER
// IS THE PRECEDENCE. Least specific first: not yet spendable, then immobilised by the alert
// system, then lost a double-spend race, then held by conflict resolution. A coin can be
// several of these at once and the caller gets one status, so which one it gets is a contract
// rather than an implementation detail.
//
// The order is AEROSPIKE'S, not a consensus of both references. The two disagree about one
// thing and only one: aerospike applies the spendable-until hold first, so conflicting and
// locked override it, while the sql store applies it LAST (sql.go:3545-3563), so a coin that is
// both held and conflicting reports IMMATURE there and CONFLICTING here. Aerospike is followed
// because it keeps the most specific state, and because it leaves this store's own pre-existing
// frozen-over-immature answer unchanged.
//
// A frozen coin keeps its SENTINEL SPENDER even when a later test overrides the status. That is
// not tidiness: conflict resolution reads the spending data to recognise a frozen coin, and
// dropping it because the coin is also conflicting would hide the freeze from the one caller
// that acts on it. Both reference stores keep it for the same reason, and on that they agree.
func liveSpendResponse(flags int16, spendableFrom int32, height uint32) *utxo.SpendResponse {
	resp := &utxo.SpendResponse{Status: int(utxo.Status_OK)}

	// Exists but not yet spendable: a coinbase inside its maturity window, or a reassigned
	// coin inside the delay that lets the reassignment be challenged before the new owner can
	// move it. Reporting OK would say the coin is spendable now, which is the one thing
	// spendable_from exists to deny.
	if spendableFrom > int32(height) { //nolint:gosec // block heights are far below 2^31
		resp.Status = int(utxo.Status_IMMATURE)
	}

	if flags&FlagFrozen != 0 {
		resp.Status = int(utxo.Status_FROZEN)
		resp.SpendingData = spendpkg.NewSpendingData(&subtree.FrozenBytesTxHash, 0)
	}

	// The coin bit, not the identity bit. The spend path reads this one and never looks at the
	// identity row, so this is the flag that actually decides whether the coin moves, and
	// reporting it is what lets a caller see the refusal coming.
	if flags&FlagConflicting != 0 {
		resp.Status = int(utxo.Status_CONFLICTING)
	}

	// The most specific answer, and the last word. Conflict resolution locks a parent while it
	// swaps which child owns the coin, so LOCKED says "held, briefly, by an operation in
	// flight" where CONFLICTING says "this coin's spender lost". A coin can be both at once
	// mid-resolution and the more transient fact is the one that explains the refusal.
	if flags&FlagLocked != 0 {
		resp.Status = int(utxo.Status_LOCKED)
	}

	return resp
}
