package utxoset

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// unspendSQL restores coins from the journal and CONSUMES the journal rows doing it.
//
// The journal row IS the authorisation. Deleting it as part of the restore makes the
// operation single-use: a second Unspend of the same outpoint finds no journal row to
// consume. That is what keeps invariant 5 intact -- the restore is authorised by the
// presence of a durable row, not by a counter that could drift. A counter drift is
// exactly what once stamped a live transaction and cascade-deleted it into a
// TX_NOT_FOUND wedge.
//
// A second call on an outpoint the first call already restored must be a no-op success,
// not an error -- BlockAssembler's conflict-intent WAL replay (stores/utxo/process_conflicting.go)
// depends on Unspend tolerating exactly this: a crash between a successful Unspend and the
// completion record for its intent means replay calls Unspend again on coins it already
// restored. The journal being single-use means that second call's DELETE matches nothing,
// which by itself is indistinguishable from the coin being genuinely unrestorable (its
// journal partition already reclaimed, or the outpoint re-spent by someone else since). The
// third output column resolves the ambiguity: it counts, from the state as it stood BEFORE this
// statement touched anything (every CTE here reads that same pre-statement snapshot,
// including live_before, so a key this call itself restores is not double-counted), how
// many requested keys already had a live coin at that ukey+txid. A key can be in `taken`
// (journal row existed and was consumed) or already live before the call, never both -- a
// coin cannot be simultaneously spent-and-journaled and live -- so restored+alreadyLive
// partitions the requested set cleanly between "this call did the work", "someone already
// did", and "genuinely gone". The check deliberately ignores which spender's undo made the
// coin live: the coin is unspent either way, so a replayed Unspend naming a different (or
// stale) spending_txid than whatever actually restored it is still the correct no-op --
// ownership only gates the journal consume, never the already-live short-circuit.
//
// Three predicates on the journal delete, each load-bearing:
//
//	j.ukey = i.ukey            locates candidates -- and is the journal's only index
//	j.txid = i.ptxid           full 32 bytes: the ukey is a non-unique 96-bit prefix and
//	                           can never establish identity on its own
//	j.spending_txid = i.stxid  THE OWNERSHIP TOKEN. A restore must name the spender that
//	                           actually took the coin. A stale reorg record whose output
//	                           has since been re-spent by a DIFFERENT transaction matches
//	                           nothing and is a no-op, rather than resurrecting a coin
//	                           that now belongs to someone else.
//
// The NOT EXISTS guard is belt-and-braces: with a non-unique key a concurrent re-create
// could otherwise produce a duplicate live row for one outpoint, which is counterfeit.
//
// $4 carries the flags to OR in, so a caller asking for flagAsLocked gets the coin back
// already locked rather than briefly spendable. IT REACHES BOTH OUTCOMES, the coin this call
// restores and the coin it found already live, and the second arm was missing.
//
// "Restore these coins AND hold them" is one instruction, and the hold is what stops anyone
// else spending a contested parent while conflict resolution decides which child gets it. ORing
// the flag only into the rows the INSERT produced meant a parent whose coin was already live
// came back unheld -- which is exactly the parent SetConflicting now names, and exactly the
// state a crash between the unspend and the lock leaves behind. That parent stayed spendable
// for the whole of the resolution and then had the driver's closing SetLocked(false) applied to
// it anyway, dropping any unrelated lock it happened to carry. The sql reference locks the
// transaction row unconditionally, which is why it has never had this gap.
//
// The locking arm is gated on $4 being non-zero, so an ordinary reorg restore, which asks for no
// hold, issues no update. It matches the live coin by its exact (leaf, ukey) with the full
// 32-byte txid rechecked -- the packed-key bound schema.go requires of every by-txid coin
// access -- and it cannot collide with the INSERT above: every CTE here reads the same
// pre-statement snapshot, so the rows this arm can see are precisely the ones `restored`
// excluded itself from touching.
//
// The restored coin's block facts are RE-RESOLVED, in three preferences, and the order is the
// immutability rule rather than a convenience.
//
// tx_mined first. Block facts can change after the spend was recorded -- a reorg can move a
// still-live parent to a different block -- so while the membership row exists it is the
// record that gets rewritten and the journal's copy is not. Reading it fresh by the parent's
// txid is what keeps a restore honest. ORDER BY seq LIMIT 1 picks the earliest row on the rare
// chance more than one exists (a coinbase re-org can leave a parent claimed at more than one
// height); seq is a global identity so "earliest" is well defined without touching mined_height.
//
// Then the journal's own copy, gated on mined_height > 0. Once the membership window has
// retired there is nothing left to re-resolve from, and before this the restore put back the
// unconfirmed sentinel on a coin that was demonstrably mined -- a coin claiming no block at
// all, which the read order would then answer from as if the transaction were in the mempool.
// The copy is safe to trust in exactly this case for the same reason readSpentParents is: a
// window that has retired is at least 1440 blocks deep and its block cannot change.
//
// Then 0, which is the unconfirmed sentinel and the correct answer for a parent that was
// genuinely unconfirmed when it was spent. Both columns move together in every branch, because
// each pair comes from one row.
const unspendSQL = `
WITH items AS (
    SELECT * FROM unnest($1::uuid[], $2::bytea[], $3::bytea[]) AS t(ukey, ptxid, stxid)
),
taken AS (
    DELETE FROM spend_journal j USING items i
     WHERE j.ukey          = i.ukey
       AND j.txid          = i.ptxid
       AND j.spending_txid = i.stxid
    RETURNING j.ukey, j.txid, j.satoshis, j.script, j.created_height,
              j.spendable_from, j.flags, j.hash_override, j.mined_height, j.block_id
),
restored AS (
    INSERT INTO utxo (leaf, txid, ukey, satoshis, script, created_height,
                      spendable_from, flags, hash_override, mined_height, block_id)
    SELECT (get_byte(t.txid, 0) & 7)::smallint, t.txid, t.ukey, t.satoshis, t.script,
           t.created_height, t.spendable_from, t.flags | $4::smallint, t.hash_override,
           COALESCE((SELECT m.mined_height FROM tx_mined m WHERE m.txid = t.txid ORDER BY m.seq LIMIT 1),
                    CASE WHEN t.mined_height > 0 THEN t.mined_height END, 0),
           COALESCE((SELECT m.block_id     FROM tx_mined m WHERE m.txid = t.txid ORDER BY m.seq LIMIT 1),
                    CASE WHEN t.mined_height > 0 THEN t.block_id END, 0)
      FROM taken t
     WHERE NOT EXISTS (
           SELECT 1 FROM utxo u
            WHERE u.leaf = (get_byte(t.txid, 0) & 7)::smallint
              AND u.ukey = t.ukey
              AND u.txid = t.txid)
    RETURNING ukey
),
live_before AS (
    -- Requested keys that already had a live coin before this statement touched
    -- anything -- a prior Unspend's work, or a coin nobody ever spent, seen here
    -- because every CTE in one WITH query reads the same pre-statement snapshot
    -- regardless of execution order. Ownership (stxid) is deliberately not checked:
    -- the coin is unspent either way, so it does not matter whose undo put it there.
    --
    -- The keys drive a LATERAL with an OFFSET 0 fence, the shape stampCoinsSQL uses
    -- and for the identical reason. Written as a plain WHERE EXISTS subquery,
    -- the planner hashes the whole coin table against the keys: measured on this
    -- schema at 40,000 coins across all eight partitions with 500 keys, a Hash Semi
    -- Join over a Seq Scan of every one of utxo_p0..p7. LIMIT 1 keeps one row per
    -- requested key, so the count below still partitions the request cleanly.
    SELECT c.leaf, c.ukey, k.ptxid
      FROM unnest($1::uuid[], $2::bytea[]) AS k(ukey, ptxid)
     CROSS JOIN LATERAL (
       SELECT u.leaf, u.ukey
         FROM utxo u
        WHERE u.leaf = (get_byte(k.ptxid, 0) & 7)::smallint
          AND u.ukey = k.ukey
          AND u.txid = k.ptxid
        LIMIT 1 OFFSET 0
     ) AS c
),
held AS (
    -- The hold on the coins this call did not have to restore. Same instruction, same flags,
    -- other outcome. No-op when the caller asked for no flags.
    --
    -- It matches on the exact (leaf, ukey) the fenced read above returned, which is the other
    -- half of stampCoinsSQL's shape: the read finds the rows by index and the update names
    -- them, rather than the update searching for them itself.
    UPDATE utxo u
       SET flags = u.flags | $4::smallint
      FROM live_before b
     WHERE $4::smallint <> 0
       AND u.leaf = b.leaf
       AND u.ukey = b.ukey
       AND u.txid = b.ptxid
)
SELECT (SELECT count(*) FROM restored), (SELECT count(*) FROM items), (SELECT count(*) FROM live_before)`

// Unspend restores previously spent UTXOs from the spend journal.
//
// Used by reorg handling and by conflicting-transaction resolution. Note that a plain
// block disconnect does NOT come through here: a transaction in a disconnected block is
// still valid and returns to assembly unmined, so its spends must STAY -- restoring them
// would create a coin that a still-live transaction is already spending.
func (s *Store) Unspend(ctx context.Context, spends []*utxo.Spend, flagAsLocked ...bool) error {
	if len(spends) == 0 {
		return nil
	}

	var extraFlags int16
	if len(flagAsLocked) > 0 && flagAsLocked[0] {
		extraFlags |= FlagLocked
	}

	ukeys := make([][16]byte, 0, len(spends))
	ptxids := make([][]byte, 0, len(spends))
	stxids := make([][]byte, 0, len(spends))

	for _, sp := range spends {
		if sp == nil || sp.TxID == nil {
			continue
		}

		if sp.SpendingData == nil || sp.SpendingData.TxID == nil {
			// Without the spender there is no ownership token, and restoring on the
			// outpoint alone could resurrect a coin a different transaction now owns.
			// Refuse rather than guess.
			return errors.NewProcessingError("[utxoset][Unspend] %s:%d has no SpendingData; the spender is required as the restore ownership token",
				sp.TxID.String(), sp.Vout)
		}

		ukeys = append(ukeys, Pack(sp.TxID[:], sp.Vout))
		ptxids = append(ptxids, sp.TxID[:])
		stxids = append(stxids, sp.SpendingData.TxID[:])
	}

	if len(ukeys) == 0 {
		return nil
	}

	var restored, requested, alreadyLive int

	if err := s.pool.QueryRow(ctx, unspendSQL, ukeys, ptxids, stxids, extraFlags).
		Scan(&restored, &requested, &alreadyLive); err != nil {
		return errors.NewStorageError("[utxoset][Unspend] restore", err)
	}

	if restored+alreadyLive != requested {
		// Silence here would be the dangerous outcome: a reorg that believes it has
		// restored coins which are in fact still missing leaves the UTXO set wrong and
		// consensus-divergent, with nothing to indicate it. Either every requested coin
		// is now accounted for -- restored by this call or already live from an earlier
		// one -- or the caller must know it is not.
		//
		// The usual causes are a journal partition already reclaimed (the spend is older
		// than retention), or a spender mismatch meaning the coin was re-spent by a
		// different transaction in the meantime. alreadyLive covers the third, benign
		// cause -- a replayed Unspend on a coin a previous call already restored, or one
		// nobody ever spent -- so it is never itself part of what is missing here. Those
		// coins are a full success rather than a tolerated miss: with flagAsLocked the
		// `held` arm above has just put the hold on them, so the caller gets the coin
		// unspent and held, which is the whole instruction.
		return errors.NewProcessingError("[utxoset][Unspend] restored %d, already live %d, of %d requested; the rest are beyond journal retention or were re-spent by a different transaction",
			restored, alreadyLive, requested)
	}

	return nil
}
