package utxoset

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// unspendSQL restores UTXOs from the journal and CONSUMES the journal rows doing it.
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
// completion record for its intent means replay calls Unspend again on UTXOs it already
// restored. The journal being single-use means that second call's DELETE matches nothing,
// which by itself is indistinguishable from the UTXO being genuinely unrestorable (its
// journal partition already reclaimed, or the outpoint re-spent by someone else since). The
// third output column resolves the ambiguity: it counts, from the state as it stood BEFORE this
// statement touched anything (every CTE here reads that same pre-statement snapshot,
// including live_before, so a key this call itself restores is not double-counted), how
// many requested keys already had a live UTXO at that ukey+txid. A key can be in `taken`
// (journal row existed and was consumed) or already live before the call, never both -- a
// UTXO cannot be simultaneously spent-and-journaled and live -- so restored+alreadyLive
// partitions the requested set cleanly between "this call did the work", "someone already
// did", and "genuinely gone". The check deliberately ignores which spender's undo made the
// UTXO live: the UTXO is unspent either way, so a replayed Unspend naming a different (or
// stale) spending_txid than whatever actually restored it is still the correct no-op --
// ownership only gates the journal consume, never the already-live short-circuit.
//
// Three predicates on the journal delete, each load-bearing:
//
//	j.ukey = i.ukey            locates candidates -- and is the journal's only index
//	j.txid = i.ptxid           full 32 bytes: the ukey is a non-unique 96-bit prefix and
//	                           can never establish identity on its own
//	j.spending_txid = i.stxid  THE OWNERSHIP TOKEN. A restore must name the spender that
//	                           actually took the UTXO. A stale reorg record whose output
//	                           has since been re-spent by a DIFFERENT transaction matches
//	                           nothing and is a no-op, rather than resurrecting a UTXO
//	                           that now belongs to someone else.
//
// The NOT EXISTS guard is belt-and-braces: with a non-unique key a concurrent re-create
// could otherwise produce a duplicate live row for one outpoint, which is counterfeit.
//
// $4 carries the flags to OR in, so a caller asking for flagAsLocked gets the UTXO back
// already locked rather than briefly spendable. IT REACHES BOTH OUTCOMES, the UTXO this call
// restores and the UTXO it found already live, and the second arm was missing.
//
// "Restore these UTXOs AND hold them" is one instruction, and the hold is what stops anyone
// else spending a contested parent while conflict resolution decides which child gets it. ORing
// the flag only into the rows the INSERT produced meant a parent whose UTXO was already live
// came back unheld -- which is exactly the parent SetConflicting now names, and exactly the
// state a crash between the unspend and the lock leaves behind. That parent stayed spendable
// for the whole of the resolution and then had the driver's closing SetLocked(false) applied to
// it anyway, dropping any unrelated lock it happened to carry. The sql reference locks the
// transaction row unconditionally, which is why it has never had this gap.
//
// The locking arm is gated on $4 being non-zero, so an ordinary reorg restore, which asks for no
// hold, issues no update. It matches the live UTXO by its exact (leaf, ukey) with the full
// 32-byte txid rechecked -- the packed-key bound schema.go requires of every by-txid UTXO
// access -- and it cannot collide with the INSERT above: every CTE here reads the same
// pre-statement snapshot, so the rows this arm can see are precisely the ones `restored`
// excluded itself from touching.
//
// The restored UTXO's pair is the copy's own, verbatim, whenever it is non-zero: a non-zero
// pair was final when it was copied and stays final. A copy at (0,0) is the REPAIR's case. The
// UTXO was spent while still unstamped, and if its window has since been stamped, the stamp
// found no live UTXO to write onto and has deleted the identity row, so nothing would ever
// come back for it. The repair reads the copy's containment rows below the stamp fence ($6),
// at or above the dropped floor ($5). Below the fence every row left names a winner, because
// the stamp deleted the losers in the same transaction that raised the fence, so exactly one
// row means its pair is written onto the restored UTXO here, with no chain access. No row and
// an identity row means the transaction is unmined, or its window is not yet stamped: (0,0) is
// kept and the stamp will reach it through the identity row. No row and no identity row, or
// more than one row, are the two failures the wrapper turns into a rolled-back storage error.
// The fence itself is read after the shared fence lock, in the same transaction, so it cannot
// move under this statement. Both floors are scalars, so the planner reads only the windows
// between them.
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
won AS (
    SELECT t.ukey, t.txid, c.n, c.mined_height, c.block_id,
           EXISTS (SELECT 1 FROM tx_ident i
                    WHERE i.leaf = (get_byte(t.txid, 0) & 7)::smallint
                      AND i.txid = t.txid) AS has_ident
      FROM taken t
     CROSS JOIN LATERAL (
       SELECT count(*) AS n, min(m.mined_height) AS mined_height, min(m.block_id) AS block_id
         FROM tx_mined m
        WHERE m.txid = t.txid
          AND m.mined_height >= $5::int
          AND m.mined_height <  $6::int
       OFFSET 0
     ) AS c
     WHERE t.mined_height = 0
),
restored AS (
    INSERT INTO utxo (leaf, txid, ukey, satoshis, script, created_height,
                      spendable_from, flags, hash_override, mined_height, block_id)
    SELECT (get_byte(t.txid, 0) & 7)::smallint, t.txid, t.ukey, t.satoshis, t.script,
           t.created_height, t.spendable_from, t.flags | $4::smallint, t.hash_override,
           CASE WHEN t.mined_height > 0 THEN t.mined_height
                WHEN w.n = 1 THEN w.mined_height ELSE 0 END,
           CASE WHEN t.mined_height > 0 THEN t.block_id
                WHEN w.n = 1 THEN w.block_id ELSE 0 END
      FROM taken t
      LEFT JOIN won w ON w.ukey = t.ukey AND w.txid = t.txid
     WHERE NOT EXISTS (
           SELECT 1 FROM utxo u
            WHERE u.leaf = (get_byte(t.txid, 0) & 7)::smallint
              AND u.ukey = t.ukey
              AND u.txid = t.txid)
    RETURNING ukey
),
live_before AS (
    -- Requested keys that already had a live UTXO before this statement touched
    -- anything -- a prior Unspend's work, or a UTXO nobody ever spent, seen here
    -- because every CTE in one WITH query reads the same pre-statement snapshot
    -- regardless of execution order. Ownership (stxid) is deliberately not checked:
    -- the UTXO is unspent either way, so it does not matter whose undo put it there.
    --
    -- The keys drive a LATERAL with an OFFSET 0 fence, the shape stampUTXOsSQL uses
    -- and for the identical reason. Written as a plain WHERE EXISTS subquery,
    -- the planner hashes the whole UTXO table against the keys: measured on this
    -- schema at 40,000 UTXOs across all eight partitions with 500 keys, a Hash Semi
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
    -- The hold on the UTXOs this call did not have to restore. Same instruction, same flags,
    -- other outcome. No-op when the caller asked for no flags.
    --
    -- It matches on the exact (leaf, ukey) the fenced read above returned, which is the other
    -- half of stampUTXOsSQL's shape: the read finds the rows by index and the update names
    -- them, rather than the update searching for them itself.
    UPDATE utxo u
       SET flags = u.flags | $4::smallint
      FROM live_before b
     WHERE $4::smallint <> 0
       AND u.leaf = b.leaf
       AND u.ukey = b.ukey
       AND u.txid = b.ptxid
)
SELECT (SELECT count(*) FROM restored),
       (SELECT count(*) FROM items),
       (SELECT count(*) FROM live_before),
       (SELECT count(*) FROM won WHERE n = 1),
       (SELECT count(*) FROM won WHERE n = 0 AND NOT has_ident),
       (SELECT count(*) FROM won WHERE n > 1),
       (SELECT string_agg(encode(txid, 'hex'), ',')
          FROM (SELECT DISTINCT txid FROM won WHERE (n = 0 AND NOT has_ident) OR n > 1 LIMIT 10) AS bad)`

// Unspend restores previously spent UTXOs from the spend journal.
//
// Used by reorg handling and by conflicting-transaction resolution. Note that a plain
// block disconnect does NOT come through here: a transaction in a disconnected block is
// still valid and returns to assembly unmined, so its spends must STAY -- restoring them
// would create a UTXO that a still-live transaction is already spending.
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
			// outpoint alone could resurrect a UTXO a different transaction now owns.
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

	// One short transaction of three statements: the shared fence lock first and on its own,
	// because whether a restored pair is (0,0) is not known until the undo row has been read;
	// the fence, read after the lock so it cannot move under the restore; then the restore.
	dbTx, err := s.pool.Begin(ctx)
	if err != nil {
		return errors.NewStorageError("[utxoset][Unspend] begin", err)
	}

	defer func() { _ = dbTx.Rollback(ctx) }()

	fence, err := s.takeFenceShared(ctx, dbTx)
	if err != nil {
		return err
	}

	var (
		restored, requested, alreadyLive int
		repaired, noSource, ambiguous    int
		offending                        *string
	)

	if err := dbTx.QueryRow(ctx, unspendSQL, ukeys, ptxids, stxids, extraFlags,
		int32(fence.droppedFloor), int32(fence.fence)). //nolint:gosec // heights fit int32
		Scan(&restored, &requested, &alreadyLive, &repaired, &noSource, &ambiguous, &offending); err != nil {
		return errors.NewStorageError("[utxoset][Unspend] restore", err)
	}

	// One bad UTXO fails the whole call, before the commit, so no UTXO of the call is restored
	// and no undo copy is consumed. That matches how the accounting failure below behaves.
	if noSource > 0 || ambiguous > 0 {
		names := ""
		if offending != nil {
			names = *offending
		}

		if noSource > 0 {
			unspendRepairNoSource.Inc()

			return errors.NewStorageError("[utxoset][Unspend] %d UTXOs restored at (0,0) have no containment row below the stamp fence %d and no identity row, so nothing could ever stamp them; the call is rolled back (%s)",
				noSource, fence.fence, names)
		}

		unspendRepairAmbiguous.Inc()

		return errors.NewStorageError("[utxoset][Unspend] %d UTXOs restored at (0,0) have more than one containment row below the stamp fence %d, where every row should name a winner; the call is rolled back (%s)",
			ambiguous, fence.fence, names)
	}

	if err := dbTx.Commit(ctx); err != nil {
		return errors.NewStorageError("[utxoset][Unspend] commit", err)
	}

	if repaired > 0 {
		unspendRepaired.Add(float64(repaired))
	}

	if restored+alreadyLive != requested {
		// Silence here would be the dangerous outcome: a reorg that believes it has
		// restored UTXOs which are in fact still missing leaves the UTXO set wrong and
		// consensus-divergent, with nothing to indicate it. Either every requested UTXO
		// is now accounted for -- restored by this call or already live from an earlier
		// one -- or the caller must know it is not.
		//
		// The usual causes are a journal partition already reclaimed (the spend is older
		// than retention), or a spender mismatch meaning the UTXO was re-spent by a
		// different transaction in the meantime. alreadyLive covers the third, benign
		// cause -- a replayed Unspend on a UTXO a previous call already restored, or one
		// nobody ever spent -- so it is never itself part of what is missing here. Those
		// UTXOs are a full success rather than a tolerated miss: with flagAsLocked the
		// `held` arm above has just put the hold on them, so the caller gets the UTXO
		// unspent and held, which is the whole instruction.
		return errors.NewProcessingError("[utxoset][Unspend] restored %d, already live %d, of %d requested; the rest are beyond journal retention or were re-spent by a different transaction",
			restored, alreadyLive, requested)
	}

	return nil
}
