package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5"
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
// It does not cover two journal rows for the same spend. The pruner's carry no longer makes
// them, because it moves the row rather than copying it (copyForwardSQL), but a database
// upgraded from a build whose carry was a copy can hold the original and its copy until the
// next pass over the leaf heals it, and both match all three predicates. Both are consumed,
// which is right, since a copy left behind would be a second authorisation. But the guard reads
// the pre-statement snapshot, in which the UTXO is absent for both, so both used to insert and
// one spent output came back as two live ones. The `one` CTE collapses them to one row per UTXO
// first.
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
// THE CONFLICTING BIT IS READ AT RESTORE TIME, never copied off the journal row. The row holds
// the flags the UTXO had when it was spent, and SetConflicting cannot reach it: it flags a
// transaction's LIVE UTXOs, and a spent one is not live. So for P -> A -> B with A and B both
// losing, A's output taken by B comes back from B's journal row carrying whatever A was when B
// spent it, normally not conflicting, and the driver's closing SetLocked(false) then left it
// spendable while A was still a loser. The other direction is the same fault: a UTXO spent
// through IgnoreConflicting while its transaction was conflicting, restored after the mark was
// cleared, came back refused. $7 is FlagConflicting, and the `cf` LATERAL asks the identity row
// and every containment row from the dropped floor up, the same three places SetConflicting
// writes, whether the transaction is conflicting now. A transaction found in none of them keeps
// the journal's bit, which is what this did before: nothing can mark it, since SetConflicting
// refuses a transaction it does not hold.
//
// The locked bit is still copied off the row, and it can be stale the same way: a winner spends
// a parent through IgnoreLocked while conflict resolution's hold is on it, so the row records
// the lock, and a later ReverseProcessConflicting restore brings the parent back locked. The
// counter's re-spend straight after normally consumes it; when that re-spend is skipped (a
// counter with no body) the parent stays locked. Deriving the lock needs the lookup floor,
// because SetLocked leaves containment rows below it stale, and is left for its own change.
//
// The restored UTXO's pair is the copy's own, verbatim, whenever it is non-zero: a non-zero
// pair was final when it was copied and stays final. A copy at (0,0) is the REPAIR's case. The
// UTXO was spent while still unstamped, and if its partition has since been stamped, the stamp
// found no live UTXO to write onto and has deleted the identity row, so nothing would ever
// come back for it. The repair reads the copy's containment rows below the stamp fence ($6),
// at or above the dropped floor ($5). Below the fence every row left names a winner, because
// the stamp deleted the losers in the same transaction that raised the fence, so exactly one
// row means its pair is written onto the restored UTXO here, with no chain access. No row and
// an identity row means the transaction is unmined, or its partition is not yet stamped: (0,0) is
// kept and the stamp will reach it through the identity row. No row and no identity row, or
// more than one row, are the two failures the wrapper turns into a rolled-back storage error.
// The fence itself is read after the shared fence lock, in the same transaction, so it cannot
// move under this statement. Both floors are scalars, so the planner reads only the partitions
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
one AS (
    -- One row per UTXO, whatever the journal held. Two rows for one spend is what a build
    -- whose carry was a copy left between its copy and its drop, until the next pass heals
    -- it, so both match and both are consumed above. The NOT EXISTS below cannot
    -- collapse them, because it reads the same pre-statement snapshot for both. A filled-in
    -- pair beats an unfilled one, which is the only way the two can differ.
    SELECT DISTINCT ON (t.ukey, t.txid) t.*
      FROM taken t
     ORDER BY t.ukey, t.txid, t.mined_height DESC
),
won AS (
    SELECT t.ukey, t.txid, c.n, c.mined_height, c.block_id,
           EXISTS (SELECT 1 FROM tx_ident i
                    WHERE i.leaf = (get_byte(t.txid, 0) & 7)::smallint
                      AND i.txid = t.txid) AS has_ident
      FROM one t
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
           t.created_height, t.spendable_from,
           CASE WHEN cf.known
                THEN (t.flags & ~$7::smallint)
                     | CASE WHEN cf.conflicting THEN $7::smallint ELSE 0::smallint END
                ELSE t.flags END | $4::smallint,
           t.hash_override,
           CASE WHEN t.mined_height > 0 THEN t.mined_height
                WHEN w.n = 1 THEN w.mined_height ELSE 0 END,
           CASE WHEN t.mined_height > 0 THEN t.block_id
                WHEN w.n = 1 THEN w.block_id ELSE 0 END
      FROM one t
      LEFT JOIN won w ON w.ukey = t.ukey AND w.txid = t.txid
     CROSS JOIN LATERAL (
       SELECT count(*) > 0 AS known, coalesce(bool_or((x.flags & $7::smallint) <> 0), false) AS conflicting
         FROM (SELECT i.flags FROM tx_ident i
                WHERE i.leaf = (get_byte(t.txid, 0) & 7)::smallint
                  AND i.txid = t.txid
               UNION ALL
               SELECT m.flags FROM tx_mined m
                WHERE m.txid = t.txid
                  AND m.mined_height >= $5::int) AS x
       OFFSET 0
     ) AS cf
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

	keys, err := unspendKeysOf(spends)
	if err != nil {
		return err
	}

	if len(keys.ukeys) == 0 {
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

	counts, err := s.unspendIn(ctx, dbTx, fence, keys, extraFlags)
	if err != nil {
		return err
	}

	if err := dbTx.Commit(ctx); err != nil {
		return errors.NewStorageError("[utxoset][Unspend] commit", err)
	}

	return counts.settle()
}

// unspendKeys is the three key arrays unspendSQL takes, one entry per spend.
type unspendKeys struct {
	ukeys  [][16]byte
	ptxids [][]byte
	stxids [][]byte
}

// unspendKeysOf builds the keys of a restore. A spend with no spender is refused: without it
// there is no ownership token, and restoring on the outpoint alone could resurrect a UTXO a
// different transaction now owns.
func unspendKeysOf(spends []*utxo.Spend) (unspendKeys, error) {
	k := unspendKeys{
		ukeys:  make([][16]byte, 0, len(spends)),
		ptxids: make([][]byte, 0, len(spends)),
		stxids: make([][]byte, 0, len(spends)),
	}

	for _, sp := range spends {
		if sp == nil || sp.TxID == nil {
			continue
		}

		if sp.SpendingData == nil || sp.SpendingData.TxID == nil {
			return unspendKeys{}, errors.NewProcessingError("[utxoset][Unspend] %s:%d has no SpendingData; the spender is required as the restore ownership token",
				sp.TxID.String(), sp.Vout)
		}

		k.ukeys = append(k.ukeys, Pack(sp.TxID[:], sp.Vout))
		k.ptxids = append(k.ptxids, sp.TxID[:])
		k.stxids = append(k.stxids, sp.SpendingData.TxID[:])
	}

	return k, nil
}

// unspendCounts is what one restore statement reports, for the check made after its commit.
type unspendCounts struct {
	restored, requested, alreadyLive, repaired int
}

// unspendIn runs the restore inside the caller's transaction, after the caller has taken the
// fence lock shared and read the fence. One bad UTXO fails the whole call, before the commit,
// so no UTXO of the call is restored and no undo copy is consumed.
func (s *Store) unspendIn(ctx context.Context, dbTx pgx.Tx, fence fenceState, keys unspendKeys, extraFlags int16) (unspendCounts, error) {
	var (
		c                   unspendCounts
		noSource, ambiguous int
		offending           *string
	)

	if err := dbTx.QueryRow(ctx, unspendSQL, keys.ukeys, keys.ptxids, keys.stxids, extraFlags,
		int32(fence.droppedFloor), int32(fence.fence), FlagConflicting). //nolint:gosec // heights fit int32
		Scan(&c.restored, &c.requested, &c.alreadyLive, &c.repaired, &noSource, &ambiguous, &offending); err != nil {
		return c, errors.NewStorageError("[utxoset][Unspend] restore", err)
	}

	if noSource > 0 || ambiguous > 0 {
		names := ""
		if offending != nil {
			names = *offending
		}

		if noSource > 0 {
			unspendRepairNoSource.Inc()

			return c, errors.NewStorageError("[utxoset][Unspend] %d UTXOs restored at (0,0) have no containment row below the stamp fence %d and no identity row, so nothing could ever stamp them; the call is rolled back (%s)",
				noSource, fence.fence, names)
		}

		unspendRepairAmbiguous.Inc()

		return c, errors.NewStorageError("[utxoset][Unspend] %d UTXOs restored at (0,0) have more than one containment row below the stamp fence %d, where every row should name a winner; the call is rolled back (%s)",
			ambiguous, fence.fence, names)
	}

	return c, nil
}

// settle is the check made after the restore committed. Unlike the checks in unspendIn it
// does not undo anything: what it reports missing was never restored, and what was restored
// stays restored.
func (c unspendCounts) settle() error {
	if c.repaired > 0 {
		unspendRepaired.Add(float64(c.repaired))
	}

	if c.restored+c.alreadyLive != c.requested {
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
			c.restored, c.alreadyLive, c.requested)
	}

	return nil
}

// writtenSpendersSQL returns the spenders among $1 that have a record: an identity row, or a
// containment row at or above the dropped floor ($2). These are the two places a create writes
// a transaction, and the same two the create's claim reads before it writes one.
const writtenSpendersSQL = `
SELECT t.txid
  FROM unnest($1::bytea[]) AS t(txid)
 WHERE EXISTS (SELECT 1 FROM tx_ident i
                WHERE i.leaf = (get_byte(t.txid, 0) & 7)::smallint
                  AND i.txid = t.txid)
    OR EXISTS (SELECT 1 FROM tx_mined m
                WHERE m.txid = t.txid
                  AND m.mined_height >= $2::int
                LIMIT 1 OFFSET 0)`

// giveBackUnlessWritten restores spends a write made for transactions it did not write, and
// keeps every spend whose spender has a record by the time the restore runs.
//
// The write's own result cannot say whether a spender was written. Another writer of the same
// transaction can create it at any moment: the validator, or a second block that holds it. A
// commit that failed on the client can also have committed on the server. A spend of a stored
// transaction restored here makes its parent coin spendable while the transaction stands, so a
// second transaction can spend it.
//
// So the store is asked, in the same database transaction as the restore, and under the
// spenders' advisory locks (lockTxids). Every create of a transaction holds its lock from before
// its claim to its commit, and SpendAndCreate holds it from before its spends, so a writer of
// one of these spenders has either committed, and its record is seen here, or it starts after
// this commit and finds its parent coins live. The locks come first and the fence second, the
// order the block-path create takes them in.
func (s *Store) giveBackUnlessWritten(ctx context.Context, spends []*utxo.Spend) error {
	keys, err := unspendKeysOf(spends)
	if err != nil {
		return err
	}

	if len(keys.ukeys) == 0 {
		return nil
	}

	seen := make(map[chainhash.Hash]struct{}, len(keys.stxids))
	spenders := make([][]byte, 0, len(keys.stxids))

	for _, id := range keys.stxids {
		h := chainhash.Hash(id)
		if _, ok := seen[h]; !ok {
			seen[h] = struct{}{}
			spenders = append(spenders, id)
		}
	}

	dbTx, err := s.pool.Begin(ctx)
	if err != nil {
		return errors.NewStorageError("[utxoset][giveBack] begin", err)
	}

	defer func() { _ = dbTx.Rollback(ctx) }()

	if err := s.lockTxids(ctx, dbTx, spenders); err != nil {
		return err
	}

	fence, err := s.takeFenceShared(ctx, dbTx)
	if err != nil {
		return err
	}

	rows, err := dbTx.Query(ctx, writtenSpendersSQL, spenders, int32(fence.droppedFloor)) //nolint:gosec // a height fits int32
	if err != nil {
		return errors.NewStorageError("[utxoset][giveBack] read spenders", err)
	}

	written := make(map[chainhash.Hash]struct{})

	for rows.Next() {
		var id []byte
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return errors.NewStorageError("[utxoset][giveBack] scan spender", err)
		}

		written[chainhash.Hash(id)] = struct{}{}
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][giveBack] spender rows", err)
	}

	if len(written) > 0 {
		kept := unspendKeys{}

		for i, id := range keys.stxids {
			if _, ok := written[chainhash.Hash(id)]; ok {
				continue
			}

			kept.ukeys = append(kept.ukeys, keys.ukeys[i])
			kept.ptxids = append(kept.ptxids, keys.ptxids[i])
			kept.stxids = append(kept.stxids, id)
		}

		s.logger.Infof("[utxoset][giveBack] %d of %d spends kept: their spender was written by the time of the give-back",
			len(keys.ukeys)-len(kept.ukeys), len(keys.ukeys))

		keys = kept
	}

	if len(keys.ukeys) == 0 {
		return nil
	}

	counts, err := s.unspendIn(ctx, dbTx, fence, keys, 0)
	if err != nil {
		return err
	}

	if err := dbTx.Commit(ctx); err != nil {
		return errors.NewStorageError("[utxoset][giveBack] commit", err)
	}

	return counts.settle()
}
