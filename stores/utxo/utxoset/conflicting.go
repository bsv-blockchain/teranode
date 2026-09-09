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

// conflictingInputsSQL reads what each named transaction spends, and proves the store holds it.
//
// It reads tx_inpoints rather than the serialized body for two reasons. The body is dropped
// once its window ages out, which is the ordinary steady state rather than an error, so a
// design resting on it would work for 288 blocks and then stop. And this store writes the body
// in plain rather than extended form, meaning its inputs carry no value or locking script, so
// even a body that is still present could not answer this.
//
// BOTH HOMES, because a transaction lives in exactly one of them and this statement does not
// know which. A transaction that lost a double-spend race is very often mined -- it arrives in
// a block on the fork being abandoned, and conflict resolution has to read what it spent so
// those spends can be undone -- and the longest-chain stamp moved its inpoints out of tx_ident
// and onto its membership row. Reading the identity table alone reported exactly those
// transactions as not held at all, so SetConflicting failed instead of resolving the race.
//
// The identity arm has THE LEAF AS A SCALAR and the txids as an array, so it runs once per leaf
// group: see leafGroups for the measurements that reject the other two key shapes. It replaced
// the paired `unnest(l[],t[]) JOIN tx_ident` form, whose plan flips with statistics.
//
// The membership arm puts the keys on the OUTSIDE of a LATERAL with an OFFSET 0 fence, the
// shape minedByTxidSQL and firstMinedRowSQL use, so it is one primary-key descent per key per
// live window rather than a hash join against every window read whole. The earliest row by seq
// is the transaction's longest-chain stamp, which is the row whose payload was carried over by
// the move; a later fork stamp only ever copies it.
//
// tx_inpoints IS NOT NULL makes a BLOCK-PATH membership row a not-found here, deliberately.
// Such a row records that a transaction is in a block, not what it spends, so it cannot be a
// conflict participant: only coinbases take that path at the tip, and below the checkpoint
// nothing conflicts. Reading it as "spends nothing" would be worse than refusing it, because an
// empty input set reports a transaction with no counter-spender at all, and that is the answer
// that lets a double spend through.
//
// A hash that comes back missing from both arms is the not-found answer.
const conflictingInputsSQL = `
SELECT i.txid, i.tx_inpoints
  FROM tx_ident i
 WHERE i.leaf = $1::smallint
   AND i.txid = ANY($2::bytea[])
UNION ALL
SELECT k.txid, m.tx_inpoints
  FROM unnest($2::bytea[]) AS k(txid)
 CROSS JOIN LATERAL (
   SELECT m.tx_inpoints
     FROM tx_mined m
    WHERE m.txid = k.txid
      AND m.tx_inpoints IS NOT NULL
    ORDER BY m.seq
    LIMIT 1 OFFSET 0
 ) AS m`

// noteConflictingChildrenSQL records the contest on every parent whose coin is wanted.
//
// A transaction that loses a double-spend race is kept rather than discarded, because
// resolving the race later has to find it, and finding it means asking the PARENT whose coin
// was contested. Without this there is no route from a contested coin back to the transactions
// competing for it.
//
// It writes to conflict_children, keyed on the parent's txid alone, and that is what lets a
// MINED parent be contested. The row this used to update lived on tx_ident, which a mined
// transaction does not have -- the longest-chain stamp moved it into tx_mined -- so the note
// was a zero-row update for exactly the parents that matter, and silently succeeded.
//
// Two consequences of the move are worth stating, because both were rules here before. It no
// longer has to be a separate statement from the flag flip to avoid PostgreSQL's
// one-update-per-row-per-statement rule, since it updates no row; it stays separate because
// the note must go FIRST, so a note that fails leaves the transaction unmarked rather than
// marked but unfindable. And the 32-byte boundary test is gone with the packed column it
// guarded: one row per child cannot be matched straddling its neighbours.
//
// Run on BOTH values of the flag, matching both reference stores. Removing an entry is
// RemoveFromConflictingChildren's job.
//
// $1 is the height, $2 the parents and $3 the children, one element per (parent, child) pair.
// ON CONFLICT DO NOTHING against the window's own unique index makes a repeat free; the
// reader still says DISTINCT, because the index is per window. See the schema comment.
const noteConflictingChildrenSQL = `
INSERT INTO conflict_children (noted_height, parent_txid, child_txid)
SELECT DISTINCT $1::int, p.ptxid, p.child
  FROM unnest($2::bytea[], $3::bytea[]) AS p(ptxid, child)
ON CONFLICT DO NOTHING`

// setConflictingSQL flips the flag on both rows and reports both answers the caller needs.
//
// BOTH rows, for the reason setLockedSQL gives: the identity row is what a metadata read
// shows, the coin row is what the spend path reads, and the spend path never looks at the
// identity row. Moving only one leaves a transaction reporting itself conflicting while its
// coins stay spendable. The coin bit is what makes the spend statement's flag mask refuse the
// delete.
//
// The coin update is bounded by a key RANGE rather than by transaction id alone. The packed key
// carries the id prefix first precisely so this is an index range scan, and the full 32-byte id
// is still rechecked because the prefix is non-unique by design.
//
// The 'parent' rows are the spends to be undone, and the join to the journal is OUTER with a
// guard, in two parts, for two different reasons.
//
// The journal row is what makes a spend undoable, so an input that still has one is reported
// with its full undo payload. An input with NO journal row is reported too, but only when the
// parent coin is currently LIVE at that outpoint. That case is not hypothetical bookkeeping:
// it is what a crash between the unspend and the unlock leaves behind, and the shared
// conflict-resolution replay depends on seeing it. The driver takes the parents named here as
// the set to unlock at the end of the run, so an input omitted because its coin was already
// restored is a parent left LOCKED FOREVER with no operator signal. Both reference stores
// report every input unconditionally for exactly this reason.
//
// Handing back a record for a coin that is already live is safe here because Unspend counts it:
// its live_before arm makes a restore of an already-restored coin a no-op success rather than a
// missing-coin failure, and its held arm puts the caller's hold on such a coin, so the record
// costs one probe and is acted on exactly as one with a journal row would be.
//
// The probe is a LATERAL with an OFFSET 0 fence rather than a plain EXISTS, and the fence is
// the whole difference. Written as `OR EXISTS (SELECT 1 FROM utxo ...)` the planner is free to
// pull the subquery up and hash it, and it does: measured on this schema at 40,000 coins across
// all eight partitions with 500 keys, the plan carried a hashed SubPlan over a Seq Scan of
// every one of utxo_p0..p7, 30.6 ms, and its cost grows with the coin table rather than with
// the batch. Fenced, it is one index descent per key on the leaf partition's ukey index with
// the full 32-byte txid rechecked on the heap -- the bound schema.go requires of every by-txid
// coin access, because the packed key is a non-unique 96-bit prefix that can find a row but
// must never justify acting on one.
//
// An input with neither a journal row nor a live coin is still omitted, and that is the half of
// the old rule that was right. Its undo payload has aged out of the journal, or the coin was
// re-spent by a different transaction since; either way this store cannot restore it, and
// Unspend FAILS the whole restore if a single record it is given cannot be accounted for. A
// cascade rooted on a transaction older than journal retention therefore still stops early,
// which is a real limit of delete-on-spend rather than something to paper over.
//
// The 'child' rows are the next level of the cascade. The journal is the only place this store
// can answer "who took this coin", because the coin row is destroyed the moment it is spent.
//
// The flag is flipped on THREE things, not two, for the same reason setLockedSQL flips three.
// A transaction lives in exactly one of tx_ident and tx_mined and this statement does not know
// which; a contested parent is very often mined, which is the whole reason the note itself
// became a side table. minedRow.toMeta reads Conflicting straight off tx_mined.flags, copied
// once by the move and updated by nothing afterwards, so without the membership arm marking a
// mined transaction conflicting set the coin bit and not the bit Get reports -- the mirror
// image of the failure setLockedSQL's own comment warns about. The membership arm is a plain
// txid equality because tx_mined's primary key leads with txid.
const setConflictingSQL = `
WITH k AS (
    SELECT * FROM unnest($1::int[], $2::smallint[], $3::bytea[], $4::uuid[], $5::uuid[])
        AS t(ref, leaf, txid, lo, hi)
),
p AS (
    SELECT * FROM unnest($6::int[], $7::bytea[], $8::smallint[], $9::bytea[], $10::uuid[], $11::int[])
        AS t(ref, child, pleaf, ptxid, pukey, pvout)
),
ident AS (
    UPDATE tx_ident i
       SET flags = CASE WHEN $12::boolean THEN i.flags |  $13::smallint
                                          ELSE i.flags & ~$13::smallint END
      FROM k
     WHERE i.leaf = k.leaf AND i.txid = k.txid
),
mined AS (
    UPDATE tx_mined m
       SET flags = CASE WHEN $12::boolean THEN m.flags |  $13::smallint
                                          ELSE m.flags & ~$13::smallint END
      FROM k
     WHERE m.txid = k.txid
),
coins AS (
    UPDATE utxo u
       SET flags = CASE WHEN $12::boolean THEN u.flags |  $13::smallint
                                          ELSE u.flags & ~$13::smallint END
      FROM k
     WHERE u.leaf  = k.leaf
       AND u.ukey BETWEEN k.lo AND k.hi
       AND u.txid  = k.txid
)
SELECT 'parent'::text AS kind, p.ref, p.ptxid, p.pvout,
       j.satoshis, j.script, j.hash_override, NULL::bytea AS spender
  FROM p
  LEFT JOIN spend_journal j
    ON j.ukey = p.pukey AND j.txid = p.ptxid AND j.spending_txid = p.child
  LEFT JOIN LATERAL (
    SELECT 1 AS live
      FROM utxo u
     WHERE u.leaf = p.pleaf AND u.ukey = p.pukey AND u.txid = p.ptxid
     LIMIT 1 OFFSET 0
  ) AS lv ON TRUE
 WHERE j.ukey IS NOT NULL OR lv.live IS NOT NULL
UNION ALL
SELECT 'child'::text, k.ref, NULL::bytea, NULL::int,
       NULL::bigint, NULL::bytea, NULL::bytea, j.spending_txid
  FROM k
  JOIN spend_journal j
    ON j.ukey BETWEEN k.lo AND k.hi AND j.txid = k.txid
ORDER BY 1 DESC, 2`

// SetConflicting marks transactions as having lost a double-spend race, or clears that mark.
//
// It returns two things the caller cannot get anywhere else. The first is the set of parent
// outputs these transactions consumed, in a form this store's own Unspend can restore, which is
// exactly what conflict resolution does with it. The second is the set of transactions that
// spent THESE transactions' outputs, which is the next level of the walk that demotes a loser's
// whole descendant tree.
//
// The spender recorded on each returned record is the transaction that took the coin. Its input
// index is the position in the flattened inpoint list, which is parent-major and therefore NOT
// the transaction's original input order, because the stored inpoints deduplicate parents.
// Nothing reads it today; it is stated here so a later reader does not assume otherwise.
//
// Both answers are bounded by the journal's retention. Beyond that this store cannot say who
// took a coin, so a cascade rooted on an older transaction stops early. That is a real limit of
// delete-on-spend rather than a gap to paper over.
func (s *Store) SetConflicting(ctx context.Context, txHashes []chainhash.Hash,
	value bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	if len(txHashes) == 0 {
		return nil, nil, nil
	}

	// One entry per DISTINCT transaction. The cascade can name the same transaction twice when
	// two of its parents were both contested, and asking twice would duplicate both answers.
	named := make([]chainhash.Hash, 0, len(txHashes))
	seen := make(map[chainhash.Hash]struct{}, len(txHashes))

	for _, h := range txHashes {
		if _, dup := seen[h]; dup {
			continue
		}

		seen[h] = struct{}{}

		named = append(named, h)
	}

	inpoints, err := s.readConflictingInputs(ctx, named)
	if err != nil {
		return nil, nil, err
	}

	plan := s.planConflicting(named, inpoints)

	// The height is read ONCE and used for both the window and the row, because a second read
	// that crossed a 48-block boundary would insert into a partition that does not exist.
	notedHeight := s.GetBlockHeight()

	// The note's window BEFORE the transaction is opened, never inside it: the DDL needs its
	// own pool connection, and taking one while holding a transaction from the same pool
	// deadlocks the pool under concurrency, with no timeout.
	if len(plan.pChild) > 0 {
		if err := s.ensureSpendJournalPartition(ctx, notedHeight); err != nil {
			return nil, nil, err
		}
	}

	dbTx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, errors.NewStorageError("[utxoset][SetConflicting] begin", err)
	}

	committed := false

	defer func() {
		if !committed {
			_ = dbTx.Rollback(ctx)
		}
	}()

	// The note runs first, so a failure here leaves the transactions unmarked rather than
	// marked but unfindable.
	if len(plan.pChild) > 0 {
		if _, err = dbTx.Exec(ctx, noteConflictingChildrenSQL,
			int32(notedHeight), plan.pTxid, plan.pChild); err != nil { //nolint:gosec // a chain height fits int32
			return nil, nil, errors.NewStorageError("[utxoset][SetConflicting] note parents", err)
		}
	}

	affected, children, err := s.runConflictingPlan(ctx, dbTx, plan, value)
	if err != nil {
		return nil, nil, err
	}

	if err = dbTx.Commit(ctx); err != nil {
		return nil, nil, errors.NewStorageError("[utxoset][SetConflicting] commit", err)
	}

	committed = true

	return affected, children, nil
}

// readConflictingInputs fetches what each named transaction spends, and reports any the store
// does not hold.
func (s *Store) readConflictingInputs(ctx context.Context,
	named []chainhash.Hash) (map[chainhash.Hash]subtree.TxInpoints, error) {
	txids := make([][]byte, 0, len(named))
	for i := range named {
		txids = append(txids, named[i][:])
	}

	out := make(map[chainhash.Hash]subtree.TxInpoints, len(named))

	// One round trip per leaf group, because the identity arm needs the leaf as a scalar to
	// keep its plan an index scan. The membership arm rides along in the same statement rather
	// than taking a round trip of its own: it does not need the leaf, and splitting the keys
	// across groups costs it nothing, since it is one primary-key descent per key either way.
	for _, g := range leafGroups(txids) {
		if err := s.readInputsForLeaf(ctx, g, out); err != nil {
			return nil, err
		}
	}

	if len(out) == len(named) {
		return out, nil
	}

	// Absent means the store does not hold it, which both reference stores report as an error
	// rather than skipping. Bounded, so one bad batch cannot produce an error the size of a
	// block.
	missing := make([]error, 0, 4)

	for i := range named {
		if _, ok := out[named[i]]; ok {
			continue
		}

		if len(missing) < 10 {
			missing = append(missing,
				errors.NewTxNotFoundError("[utxoset][SetConflicting] %s", named[i].String()))
		}
	}

	return nil, errors.Join(missing...)
}

// readInputsForLeaf runs both arms of conflictingInputsSQL for one leaf group, folding the
// answers into out.
func (s *Store) readInputsForLeaf(ctx context.Context, g leafBatch,
	out map[chainhash.Hash]subtree.TxInpoints) error {
	rows, err := s.pool.Query(ctx, conflictingInputsSQL, g.leaf, g.txids)
	if err != nil {
		return errors.NewStorageError("[utxoset][SetConflicting] read inputs", err)
	}

	defer rows.Close()

	for rows.Next() {
		var (
			txid []byte
			raw  []byte
		)

		if err := rows.Scan(&txid, &raw); err != nil {
			return errors.NewStorageError("[utxoset][SetConflicting] scan inputs", err)
		}

		var h chainhash.Hash

		copy(h[:], txid)

		if len(raw) == 0 {
			// A coinbase stores no inpoints. It spends nothing, so it contributes no parents.
			out[h] = subtree.NewTxInpoints()
			continue
		}

		ip, ierr := subtree.NewTxInpointsFromBytes(raw)
		if ierr != nil {
			return errors.NewStorageError("[utxoset][SetConflicting] decode inpoints %s", h.String(), ierr)
		}

		out[h] = ip
	}

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][SetConflicting] input rows", err)
	}

	return nil
}

// conflictingPlan is the argument set for one call of the statements above, flattened across
// however many transactions were named, in the same shape the spend and create paths use.
type conflictingPlan struct {
	// One element per named transaction.
	kRef  []int32
	kLeaf []int16
	kTxid [][]byte
	kLo   [][16]byte
	kHi   [][16]byte
	named []chainhash.Hash

	// One element per input, across every named transaction.
	pRef   []int32
	pChild [][]byte
	pLeaf  []int16
	pTxid  [][]byte
	pUkey  [][16]byte
	pVout  []int32
	pVin   []int
	pOwner []int
}

// planConflicting flattens the named transactions and their inputs into arrays.
func (s *Store) planConflicting(named []chainhash.Hash,
	inpoints map[chainhash.Hash]subtree.TxInpoints) *conflictingPlan {
	p := &conflictingPlan{named: named}

	for i := range named {
		p.kRef = append(p.kRef, int32(i)) //nolint:gosec // bounded by batch size
		p.kLeaf = append(p.kLeaf, LeafFor(named[i][:]))
		p.kTxid = append(p.kTxid, named[i][:])
		p.kLo = append(p.kLo, Pack(named[i][:], 0))
		p.kHi = append(p.kHi, Pack(named[i][:], ^uint32(0)))

		ip := inpoints[named[i]]
		flat := ip.GetTxInpoints()

		for vin, in := range flat {
			// The coinbase placeholder is not a real parent, and following it would look up a
			// transaction that does not exist. Skipped cleanly rather than left as a hole.
			if in.Hash == subtree.CoinbasePlaceholderHashValue {
				continue
			}

			parent := in.Hash

			p.pRef = append(p.pRef, int32(len(p.pOwner))) //nolint:gosec // bounded by batch size
			p.pChild = append(p.pChild, named[i][:])
			p.pLeaf = append(p.pLeaf, LeafFor(parent[:]))
			p.pTxid = append(p.pTxid, parent[:])
			p.pUkey = append(p.pUkey, Pack(parent[:], in.Index))
			p.pVout = append(p.pVout, int32(in.Index)) //nolint:gosec // a vout fits int32
			p.pVin = append(p.pVin, vin)
			p.pOwner = append(p.pOwner, i)
		}
	}

	return p
}

// runConflictingPlan flips the flags and reads back both answers.
func (s *Store) runConflictingPlan(ctx context.Context, q querier, p *conflictingPlan,
	value bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	rows, err := q.Query(ctx, setConflictingSQL,
		p.kRef, p.kLeaf, p.kTxid, p.kLo, p.kHi,
		p.pRef, p.pChild, p.pLeaf, p.pTxid, p.pUkey, p.pVout,
		value, FlagConflicting)
	if err != nil {
		return nil, nil, errors.NewStorageError("[utxoset][SetConflicting] set", err)
	}

	var (
		affected []*utxo.Spend
		children []chainhash.Hash
	)

	childSeen := make(map[chainhash.Hash]struct{})

	for rows.Next() {
		var (
			kind         string
			ref          int32
			ptxid        []byte
			pvout        *int32
			satoshis     *int64
			script       []byte
			hashOverride []byte
			spender      []byte
		)

		if err := rows.Scan(&kind, &ref, &ptxid, &pvout, &satoshis, &script, &hashOverride, &spender); err != nil {
			rows.Close()
			return nil, nil, errors.NewStorageError("[utxoset][SetConflicting] scan", err)
		}

		if kind == "child" {
			var h chainhash.Hash

			copy(h[:], spender)

			// Deduplicated on the way out: a transaction that spends several outputs of one
			// parent would otherwise be named once per output.
			if _, dup := childSeen[h]; dup {
				continue
			}

			childSeen[h] = struct{}{}

			children = append(children, h)

			continue
		}

		sp, serr := p.spendFor(ref, ptxid, pvout, satoshis, script, hashOverride)
		if serr != nil {
			rows.Close()
			return nil, nil, serr
		}

		affected = append(affected, sp)
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return nil, nil, errors.NewStorageError("[utxoset][SetConflicting] rows", err)
	}

	return affected, children, nil
}

// spendFor turns one journal row into the restorable record conflict resolution hands back to
// Unspend.
func (p *conflictingPlan) spendFor(ref int32, ptxid []byte, pvout *int32, satoshis *int64,
	script []byte, hashOverride []byte) (*utxo.Spend, error) {
	// The vout comes from the plan's own inpoint rather than from the journal, so it is never
	// absent. A missing one would mean the statement returned a row the plan did not ask for.
	if pvout == nil {
		return nil, errors.NewStorageError("[utxoset][SetConflicting] row %d names no output", ref)
	}

	parent, err := chainhash.NewHash(ptxid)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][SetConflicting] parent hash", err)
	}

	child := p.named[p.pOwner[ref]]
	vout := uint32(*pvout) //nolint:gosec // a vout is never negative

	sp := &utxo.Spend{
		TxID:         parent,
		Vout:         vout,
		SpendingData: spendpkg.NewSpendingData(&child, p.pVin[ref]),
	}

	// The reassigned identity wins when there is one, exactly as the spend path treats it.
	if len(hashOverride) > 0 {
		if h, herr := chainhash.NewHash(hashOverride); herr == nil {
			sp.UTXOHash = h
			return sp, nil
		}
	}

	// No journal row means the coin is already live, so there is no captured amount or script
	// to compute the coin's identity from and the record carries none. Nothing in this store
	// reads it -- Unspend restores on the outpoint and the spender, and that is all the driver
	// does with these -- and inventing one from a coin row would be a second, unverified
	// opinion about a value the journal is the record for.
	if satoshis == nil {
		return sp, nil
	}

	if h, herr := util.UTXOHash(parent, vout, bscript.NewFromBytes(script), uint64(*satoshis)); herr == nil { //nolint:gosec // satoshis are never negative
		sp.UTXOHash = h
	}

	return sp, nil
}
