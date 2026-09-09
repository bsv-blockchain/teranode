package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
)

// BatchDecorate fills in metadata for many transactions at once.
//
// A transaction the store does not hold is reported on ITS OWN entry, not by failing the
// call. The validator turns a missing parent into a rejection for that transaction alone, so
// failing the batch would reject every transaction that happened to be resolved beside it.
//
// The field list is accepted and ignored with one exception, as it is on the single read:
// whichever step answers returns every column it holds on one row, so narrowing the projection
// would save a little on the wire and hand the caller a partly populated record it might
// dereference. The exception is the contest, which is not on that row at all -- it is keyed on
// the txid in conflict_children, because a contested parent is usually mined -- so it costs a
// statement and is read only when named.
//
// The read order lives in lookupMany, which this, Get and the get batcher all go through, so
// there is one answer to "where does a transaction come from" rather than three.
func (s *Store) BatchDecorate(ctx context.Context, items []*utxo.UnresolvedMetaData, fieldNames ...fields.FieldName) error {
	if len(items) == 0 {
		return nil
	}

	hashes := make([]chainhash.Hash, 0, len(items))
	for _, it := range items {
		hashes = append(hashes, it.Hash)
	}

	res, err := s.lookupMany(ctx, hashes, wantsConflictingChildren(fieldNames))
	if err != nil {
		return errors.NewStorageError("[utxoset][BatchDecorate]", err)
	}

	// One record per ENTRY, even where two entries name the same transaction. lookupMany
	// resolves a repeated hash once, and handing both entries the same pointer would let a
	// caller decorating one of them mutate the other's answer.
	given := make(map[chainhash.Hash]struct{}, len(items))

	for _, it := range items {
		// A row that would not decode is reported on its own entry, the same way a miss is.
		// That is this call's whole contract: one corrupt tx_inpoints must not reject every
		// transaction that happened to be resolved beside it.
		if derr, bad := res.failed[it.Hash]; bad {
			it.Err = derr
			continue
		}

		data, ok := res.found[it.Hash]
		if !ok {
			it.Err = errors.NewTxNotFoundError("[utxoset][BatchDecorate] %s", it.Hash.String())
			continue
		}

		if _, dup := given[it.Hash]; dup {
			copied := *data
			data = &copied
		} else {
			given[it.Hash] = struct{}{}
		}

		it.Data = data
	}

	return nil
}

// setLockedSQL flips the locked bit on the transaction row AND on every coin it created.
//
// Both, because they are read by different things. The transaction row is what a caller sees
// through Get; the coin row is what the spend path reads, and the spend path never looks at
// the transaction row. Setting only one would leave a transaction reporting itself locked
// while its coins were still spendable, or the reverse.
//
// "The transaction row" is EITHER home, so there are three arms rather than two. A transaction
// lives in exactly one of tx_ident and tx_mined, this call does not know which, and
// minedRow.toMeta reads Locked straight off tx_mined.flags -- copied once by the move and
// updated by nothing afterwards. Without the membership arm a flag set after the stamp was
// invisible to Get, silently, and in the direction that matters: a transaction reporting itself
// locked forever. The two-phase-commit release ordinarily runs long before the stamp, so this
// is narrow, but narrow and silent is the combination worth closing.
//
// The membership arm is a plain txid equality because tx_mined's primary key LEADS with txid,
// so it is one descent per key per live window, and the update touches every membership row the
// transaction has -- a transaction stamped into two blocks is one transaction and one flag.
//
// The coin UPDATE carries the packed-key range, AND it locates its rows through a fenced
// LATERAL first. schema.go states the rule in its own words: "There is deliberately no index on
// txid: every by-txid access is a ukey range scan with a full-txid heap recheck. Any query
// filtering on txid without a ukey range bound is a review failure." This was that query, on
// the two-phase-commit path, one call per mempool transaction, and every sibling statement --
// setConflictingSQL, deleteTxSQL, resetCoinsSQL, stampCoinsSQL, coinFactsSQL -- already carried
// lo/hi. The answer was never wrong, because the full txid was already rechecked.
//
// The bound alone does not buy the plan. Measured at 500 keys against 40,000 coin rows: with
// the range added straight to `UPDATE utxo u FROM k`, the planner still hash-joined the keys
// against a Seq Scan of all eight leaf partitions and applied the range as a Join Filter --
// 13.5-14.1 ms, the same plan as without it. That is exactly what resetCoinsSQL's comment
// records ("the obvious UPDATE ... FROM unnest read all eight coin partitions for 98 ms of a
// 108 ms statement"), so this takes resetCoinsSQL's shape: a CTE that locates the rows through
// a CROSS JOIN LATERAL with an OFFSET 0 fence, then an UPDATE keyed on what it found. The fence
// is what stops the subquery being pulled up into the join, which is what re-admits the scan.
// No LIMIT inside it, because a transaction has as many coins as it has unspent outputs and all
// of them take the flag. Measured, eight runs at 500 keys against 40,000 coin rows: 13.2-14.5 ms
// with the bound as a Join Filter, 8.5-9.9 ms fenced, with a Bitmap Index Scan on each leaf's
// ukey index and no Seq Scan on any coin partition.
//
// The ident arm used to live in here too, as the same paired-unnest join every other
// identity statement was measured and rejected for: leafGroups (set_mined.go) found it
// 5.19-5.31 ms for 500 keys against 40,000 identity rows, a Hash Join over a Seq Scan of
// all eight partitions, against 0.29-0.33 ms per leaf group for the leaf-scalar form. It is
// now setLockedIdentSQL below, run once per leaf group the same way stampSQL and
// provePresentSQL are, and setLockedDirect wraps that loop and this statement in one
// transaction so the three arms still commit or fail together.
const setLockedSQL = `
WITH k AS (
    SELECT * FROM unnest($1::smallint[], $2::bytea[], $3::uuid[], $4::uuid[])
        AS t(leaf, txid, lo, hi)
),
mined AS (
    UPDATE tx_mined m SET flags = CASE WHEN $5::boolean
                                       THEN m.flags | $6::smallint
                                       ELSE m.flags & ~$6::smallint END
      FROM k WHERE m.txid = k.txid
),
hit AS (
    SELECT c.leaf, c.ukey, k.txid
      FROM k
     CROSS JOIN LATERAL (
       SELECT u.leaf, u.ukey
         FROM utxo u
        WHERE u.leaf = k.leaf AND u.ukey >= k.lo AND u.ukey <= k.hi AND u.txid = k.txid
       OFFSET 0
     ) AS c
)
UPDATE utxo u SET flags = CASE WHEN $5::boolean
                               THEN u.flags | $6::smallint
                               ELSE u.flags & ~$6::smallint END
  FROM hit
 WHERE u.leaf = hit.leaf AND u.ukey = hit.ukey AND u.txid = hit.txid`

// setLockedIdentSQL is the ident arm on its own, in the leaf-scalar shape: LEAF AS A SCALAR
// and the txids as an array, run once per leaf group (leafGroups, set_mined.go) rather than
// joined against a paired unnest(leaf[], txid[]). See setLockedSQL's comment for the
// measurements this shape is chosen on.
const setLockedIdentSQL = `
UPDATE tx_ident i SET flags = CASE WHEN $3::boolean
                                   THEN i.flags | $4::smallint
                                   ELSE i.flags & ~$4::smallint END
 WHERE i.leaf = $1::smallint
   AND i.txid = ANY($2::bytea[])`

// SetLocked marks transactions as locked for spending, or releases them.
//
// The release is the two-phase commit path: a transaction created for the mempool is locked,
// and unlocked once it commits. That is one call per transaction on a hot path, which is why
// the sql store batches it, and why this takes a slice.
func (s *Store) SetLocked(ctx context.Context, txHashes []chainhash.Hash, value bool) error {
	if len(txHashes) == 0 {
		return nil
	}

	// A single-hash call is the two-phase commit release, one per mempool transaction, and
	// that is what the batcher is for. A caller that already has many hashes is its own
	// batch and goes straight through.
	if s.lockBatcher != nil && len(txHashes) == 1 {
		errCh := make(chan error, 1)

		s.lockBatcher.PutCtx(ctx, &lockItem{hash: txHashes[0], value: value, errCh: errCh})

		select {
		case err := <-errCh:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return s.setLockedDirect(ctx, txHashes, value)
}

// setLockedDirect issues the update, and is what both the direct and the batched path end at.
//
// The ident arm runs first, once per leaf group (leafGroups), and the mined+coin statement
// runs after it -- both inside ONE transaction, so a caller reading either row mid-flight
// still sees them agree: never a transaction whose identity row is locked and whose coin or
// membership row is not, or the reverse.
func (s *Store) setLockedDirect(ctx context.Context, txHashes []chainhash.Hash, value bool) error {
	leaves := make([]int16, 0, len(txHashes))
	txids := make([][]byte, 0, len(txHashes))
	los := make([][16]byte, 0, len(txHashes))
	his := make([][16]byte, 0, len(txHashes))

	for i := range txHashes {
		leaves = append(leaves, LeafFor(txHashes[i][:]))
		txids = append(txids, txHashes[i][:])
		los = append(los, Pack(txHashes[i][:], 0))
		his = append(his, Pack(txHashes[i][:], ^uint32(0)))
	}

	dbTx, err := s.pool.Begin(ctx)
	if err != nil {
		return errors.NewStorageError("[utxoset][SetLocked] begin", err)
	}

	for _, g := range leafGroups(txids) {
		if _, err := dbTx.Exec(ctx, setLockedIdentSQL, g.leaf, g.txids, value, FlagLocked); err != nil {
			_ = dbTx.Rollback(ctx)

			return errors.NewStorageError("[utxoset][SetLocked] ident", err)
		}
	}

	if _, err := dbTx.Exec(ctx, setLockedSQL, leaves, txids, los, his, value, FlagLocked); err != nil {
		_ = dbTx.Rollback(ctx)

		return errors.NewStorageError("[utxoset][SetLocked]", err)
	}

	if err := dbTx.Commit(ctx); err != nil {
		return errors.NewStorageError("[utxoset][SetLocked] commit", err)
	}

	return nil
}
