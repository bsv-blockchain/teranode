package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5"
)

// The DELETE is the whole design, and it lives in spendJournalSQL because the journal is
// not optional. That one statement does five jobs. It ARBITRATES DOUBLE-SPENDS: an
// outpoint that is absent deletes zero rows, and absence is the rejection -- there is no
// spent-set to consult. It is the DECORATE FETCH: RETURNING hands back satoshis and the
// locking script, so script validation never fetches or deserialises a parent
// transaction. It CAPTURES THE UNDO PAYLOAD, in the same statement rather than merely the
// same transaction. It is the RECLAIM: the coin row is gone. And it is the write.
//
// There used to be a second, journal-free variant of it here for below-checkpoint sync.
// It is gone rather than kept behind a flag, because two copies of a consensus predicate
// that "must stay identical" is a defect waiting for one of them to be edited.

// classifySQL explains a miss. Reached only when the DELETE affected fewer rows than
// inputs offered, which is the uncommon path.
//
// The distinction matters and cannot be skipped: absence alone cannot tell "already
// spent" from "never existed" from "exists but is not yet spendable", and the validator
// behaves differently for each. Note the deliberate absence of the flag and maturity
// predicates here — this asks "does the row exist at all", precisely so a row excluded
// by the DELETE's eligibility tests surfaces as frozen or immature rather than as spent.
// spenderSQL asks the journal WHO took a coin that is no longer there.
//
// The coin row is destroyed by the spend, so absence is how a double spend is rejected. That
// answers "no" but not "who", and the caller needs "who": it marks the losing transaction
// conflicting and walks its descendants.
//
// The journal already recorded the spending transaction against every coin it destroyed, so
// a reorg could match the spender that actually took it. The same row answers this question.
//
// Matched on the full 32-byte parent txid as well as the ukey, for the same reason every
// other predicate here is: the ukey is a non-unique 96-bit prefix, so it can locate a row but
// never authorise one.
//
// Bounded by the journal's retention. Beyond it the store genuinely cannot say who took a
// coin, and that is a stated limit of delete-on-spend rather than a gap to paper over.
const spenderSQL = `
SELECT k.vin, j.spending_txid, j.satoshis, j.script, j.hash_override
  FROM unnest($1::smallint[], $2::uuid[], $3::bytea[], $4::int[]) AS k(leaf, ukey, txid, vin)
  JOIN spend_journal j ON j.ukey = k.ukey AND j.txid = k.txid`

// hash_override travels with the flags because the classifier needs it to tell one hold from
// another. spendable_from carries both coinbase maturity and the alert system's reassignment
// delay, and the two get different errors; reassignSQL is the only writer of an override on a
// live coin and writes both columns together, so its presence is what says which hold this is.
// The column costs nothing: the index probe already reads the whole row.
const classifySQL = `
SELECT k.vin, u.flags, u.spendable_from, u.hash_override
  FROM unnest($1::smallint[], $2::uuid[], $3::bytea[], $4::int[]) AS k(leaf, ukey, txid, vin)
  JOIN utxo u ON u.leaf = k.leaf AND u.ukey = k.ukey AND u.txid = k.txid`

// spendIn consumes every input of tx, inside a caller-supplied database transaction.
//
// It takes pgx.Tx rather than the package's querier interface, and that is the point. querier
// is satisfied by the connection pool as well, so a spend issued through it would autocommit:
// a transaction rejected on its third input would keep the first two inputs' coins destroyed,
// with nothing left to roll back. This store used to export exactly that as Store.Spend. The
// method is gone and the parameter type is what stops it coming back, because reintroducing it
// is now a compile error rather than a review catch.
//
// SpendAndCreate is therefore the only way in, which is what makes rejection all-or-nothing:
// per-input failures are reported on the returned records, and it rolls the whole transaction
// back when it sees any.
//
// It is a batch of one, deliberately sharing planSpends and runSpendPlan with the multi-item
// form. The predicates that authorise a spend are consensus rules, and two copies of them is a
// defect waiting for one to be edited alone.
func (s *Store) spendIn(ctx context.Context, q pgx.Tx, tx *bt.Tx, blockHeight uint32, ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	if tx == nil || tx.IsCoinbase() {
		return nil, nil
	}

	var flags utxo.IgnoreFlags
	if len(ignoreFlags) > 0 {
		flags = ignoreFlags[0]
	}

	plan := planSpends([]*spendItem{{tx: tx, blockHeight: blockHeight, ignoreFlags: flags}})

	if err := s.runSpendPlan(ctx, q, plan); err != nil {
		return nil, err
	}

	return plan.perItem[0], nil
}
