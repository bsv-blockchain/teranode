package utxoset

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// requireCheckViolation asserts the error is postgres rejecting a CHECK constraint, not any
// error at all. Without this a missing table satisfies require.Error and the test passes
// while proving nothing.
func requireCheckViolation(t *testing.T, err error, msgAndArgs ...any) {
	t.Helper()

	require.Error(t, err, msgAndArgs...)

	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr), "want a postgres error, got %T: %v", err, err)
	require.Equal(t, "23514", pgErr.Code,
		"want SQLSTATE 23514 check_violation, got %s: %s", pgErr.Code, pgErr.Message)
}

// packTriples builds the packed form tx_ident.membership carries: 12-byte triples of
// blockID, height and subtree index, all big-endian.
func packTriples(t *testing.T, triples ...[3]uint32) []byte {
	t.Helper()

	b := make([]byte, 0, len(triples)*12)
	for _, tr := range triples {
		var e [12]byte
		binary.BigEndian.PutUint32(e[0:4], tr[0])
		binary.BigEndian.PutUint32(e[4:8], tr[1])
		binary.BigEndian.PutUint32(e[8:12], tr[2])
		b = append(b, e[:]...)
	}

	return b
}

// TestTxIdentRejectsAWrongLeaf is the constraint that makes the primary key mean what the
// design says it means.
//
// PostgreSQL enforces PRIMARY KEY (leaf, txid) only WITHIN one partition. Verified on 17.11
// and 18.6: the same txid inserted under leaf 0 and leaf 1 is accepted, giving two rows, and
// ON CONFLICT (leaf, txid) DO NOTHING reports the second as a fresh insert. Seven of the
// eight wrong values are in range, so nothing about a mistake fails safely. The CHECK is
// therefore not defensive tidiness, it IS the global uniqueness rule.
func TestTxIdentRejectsAWrongLeaf(t *testing.T) {
	s, ctx := newTestStore(t)

	txid := make([]byte, 32)
	txid[0] = 0x08 // 0x08 & 7 == 0, so leaf 0 is the only correct routing

	_, err := s.pool.Exec(ctx,
		`INSERT INTO tx_ident (leaf, txid, created_height) VALUES ($1, $2, $3)`,
		int16(0), txid, int32(100))
	require.NoError(t, err, "the correct leaf must be accepted")

	_, err = s.pool.Exec(ctx,
		`INSERT INTO tx_ident (leaf, txid, created_height) VALUES ($1, $2, $3)`,
		int16(1), txid, int32(100))
	requireCheckViolation(t, err,
		"a wrong leaf must be rejected by the CHECK: without it the same txid lands in two partitions and the primary key never sees the collision")
}

// TestTxIdentRejectsATxidThatIsNotThirtyTwoBytes covers the other half of the same CHECK,
// and the reason the length test has to come FIRST and sit in the same AND.
//
// get_byte on an empty bytea raises a database error, where Go's LeafFor returns 0 for an
// empty txid (schema.go). The AND short-circuits, so that disagreement surfaces as an
// ordinary constraint violation instead of an error from inside the expression.
func TestTxIdentRejectsATxidThatIsNotThirtyTwoBytes(t *testing.T) {
	s, ctx := newTestStore(t)

	for _, tc := range []struct {
		name string
		txid []byte
	}{
		{"empty", []byte{}},
		{"thirty one bytes", make([]byte, 31)},
		{"thirty three bytes", make([]byte, 33)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.pool.Exec(ctx,
				`INSERT INTO tx_ident (leaf, txid, created_height) VALUES ($1, $2, $3)`,
				int16(0), tc.txid, int32(100))
			requireCheckViolation(t, err, "identity is the full 32 bytes; anything else must not be storable")
		})
	}
}

// TestMempoolReloadUsesThePartialIndex pins the one query whose cost decides this design.
//
// Block assembly rebuilds the ENTIRE mempool from this query at startup and on every reset,
// with no height bound, and a reorg is what triggers a reset. Measured on PG 18.6 at 43M
// rows with a 43,100-transaction mempool: 3,484 page fetches through the partial index when
// the mempool is fresh, against 978,145 for the sequential scan it falls back to. A partial
// index only holds entries for rows matching its condition, so while the mempool is a tiny
// fraction of the table the index stays tiny too -- 524,288 bytes, 0.0122 bytes per table
// row.
//
// This asserts the PLAN, not the timing, because at test scale postgres will reasonably
// prefer a sequential scan over a handful of rows. The plan is what transfers to production.
func TestMempoolReloadUsesThePartialIndex(t *testing.T) {
	s, ctx := newTestStore(t)

	var indexed bool
	require.NoError(t, s.pool.QueryRow(ctx, `
        SELECT EXISTS (
            SELECT 1 FROM pg_index i
              JOIN pg_class c ON c.oid = i.indexrelid
             WHERE i.indrelid = 'tx_ident'::regclass
               AND i.indpred IS NOT NULL
               AND c.relname = 'tx_ident_off_chain_idx')`).Scan(&indexed))
	require.True(t, indexed,
		"the mempool reload must be served by a PARTIAL index on off_chain_since, not a full one: a full index would carry an entry for every transaction the store has ever held")
}

// TestMembershipMaxHeightTakesTheHighest pins the reducer the settled predicate depends on.
//
// A transaction is settled when its off-chain marker is NULL and the HIGHEST block height in
// its membership is at or below the tip minus 288. Taking the highest is what makes it sound:
// it is at least the main-chain height, so if the highest is 288 deep the main-chain block is
// too. The incumbent SQL pruner takes the most favourable height instead, with no best-chain
// filter, so a child mined low on a fork and re-mined recently reads as stable
// (stores/utxo/sql/pruner/pruner_service.go:172-176). Do not copy that half.
func TestMembershipMaxHeightTakesTheHighest(t *testing.T) {
	s, ctx := newTestStore(t)

	t.Run("takes the maximum rather than the first", func(t *testing.T) {
		m := packTriples(t, [3]uint32{11, 2_000, 0}, [3]uint32{22, 1_000, 3})

		var got int64
		require.NoError(t, s.pool.QueryRow(ctx, `SELECT mh_max($1)`, m).Scan(&got))
		require.Equal(t, int64(2_000), got,
			"a fork entry listed first must not hide a later main-chain height")
	})

	t.Run("does not wrap on a height above the signed 32-bit boundary", func(t *testing.T) {
		// 0xFF000000 shifted left 24 as int4 wraps to a NEGATIVE number in postgres, silently,
		// which would make every "<= cutoff" test come back true and settle everything.
		m := packTriples(t, [3]uint32{1, 0xFF00_0001, 0})

		var got int64
		require.NoError(t, s.pool.QueryRow(ctx, `SELECT mh_max($1)`, m).Scan(&got))
		require.Equal(t, int64(0xFF00_0001), got, "the casts must be bigint, not int")
	})

	t.Run("is null for a transaction with no membership", func(t *testing.T) {
		var got *int64
		require.NoError(t, s.pool.QueryRow(ctx, `SELECT mh_max($1)`, []byte{}).Scan(&got))
		require.Nil(t, got, "no membership means no height, which must not read as height zero")
	})
}

// identRow is what tx_ident holds for one transaction, read back for assertions.
type identRow struct {
	createdHeight int32
	offChainSince *int32
	membership    []byte
	fee           *int64
	sizeInBytes   *int32
	locktime      *int32
	flags         int16
}

func readIdent(t *testing.T, s *Store, ctx context.Context, txid []byte) identRow {
	t.Helper()

	var r identRow
	require.NoError(t, s.pool.QueryRow(ctx, `
        SELECT created_height, off_chain_since, membership, fee, size_in_bytes, locktime, flags
          FROM tx_ident WHERE leaf = $1 AND txid = $2`,
		LeafFor(txid), txid).Scan(&r.createdHeight, &r.offChainSince, &r.membership,
		&r.fee, &r.sizeInBytes, &r.locktime, &r.flags))

	return r
}

// TestCreateWritesTheIdentityRow is the row everything else in the transaction window hangs
// off. Without it Get, SetMinedMulti, the mempool reload and the reclaimer all have nothing
// to read.
func TestCreateWritesTheIdentityRow(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 2, 1_000)
	_, err := s.Create(ctx, tx, 700_000)
	require.NoError(t, err)

	h := tx.TxIDChainHash()
	r := readIdent(t, s, ctx, h[:])

	require.Equal(t, int32(700_000), r.createdHeight)
	require.NotNil(t, r.sizeInBytes)
	require.Equal(t, int32(tx.Size()), *r.sizeInBytes, "block assembly rebuilds a candidate from size, fee and inputs, not from bytes")
	require.NotNil(t, r.locktime)
	require.Equal(t, int32(tx.LockTime), *r.locktime)
}

// TestCreateMarksAMempoolArrivalAsOffChain covers the ordinary case: nothing told us a block
// contains this transaction, so it is in the mempool set and block assembly must see it.
func TestCreateMarksAMempoolArrivalAsOffChain(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, tx, 700_000)
	require.NoError(t, err)

	h := tx.TxIDChainHash()
	r := readIdent(t, s, ctx, h[:])

	require.NotNil(t, r.offChainSince, "a mempool arrival must be in the mempool set")
	require.Equal(t, int32(700_000), *r.offChainSince)
	require.Empty(t, r.membership, "nothing has told us a block contains it")
}

// TestAForkMinedTransactionCarriesBothMembershipAndTheWaitingMarker.
//
// This state is real and has to work: a transaction mined only into a block that is NOT on the
// main chain is in a block AND is still waiting to be mined. The waiting-set query keys on the
// marker rather than on empty membership precisely so it finds these.
//
// What changed is how the state is REACHED. The store used to fake it at create time, marking
// every transaction created by block application because the block's chain status was not yet
// known. That treated "unknown" as "not mined", and since block application never claims the
// longest chain, it applied to every transaction a sync created. On the mainnet box it reached
// 3.8 million rows, 91% of the store, and stalled the reclaim behind it.
//
// The state is now reached the way production reaches it, by block assembly telling the store
// the transaction is not on the longest chain. That is a fact someone has established, rather
// than a guess made before anyone could know.
// The mined state is now reached by a stamp rather than by a create carrying block
// information. That create takes the block path, which writes no identity row at all, and
// this test is about two columns of the identity row. At the tip the stamp IS the shape:
// everything but the coinbase arrives from the mempool, is stamped by the block that mines
// it, and only later learns whether that block won.
//
// It takes TWO blocks to reach a row that is both settled and still present, and that is the
// rule rather than a trick to keep an old test alive. A longest-chain stamp on a row claiming
// one block moves it into the membership table; a row claiming two stays, because the id-less
// mark call cannot later say which of the two is main. So the row this test needs -- an
// identity row carrying membership -- is a two-block row, and it is exactly the row a reorg
// produces at the tip.
func TestAForkMinedTransactionCarriesBothMembershipAndTheWaitingMarker(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, tx, 700_000)
	require.NoError(t, err)

	h := tx.TxIDChainHash()

	_, err = s.SetMinedMulti(ctx, []*chainhash.Hash{h}, utxo.MinedBlockInfo{
		BlockID: 41, BlockHeight: 700_000, SubtreeIdx: 1,
	})
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, []*chainhash.Hash{h}, utxo.MinedBlockInfo{
		BlockID: 42, BlockHeight: 700_000, SubtreeIdx: 3, OnLongestChain: true,
	})
	require.NoError(t, err)

	require.Nil(t, readIdent(t, s, ctx, h[:]).offChainSince,
		"stamped into a block on the longest chain, so not waiting")

	// Block assembly determines the block is not on the main chain.
	require.NoError(t, s.SetBlockHeight(700_050))
	require.NoError(t, s.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*h}, false))

	r := readIdent(t, s, ctx, h[:])
	require.NotNil(t, r.offChainSince, "now it is genuinely waiting again")
	require.Equal(t, int32(700_050), *r.offChainSince, "with a clock from the current tip")
	require.NotEmpty(t, r.membership,
		"and the block it was in is still recorded, which is why the query cannot test for empty membership")
}

// TestCreateLeavesAConfirmedBlockApplicationOnChain is the other half of the gate. Once a
// caller states the block is on the longest chain, the transaction is mined and belongs
// nowhere near the mempool set -- and now says so by leaving the mempool table, which is the
// strongest form that statement can take.
// Reached by a stamp on a mempool arrival, for the reason the test above it explains: a
// create carrying block information writes no identity row, and these are its columns. What
// the moved row carries once it is in the membership table is pinned in set_mined_test.go;
// this is the tx_ident view of the same event.
func TestCreateLeavesAConfirmedBlockApplicationOnChain(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, tx, 700_000)
	require.NoError(t, err)

	h := tx.TxIDChainHash()

	_, err = s.SetMinedMulti(ctx, []*chainhash.Hash{h}, utxo.MinedBlockInfo{
		BlockID: 42, BlockHeight: 700_000, SubtreeIdx: 3, OnLongestChain: true,
	})
	require.NoError(t, err)

	require.False(t, identExists(t, s, ctx, tx), "confirmed on the longest chain means mined")
	require.Equal(t, 1, minedRows(t, s, ctx, tx), "and its one block is recorded there")
}

// TestCreatePacksEveryBlockInInsertionOrder pins the packing, including the ordering the
// conformance suite asserts: subtree indexes come back in insertion order, never sorted.
// Two blocks are now two stamps. A create that carries block information takes the block
// path, which claims one membership row for the one block it was handed, so a transaction in
// two blocks can only get there by being stamped twice -- which is exactly what happens at the
// tip, where each block that contains it stamps it in turn.
//
// The FORK stamp comes first and the longest-chain stamp second, which is the only order that
// leaves two triples on an identity row to read. The other order settles the row on the first
// stamp and moves it out, so the second stamp has no identity row to append to.
func TestCreatePacksEveryBlockInInsertionOrder(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, tx, 700_000)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx),
		utxo.MinedBlockInfo{BlockID: 9, BlockHeight: 700_002, SubtreeIdx: 7, OnLongestChain: false})
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx),
		utxo.MinedBlockInfo{BlockID: 4, BlockHeight: 700_001, SubtreeIdx: 2, OnLongestChain: true})
	require.NoError(t, err)

	h := tx.TxIDChainHash()
	r := readIdent(t, s, ctx, h[:])

	require.Equal(t,
		packTriples(t, [3]uint32{9, 700_002, 7}, [3]uint32{4, 700_001, 2}),
		r.membership, "insertion order, not sorted, and not deduplicated")

	var mh *int64
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT mh_max(membership) FROM tx_ident WHERE txid = $1`, h[:]).Scan(&mh))
	require.NotNil(t, mh)
	require.Equal(t, int64(700_002), *mh, "the settled predicate reads the highest height, not the last written")
}

// TestCreateRejectsATransactionTheStoreAlreadyHolds is the contract nine production sites
// consume, and the failure it prevents was demonstrated on this store: applying one block
// twice took a transaction's two outputs to four.
//
// The primary key on (leaf, txid) is what makes it work, which is why the CHECK tying leaf
// to txid is load-bearing rather than decorative.
func TestCreateRejectsATransactionTheStoreAlreadyHolds(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 2, 1_000)
	_, err := s.Create(ctx, tx, 700_000)
	require.NoError(t, err)

	_, err = s.Create(ctx, tx, 700_001)
	require.True(t, errors.Is(err, errors.ErrTxExists),
		"a duplicate create must be reported, not silently applied twice: got %v", err)

	h := tx.TxIDChainHash()

	var idents int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM tx_ident WHERE txid = $1`, h[:]).Scan(&idents))
	require.Equal(t, 1, idents)

	require.Equal(t, int32(700_000), readIdent(t, s, ctx, h[:]).createdHeight,
		"the first sighting wins; created_height is immutable because tx_body is filed by it")

	var coins int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM utxo WHERE txid = $1`, h[:]).Scan(&coins))
	require.Equal(t, 2, coins, "the outputs must not be created a second time")
}

// TestSpendAndCreateKeepsTheSpendsWhenTheTransactionExists is the contract that all three
// designs in the investigation got wrong, in the same way, and it is worse than a failed
// test in production.
//
// Interface.go:433-435 says ErrTxExists comes back WITH THE SPENDS LEFT IN PLACE, and
// :441-443 makes the returned slice the signal. That is not decorative. Both block
// application paths create every transaction in one pass and spend the inputs in a separate
// pass, so a transaction can genuinely be present while its own inputs are still unspent.
// Abandon the database transaction at that point and the caller is told "already have it,
// nothing to do" while the parent coins are still sitting there, spendable by anyone else.
// A double spend becomes mineable by our own node.
func TestSpendAndCreateKeepsTheSpendsWhenTheTransactionExists(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash: parent.TxIDChainHash(), Vout: 0,
		LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
	}))
	child.AddOutput(&bt.Output{Satoshis: 4_000, LockingScript: parent.Outputs[0].LockingScript})

	// The state that matters: the child already exists, created without spending anything,
	// exactly as a block-application first pass leaves it.
	_, err = s.Create(ctx, child, 200)
	require.NoError(t, err)

	_, spends, err := s.SpendAndCreate(ctx, child, 200)

	require.True(t, errors.Is(err, errors.ErrTxExists), "want ErrTxExists, got %v", err)
	require.Len(t, spends, 1, "the returned spends are the signal that the inputs were taken")

	parentHash := parent.TxIDChainHash()

	var live int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM utxo WHERE txid = $1`, parentHash[:]).Scan(&live))
	require.Zero(t, live,
		"the parent output must be SPENT: rolling back here leaves it live and spendable by someone else, which makes a double spend mineable")
}

// TestDoubleSpendNamesTheTransactionThatTookTheCoin.
//
// A delete-on-spend store destroys the coin row, so absence is how it rejects a double
// spend. That answers "no" but not "who", and the caller needs "who" to mark the loser as
// conflicting and to walk its descendants.
//
// The spend journal already holds the answer. It records the spending transaction against
// every coin it destroys, precisely so a reorg can match the spender that actually took it.
// So within the journal's retention the store can name the winner, and beyond it cannot,
// which is a real and stated limit rather than a gap.
func TestDoubleSpendNamesTheTransactionThatTookTheCoin(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	winner := bt.NewTx()
	require.NoError(t, winner.FromUTXOs(&bt.UTXO{
		TxIDHash: parent.TxIDChainHash(), Vout: 0,
		LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
	}))
	winner.AddOutput(&bt.Output{Satoshis: 4_000, LockingScript: parent.Outputs[0].LockingScript})

	_, err = spendOnly(ctx, s, winner, 100)
	require.NoError(t, err)

	// A different transaction reaching for the same coin.
	loser := bt.NewTx()
	require.NoError(t, loser.FromUTXOs(&bt.UTXO{
		TxIDHash: parent.TxIDChainHash(), Vout: 0,
		LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
	}))
	loser.AddOutput(&bt.Output{Satoshis: 3_000, LockingScript: parent.Outputs[0].LockingScript})

	spends, err := spendOnly(ctx, s, loser, 100)
	require.Error(t, err, "a rejected spend is now a returned error, and rolled back")
	require.Len(t, spends, 1)

	require.True(t, errors.Is(spends[0].Err, errors.ErrSpent), "the coin is gone, so this is a double spend")
	require.NotNil(t, spends[0].ConflictingTxID,
		"and the caller needs to know WHICH transaction took it, to mark this one conflicting and walk its children")
	require.Equal(t, winner.TxID(), spends[0].ConflictingTxID.String())
}
