package utxoset

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) (*Store, context.Context) {
	t.Helper()

	return newTestStoreWith(t, nil)
}

// newTestStoreWith is newTestStore with the settings adjusted by tune before the store is
// opened, for tests that need a particular batcher shape.
func newTestStoreWith(t *testing.T, tune func(*settings.Settings)) (*Store, context.Context) {
	t.Helper()

	ctx := context.Background()
	dsn := testDSN(t)

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err, "opening the test pool")
	require.NoError(t, pool.Ping(ctx), "reaching the test postgres")

	// clean slate
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS utxo CASCADE;
	                       DROP TABLE IF EXISTS spend_journal CASCADE;
	                       DROP TABLE IF EXISTS applied_block CASCADE;
	                       DROP TABLE IF EXISTS applied_chunk CASCADE;
	                       DROP TABLE IF EXISTS tx_ident CASCADE;
	                       DROP TABLE IF EXISTS tx_body CASCADE;
	                       DROP TABLE IF EXISTS tx_mined CASCADE;
	                       DROP TABLE IF EXISTS tx_mined_floor CASCADE;
	                       DROP TABLE IF EXISTS conflict_children CASCADE;
	                       DROP TABLE IF EXISTS conflict_intents CASCADE;
	                       DROP TABLE IF EXISTS preserved_parent CASCADE;`)

	// Sweep up detached partitions the parent drop cannot reach. DROP TABLE removes a
	// partitioned parent and everything still ATTACHED to it; anything detached is an
	// ordinary table by then and survives, which later makes CREATE TABLE IF NOT EXISTS skip
	// a name it thinks is taken. A test that crashes mid-detach cannot clean up after itself,
	// so the setup does it.
	orphans, oerr := pool.Query(ctx, `
        SELECT c.relname FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = current_schema() AND c.relkind = 'r'
           AND c.relname ~ '^(tx_body_w|tx_ident_l|utxo_p|spend_journal_|conflict_children_|tx_mined_w)[0-9]+$'`)
	if oerr == nil {
		var names []string

		for orphans.Next() {
			var n string
			if orphans.Scan(&n) == nil {
				names = append(names, n)
			}
		}

		orphans.Close()

		for _, n := range names {
			_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS `+n)
		}
	}
	pool.Close()

	u, err := url.Parse(dsn)
	require.NoError(t, err)

	tSettings := settings.NewSettings()
	if tune != nil {
		tune(tSettings)
	}

	s, err := New(ctx, ulogger.TestLogger{}, tSettings, u)
	require.NoError(t, err)

	t.Cleanup(func() { _ = s.Close(ctx) })

	return s, ctx
}

// mkTx builds a transaction with n spendable P2PKH-ish outputs.
func mkTx(t *testing.T, nOut int, sats uint64) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	// a non-coinbase input so IsCoinbase() is false
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000001", 0,
		"76a914000000000000000000000000000000000000000088ac", 100000))

	for i := 0; i < nOut; i++ {
		script, err := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
		require.NoError(t, err)
		tx.AddOutput(&bt.Output{Satoshis: sats, LockingScript: script})
	}

	return tx
}

// TestUTXOTableCreateSpendRoundTrip is the core loop: an output created by Create must be
// spendable exactly once, must hand back its satoshis and locking script on the way out
// (that RETURNING clause is what replaces PreviousOutputsDecorate), and a second attempt
// must be rejected by ABSENCE rather than by consulting a spent-set.
func TestUTXOTableCreateSpendRoundTrip(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	parentHash := parent.TxIDChainHash()

	// a child spending parent output 0
	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash:      parentHash,
		Vout:          0,
		LockingScript: parent.Outputs[0].LockingScript,
		Satoshis:      parent.Outputs[0].Satoshis,
	}))

	spends, err := spendOnly(ctx, s, child, 200)
	require.NoError(t, err)
	require.Len(t, spends, 1)
	require.NoError(t, spends[0].Err, "first spend must succeed")

	// the decorate fetch: Spend must have populated the input from the UTXO row,
	// so script validation needs no parent lookup at all
	require.Equal(t, parent.Outputs[0].Satoshis, child.Inputs[0].PreviousTxSatoshis,
		"Spend must return the satoshis via RETURNING")
	require.NotNil(t, child.Inputs[0].PreviousTxScript,
		"Spend must return the locking script via RETURNING")

	// A DIFFERENT transaction reaching for the same coin: the row is gone, and absence IS the
	// rejection.
	//
	// The extra output is what makes it different, and it is load-bearing rather than
	// incidental. This used to build child2 exactly as child was built, which gave it the same
	// transaction id, so it was really testing a REPLAY of one transaction and calling the
	// result a double spend. A replay is not a double spend and now succeeds, which is what
	// stops a half-applied block wedging the node. The id assertion below keeps the two cases
	// from quietly becoming one again. Replay has its own test in spend_replay_test.go.
	child2 := bt.NewTx()
	require.NoError(t, child2.FromUTXOs(&bt.UTXO{
		TxIDHash:      parentHash,
		Vout:          0,
		LockingScript: parent.Outputs[0].LockingScript,
		Satoshis:      parent.Outputs[0].Satoshis,
	}))
	child2.AddOutput(&bt.Output{Satoshis: 1, LockingScript: parent.Outputs[0].LockingScript})

	require.NotEqual(t, child.TxIDChainHash().String(), child2.TxIDChainHash().String(),
		"the rival must be a different transaction, or this is a replay test in disguise")

	spends2, err := spendOnly(ctx, s, child2, 201)
	require.Error(t, err, "a rejected spend is now a returned error, and rolled back")
	require.Len(t, spends2, 1)
	require.Error(t, spends2[0].Err, "double spend must be rejected")
	require.True(t, errors.Is(spends2[0].Err, errors.ErrSpent),
		"rejection must be ErrSpent, got %v", spends2[0].Err)

	// the sibling output must be untouched by all of this
	var remaining int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM utxo WHERE txid = $1`,
		parentHash[:]).Scan(&remaining))
	require.Equal(t, 1, remaining, "output 1 must survive its sibling being spent")
}

// TestUTXOTableUnspendableOutputsCreateNoRow pins the rule that keeps the UTXO table's size
// bounded: an output that can never be spent must never occupy a row, because a row
// nothing can ever delete would sit in the budget forever.
func TestUTXOTableUnspendableOutputsCreateNoRow(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000002", 0,
		"76a914000000000000000000000000000000000000000088ac", 100000))

	// one spendable output, one provably-unspendable OP_RETURN data carrier
	spendableScript, err := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
	require.NoError(t, err)
	tx.AddOutput(&bt.Output{Satoshis: 1000, LockingScript: spendableScript})

	opReturn, err := bscript.NewFromHexString("006a0568656c6c6f")
	require.NoError(t, err)
	tx.AddOutput(&bt.Output{Satoshis: 0, LockingScript: opReturn})

	_, err = s.Create(ctx, tx, 100)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()

	var rows int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM utxo WHERE txid = $1`,
		txHash[:]).Scan(&rows))
	require.Equal(t, 1, rows, "only the spendable output may occupy a UTXO row")
}

// TestDecorateFromUTXOTable answers the question directly: can PreviousOutputsDecorate be
// served from the UTXO table alone?
//
// Today it cannot -- the postgres store fetches the parent's raw_tx (~1.7 KB) and runs
// bt.NewTxFromBytes over it to pull out two fields. Here those two fields ARE the row,
// so decorate is an index probe. Note this store holds no transaction bodies at all, so
// if the input comes back decorated it can only have come from the UTXO table.
func TestDecorateFromUTXOTable(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 7777)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          0,
		LockingScript: parent.Outputs[0].LockingScript,
		Satoshis:      parent.Outputs[0].Satoshis,
	}))

	// strip what FromUTXOs pre-filled, so decorate has to do the work
	child.Inputs[0].PreviousTxScript = nil
	child.Inputs[0].PreviousTxSatoshis = 0

	require.NoError(t, s.PreviousOutputsDecorate(ctx, child))

	require.Equal(t, uint64(7777), child.Inputs[0].PreviousTxSatoshis,
		"satoshis must come from the UTXO row")
	require.NotNil(t, child.Inputs[0].PreviousTxScript,
		"locking script must come from the UTXO row")
	require.Equal(t, parent.Outputs[0].LockingScript.String(), child.Inputs[0].PreviousTxScript.String(),
		"the decorated script must be the parent's actual locking script")

	// and once spent, the row is gone -- decorate must say so rather than inventing an answer
	spent := bt.NewTx()
	require.NoError(t, spent.FromUTXOs(&bt.UTXO{
		TxIDHash: parent.TxIDChainHash(), Vout: 0,
		LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
	}))
	_, err = spendOnly(ctx, s, spent, 200)
	require.NoError(t, err)

	orphan := bt.NewTx()
	require.NoError(t, orphan.FromUTXOs(&bt.UTXO{
		TxIDHash: parent.TxIDChainHash(), Vout: 0,
		LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
	}))
	orphan.Inputs[0].PreviousTxScript = nil

	require.Error(t, s.PreviousOutputsDecorate(ctx, orphan),
		"a spent parent has no UTXO row and decorate must not fabricate one")
}

// TestSpendAndCreateAtomic covers the contract PR 1326 introduces, and the property that
// makes this store's implementation different from the sequential helper: spend and
// create share ONE transaction, so there is no window in which the inputs are spent and
// the outputs missing, and no compensating Unspend on failure.
func TestSpendAndCreateAtomic(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 9000)
	_, _, err := s.SpendAndCreate(ctx, parent, 100, utxo.WithCreateOnly())
	require.NoError(t, err)

	parentHash := parent.TxIDChainHash()

	var n int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM utxo WHERE txid = $1`, parentHash[:]).Scan(&n))
	require.Equal(t, 1, n, "WithCreateOnly must create outputs")

	// a child spending the parent and creating its own outputs, in one call
	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash: parentHash, Vout: 0,
		LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
	}))

	script, err := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
	require.NoError(t, err)
	child.AddOutput(&bt.Output{Satoshis: 8000, LockingScript: script})

	_, spends, err := s.SpendAndCreate(ctx, child, 200)
	require.NoError(t, err)
	require.Len(t, spends, 1)
	require.NoError(t, spends[0].Err)

	// both halves must have landed: the parent output consumed AND the child's created
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM utxo WHERE txid = $1`, parentHash[:]).Scan(&n))
	require.Equal(t, 0, n, "the spent parent output must be gone")

	childHash := child.TxIDChainHash()
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM utxo WHERE txid = $1`, childHash[:]).Scan(&n))
	require.Equal(t, 1, n, "the child's output must exist")
}

// TestSpendAndCreateRejectsContradictoryOptions pins the one invalid combination.
func TestSpendAndCreateRejectsContradictoryOptions(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 100)

	_, _, err := s.SpendAndCreate(ctx, tx, 100, utxo.WithCreateOnly(), utxo.WithSpendOnly())
	require.Error(t, err, "WithCreateOnly and WithSpendOnly are mutually exclusive")
}

// TestSpendWritesJournal is the property that makes a delete-on-spend store
// recoverable at all: the coin's payload must be captured at the instant it is
// destroyed, in the same statement, or a reorg and ProcessConflicting have nothing to
// restore from. It cannot be re-derived -- the node keeps almost no blocks, and the
// subtree data it does keep carries outpoints without satoshis or scripts.
func TestSpendWritesJournal(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 4242)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	parentHash := parent.TxIDChainHash()

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash: parentHash, Vout: 0,
		LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
	}))

	_, err = spendOnly(ctx, s, child, 200)
	require.NoError(t, err)

	// the UTXO row is gone...
	var live int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM utxo WHERE txid = $1`, parentHash[:]).Scan(&live))
	require.Equal(t, 0, live)

	// ...and everything needed to put it back is in the journal, with the spender
	// recorded as the ownership token
	var (
		sats      int64
		script    []byte
		spender   []byte
		spentAt   int32
		createdAt int32
	)
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT satoshis, script, spending_txid, spent_height, created_height
		   FROM spend_journal WHERE txid = $1`, parentHash[:]).
		Scan(&sats, &script, &spender, &spentAt, &createdAt))

	childHash := child.TxIDChainHash()
	require.Equal(t, int64(4242), sats, "the journal must carry the satoshis")
	require.Equal(t, []byte(*parent.Outputs[0].LockingScript), script, "and the locking script")
	require.Equal(t, childHash[:], spender, "and the spender, as the restore ownership token")
	require.Equal(t, int32(200), spentAt)
	require.Equal(t, int32(100), createdAt, "created_height must survive, for BIP68 and maturity")
}

// TestSpendJournalReclaimIsDrivenByThePruner pins BOTH halves of moving reclaim off the
// spend path.
//
// Reclaim used to run from ensureSpendJournalPartition, which the spend path calls on
// every spend. The reasoning was sound for a catalog operation -- dropping a partition is
// constant time, and a background reclaimer that falls behind is what killed the previous
// store. But DETACH CONCURRENTLY waits for every open transaction on the parent, so from
// inside a spend it stalls the pipeline with 6,400 goroutines queued behind it, and the
// caller could only swallow the error to avoid failing the spend.
//
// So: spending must NOT reclaim, and the pruner service MUST. The pruner service is the
// existing height-driven trigger -- services/pruner/worker.go calls Prune once per block
// off its own goroutine, logs a returned error at Errorf with a metric, and times it.
func TestSpendJournalReclaimIsDrivenByThePruner(t *testing.T) {
	s, ctx := newTestStore(t)

	s.journalRetention = 96 // 2 leaves, so the test does not need 1440 blocks

	// spend across a span wide enough to roll several leaves over
	for h := uint32(100); h <= 500; h += 40 {
		parent := mkTx(t, 1, uint64(1000+h))
		_, err := s.Create(ctx, parent, h)
		require.NoError(t, err)

		child := bt.NewTx()
		require.NoError(t, child.FromUTXOs(&bt.UTXO{
			TxIDHash: parent.TxIDChainHash(), Vout: 0,
			LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
		}))

		_, err = spendOnly(ctx, s, child, h)
		require.NoError(t, err)
	}

	// Heights 100..500 at 48 per leaf touch leaves 2,3,4,5,6,7,8,9,10.
	require.Equal(t, 9, journalLeaves(t, s, ctx),
		"the spend path must create leaves and reclaim NOTHING: DETACH CONCURRENTLY waits on every open transaction on the parent")

	svc, err := s.GetPrunerService()
	require.NoError(t, err)

	n, err := svc.Prune(ctx, 500, "deadbeef")
	require.NoError(t, err)
	require.Zero(t, n, "no transaction records are deleted yet, and reporting journal rows in a children-deleted counter would be a lie")

	// retention 96 / 48 per leaf = 2, plus the one being filled, plus at most one not
	// yet crossed. The point is that it is bounded, not that it is exact.
	require.LessOrEqual(t, journalLeaves(t, s, ctx), 4,
		"journal leaves must be reclaimed as the chain advances, not accumulate")
	require.Positive(t, journalLeaves(t, s, ctx), "recent history must still be retained")
}

// TestSpendJournalReclaimRecoversOrphanedPartitions covers the crash window that moving
// the drop into a session is supposed to close.
//
// Reclaim is two statements: DETACH PARTITION ... CONCURRENTLY, then DROP TABLE. A crash
// between them leaves a fully detached standalone table, which is verifiably invisible to
// any listing that joins pg_inherits -- relispartition goes false and the inheritance row
// is gone. Being "the last step of an idempotent session" does not help if the next
// session cannot SEE the leak, so the listing has to find orphans by name in the journal's
// own schema, not just attached partitions.
func TestSpendJournalReclaimRecoversOrphanedPartitions(t *testing.T) {
	s, ctx := newTestStore(t)

	s.journalRetention = 96

	require.NoError(t, s.ensureSpendJournalPartition(ctx, 100)) // leaf 2
	require.NoError(t, s.ensureSpendJournalPartition(ctx, 500)) // leaf 10

	// Simulate the crash: detach leaf 2 and stop, exactly as a kill between the two
	// statements would leave it.
	_, err := s.pool.Exec(ctx, `ALTER TABLE spend_journal DETACH PARTITION spend_journal_2 CONCURRENTLY`)
	require.NoError(t, err, "single-statement DETACH CONCURRENTLY is not in a transaction block and must succeed")

	var isPartition bool
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT relispartition FROM pg_class WHERE oid = 'spend_journal_2'::regclass`).Scan(&isPartition))
	require.False(t, isPartition, "the orphan is no longer a partition, which is why pg_inherits cannot find it")

	svc, err := s.GetPrunerService()
	require.NoError(t, err)
	_, err = svc.Prune(ctx, 500, "deadbeef")
	require.NoError(t, err)

	var stillThere bool
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.relname = 'spend_journal_2' AND n.nspname = current_schema())`).Scan(&stillThere))
	require.False(t, stillThere, "an orphaned partition must be reclaimed by the next session, or it leaks forever")
}

// journalLeaves counts the journal's partitions, resolving the parent the same way the DDL
// does so another schema's spend_journal cannot be counted here.
func journalLeaves(t *testing.T, s *Store, ctx context.Context) int {
	t.Helper()

	var n int
	require.NoError(t, s.pool.QueryRow(ctx, `
        SELECT count(*) FROM pg_inherits i
         WHERE i.inhparent = 'spend_journal'::regclass`).Scan(&n))

	return n
}

// spendOne creates a parent, spends output 0, and returns the Spend record describing it.
func spendOne(t *testing.T, s *Store, ctx context.Context, sats uint64, h uint32) (*bt.Tx, *bt.Tx, []*utxo.Spend) {
	t.Helper()

	parent := mkTx(t, 1, sats)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash: parent.TxIDChainHash(), Vout: 0,
		LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
	}))

	spends, err := spendOnly(ctx, s, child, h)
	require.NoError(t, err)
	require.Len(t, spends, 1)

	// Spend fills the spender in now. The assignment is kept because it is what a caller
	// holding a record built elsewhere would do, and it must stay harmless.
	spends[0].SpendingData = spend.NewSpendingData(child.TxIDChainHash(), 0)

	return parent, child, spends
}

// TestUnspendRestoresFromJournal is the round trip that makes a reorg survivable: a coin
// destroyed by a spend must come back byte-identical, and the journal row must be
// CONSUMED so a second restore cannot duplicate it.
func TestUnspendRestoresFromJournal(t *testing.T) {
	s, ctx := newTestStore(t)

	parent, _, spends := spendOne(t, s, ctx, 3131, 200)
	parentHash := parent.TxIDChainHash()

	require.NoError(t, s.Unspend(ctx, spends))

	// restored byte-identical
	var (
		sats    int64
		script  []byte
		created int32
	)
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT satoshis, script, created_height FROM utxo WHERE txid = $1`, parentHash[:]).
		Scan(&sats, &script, &created))
	require.Equal(t, int64(3131), sats)
	require.Equal(t, []byte(*parent.Outputs[0].LockingScript), script)
	require.Equal(t, int32(100), created, "created_height must survive the round trip, for BIP68 and maturity")

	// the journal row is consumed: the restore is single-use
	var remaining int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM spend_journal WHERE txid = $1`, parentHash[:]).Scan(&remaining))
	require.Equal(t, 0, remaining, "the journal row must be consumed by the restore")

	// so a second restore's DELETE finds nothing to consume -- but the coin it would have
	// restored is already live, and Unspend must recognise that and say so as success, not
	// error: this is the shape a crashed-and-replayed WAL intent produces (see unspend.go's
	// live_before), and BlockAssembler's conflict-intent replay depends on Unspend
	// tolerating it.
	require.NoError(t, s.Unspend(ctx, spends), "a second restore of an already-live coin must be a no-op, not an error")

	var live int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM utxo WHERE txid = $1`, parentHash[:]).Scan(&live))
	require.Equal(t, 1, live, "exactly one live row after a double restore attempt, never a duplicate")
}

// TestUnspendRefusesWrongSpender is the ownership token doing its job. A stale reorg
// record must never resurrect a coin that a DIFFERENT transaction has since taken.
func TestUnspendRefusesWrongSpender(t *testing.T) {
	s, ctx := newTestStore(t)

	parent, _, spends := spendOne(t, s, ctx, 4141, 200)

	// claim it was spent by some other transaction
	other := mkTx(t, 1, 1)
	spends[0].SpendingData = spend.NewSpendingData(other.TxIDChainHash(), 0)

	require.Error(t, s.Unspend(ctx, spends),
		"a restore naming the wrong spender must fail, not resurrect the coin")

	parentHash := parent.TxIDChainHash()

	var live int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM utxo WHERE txid = $1`, parentHash[:]).Scan(&live))
	require.Equal(t, 0, live, "the coin must stay spent")

	var journalled int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM spend_journal WHERE txid = $1`, parentHash[:]).Scan(&journalled))
	require.Equal(t, 1, journalled, "and its journal row must be left intact for the rightful restore")
}

// TestUnspendRequiresSpender refuses to guess. Restoring on the outpoint alone could
// resurrect a coin a different transaction now owns.
func TestUnspendRequiresSpender(t *testing.T) {
	s, ctx := newTestStore(t)

	_, _, spends := spendOne(t, s, ctx, 5151, 200)
	spends[0].SpendingData = nil

	require.Error(t, s.Unspend(ctx, spends), "no ownership token means no restore")
}

// TestSpendBelowFirstJournalLeafCreatesPartition pins a bug that only ever appears on a
// fresh sync from genesis, which is exactly the workload this store exists for.
//
// The partition cache used to hold the leaf number itself, so its zero value was
// indistinguishable from "leaf 0 has already been created". Every spend below height 48
// therefore saw a cache hit, skipped the CREATE TABLE, and failed the journal insert with
// "no partition of relation spend_journal found for row". Higher leaves worked fine, so
// every test that happened to pick a height above 48 passed and the store looked healthy.
func TestSpendBelowFirstJournalLeafCreatesPartition(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 7777)
	_, err := s.Create(ctx, parent, 1)
	require.NoError(t, err)

	parentHash := parent.TxIDChainHash()

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash: parentHash, Vout: 0,
		LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
	}))

	// Height 2 lands in leaf 0 -- the leaf the zero value shadowed.
	_, err = spendOnly(ctx, s, child, 2)
	require.NoError(t, err, "a spend in the first journal leaf must create its partition")

	var journaled int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM spend_journal_0 WHERE txid = $1`, parentHash[:]).Scan(&journaled))
	require.Equal(t, 1, journaled)
}

// TestCreateDoesNotGateOrdinaryOutputsOnHeight guards against re-inventing a consensus
// rule that does not exist.
//
// spendable_from used to be set to the creation height for every output, which quietly
// meant "an output may not be spent below the height it was created at". Nothing in
// consensus says that -- only coinbase maturity and an explicit reassignment delay hold
// an output back -- and it would reject valid spends during a reorg or whenever a caller
// passes a height that is not strictly increasing.
func TestCreateDoesNotGateOrdinaryOutputsOnHeight(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5150)
	_, err := s.Create(ctx, parent, 1000)
	require.NoError(t, err)

	parentHash := parent.TxIDChainHash()

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash: parentHash, Vout: 0,
		LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
	}))

	// Spent far below the height it was created at.
	//
	// Assert on the Spend records, not just the returned error: a miss is reported per
	// input on spends[i].Err and classifyMisses can leave the top-level error nil, so
	// require.NoError alone would sail straight past the bug this test exists for.
	spends, err := spendOnly(ctx, s, child, 1)
	require.NoError(t, err)
	require.Len(t, spends, 1)
	require.NoError(t, spends[0].Err, "an ordinary output must not be gated on its own creation height")

	var live int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM utxo WHERE txid = $1`, parentHash[:]).Scan(&live))
	require.Equal(t, 0, live, "the coin must actually be gone, not merely reported as spent")
}

// spendOnly is what the tests used to get from Store.Spend, which no longer exists.
//
// SpendAndCreate with the spend-only option is now the only way to consume inputs, so this
// wrapper keeps the call sites readable. The difference from the old method is deliberate and
// is the whole point of deleting it: a per-input failure now surfaces as a returned error AND
// rolls the whole transaction back, so a sibling coin is no longer destroyed by a transaction
// that was rejected.
func spendOnly(ctx context.Context, s *Store, tx *bt.Tx, blockHeight uint32,
	opts ...utxo.CreateOption) ([]*utxo.Spend, error) {
	_, spends, err := s.SpendAndCreate(ctx, tx, blockHeight, append(opts, utxo.WithSpendOnly())...)
	return spends, err
}

// TestSpendJournalDropTakesTheOldestLeafFirst pins the order the drop loop works in.
//
// The listing query has no ORDER BY, so without an explicit sort the catalog returns leaves
// in whatever order it scanned them, and that order shifts as tables are created and dropped.
// With one leaf retiring every 48 blocks and nothing behind, the order does not matter. With
// a backlog it decides which work gets done before the session ends, and a session ends when
// the daemon restarts rather than when the work runs out.
//
// The mainnet box showed the failure: leaf 4,676 was still present while the session was
// working on leaf 9,353, so the oldest leaf never got attacked and the oldest-surviving-leaf
// watermark never moved.
//
// Creating the leaves out of order is the point of this test. Doing it ascending would pass
// against the unsorted code by luck, because the catalog would most likely hand them back in
// creation order.
//
// The order is observed by stopping the run part way. This used to be read off a callback the
// pruner passed in to use each retiring leaf as a work list; nothing reads a retiring leaf any
// more, so the leaf that survives an interrupted run is what says where the run had got to.
func TestSpendJournalDropTakesTheOldestLeafFirst(t *testing.T) {
	s, ctx := newTestStore(t)

	// Deliberately scrambled: leaves 10, 2, 7, 4 in creation order.
	for _, h := range []uint32{500, 100, 350, 200} {
		require.NoError(t, s.ensureSpendJournalPartition(ctx, h))
	}

	// Leaf 7 is pinned so the run fails there. A view is the cheapest dependency postgres
	// refuses to drop through, and it has to be removed again: the test database is shared by
	// the whole binary, and a pinned leaf left behind would make a later test's
	// CREATE TABLE IF NOT EXISTS skip a name it thinks is taken.
	_, err := s.pool.Exec(ctx, `CREATE VIEW journal_leaf_7_pin AS SELECT * FROM spend_journal_7`)
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = s.pool.Exec(ctx, `DROP VIEW IF EXISTS journal_leaf_7_pin`)
		_, _ = s.pool.Exec(ctx, `DROP TABLE IF EXISTS spend_journal_7`)
	})

	// A cutoff above every leaf, so all four are eligible and the order is the only variable.
	_, err = s.dropSpendJournalPartitionsBelow(ctx, 100_000)
	require.Error(t, err, "leaf 7 cannot be dropped, and the run must report that rather than skip it")

	require.False(t, tableExists(t, s, ctx, "spend_journal_2"), "the oldest leaf must go first")
	require.False(t, tableExists(t, s, ctx, "spend_journal_4"), "then the next oldest")
	require.True(t, tableExists(t, s, ctx, "spend_journal_7"), "the run stopped here")
	require.True(t, tableExists(t, s, ctx, "spend_journal_10"),
		"and never reached the newest leaf, which is the whole point of oldest-first")
}

// TestStorePoolDisablesJIT pins the one setting that stands between this store and a stall.
//
// A plan assertion can pin the SHAPE of a statement's plan but not its estimated COST, and the
// cost is what postgres reads when it decides whether to hand the plan to LLVM. Because every
// batch arrives as an array the planner has no statistics for, and because each table is
// partitioned so the resulting guess gets multiplied, the estimate lands at 679,043 on real
// mainnet data for a query that reads about twenty index pages. Postgres compiles above
// 100,000.
//
// On the mainnet soak box that compile was measured at 1,430,530 ms, then 499 ms on the very
// next execution in the same session, against 28 ms with JIT off. The spread is memory: nothing
// caches compiled code between executions, so the cost is really the cost of faulting LLVM's
// pages back in, which on a loaded box takes minutes.
//
// The setting travels in the connection's startup packet, so this also proves a reconnect
// carries it rather than silently coming back with the server default.
func TestStorePoolDisablesJIT(t *testing.T) {
	s, ctx := newTestStore(t)

	var jit string

	require.NoError(t, s.pool.QueryRow(ctx, `SELECT current_setting('jit')`).Scan(&jit),
		"reading jit from the store's own pool")

	require.Equal(t, "off", jit,
		"the store's pool must disable JIT; see the comment on the pool setup in New")
}
