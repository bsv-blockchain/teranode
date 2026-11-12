package sql

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func createTwoOutputParent(ctx context.Context, t *testing.T, store *Store, prevHash string) *bt.Tx {
	t.Helper()

	parent := bt.NewTx()
	require.NoError(t, parent.From(prevHash, 0, "51", 10_000))
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1_000))
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 2_000))
	parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})

	_, err := store.Create(ctx, parent, floorCachedTip)
	require.NoError(t, err)

	return parent
}

type parentOutput struct {
	parent *bt.Tx
	vout   uint32
}

func spendParentOutputs(ctx context.Context, t *testing.T, store *Store, outputs []parentOutput) []*utxo.Spend {
	t.Helper()

	child := bt.NewTx()

	for _, o := range outputs {
		out := o.parent.Outputs[o.vout]
		require.NoError(t, child.From(o.parent.TxIDChainHash().String(), o.vout, out.LockingScript.String(), out.Satoshis))
	}

	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 100))

	spends, err := store.Spend(ctx, child, floorCachedTip+1)
	require.NoError(t, err)
	require.Len(t, spends, len(outputs))

	byOutput := make(map[chainhash.Hash]map[uint32]*utxo.Spend)

	for _, sp := range spends {
		if byOutput[*sp.TxID] == nil {
			byOutput[*sp.TxID] = make(map[uint32]*utxo.Spend)
		}

		byOutput[*sp.TxID][sp.Vout] = sp
	}

	ordered := make([]*utxo.Spend, len(outputs))

	for i, o := range outputs {
		ordered[i] = byOutput[*o.parent.TxIDChainHash()][o.vout]
		require.NotNil(t, ordered[i])
	}

	return ordered
}

func countUnspent(ctx context.Context, t *testing.T, store *Store, parent *bt.Tx, vout uint32) int {
	t.Helper()

	var n int

	require.NoError(t, store.db.QueryRowContext(ctx, `
		SELECT count(*) FROM outputs o JOIN transactions t ON t.id = o.transaction_id
		WHERE t.hash = $1 AND o.idx = $2 AND o.spending_data IS NULL`, parent.TxIDChainHash()[:], vout).Scan(&n))

	return n
}

type lockSequence struct {
	mu  sync.Mutex
	ids []int
}

func (l *lockSequence) record(id int) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.ids = append(l.ids, id)

	return len(l.ids)
}

func (l *lockSequence) snapshot() []int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]int(nil), l.ids...)
}

func parentLocked(ctx context.Context, t *testing.T, store *Store, id int) bool {
	t.Helper()

	var locked bool

	require.NoError(t, store.db.QueryRowContext(ctx, "SELECT locked FROM transactions WHERE id = $1", id).Scan(&locked))

	return locked
}

// Unspend must lock each distinct parent once, in ascending id order, whatever order
// the spends arrive in, so it cannot form a lock cycle with a spend batch that locks
// the same parents in id order. Passing flagAsLocked writes the locked flag on each
// parent inside that same order.
func TestUnspendLocksParentsInIDOrderOnce_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)

	require.NoError(t, store.SetBlockHeight(floorCachedTip))

	for i, tc := range []struct {
		name       string
		flags      []bool
		wantLocked bool
	}{
		{name: "no locked flag", flags: nil, wantLocked: false},
		{name: "flagAsLocked true", flags: []bool{true}, wantLocked: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parentA := createTwoOutputParent(ctx, t, store, fmt.Sprintf("4ff7de8f0b6d5c4a3e2f1d0c9b8a7968574635241302f1e0d9c8b7a6958473%02d", i))
			parentB := createTwoOutputParent(ctx, t, store, fmt.Sprintf("5aa7de8f0b6d5c4a3e2f1d0c9b8a7968574635241302f1e0d9c8b7a6958473%02d", i))

			idA := floorRowID(ctx, t, store, *parentA.TxIDChainHash())
			idB := floorRowID(ctx, t, store, *parentB.TxIDChainHash())

			require.Less(t, idA, idB)

			spends := spendParentOutputs(ctx, t, store, []parentOutput{{parentB, 0}, {parentA, 0}, {parentB, 1}})

			for _, o := range []parentOutput{{parentB, 0}, {parentA, 0}, {parentB, 1}} {
				require.Zero(t, countUnspent(ctx, t, store, o.parent, o.vout), "setup: the output must be spent")
			}

			require.False(t, parentLocked(ctx, t, store, idA), "setup: the first parent starts unlocked")
			require.False(t, parentLocked(ctx, t, store, idB), "setup: the second parent starts unlocked")

			probe := func(id int) error {
				txn, err := store.db.BeginTx(ctx, nil)
				require.NoError(t, err)

				defer func() { _ = txn.Rollback() }()

				_, err = txn.ExecContext(ctx, "SELECT id FROM transactions WHERE id = $1 FOR UPDATE NOWAIT", id)

				return err
			}

			seq := &lockSequence{}

			setDAHAfterLock = func(id int) {
				if seq.record(id) != 1 {
					return
				}

				err := probe(idA)
				require.Error(t, err, "the lowest parent must be locked")

				var pgErr *pgconn.PgError

				require.True(t, errors.As(err, &pgErr), "unexpected error type: %v", err)
				require.Equal(t, pgLockNotAvailable, pgErr.Code)

				require.NoError(t, probe(idB), "the higher parent must still be free")
			}

			t.Cleanup(func() { setDAHAfterLock = nil })

			require.NoError(t, store.Unspend(ctx, spends, tc.flags...))

			require.Equal(t, []int{idA, idB}, seq.snapshot())

			for _, o := range []parentOutput{{parentB, 0}, {parentA, 0}, {parentB, 1}} {
				require.Equal(t, 1, countUnspent(ctx, t, store, o.parent, o.vout))
			}

			require.Equal(t, tc.wantLocked, parentLocked(ctx, t, store, idA), "locked flag of the first parent")
			require.Equal(t, tc.wantLocked, parentLocked(ctx, t, store, idB), "locked flag of the second parent")
		})
	}
}

// A PostgreSQL deadlock aborts one side of a lock cycle. When Unspend is that side it
// must roll back and run again instead of leaving the outputs for operator recovery.
func TestUnspendRetriesAfterAPostgresDeadlock_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)

	require.NoError(t, store.SetBlockHeight(floorCachedTip))

	parentA := createTwoOutputParent(ctx, t, store, "4ff7de8f0b6d5c4a3e2f1d0c9b8a7968574635241302f1e0d9c8b7a695847362")
	parentB := createTwoOutputParent(ctx, t, store, "5aa7de8f0b6d5c4a3e2f1d0c9b8a7968574635241302f1e0d9c8b7a695847363")

	idA := floorRowID(ctx, t, store, *parentA.TxIDChainHash())
	idB := floorRowID(ctx, t, store, *parentB.TxIDChainHash())

	spends := spendParentOutputs(ctx, t, store, []parentOutput{{parentA, 0}, {parentB, 0}})

	holder, err := store.db.BeginTx(ctx, nil)
	require.NoError(t, err)

	defer func() { _ = holder.Rollback() }()

	_, err = holder.ExecContext(ctx, "SET LOCAL deadlock_timeout = '10s'")
	require.NoError(t, err)

	_, err = holder.ExecContext(ctx, "SELECT id FROM transactions WHERE id = $1 FOR UPDATE", idB)
	require.NoError(t, err)

	seq := &lockSequence{}
	holderLockedA := make(chan error, 1)
	unspendDone := make(chan error, 1)

	setDAHAfterLock = func(id int) {
		if seq.record(id) != 1 || id != idA {
			return
		}

		go func() {
			deadline := time.Now().Add(10 * time.Second)

			for time.Now().Before(deadline) {
				var waiting int

				if err := store.db.QueryRowContext(ctx, "SELECT count(*) FROM pg_locks WHERE NOT granted").Scan(&waiting); err != nil {
					holderLockedA <- err

					return
				}

				if waiting > 0 {
					break
				}

				time.Sleep(10 * time.Millisecond)
			}

			_, err := holder.ExecContext(ctx, "SELECT id FROM transactions WHERE id = $1 FOR UPDATE", idA)
			holderLockedA <- err
		}()
	}

	t.Cleanup(func() { setDAHAfterLock = nil })

	go func() { unspendDone <- store.Unspend(ctx, spends) }()

	select {
	case err := <-holderLockedA:
		require.NoError(t, err, "the holder must win the lock on the first parent once Unspend is aborted")
	case <-time.After(30 * time.Second):
		t.Fatal("the holder never obtained the lock on the first parent")
	}

	require.NoError(t, holder.Rollback())

	select {
	case err := <-unspendDone:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("Unspend did not finish after the holder released its locks")
	}

	seqIDs := seq.snapshot()

	require.Greater(t, countID(seqIDs, idA), 1, "the first parent must be locked again by a retry: %v", seqIDs)
	require.Equal(t, 1, countUnspent(ctx, t, store, parentA, 0))
	require.Equal(t, 1, countUnspent(ctx, t, store, parentB, 0))
}

func countID(ids []int, id int) int {
	n := 0

	for _, v := range ids {
		if v == id {
			n++
		}
	}

	return n
}
