package utxo

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

func TestOrphanLedgerAcquireReturnsAtOnceForFreeSpender(t *testing.T) {
	var ledger OrphanLedger

	id := chainhash.Hash{1}

	require.NoError(t, ledger.Acquire(context.Background(), &id))
}

func TestOrphanLedgerAcquireBlocksUntilEveryHoldOfTheSpenderEnds(t *testing.T) {
	var ledger OrphanLedger

	id := chainhash.Hash{1}
	require.NoError(t, ledger.Acquire(context.Background(), &id))
	ledger.Begin(&id)

	acquired := make(chan error, 1)

	go func() { acquired <- ledger.Acquire(context.Background(), &id) }()

	ledger.End(&id)

	select {
	case <-acquired:
		t.Fatal("Acquire returned while one hold was unresolved")
	case <-time.After(100 * time.Millisecond):
	}

	ledger.End(&id)

	select {
	case err := <-acquired:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Acquire did not return after the last hold ended")
	}
}

func TestOrphanLedgerAcquireIgnoresHoldsOfOtherSpenders(t *testing.T) {
	var ledger OrphanLedger

	other := chainhash.Hash{2}
	id := chainhash.Hash{1}
	require.NoError(t, ledger.Acquire(context.Background(), &other))

	require.NoError(t, ledger.Acquire(context.Background(), &id))
}

func TestOrphanLedgerAcquireReturnsContextError(t *testing.T) {
	var ledger OrphanLedger

	id := chainhash.Hash{1}
	require.NoError(t, ledger.Acquire(context.Background(), &id))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, ledger.Acquire(ctx, &id), context.DeadlineExceeded)
}

func TestOrphanLedgerSpenderCanBeAcquiredAgainAfterItEnded(t *testing.T) {
	var ledger OrphanLedger

	id := chainhash.Hash{1}
	require.NoError(t, ledger.Acquire(context.Background(), &id))
	ledger.End(&id)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	require.NoError(t, ledger.Acquire(ctx, &id))
}

func TestOrphanLedgerAwaitSpendFailsAtBudgetWhileSpenderIsHeld(t *testing.T) {
	var ledger OrphanLedger

	id := chainhash.Hash{1}
	require.NoError(t, ledger.Acquire(context.Background(), &id))

	err := ledger.AwaitSpend(context.Background(), &id, 50*time.Millisecond)
	require.ErrorIs(t, err, errors.ErrServiceUnavailable)
}

func TestOrphanLedgerAwaitSpendReportsCallerContextEnd(t *testing.T) {
	var ledger OrphanLedger

	id := chainhash.Hash{1}
	require.NoError(t, ledger.Acquire(context.Background(), &id))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := ledger.AwaitSpend(ctx, &id, time.Second)
	require.ErrorIs(t, err, errors.ErrContextCanceled)
}
