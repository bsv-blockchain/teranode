package utxo

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func TestOrphanLedgerWaitReturnsAtOnceWithoutOrphan(t *testing.T) {
	var ledger OrphanLedger

	id := chainhash.Hash{1}

	require.NoError(t, ledger.Wait(context.Background(), &id))
}

func TestOrphanLedgerWaitBlocksUntilEveryOrphanOfTheSpenderEnds(t *testing.T) {
	var ledger OrphanLedger

	id := chainhash.Hash{1}
	ledger.Begin(&id)
	ledger.Begin(&id)

	released := make(chan error, 1)

	go func() { released <- ledger.Wait(context.Background(), &id) }()

	ledger.End(&id)

	select {
	case <-released:
		t.Fatal("Wait returned while one orphan was unresolved")
	case <-time.After(100 * time.Millisecond):
	}

	ledger.End(&id)

	select {
	case err := <-released:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after the last orphan ended")
	}
}

func TestOrphanLedgerWaitIgnoresOrphansOfOtherSpenders(t *testing.T) {
	var ledger OrphanLedger

	other := chainhash.Hash{2}
	id := chainhash.Hash{1}
	ledger.Begin(&other)

	require.NoError(t, ledger.Wait(context.Background(), &id))
}

func TestOrphanLedgerWaitReturnsContextError(t *testing.T) {
	var ledger OrphanLedger

	id := chainhash.Hash{1}
	ledger.Begin(&id)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, ledger.Wait(ctx, &id), context.DeadlineExceeded)
}

func TestOrphanLedgerSpenderCanBeginAgainAfterItEnded(t *testing.T) {
	var ledger OrphanLedger

	id := chainhash.Hash{1}
	ledger.Begin(&id)
	ledger.End(&id)
	ledger.Begin(&id)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, ledger.Wait(ctx, &id), context.DeadlineExceeded)

	ledger.End(&id)
	require.NoError(t, ledger.Wait(context.Background(), &id))
}
