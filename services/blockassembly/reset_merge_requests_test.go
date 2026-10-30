package blockassembly

import (
	"context"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	utxoStore "github.com/bsv-blockchain/teranode/stores/utxo"
	utxostoresql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/stretchr/testify/require"
)

// seedConflictedUnminedTx stores an unmined transaction whose only input is spent
// by a different transaction, the state only a validating reload can detect. It
// returns the unmined transaction's hash.
func seedConflictedUnminedTx(ctx context.Context, t *testing.T, items *baTestItems) *chainhash.Hash {
	t.Helper()

	txParent := bt.NewTx()
	parentIn := &bt.Input{
		PreviousTxOutIndex: 0,
		PreviousTxSatoshis: 200000,
		SequenceNumber:     0xFFFFFFFF,
		UnlockingScript:    bscript.NewFromBytes([]byte{}),
	}
	require.NoError(t, parentIn.PreviousTxIDAdd(&chainhash.Hash{1, 2, 3}))
	txParent.Inputs = []*bt.Input{parentIn}
	txParent.Outputs = []*bt.Output{
		{Satoshis: 100000, LockingScript: bscript.NewFromBytes([]byte{0x76, 0xa9, 0x14, 0x00, 0x88, 0xac})},
	}

	_, _, err := items.utxoStore.SpendAndCreate(ctx, txParent, 1, utxoStore.WithCreateOnly())
	require.NoError(t, err)

	parentHash := txParent.TxIDChainHash()
	require.NoError(t, items.utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*parentHash}, true))

	txA := bt.NewTx()
	require.NoError(t, txA.From(parentHash.String(), 0, txParent.Outputs[0].LockingScript.String(), txParent.Outputs[0].Satoshis))
	txA.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})
	txA.Outputs = []*bt.Output{{Satoshis: 90000, LockingScript: bscript.NewFromBytes([]byte{0x52})}}

	require.NoError(t, items.utxoStore.SetBlockHeight(5))
	_, _, err = items.utxoStore.SpendAndCreate(ctx, txA, 5, utxoStore.WithCreateOnly())
	require.NoError(t, err)

	// Point the parent output's spending data at a winner that is not txA.
	winnerHash := chainhash.HashH([]byte("mined-winner-tx"))
	sdBytes := make([]byte, 36)
	copy(sdBytes[:32], winnerHash.CloneBytes())

	sqlStore, ok := items.utxoStore.(*utxostoresql.Store)
	require.True(t, ok, "test requires the SQLite store")

	_, err = sqlStore.RawDB().Exec(
		"UPDATE outputs SET spending_data = ? WHERE transaction_id = (SELECT id FROM transactions WHERE hash = ?) AND idx = 0",
		sdBytes, parentHash[:],
	)
	require.NoError(t, err)

	return txA.TxIDChainHash()
}

func setupConflictedReset(ctx context.Context, t *testing.T) (*baTestItems, *chainhash.Hash) {
	t.Helper()

	initPrometheusMetrics()

	items := setupBlockAssemblyTestWithUtxoStore(t)
	items.blockAssembler.settings.BlockAssembly.OnRestartValidateParentChain = false

	txAHash := seedConflictedUnminedTx(ctx, t, items)
	genesisChainForMergedReset(t, items)

	return items, txAHash
}

// TestHandleResetRequests_QueuedValidatingRequestIsHonoured checks the merged input validation.
// A plain reset received first used to run alone and then drain a queued validating request,
// answering it nil. The end state shows which reset ran: only a validating one keeps the
// transaction whose input is spent elsewhere out of the template.
func TestHandleResetRequests_QueuedValidatingRequestIsHonoured(t *testing.T) {
	t.Run("a plain reset alone leaves the conflict undetected", func(t *testing.T) {
		ctx := t.Context()
		items, txAHash := setupConflictedReset(ctx, t)

		plainCh := make(chan error, 1)
		items.blockAssembler.handleResetRequests(ctx, resetRequest{ErrCh: plainCh})

		require.NoError(t, <-plainCh)
		require.True(t, containsHash(items.blockAssembler.subtreeProcessor.GetTransactionHashes(ctx), *txAHash),
			"a plain reset reloads the conflicted transaction, which is what the merge must prevent")
	})

	t.Run("a validating request queued behind a plain one runs validated", func(t *testing.T) {
		ctx := t.Context()
		items, txAHash := setupConflictedReset(ctx, t)

		plainCh := make(chan error, 1)
		validatingCh := make(chan error, 1)

		// The loop received the plain request; the validating one is queued behind it.
		items.blockAssembler.resetCh <- resetRequest{ValidateInputs: true, ErrCh: validatingCh}
		items.blockAssembler.handleResetRequests(ctx, resetRequest{ErrCh: plainCh})

		require.NoError(t, <-plainCh)
		require.NoError(t, <-validatingCh)
		require.Empty(t, items.blockAssembler.resetCh, "the queued request was taken into the reset")

		require.False(t, containsHash(items.blockAssembler.subtreeProcessor.GetTransactionHashes(ctx), *txAHash),
			"the conflicted transaction is not in the template")
	})
}

// scanCountingStore counts the unmined_since consistency scans asked of the real store. The SQL
// store answers the scan with a nil iterator and has nothing to repair, so the call is the only
// observable effect of fixUnminedSinceInconsistencies here.
type scanCountingStore struct {
	utxoStore.Store

	scans atomic.Int64
}

func (s *scanCountingStore) ScanInconsistentUnminedTxs() (utxoStore.ConsistencyScanIterator, error) {
	s.scans.Add(1)

	return s.Store.ScanInconsistentUnminedTxs()
}

// TestHandleResetRequests_QueuedFullResetRunsTheScan checks the FullReset flag is merged too: the
// unmined_since scan runs once when only a drained request asked for it, and not at all when
// nobody did. The scan is observed as a call on a counting wrapper over the real SQL store.
func TestHandleResetRequests_QueuedFullResetRunsTheScan(t *testing.T) {
	ctx := t.Context()
	items, _ := setupConflictedReset(ctx, t)

	counting := &scanCountingStore{Store: items.utxoStore}
	items.blockAssembler.utxoStore = counting

	plainCh := make(chan error, 1)
	items.blockAssembler.handleResetRequests(ctx, resetRequest{ErrCh: plainCh})
	require.NoError(t, <-plainCh)
	require.Zero(t, counting.scans.Load(), "nobody asked for a full reset")

	fullCh := make(chan error, 1)
	items.blockAssembler.resetCh <- resetRequest{FullReset: true, ErrCh: fullCh}
	items.blockAssembler.handleResetRequests(ctx, resetRequest{ErrCh: plainCh})

	require.NoError(t, <-plainCh)
	require.NoError(t, <-fullCh)
	require.Equal(t, int64(1), counting.scans.Load(), "the scan the drained request asked for ran, once")
}

// TestHandleResetRequests_EveryRequestGetsTheRealResult checks the drained requests are told how
// the reset went, not nil: a store that is down fails the reload, and each sender must see that.
func TestHandleResetRequests_EveryRequestGetsTheRealResult(t *testing.T) {
	ctx := t.Context()
	items, _ := setupConflictedReset(ctx, t)

	flaky := &iteratorDownStore{Store: items.utxoStore}
	flaky.iteratorDown.Store(true)
	items.blockAssembler.utxoStore = flaky

	firstCh := make(chan error, 1)
	queuedCh := make(chan error, 1)

	items.blockAssembler.resetCh <- resetRequest{ValidateInputs: true, ErrCh: queuedCh}
	items.blockAssembler.handleResetRequests(ctx, resetRequest{ErrCh: firstCh})

	require.Error(t, <-firstCh)
	require.Error(t, <-queuedCh, "the drained request is told the reset failed")
}

// midResetQueueStore queues a reset request the first time the reload asks for the unmined
// iterator, which is while handleResetRequests is running the reset.
type midResetQueueStore struct {
	utxoStore.Store

	ba     *BlockAssembler
	req    resetRequest
	queued atomic.Bool
}

func (s *midResetQueueStore) GetUnminedTxIterator() (utxoStore.UnminedTxIterator, error) {
	if s.queued.CompareAndSwap(false, true) {
		s.ba.resetCh <- s.req
	}

	return s.Store.GetUnminedTxIterator()
}

// TestHandleResetRequests_RequestDuringTheResetIsNotAnswered checks that a request arriving while
// the reset runs is not answered by it, because the reset began before the request existed. It
// stays queued, and the next pass runs it.
func TestHandleResetRequests_RequestDuringTheResetIsNotAnswered(t *testing.T) {
	ctx := t.Context()
	items, txAHash := setupConflictedReset(ctx, t)

	lateCh := make(chan error, 1)
	late := &midResetQueueStore{
		Store: items.utxoStore,
		ba:    items.blockAssembler,
		req:   resetRequest{ValidateInputs: true, ErrCh: lateCh},
	}
	items.blockAssembler.utxoStore = late

	firstCh := make(chan error, 1)
	items.blockAssembler.handleResetRequests(ctx, resetRequest{ErrCh: firstCh})

	require.True(t, late.queued.Load(), "the request was queued during the reset")
	require.NoError(t, <-firstCh)
	require.Empty(t, lateCh, "the running reset did not answer a request that arrived after it began")
	require.Len(t, items.blockAssembler.resetCh, 1, "the request is still queued")
	require.True(t, containsHash(items.blockAssembler.subtreeProcessor.GetTransactionHashes(ctx), *txAHash),
		"the plain reset that was running did not validate, so it reloaded the conflicted transaction")

	next := <-items.blockAssembler.resetCh
	items.blockAssembler.handleResetRequests(ctx, next)

	require.NoError(t, <-lateCh)
	require.False(t, containsHash(items.blockAssembler.subtreeProcessor.GetTransactionHashes(ctx), *txAHash),
		"the next pass ran the validating reset and left the conflicted transaction out")
}

// TestHandleResetRequests_FlagOnTheFirstRequestSurvivesTheMerge puts each flag on the request
// the loop received and a plain request behind it. The merge reads the batch in order, so a
// merge that assigned each flag instead of ORing it would let the plain request clear it.
func TestHandleResetRequests_FlagOnTheFirstRequestSurvivesTheMerge(t *testing.T) {
	t.Run("ValidateInputs", func(t *testing.T) {
		ctx := t.Context()
		items, txAHash := setupConflictedReset(ctx, t)

		validatingCh := make(chan error, 1)
		plainCh := make(chan error, 1)

		items.blockAssembler.resetCh <- resetRequest{ErrCh: plainCh}
		items.blockAssembler.handleResetRequests(ctx, resetRequest{ValidateInputs: true, ErrCh: validatingCh})

		require.NoError(t, <-validatingCh)
		require.NoError(t, <-plainCh)
		require.False(t, containsHash(items.blockAssembler.subtreeProcessor.GetTransactionHashes(ctx), *txAHash),
			"the reset validated, so the conflicted transaction is not in the template")
	})

	t.Run("FullReset", func(t *testing.T) {
		ctx := t.Context()
		items, _ := setupConflictedReset(ctx, t)

		counting := &scanCountingStore{Store: items.utxoStore}
		items.blockAssembler.utxoStore = counting

		fullCh := make(chan error, 1)
		plainCh := make(chan error, 1)

		items.blockAssembler.resetCh <- resetRequest{ErrCh: plainCh}
		items.blockAssembler.handleResetRequests(ctx, resetRequest{FullReset: true, ErrCh: fullCh})

		require.NoError(t, <-fullCh)
		require.NoError(t, <-plainCh)
		require.Equal(t, int64(1), counting.scans.Load(), "the full reset's scan ran")
	})

	t.Run("StorageTriggered", func(t *testing.T) {
		ctx := t.Context()
		items, _ := setupConflictedReset(ctx, t)

		storageCh := make(chan error, 1)
		plainCh := make(chan error, 1)

		items.blockAssembler.resetCh <- resetRequest{ErrCh: plainCh}
		items.blockAssembler.handleResetRequests(ctx, resetRequest{StorageTriggered: true, ErrCh: storageCh})

		require.NoError(t, <-storageCh)
		require.NoError(t, <-plainCh)
		require.False(t, items.blockAssembler.lastStorageResetDone.IsZero(), "the reset counted as storage-triggered")
	})

	t.Run("StorageTriggered on a queued request", func(t *testing.T) {
		ctx := t.Context()
		items, _ := setupConflictedReset(ctx, t)

		plainCh := make(chan error, 1)
		storageCh := make(chan error, 1)

		items.blockAssembler.resetCh <- resetRequest{StorageTriggered: true, ErrCh: storageCh}
		items.blockAssembler.handleResetRequests(ctx, resetRequest{ErrCh: plainCh})

		require.NoError(t, <-plainCh)
		require.NoError(t, <-storageCh)
		require.False(t, items.blockAssembler.lastStorageResetDone.IsZero(), "the reset counted as storage-triggered")
	})
}

// blockedResetSenders counts this test's sender goroutines parked on a send to resetCh. A
// goroutine's stack header names its state, and the sender closure's frame names the test.
func blockedResetSenders() int {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]

	n := 0

	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, "[chan send") && strings.Contains(g, "TestHandleResetRequests_BlockedSendersJoinOneReset.func") {
			n++
		}
	}

	return n
}

// TestHandleResetRequests_BlockedSendersJoinOneReset queues more requests than resetCh holds, as
// the production senders do: each is a goroutine, and the ones that find the channel full block
// on the send. One reset must take all of them. Counting len(resetCh) once would take only the
// buffered two and leave the blocked senders to run back-to-back resets of their own.
func TestHandleResetRequests_BlockedSendersJoinOneReset(t *testing.T) {
	ctx := t.Context()
	items, _ := setupConflictedReset(ctx, t)

	const senders = 6

	queueCap := cap(items.blockAssembler.resetCh)
	require.Greater(t, senders, queueCap, "the test needs senders blocked behind a full channel")

	errChs := make([]chan error, senders)

	for i := range errChs {
		errChs[i] = make(chan error, 1)

		go func(errCh chan error) {
			items.blockAssembler.resetCh <- resetRequest{ErrCh: errCh}
		}(errChs[i])
	}

	require.Eventually(t, func() bool {
		return len(items.blockAssembler.resetCh) == queueCap && blockedResetSenders() == senders-queueCap
	}, 10*time.Second, time.Millisecond, "every sender is buffered or blocked on the send")

	firstCh := make(chan error, 1)
	items.blockAssembler.handleResetRequests(ctx, resetRequest{ErrCh: firstCh})

	require.NoError(t, <-firstCh)

	for i, errCh := range errChs {
		select {
		case err := <-errCh:
			require.NoError(t, err, "sender %d", i)
		default:
			require.Failf(t, "sender left unanswered", "sender %d was not taken into the reset", i)
		}
	}

	require.Empty(t, items.blockAssembler.resetCh, "no request is left for another reset")
}

// genesisChainForMergedReset uses the SQLite blockchain client at genesis.
// The reset reloads the unmined transactions without block movement.
func genesisChainForMergedReset(t *testing.T, items *baTestItems) {
	t.Helper()

	header := genesisHeader(t, items)
	items.blockAssembler.setBestBlockHeader(header, 0)
	items.blockAssembler.subtreeProcessor.InitCurrentBlockHeader(header)
}

// iteratorDownStore wraps the real store and can fail GetUnminedTxIterator, so a reload fails
// whether it validates inputs or not.
type iteratorDownStore struct {
	utxoStore.Store

	iteratorDown atomic.Bool
}

func (s *iteratorDownStore) GetUnminedTxIterator() (utxoStore.UnminedTxIterator, error) {
	if s.iteratorDown.Load() {
		return nil, errors.NewStorageError("[iteratorDownStore] the store is down")
	}

	return s.Store.GetUnminedTxIterator()
}
