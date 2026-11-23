package sql

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// fatalRecorder is a test logger whose Fatalf is counted instead of only logged:
// the production logger's Fatalf exits the process, which the test loggers hide.
type fatalRecorder struct {
	ulogger.TestLogger
	fatals atomic.Int32
}

func (r *fatalRecorder) Fatalf(string, ...interface{}) { r.fatals.Add(1) }

func (r *fatalRecorder) New(string, ...ulogger.Option) ulogger.Logger { return r }

// A winning tx the store no longer holds (a pruned side-chain tx a reorg brings
// forward) has nothing to clear, and must not take the node down: reorgBlocks
// reaches this call after its commit point. The other hashes are still marked and
// no error is returned. Marking a missing tx not on the longest chain still
// exits: a moveBack tx without a record would go back into block assembly.
func TestMarkTransactionsOnLongestChain_MissingIsSkippedNotFatal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	rec := &fatalRecorder{}
	store.logger = rec

	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)

	missing, err := chainhash.NewHashFromStr("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	require.NoError(t, err)

	txHash := *tx.TxIDChainHash()

	// put the existing tx off the longest chain, so marking it back is observable
	require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{txHash}, false))

	meta, err := store.Get(ctx, &txHash, fields.UnminedSince)
	require.NoError(t, err)
	require.NotZero(t, meta.UnminedSince)

	require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*missing, txHash}, true))
	require.Zero(t, rec.fatals.Load(), "mark true")

	meta, err = store.Get(ctx, &txHash, fields.UnminedSince)
	require.NoError(t, err)
	require.Zero(t, meta.UnminedSince, "the tx the store holds is still marked")

	_ = store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*missing, txHash}, false)
	require.Positive(t, rec.fatals.Load(), "a missing tx marked not on the longest chain is still fatal")
}
