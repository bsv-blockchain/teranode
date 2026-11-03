package aerospike_test

import (
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// fatalRecorder counts Fatalf instead of only logging it: the production
// logger's Fatalf exits the process, which the test loggers hide.
type fatalRecorder struct {
	*ulogger.ErrorTestLogger
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
	rec := &fatalRecorder{ErrorTestLogger: ulogger.NewErrorTestLogger(t)}
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, rec)
	defer deferFn()

	cleanDB(t, client)

	require.NoError(t, store.SetBlockHeight(100))

	tx, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17032dff0c2f71646c6e6b2f5e931c7f7b6199adf35e1300ffffffff01d15fa012000000001976a91417db35d440a673a218e70a5b9d07f895facf50d288ac00000000")
	require.NoError(t, err)

	_, err = store.Create(ctx, tx, 100)
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
