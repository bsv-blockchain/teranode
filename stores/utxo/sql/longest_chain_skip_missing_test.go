package sql

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// fatalRecordingLogger records Fatalf instead of exiting.
type fatalRecordingLogger struct {
	ulogger.TestLogger
	fatal atomic.Bool
}

func (l *fatalRecordingLogger) Fatalf(format string, args ...interface{}) {
	l.fatal.Store(true)
}

// TestMarkTransactionsOnLongestChain_SkipMissing: a not-on-longest-chain mark over records
// that may have been pruned since they were read (the overtaken-tip repair races the
// pruner) must skip the missing ones instead of treating them as corruption, and still
// stamp the rest. Without the option a missing record stays fatal.
func TestMarkTransactionsOnLongestChain_SkipMissing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	initPrometheusMetrics()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 30 * time.Second
	tSettings.BatcherDrainMode = true

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	logger := &fatalRecordingLogger{}
	store, err := New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	_, tx := setup(ctx, t)
	_, err = store.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)
	require.NoError(t, store.SetBlockHeight(200))

	missing, err := chainhash.NewHashFromStr("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	require.NoError(t, err)

	require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*tx.TxIDChainHash(), *missing}, false, utxo.WithSkipMissing()))
	require.False(t, logger.fatal.Load(), "a missing record is not fatal with WithSkipMissing")

	m, err := store.Get(ctx, tx.TxIDChainHash(), fields.UnminedSince)
	require.NoError(t, err)
	require.NotZero(t, m.UnminedSince, "the record that exists is still stamped")
}

// TestMarkTransactionsOnLongestChain_MissingStaysFatal: without the option a missing record
// on a not-on-longest-chain mark still means corruption.
func TestMarkTransactionsOnLongestChain_MissingStaysFatal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	initPrometheusMetrics()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 30 * time.Second
	tSettings.BatcherDrainMode = true

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	logger := &fatalRecordingLogger{}
	store, err := New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	missing, err := chainhash.NewHashFromStr("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	require.NoError(t, err)

	_ = store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*missing}, false)
	require.True(t, logger.fatal.Load())
}
