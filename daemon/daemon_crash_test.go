package daemon

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	storelogger "github.com/bsv-blockchain/teranode/stores/utxo/logger"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// warnRecorder keeps the Warnf messages so a test can check what was logged.
type warnRecorder struct {
	ulogger.TestLogger
	warnings []string
}

func (l *warnRecorder) Warnf(format string, args ...interface{}) {
	l.warnings = append(l.warnings, fmt.Sprintf(format, args...))
}

// clientCloserStore stands in for the Aerospike UTXO store, recording CloseClient.
type clientCloserStore struct {
	utxostore.Store
	closed int
}

func (s *clientCloserStore) CloseClient() { s.closed++ }

// opaqueStore is a UTXO store with no database handle the crash can close.
type opaqueStore struct{ utxostore.Store }

// TestCrashCloseUtxoStore_ClosesSQLDatabaseBehindLoggingWrapper checks that a
// UTXO store wrapped for logging=true still has its database closed by a crash.
func TestCrashCloseUtxoStore_ClosesSQLDatabaseBehindLoggingWrapper(t *testing.T) {
	ctx := context.Background()
	logger := &warnRecorder{}

	storeURL, err := url.Parse("sqlitememory:///crash_close_logging")
	require.NoError(t, err)

	raw, err := utxosql.New(ctx, ulogger.TestLogger{}, settings.NewSettings(), storeURL)
	require.NoError(t, err)
	require.NoError(t, raw.RawDB().PingContext(ctx))

	crashCloseUtxoStore(logger, storelogger.New(ctx, ulogger.TestLogger{}, raw))

	require.Error(t, raw.RawDB().PingContext(ctx), "the database must be closed")
	require.Empty(t, logger.warnings)
}

// TestCrashCloseUtxoStore_ClosesAerospikeClient checks that a crash closes the
// Aerospike store's client through CloseClient, which also evicts it from the
// per-host cache, and does so through the logging wrapper too.
func TestCrashCloseUtxoStore_ClosesAerospikeClient(t *testing.T) {
	ctx := context.Background()

	direct := &clientCloserStore{}
	crashCloseUtxoStore(&warnRecorder{}, direct)
	require.Equal(t, 1, direct.closed)

	wrapped := &clientCloserStore{}
	crashCloseUtxoStore(&warnRecorder{}, storelogger.New(ctx, ulogger.TestLogger{}, wrapped))
	require.Equal(t, 1, wrapped.closed)
}

// TestCrashCloseUtxoStore_WarnsWhenNothingToClose checks that a store the crash
// cannot close is logged rather than silently left open.
func TestCrashCloseUtxoStore_WarnsWhenNothingToClose(t *testing.T) {
	logger := &warnRecorder{}

	crashCloseUtxoStore(logger, &opaqueStore{})

	require.Len(t, logger.warnings, 1)
	require.Contains(t, logger.warnings[0], "opaqueStore")

	logger.warnings = nil
	crashCloseUtxoStore(logger, nil)
	require.Empty(t, logger.warnings)
}
