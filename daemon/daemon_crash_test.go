package daemon

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/settings"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	storelogger "github.com/bsv-blockchain/teranode/stores/utxo/logger"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test"
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

// TestCrash_ClosesBlockchainDatabaseAndP2PHost crashes a running daemon and
// checks the parts of a crash the UTXO-only e2e test does not: the blockchain
// database is closed, the libp2p client is closed, and doneCh is closed.
func TestCrash_ClosesBlockchainDatabaseAndP2PHost(t *testing.T) {
	td := NewTestDaemon(t, TestOptions{
		EnableP2P:            true,
		SettingsOverrideFunc: test.ComposeSettings(test.SystemTestSettings()),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Crash's cleanup resets the store fields, so take the handles first.
	rawBlockchain, ok := td.d.daemonStores.rawBlockchainStore.(blockchainRawDB)
	require.True(t, ok, "the test daemon's blockchain store must be a SQL store")

	blockchainDB := rawBlockchain.GetDB()
	require.NoError(t, blockchainDB.PingContext(ctx))

	service, err := td.d.ServiceManager.GetService(serviceNameP2PFormal)
	require.NoError(t, err)

	p2pServer, ok := service.(*p2p.Server)
	require.True(t, ok)
	require.NotNil(t, p2pServer.P2PClient)

	p2pClient := p2pServer.P2PClient

	td.Crash(t)

	require.True(t, td.d.crashed.Load())
	require.Error(t, blockchainDB.PingContext(ctx), "the blockchain database must be closed")

	// A closed client hands out an already-closed channel.
	select {
	case _, open := <-p2pClient.Subscribe("crash-test"):
		require.False(t, open, "the p2p client must be closed")
	case <-ctx.Done():
		t.Fatal("subscribing on the crashed p2p client blocked")
	}

	select {
	case <-td.d.doneCh:
	default:
		t.Fatal("crash must close doneCh")
	}
}

// recordingClient is a daemon-constructed client that counts its closes.
type recordingClient struct{ closes int }

func (c *recordingClient) Close() error {
	c.closes++
	return nil
}

// recordingCloseStore is a UTXO store that counts its closes.
type recordingCloseStore struct {
	utxostore.Store
	closes int
}

func (s *recordingCloseStore) Close(context.Context) error {
	s.closes++
	return nil
}

// TestCrash_SkipsGracefulClientAndStoreClose checks that after a crash the
// daemon's own shutdown closes no client and no store, as a killed process
// would not, and that the crashed flag is what skips them.
func TestCrash_SkipsGracefulClientAndStoreClose(t *testing.T) {
	logger := ulogger.TestLogger{}

	client := &recordingClient{}
	store := &recordingCloseStore{}

	d := New()
	d.daemonStores.constructedClients = []clientCloser{client}
	d.daemonStores.mainUtxoStore = store

	d.crashed.Store(true)
	d.closeClients(logger)
	d.closeStores(logger)

	require.Zero(t, client.closes, "closeClients must skip a crashed daemon")
	require.Zero(t, store.closes, "closeStores must skip a crashed daemon")

	d.crashed.Store(false)
	d.closeClients(logger)
	d.closeStores(logger)

	require.Equal(t, 1, client.closes)
	require.Equal(t, 1, store.closes)
}
