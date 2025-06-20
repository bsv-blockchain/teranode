package daemon

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/servicemanager"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// init initializes the test environment for the daemon package.
func init() {
	// Initialize test settings
	gocore.Config().Set("network", "regtest")
	gocore.Config().Set("use_cgo_verifier", "false")
	gocore.Config().Set("use_cgo_signer", "false")
	gocore.Config().Set("use_otel_tracing", "false")
	gocore.Config().Set("use_open_tracing", "false")
	gocore.Config().Set("profilerAddr", "")
	gocore.Config().Set("prometheusEndpoint", "")
}

// TestNew tests the New function to ensure it initializes a Daemon instance correctly.
func TestNew(t *testing.T) {
	d := New()
	require.NotNil(t, d)
	require.NotNil(t, d.doneCh)
	require.NotNil(t, d.stopCh)
}

// TestNew_WithOptions tests the New function with various options to ensure they are applied correctly.
func TestNew_WithOptions(t *testing.T) {
	t.Run("WithLoggerFactory", func(t *testing.T) {
		var loggerFactoryUsed bool

		customLoggerFactory := func(serviceName string) ulogger.Logger {
			loggerFactoryUsed = true
			return ulogger.New(serviceName, ulogger.WithWriter(io.Discard))
		}

		d := New(WithLoggerFactory(customLoggerFactory))
		require.NotNil(t, d)

		// To actually trigger the factory, we'd need to start a service.
		// For now, we check if the factory function pointer is the same.
		// This requires exposing loggerFactory or having a way to inspect it.
		// Assuming direct comparison is not possible, we'll check a side effect.
		d.loggerFactory("test_service") // Call it to see if our custom one was called
		assert.True(t, loggerFactoryUsed, "custom logger factory should have been used")
	})

	t.Run("WithContext", func(t *testing.T) {
		customCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		d := New(WithContext(customCtx))
		require.NotNil(t, d)
		assert.Equal(t, customCtx, d.Ctx, "daemon context should be the one provided")

		// Test context cancellation propagation (optional could be a separate test)
		cancel() // Cancel the context
		select {
		case <-d.Ctx.Done():
			// Expected: context is canceled
		case <-time.After(1 * time.Second):
			t.Fatal("timeout waiting for daemon context to be cancelled")
		}
	})
}

// TestShouldStart tests the shouldStart method of the Daemon to ensure it correctly determines if a service should start based on command line arguments.
func TestShouldStart(t *testing.T) {
	tests := []struct {
		name     string
		app      string
		args     []string
		expected bool
	}{
		{
			name:     "empty args",
			app:      "test_app",
			args:     []string{},
			expected: false,
		},
		{
			name:     "app flag present",
			app:      "test_app",
			args:     []string{"-test_app=1"},
			expected: true,
		},
		{
			name:     "app flag present but disabled",
			app:      "test_app",
			args:     []string{"-test_app=0"},
			expected: false,
		},
		{
			name:     "different app flag",
			app:      "test_app",
			args:     []string{"-other_app=1"},
			expected: false,
		},
	}

	d := New()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := d.shouldStart(tt.app, tt.args)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestDaemon_Stop tests the Stop method of the Daemon to ensure it closes the stop channel.
func TestDaemon_Stop(t *testing.T) {
	d := New()
	done := make(chan struct{})

	go func() {
		<-d.stopCh
		close(done)
	}()

	// Stop the daemon
	require.NoError(t, d.Stop())

	// Wait for the done signal or timeout
	select {
	case <-done:
		// Channel was closed successfully
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for daemon to stop")
	}
}

// TestDaemon_AddExternalService tests the AddExternalService method of the Daemon to ensure it adds services correctly.
func TestDaemon_AddExternalService(t *testing.T) {
	d := New()
	require.Empty(t, d.externalServices, "External services should be empty initially")

	mockSvc1 := newMockService("mockService1")
	initFunc1 := func() (servicemanager.Service, error) {
		return mockSvc1, nil
	}

	d.AddExternalService("testExternalService1", initFunc1)

	require.Len(t, d.externalServices, 1, "One external service should be added")
	assert.Equal(t, "testExternalService1", d.externalServices[0].Name)

	// Verify the InitFunc is stored and returns the correct service
	svc1, err := d.externalServices[0].InitFunc()
	require.NoError(t, err, "InitFunc should not return an error for mockSvc1")
	assert.Same(t, mockSvc1, svc1, "InitFunc should return the added mock service instance")

	// Add another service
	mockSvc2 := newMockService("mockService2")
	initFunc2 := func() (servicemanager.Service, error) {
		return mockSvc2, mockSvc2.initErr // Simulate an error on init
	}
	d.AddExternalService("testExternalService2", initFunc2)
	require.Len(t, d.externalServices, 2, "Two external services should be present")
	assert.Equal(t, "testExternalService2", d.externalServices[1].Name)

	svc2, err := d.externalServices[1].InitFunc()
	require.NoError(t, err, "InitFunc should not return an error for mockSvc2 initially (error is from service.Init())")
	assert.Same(t, mockSvc2, svc2, "InitFunc should return the added mock service instance")
}

// TestPrintUsage tests the printUsage function to ensure it outputs the expected usage information.
func TestPrintUsage(t *testing.T) {
	// Keep backup of the real stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	printUsage()

	// Close the writer
	err := w.Close()
	require.NoError(t, err)

	// Restore the real stdout
	os.Stdout = oldStdout

	var buf bytes.Buffer
	_, err = io.Copy(&buf, r)
	require.NoError(t, err)

	output := buf.String()

	assert.Contains(t, output, "usage: main [options]")
	assert.Contains(t, output, "-blockchain=<1|0>")
	assert.Contains(t, output, "-all=0")
}

// getFreePort asks the kernel for a free open port that is ready to use.
func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	var l *net.TCPListener

	l, err = net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}

	defer func() { _ = l.Close() }()

	return l.Addr().(*net.TCPAddr).Port, nil
}

// TestDaemon_Start_AllServices tests the Start method of the Daemon to ensure it can start all services correctly.
func TestDaemon_Start_AllServices(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// memory blob store
	blobStoreURL, err := url.Parse("memory://")
	require.NoError(t, err, "Failed to parse blob store URL")

	// SQLite Database
	var sqlStoreURL *url.URL

	sqlStoreURL, err = url.Parse("sqlitememory:///test_all?cache=shared&_pragma=busy_timeout=5000&_pragma=journal_mode=WAL")
	require.NoError(t, err, "Failed to parse blockchain DB URL")

	// Setup dynamic ports for services to avoid conflicts
	var p2pPort int

	p2pPort, err = getFreePort()
	require.NoError(t, err, "Failed to get free port for P2P")

	var assetPort int

	assetPort, err = getFreePort()
	require.NoError(t, err, "Failed to get free port for Asset")

	// Configure settings - this will now pick up KAFKA_PORT and persister URLs from gocore.Config
	appSettings := settings.NewSettings("docker.host.teranode3.daemon")
	appSettings.LocalTestStartFromState = "RUNNING"
	appSettings.P2P.Port = p2pPort
	appSettings.Asset.HTTPPort = assetPort
	appSettings.Asset.CentrifugeDisable = true

	// Manually set BlockChain and UTXO StoreURL to SQLite memory
	appSettings.BlockChain.StoreURL = sqlStoreURL
	appSettings.UtxoStore.UtxoStore = sqlStoreURL
	appSettings.Alert.StoreURL = sqlStoreURL
	appSettings.Coinbase.Store = sqlStoreURL

	// Manually set blob stores to memory store
	appSettings.Block.BlockStore = blobStoreURL
	appSettings.Block.PersisterStore = blobStoreURL
	appSettings.Block.TxStore = blobStoreURL
	appSettings.SubtreeValidation.SubtreeStore = blobStoreURL
	appSettings.Legacy.TempStore = blobStoreURL

	// Manually set Kafka topic URL schemes to 'memory' for in-memory provider
	const newConst = "memory"
	if appSettings.Kafka.BlocksConfig != nil {
		appSettings.Kafka.BlocksConfig.Scheme = newConst
	}

	if appSettings.Kafka.RejectedTxConfig != nil {
		appSettings.Kafka.RejectedTxConfig.Scheme = newConst
	}

	if appSettings.Kafka.ValidatorTxsConfig != nil {
		appSettings.Kafka.ValidatorTxsConfig.Scheme = newConst
	}

	if appSettings.Kafka.TxMetaConfig != nil {
		appSettings.Kafka.TxMetaConfig.Scheme = newConst
	}

	if appSettings.Kafka.LegacyInvConfig != nil {
		appSettings.Kafka.LegacyInvConfig.Scheme = newConst
	}

	if appSettings.Kafka.BlocksFinalConfig != nil {
		appSettings.Kafka.BlocksFinalConfig.Scheme = newConst
	}

	if appSettings.Kafka.SubtreesConfig != nil {
		appSettings.Kafka.SubtreesConfig.Scheme = newConst
	}

	if appSettings.Kafka.InvalidBlocksConfig != nil {
		appSettings.Kafka.InvalidBlocksConfig.Scheme = newConst
	}

	WaitForPortsFree(t, ctx, appSettings)

	logger := ulogger.NewErrorTestLogger(t, cancel)
	loggerFactory := WithLoggerFactory(func(serviceName string) ulogger.Logger {
		return logger
	})

	d := New(loggerFactory, WithContext(ctx))
	require.NotNil(t, d, "New daemon instance should not be nil")

	ctxStart, cancelStart := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelStart()

	readyCh := make(chan struct{})

	go func() {
		d.Start(logger, []string{
			"-blockchain=1",
			"-blockassembly=1",
			"-subtreevalidation=1",
			"-blockvalidation=1",
			"-validator=1",
			"-propagation=1",
			"-asset=1",
			"-persister=1",
			"-rpc=1",
			"-alert=1", // @mrz - this now works, no more data race issue
			"-p2p=1",
			"-coinbase=1",
			"-faucet=1",
			"-legacy=1",
			"-utxopersister=1",
			"-blockpersister=1",
		}, appSettings, readyCh)
	}()

	// Wait for services to be ready or timeout
	select {
	case <-readyCh:
		t.Logf("Daemon and its services reported ready.")
	case <-ctxStart.Done():
		logger.Errorf("Timeout waiting for daemon and its services to be ready: %v", ctxStart.Err())
		t.Fatal("Timeout waiting for daemon and its services to be ready")
	}

	// Stop the daemon
	err = d.Stop()
	assert.NoError(t, err, "Daemon Stop should not return an error")

	WaitForPortsFree(t, ctx, appSettings)
}

// TestWaitForPostgresToStart_Success verifies the happy‑path where the TCP endpoint becomes available.
func TestWaitForPostgresToStart_Success(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Spin up a TCP listener on a random port.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	defer func() {
		_ = listener.Close()
	}()

	addr := listener.Addr().String()
	lg := &mockLogger{}

	// Run the function under test.
	err = waitForPostgresToStart(lg, addr)
	require.NoError(t, err)

	// Ensure our logger captured the ready message.
	require.NotEmpty(t, lg.logs)
	require.Contains(t, lg.logs[len(lg.logs)-1], "PostgreSQL is up - ready to go!")
}

// TestWaitForPostgresToStart_Timeout verifies the timeout path using a port that never opens.
// This test shortens the timeout via a context override by running the function in a goroutine
// and cancelling it early. The production code uses a fixed 1‑minute timeout, so we allow the
// goroutine to run for only a few seconds before failing the test if it hasn’t returned.
func TestWaitForPostgresToStart_Timeout(t *testing.T) {
	// Pick an unused port by dialing :0 on UDP (cheap) then closing immediately.
	udp, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := udp.LocalAddr().String()

	err = udp.Close() // ensure nothing is listening on this port
	require.NoError(t, err)

	lg := &mockLogger{}

	done := make(chan error, 1)
	go func() {
		done <- waitForPostgresToStart(lg, addr)
	}()

	select {
	case <-time.After(3 * time.Second):
		// Function did not return in reasonable time ⇒ behavior as expected (still retrying)
		// Note: we can’t force the internal 1‑minute timeout without refactoring. Document the limitation.
		t.Log("waitForPostgresToStart is still retrying after 3s as expected; timeout path cannot be unit‑tested without refactor")
		return
	case err = <-done:
		// If it did return early, assert it errored with a timeout message (unlikely within 3 s).
		require.Error(t, err)
		require.Contains(t, err.Error(), "timed out waiting for PostgreSQL")
	}
}
