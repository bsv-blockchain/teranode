package daemon

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
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

func TestNew(t *testing.T) {
	d := New()
	require.NotNil(t, d)
	require.NotNil(t, d.doneCh)
	require.NotNil(t, d.stopCh)
}

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

		// Test context cancellation propagation (optional, could be a separate test)
		cancel() // Cancel the context
		select {
		case <-d.Ctx.Done():
			// Expected: context is cancelled
		case <-time.After(1 * time.Second):
			t.Fatal("timeout waiting for daemon context to be cancelled")
		}
	})
}

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

func TestDaemon_Stop(t *testing.T) {
	d := New()
	done := make(chan struct{})

	go func() {
		<-d.stopCh
		close(done)
	}()

	// Stop the daemon
	require.NoError(t, d.Stop(1*time.Second))

	// Wait for the done signal or timeout
	select {
	case <-done:
		// Channel was closed successfully
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for daemon to stop")
	}
}

// mockService is a simple mock implementation of servicemanager.Service for testing.
type mockService struct {
	name        string
	initialised bool
	running     bool
	initErr     error
	startErr    error
	stopErr     error
}

func newMockService(name string) *mockService {
	return &mockService{name: name}
}

func (m *mockService) Init(ctx context.Context) error {
	if m.initErr != nil {
		return m.initErr
	}

	m.initialised = true

	return nil
}

func (m *mockService) Start(ctx context.Context, ready chan<- struct{}) error { // Corrected signature
	if m.startErr != nil {
		return m.startErr
	}

	m.running = true

	close(ready) // Signal that the service is ready

	return nil
}

func (m *mockService) Stop(ctx context.Context) error { // Added ctx context.Context
	m.running = false
	return m.stopErr
}

func (m *mockService) Name() string {
	return m.name
}

func (m *mockService) IsRunning() bool {
	return m.running
}

func (m *mockService) IsInitialised() bool {
	return m.initialised
}

func (m *mockService) IsHealthy() bool {
	return m.initialised && m.running // Simple health check
}

func (m *mockService) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if m.IsHealthy() {
		return http.StatusOK, "mock service is healthy", nil
	}

	return http.StatusServiceUnavailable, "mock service is not healthy", nil
}

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
		return mockSvc2, mockSvc2.initErr // Simulate an error on init if set
	}
	d.AddExternalService("testExternalService2", initFunc2)
	require.Len(t, d.externalServices, 2, "Two external services should be present")
	assert.Equal(t, "testExternalService2", d.externalServices[1].Name)

	svc2, err := d.externalServices[1].InitFunc()
	require.NoError(t, err, "InitFunc should not return an error for mockSvc2 initially (error is from service.Init())")
	assert.Same(t, mockSvc2, svc2, "InitFunc should return the added mock service instance")
}

func TestPrintUsage(t *testing.T) {
	// Keep backup of the real stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	printUsage()

	// Close the writer
	w.Close()
	// Restore the real stdout
	os.Stdout = oldStdout

	var buf bytes.Buffer
	_, err := io.Copy(&buf, r)
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

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}

	defer func() { _ = l.Close() }()

	return l.Addr().(*net.TCPAddr).Port, nil
}

func TestDaemon_Start_AllServices(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// memory blob store
	blobStoreURL, err := url.Parse("memory://")
	require.NoError(t, err, "Failed to parse blob store URL")

	// SQLite Database
	sqlStoreURL, err := url.Parse("sqlitememory:///test_all?cache=shared&_pragma=busy_timeout=5000&_pragma=journal_mode=WAL")
	require.NoError(t, err, "Failed to parse blockchain DB URL")

	// Setup dynamic ports for services to avoid conflicts
	p2pPort, err := getFreePort()
	require.NoError(t, err, "Failed to get free port for P2P")
	assetPort, err := getFreePort()
	require.NoError(t, err, "Failed to get free port for Asset")

	// Configure settings - this will now pick up KAFKA_PORT and persister URLs from gocore.Config
	tSettings := settings.NewSettings("docker.host.teranode3.daemon")
	tSettings.LocalTestStartFromState = "RUNNING"
	tSettings.P2P.Port = p2pPort
	tSettings.Asset.HTTPPort = assetPort
	tSettings.Asset.CentrifugeDisable = true

	// Manually set BlockChain and UTXO StoreURL to SQLite memory
	tSettings.BlockChain.StoreURL = sqlStoreURL
	tSettings.UtxoStore.UtxoStore = sqlStoreURL
	tSettings.Alert.StoreURL = sqlStoreURL
	tSettings.Coinbase.Store = sqlStoreURL

	// Manually set blob stores to memory store
	tSettings.Block.BlockStore = blobStoreURL
	tSettings.Block.PersisterStore = blobStoreURL
	tSettings.Block.TxStore = blobStoreURL
	tSettings.SubtreeValidation.SubtreeStore = blobStoreURL
	tSettings.Legacy.TempStore = blobStoreURL

	// Manually set Kafka topic URL schemes to 'memory' for in-memory provider
	const newConst = "memory"
	if tSettings.Kafka.BlocksConfig != nil {
		tSettings.Kafka.BlocksConfig.Scheme = newConst
	}

	if tSettings.Kafka.RejectedTxConfig != nil {
		tSettings.Kafka.RejectedTxConfig.Scheme = newConst
	}

	if tSettings.Kafka.ValidatorTxsConfig != nil {
		tSettings.Kafka.ValidatorTxsConfig.Scheme = newConst
	}

	if tSettings.Kafka.TxMetaConfig != nil {
		tSettings.Kafka.TxMetaConfig.Scheme = newConst
	}

	if tSettings.Kafka.LegacyInvConfig != nil {
		tSettings.Kafka.LegacyInvConfig.Scheme = newConst
	}

	if tSettings.Kafka.BlocksFinalConfig != nil {
		tSettings.Kafka.BlocksFinalConfig.Scheme = newConst
	}

	if tSettings.Kafka.SubtreesConfig != nil {
		tSettings.Kafka.SubtreesConfig.Scheme = newConst
	}

	WaitForPortsFree(t, ctx, tSettings)

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
			"-alert=0", // causes a DATA_RACE
			"-p2p=1",
			"-coinbase=1",
			"-faucet=1",
			"-legacy=1",
			"-utxopersister=1",
			"-blockpersister=1",
		}, tSettings, readyCh)
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

	WaitForPortsFree(t, ctx, tSettings)
}
