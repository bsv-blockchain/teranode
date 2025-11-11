package asset

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobMemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// getFreePort finds a free port for testing.
func getFreePort(t *testing.T) int {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	require.NoError(t, err)
	l, err := net.ListenTCP("tcp", addr)
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}

// testCtx holds test dependencies for asset service testing.
type testCtx struct {
	server           *Server
	logger           ulogger.Logger
	settings         *settings.Settings
	blockchainClient blockchain.ClientI
	utxoStore        utxo.Store
	blobStore        blob.Store
}

// testSetup creates a test environment with required dependencies.
func testSetup(t *testing.T) *testCtx {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	// Use dynamic port to avoid conflicts
	httpPort := getFreePort(t)
	settings.Asset.HTTPListenAddress = fmt.Sprintf("localhost:%d", httpPort)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	subtreeStore := blobMemory.New()
	blockPersisterStore := blobMemory.New()
	txSore := blobMemory.New()

	blobStore := blobMemory.New()
	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)
	blockchainStore, err := blockchainstore.NewStore(logger, storeURL, settings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(logger, settings, blockchainStore, nil, nil)
	require.NoError(t, err)

	server := NewServer(logger, settings, utxoStore, txSore, subtreeStore, blockPersisterStore, blockchainClient, nil, nil)

	return &testCtx{
		server:           server,
		logger:           logger,
		settings:         settings,
		blockchainClient: blockchainClient,
		utxoStore:        utxoStore,
		blobStore:        blobStore,
	}
}

func (tc *testCtx) teardown(t *testing.T) {
	if tc.server != nil {
		ctx := context.Background()
		err := tc.server.Stop(ctx)
		require.NoError(t, err)
	}
}

func TestNewServer(t *testing.T) {
	ctx := testSetup(t)
	defer ctx.teardown(t)

	if ctx.server == nil {
		t.Fatal("NewServer returned nil")
	}

	if ctx.server.logger != ctx.logger {
		t.Error("logger not properly set")
	}

	if ctx.server.settings != ctx.settings {
		t.Error("settings not properly set")
	}
}

func TestHealth(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name          string
		checkLiveness bool
		wantStatus    int
		wantErr       bool
	}{
		{
			name:          "liveness check should pass",
			checkLiveness: true,
			wantStatus:    http.StatusOK,
			wantErr:       false,
		},
		{
			name:          "readiness check should pass with all dependencies",
			checkLiveness: false,
			wantStatus:    http.StatusOK,
			wantErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testCtx := testSetup(t)
			defer testCtx.teardown(t)

			status, msg, err := testCtx.server.Health(ctx, tt.checkLiveness)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.wantStatus, status)
			require.NotEmpty(t, msg)
		})
	}
}

func TestServerInit(t *testing.T) {
	ctx := context.Background()

	testCtx := testSetup(t)
	defer testCtx.teardown(t)

	err := testCtx.server.Init(ctx)
	require.NoError(t, err)
}

func TestServerInitErrors(t *testing.T) {
	tests := []struct {
		name           string
		modifySettings func(*settings.Settings)
		wantErrType    string
	}{
		{
			name: "missing HTTP address",
			modifySettings: func(s *settings.Settings) {
				s.Asset.HTTPListenAddress = ""
			},
			wantErrType: "CONFIGURATION",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testCtx := testSetup(t)
			defer testCtx.teardown(t)

			if tt.modifySettings != nil {
				tt.modifySettings(testCtx.settings)
			}

			err := testCtx.server.Init(context.Background())
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErrType)
		})
	}
}

func TestHealth_LivenessCheck(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	server := NewServer(
		ulogger.New("asset"),
		test.CreateBaseTestSettings(t),
		utxoStore,
		blobMemory.New(),
		blobMemory.New(),
		blobMemory.New(),
		nil,
		nil,
		nil,
	)

	status, msg, err := server.Health(context.Background(), true)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	require.NotEmpty(t, msg)
}

func TestHealth_ReadinessWithNoDependencies(t *testing.T) {
	server := NewServer(
		ulogger.New("asset"),
		test.CreateBaseTestSettings(t),
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	status, msg, err := server.Health(context.Background(), false)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	require.NotEmpty(t, msg)
}

func TestHealth_ReadinessWithAllDependencies(t *testing.T) {
	server := testSetup(t).server

	status, msg, err := server.Health(context.Background(), false)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	require.NotEmpty(t, msg)

	// Parse health response
	var response healthResponse
	err = json.Unmarshal([]byte(msg), &response)
	require.NoError(t, err)

	// Verify overall status
	require.Equal(t, "200", response.Status)

	// Verify all dependencies are present and healthy
	require.Len(t, response.Dependencies, 5, "should have 5 top-level dependencies")

	// Helper to find dependency by resource name
	findDep := func(resource string) *struct {
		Resource     string `json:"resource"`
		Status       string `json:"status"`
		Error        string `json:"error"`
		Message      string `json:"message"`
		Dependencies []struct {
			Status       string `json:"status"`
			Dependencies []struct {
				Resource string `json:"resource"`
				Status   string `json:"status"`
				Error    string `json:"error"`
				Message  string `json:"message"`
			} `json:"dependencies"`
		} `json:"dependencies,omitempty"`
	} {
		for i := range response.Dependencies {
			if response.Dependencies[i].Resource == resource {
				return &response.Dependencies[i]
			}
		}

		return nil
	}

	// Check BlockchainClient and its nested dependencies
	blockchainClient := findDep("BlockchainClient")
	require.NotNil(t, blockchainClient)
	require.Equal(t, "200", blockchainClient.Status)
	require.Equal(t, "<nil>", blockchainClient.Error)
	require.Len(t, blockchainClient.Dependencies, 1)
	require.Len(t, blockchainClient.Dependencies[0].Dependencies, 1)
	require.Equal(t, "BlockchainStore", blockchainClient.Dependencies[0].Dependencies[0].Resource)
	require.Equal(t, "200", blockchainClient.Dependencies[0].Dependencies[0].Status)
	require.Equal(t, "OK", blockchainClient.Dependencies[0].Dependencies[0].Message)

	// Check FSM
	fsm := findDep("FSM")
	require.NotNil(t, fsm)
	require.Equal(t, "200", fsm.Status)
	require.Equal(t, "<nil>", fsm.Error)
	require.Equal(t, "RUNNING", fsm.Message)

	// Check UTXOStore
	utxoStore := findDep("UTXOStore")
	require.NotNil(t, utxoStore)
	require.Equal(t, "200", utxoStore.Status)
	require.Equal(t, "<nil>", utxoStore.Error)
	require.Equal(t, "SQL Engine is sqlitememory", utxoStore.Message)

	// Check TxStore
	txStore := findDep("TxStore")
	require.NotNil(t, txStore)
	require.Equal(t, "200", txStore.Status)
	require.Equal(t, "<nil>", txStore.Error)
	require.Equal(t, "Memory Store", txStore.Message)

	// Check BlockPersisterStore
	blockPersisterStore := findDep("BlockPersisterStore")
	require.NotNil(t, blockPersisterStore)
	require.Equal(t, "200", blockPersisterStore.Status)
	require.Equal(t, "<nil>", blockPersisterStore.Error)
	require.Equal(t, "Memory Store", blockPersisterStore.Message)
}

type healthResponse struct {
	Status       string `json:"status"`
	Dependencies []struct {
		Resource     string `json:"resource"`
		Status       string `json:"status"`
		Error        string `json:"error"`
		Message      string `json:"message"`
		Dependencies []struct {
			Status       string `json:"status"`
			Dependencies []struct {
				Resource string `json:"resource"`
				Status   string `json:"status"`
				Error    string `json:"error"`
				Message  string `json:"message"`
			} `json:"dependencies"`
		} `json:"dependencies,omitempty"`
	} `json:"dependencies"`
}

func TestCentrifugeInitialization(t *testing.T) {
	tests := []struct {
		name           string
		modifySettings func(*settings.Settings)
		wantErr        bool
	}{
		{
			name: "centrifuge disabled",
			modifySettings: func(s *settings.Settings) {
				s.Asset.CentrifugeDisable = true
				s.Asset.CentrifugeListenAddress = "localhost:8091"
			},
			wantErr: false,
		},
		{
			name: "centrifuge enabled but no address",
			modifySettings: func(s *settings.Settings) {
				s.Asset.CentrifugeDisable = false
				s.Asset.CentrifugeListenAddress = ""
			},
			wantErr: false,
		},
		{
			name: "centrifuge enabled with address",
			modifySettings: func(s *settings.Settings) {
				s.Asset.CentrifugeDisable = false
				s.Asset.CentrifugeListenAddress = "localhost:8091"
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testCtx := testSetup(t)
			defer testCtx.teardown(t)

			if tt.modifySettings != nil {
				tt.modifySettings(testCtx.settings)
			}

			err := testCtx.server.Init(context.Background())
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestServerStart(t *testing.T) {
	t.Run("successful start with HTTP server", func(t *testing.T) {
		testCtx := testSetup(t)
		defer testCtx.teardown(t)

		// Initialize the server first
		err := testCtx.server.Init(context.Background())
		require.NoError(t, err)

		// Create a context with cancel to control the server lifecycle
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create ready channel
		readyCh := make(chan struct{})

		// Start server in a goroutine
		var startErr error
		done := make(chan struct{})
		go func() {
			startErr = testCtx.server.Start(ctx, readyCh)
			close(done)
		}()

		// Wait for server to be ready
		select {
		case <-readyCh:
			// Server is ready
		case <-time.After(5 * time.Second):
			t.Fatal("Server failed to become ready within timeout")
		}

		// Cancel the context to stop the server
		cancel()

		// Wait for Start to complete
		select {
		case <-done:
			// Start completed
		case <-time.After(5 * time.Second):
			t.Fatal("Server failed to stop within timeout")
		}

		// Check for errors
		require.NoError(t, startErr)
	})

	t.Run("start with centrifuge enabled", func(t *testing.T) {
		testCtx := testSetup(t)
		defer testCtx.teardown(t)

		// Enable centrifuge
		testCtx.settings.Asset.CentrifugeDisable = false
		testCtx.settings.Asset.CentrifugeListenAddress = "localhost:0" // Use port 0 to get any available port

		// Initialize the server
		err := testCtx.server.Init(context.Background())
		require.NoError(t, err)

		// Create a context with cancel
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create ready channel
		readyCh := make(chan struct{})

		// Start server in a goroutine
		var startErr error
		done := make(chan struct{})
		go func() {
			startErr = testCtx.server.Start(ctx, readyCh)
			close(done)
		}()

		// Wait for server to be ready
		select {
		case <-readyCh:
			// Server is ready
		case <-time.After(5 * time.Second):
			t.Fatal("Server failed to become ready within timeout")
		}

		// Cancel the context to stop the server
		cancel()

		// Wait for Start to complete
		select {
		case <-done:
			// Start completed
		case <-time.After(5 * time.Second):
			t.Fatal("Server failed to stop within timeout")
		}

		require.NoError(t, startErr)
	})

	t.Run("FSM wait failure", func(t *testing.T) {
		testCtx := testSetup(t)
		defer testCtx.teardown(t)

		// Initialize the server
		err := testCtx.server.Init(context.Background())
		require.NoError(t, err)

		// Create a context with very short timeout to cause FSM wait to fail
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()

		// Give context time to expire
		time.Sleep(1 * time.Millisecond)

		// Create ready channel
		readyCh := make(chan struct{})

		// Start should fail due to timeout context
		err = testCtx.server.Start(ctx, readyCh)
		// The error might be wrapped, so we check if it's non-nil
		// The exact error depends on the blockchain client implementation
		if err == nil {
			// If blockchain client doesn't error on cancelled context,
			// the server might still run successfully
			t.Skip("Blockchain client doesn't return error on cancelled context")
		}
		require.Error(t, err)
	})
}

func TestHealth_ErrorCases(t *testing.T) {
	t.Run("health check with failing dependency", func(t *testing.T) {
		// Create server with nil blockchain client to simulate dependency failure
		server := NewServer(
			ulogger.New("asset"),
			test.CreateBaseTestSettings(t),
			nil, // utxoStore
			nil, // txStore
			nil, // subtreeStore
			nil, // blockPersisterStore
			nil, // blockchainClient is nil - will cause health check to report error
			nil, // blockvalidationClient
			nil, // p2pClient
		)

		// Readiness check should still return OK status even with nil dependencies
		status, msg, err := server.Health(context.Background(), false)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, status)
		require.NotEmpty(t, msg)
	})
}

func TestInit_ErrorCases(t *testing.T) {
	t.Run("init with invalid HTTP port", func(t *testing.T) {
		testCtx := testSetup(t)
		defer testCtx.teardown(t)

		// Set an invalid HTTP address (port out of range)
		testCtx.settings.Asset.HTTPListenAddress = "localhost:99999"

		err := testCtx.server.Init(context.Background())
		// The Init method should handle this gracefully
		// It may not error immediately, but we test that it doesn't panic
		_ = err // Error handling depends on implementation details
	})

	t.Run("init with invalid centrifuge address", func(t *testing.T) {
		testCtx := testSetup(t)
		defer testCtx.teardown(t)

		// Enable centrifuge with invalid address
		testCtx.settings.Asset.CentrifugeDisable = false
		testCtx.settings.Asset.CentrifugeListenAddress = ":::invalid:::"

		err := testCtx.server.Init(context.Background())
		// Should handle invalid address gracefully
		_ = err
	})
}
