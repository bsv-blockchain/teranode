package asset

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/settings"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	blobMemory "github.com/bitcoin-sv/ubsv/stores/blob/memory"
	blockchainstore "github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo/memory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/test"
	"github.com/stretchr/testify/require"
)

type testCtx struct {
	server           *Server
	logger           ulogger.Logger
	settings         *settings.Settings
	blockchainClient blockchain.ClientI
	utxoStore        utxo.Store
	blobStore        blob.Store
}

func testSetup(t *testing.T) *testCtx {
	logger := ulogger.New("asset")
	tSettings := test.CreateBaseTestSettings()
	tSettings.Asset.CentrifugeDisable = true
	utxoStore := utxostore.New(ulogger.TestLogger{})
	subtreeStore := blobMemory.New()
	blockPersisterStore := blobMemory.New()
	txSore := blobMemory.New()

	blobStore := blobMemory.New()
	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)
	blockchainStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockchainStore, nil, nil)
	require.NoError(t, err)

	server := NewServer(logger, tSettings, utxoStore, txSore, subtreeStore, blockPersisterStore, blockchainClient)

	return &testCtx{
		server:           server,
		logger:           logger,
		settings:         tSettings,
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
	server := NewServer(
		ulogger.New("asset"),
		test.CreateBaseTestSettings(),
		utxostore.New(ulogger.TestLogger{}),
		blobMemory.New(),
		blobMemory.New(),
		blobMemory.New(),
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
		test.CreateBaseTestSettings(),
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
	require.Equal(t, "Memory Store available", utxoStore.Message)

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
