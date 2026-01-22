// Package alert implements the Bitcoin SV alert system server and related functionality.
package alert

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-alert-system/app/config"
	"github.com/bsv-blockchain/go-alert-system/app/models"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

// TestNew verifies that the New function correctly creates a Server instance
// with all provided dependencies properly initialized and assigned.
func TestNew(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	server := New(logger, tSettings, nil, nil, nil, nil, nil)

	require.NotNil(t, server)
	require.Equal(t, logger, server.logger)
	require.Equal(t, tSettings, server.settings)
	require.NotNil(t, server.stats)
}

// TestServer_Health validates the HTTP health check functionality for both
// liveness and readiness checks, ensuring proper status codes and messages are returned.
func TestServer_Health(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("liveness_check", func(t *testing.T) {
		server := New(logger, tSettings, nil, nil, nil, nil, nil)

		status, message, err := server.Health(ctx, true)

		require.NoError(t, err)
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, "OK", message)
	})

	t.Run("readiness_check_no_clients", func(t *testing.T) {
		server := New(logger, tSettings, nil, nil, nil, nil, nil)

		status, message, err := server.Health(ctx, false)

		require.NoError(t, err)
		require.Equal(t, http.StatusOK, status)
		require.Contains(t, message, "200")
	})

}

// TestServer_HealthGRPC validates the gRPC health check endpoint functionality,
// verifying that it returns the expected response structure with timestamp and status.
func TestServer_HealthGRPC(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("successful_health_check", func(t *testing.T) {
		server := New(logger, tSettings, nil, nil, nil, nil, nil)

		response, err := server.HealthGRPC(ctx, &emptypb.Empty{})

		require.NoError(t, err)
		require.NotNil(t, response)
		require.True(t, response.Ok)
		require.Contains(t, response.Details, "200")
		require.NotNil(t, response.Timestamp)
		require.True(t, time.Since(response.Timestamp.AsTime()) < time.Second)
	})
}

func TestServer_Init_basic(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Set up required alert settings
	tSettings.Alert.GenesisKeys = []string{"test_key_1", "test_key_2"}
	tSettings.Alert.P2PPort = 8080
	tSettings.Alert.ProtocolID = "/alert/test/1.0.0"
	tSettings.Alert.P2PPrivateKey = "test_private_key"

	// Create a temporary store URL
	storeURL, err := url.Parse("sqlitememory:///test_alert")
	require.NoError(t, err)
	tSettings.Alert.StoreURL = storeURL

	server := New(logger, tSettings, nil, nil, nil, nil, nil)

	err = server.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, server.appConfig)
	require.NotNil(t, server.appConfig.Services.Datastore)
}

// TestServer_requireP2P validates P2P configuration processing and default value assignment,
// ensuring proper protocol ID, topic name, and discovery interval setup.
func TestServer_requireP2P(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	server := New(logger, tSettings, nil, nil, nil, nil, nil)

	// Initialize basic config
	server.appConfig = &config.Config{
		P2P: config.P2PConfig{
			IP:                    "127.0.0.1",
			Port:                  "8080",
			PrivateKey:            "test_key",
			PeerDiscoveryInterval: 0,
		},
	}

	err := server.requireP2P()
	require.NoError(t, err)

	// Check that defaults were applied
	require.NotEmpty(t, server.appConfig.P2P.AlertSystemProtocolID)
	require.NotEmpty(t, server.appConfig.P2P.TopicName)
	require.Greater(t, server.appConfig.P2P.PeerDiscoveryInterval, time.Duration(0))
}

func TestServer_requireP2P_validation_errors(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	server := New(logger, tSettings, nil, nil, nil, nil, nil)

	t.Run("invalid_ip", func(t *testing.T) {
		server.appConfig = &config.Config{
			P2P: config.P2PConfig{
				IP:   "bad", // Too short
				Port: "8080",
			},
		}

		err := server.requireP2P()
		require.Error(t, err)
	})

	t.Run("invalid_port", func(t *testing.T) {
		server.appConfig = &config.Config{
			P2P: config.P2PConfig{
				IP:   "127.0.0.1",
				Port: "8", // Too short
			},
		}

		err := server.requireP2P()
		require.Error(t, err)
	})
}

func TestServer_createPrivateKeyDirectory(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	server := New(logger, tSettings, nil, nil, nil, nil, nil)
	server.appConfig = &config.Config{
		P2P: config.P2PConfig{},
	}

	err := server.createPrivateKeyDirectory()
	require.NoError(t, err)
	require.NotEmpty(t, server.appConfig.P2P.PrivateKeyPath)
	require.Contains(t, server.appConfig.P2P.PrivateKeyPath, "alert_system_private_key")
}

func TestServer_loadDatastore(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	server := New(logger, tSettings, nil, nil, nil, nil, nil)
	server.appConfig = &config.Config{
		Datastore: config.DatastoreConfig{
			AutoMigrate: true,
		},
	}

	t.Run("sqlite_memory", func(t *testing.T) {
		dbURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		err = server.loadDatastore(ctx, models.BaseModels, dbURL)
		require.NoError(t, err)
		require.NotNil(t, server.appConfig.Services.Datastore)
	})

	t.Run("unsupported_scheme", func(t *testing.T) {
		dbURL, err := url.Parse("unsupported:///test")
		require.NoError(t, err)

		err = server.loadDatastore(ctx, models.BaseModels, dbURL)
		require.Error(t, err)
	})
}

// TestServer_Stop is omitted as it requires a fully initialized p2pServer
// which would require extensive setup beyond the scope of unit testing

// TestServer_basic_functionality performs an integration test of the complete server workflow,
// testing initialization, configuration setup, and basic operational readiness.
func TestServer_basic_functionality(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Set up alert settings
	tSettings.Alert.GenesisKeys = []string{"test_key_1"}
	tSettings.Alert.P2PPort = 8080
	tSettings.Alert.ProtocolID = "/alert/test/1.0.0"
	tSettings.Alert.P2PPrivateKey = "test_private_key"

	storeURL, err := url.Parse("sqlitememory:///test_integration")
	require.NoError(t, err)
	tSettings.Alert.StoreURL = storeURL

	server := New(logger, tSettings, nil, nil, nil, nil, nil)

	// Test initialization
	err = server.Init(ctx)
	require.NoError(t, err)

	// Test health check
	status, message, err := server.Health(ctx, false)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	require.Contains(t, message, "200")
}
