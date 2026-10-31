// Package alert implements the BSV Blockchain alert system server and related functionality.
package alert

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-alert-system/app/config"
	"github.com/bsv-blockchain/go-alert-system/app/models"
	"github.com/bsv-blockchain/go-alert-system/app/models/model"
	"github.com/bsv-blockchain/go-alert-system/app/p2p"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-sdk/util"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"
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
	tSettings.Alert.GenesisKeys = upstreamNetwork(t, "mainnet").GenesisKeys[:3]
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

// TestServer_loadConfig_networkDefaults verifies that the alert network identity
// (genesis keys, protocol ID, topic) follows the configured chain when the
// settings are left empty, and that explicit settings still win.
func TestServer_loadConfig_networkDefaults(t *testing.T) {
	newServer := func(t *testing.T, chainParams *chaincfg.Params, alert settings.AlertSettings) *Server {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.ChainCfgParams = chainParams

		alert.P2PPort = 8080
		alert.P2PPrivateKey = "test_private_key"
		alert.P2PDHTMode = tSettings.Alert.P2PDHTMode

		storeURL, err := url.Parse("sqlitememory:///test_alert_" + chainParams.Name)
		require.NoError(t, err)

		alert.StoreURL = storeURL
		tSettings.Alert = alert

		return New(ulogger.TestLogger{}, tSettings, nil, nil, nil, nil, nil)
	}

	t.Run("testnet uses testnet defaults", func(t *testing.T) {
		server := newServer(t, &chaincfg.TestNetParams, settings.AlertSettings{})

		require.NoError(t, server.loadConfig(context.Background(), nil, true))

		testnet := upstreamNetwork(t, "testnet")
		require.Equal(t, testnet.GenesisKeys, server.appConfig.GenesisKeys)
		require.Equal(t, "/bitcoin-testnet/alert-system/0.0.1", server.appConfig.P2P.AlertSystemProtocolID)
		require.Equal(t, "bitcoin_alert_system_testnet", server.appConfig.P2P.TopicName)
	})

	t.Run("mainnet uses mainnet defaults", func(t *testing.T) {
		server := newServer(t, &chaincfg.MainNetParams, settings.AlertSettings{})

		require.NoError(t, server.loadConfig(context.Background(), nil, true))

		mainnet := upstreamNetwork(t, "mainnet")
		require.Equal(t, mainnet.GenesisKeys, server.appConfig.GenesisKeys)
		require.Equal(t, "/bitcoin/alert-system/1.0.0", server.appConfig.P2P.AlertSystemProtocolID)
		require.Equal(t, "bitcoin_alert_system", server.appConfig.P2P.TopicName)
	})

	t.Run("regtest falls back to mainnet keys and protocol", func(t *testing.T) {
		server := newServer(t, &chaincfg.RegressionNetParams, settings.AlertSettings{})

		require.NoError(t, server.loadConfig(context.Background(), nil, true))

		mainnet := upstreamNetwork(t, "mainnet")
		require.Equal(t, mainnet.GenesisKeys, server.appConfig.GenesisKeys)
		require.Equal(t, mainnet.ProtocolID, server.appConfig.P2P.AlertSystemProtocolID)
		require.Equal(t, "bitcoin_alert_system_regtest", server.appConfig.P2P.TopicName)
	})

	t.Run("explicit settings override testnet defaults", func(t *testing.T) {
		customKeys := upstreamNetwork(t, "mainnet").GenesisKeys[:3]
		server := newServer(t, &chaincfg.TestNetParams, settings.AlertSettings{
			GenesisKeys: append(append([]string{}, customKeys...), ""), // trailing separator
			ProtocolID:  "/custom/alert-system/1.0.0",
			TopicName:   "custom_topic",
		})

		require.NoError(t, server.loadConfig(context.Background(), nil, true))

		require.Equal(t, customKeys, server.appConfig.GenesisKeys)
		require.Equal(t, "/custom/alert-system/1.0.0", server.appConfig.P2P.AlertSystemProtocolID)
		require.Equal(t, "custom_topic", server.appConfig.P2P.TopicName)
	})

	t.Run("an invalid genesis key is a configuration error", func(t *testing.T) {
		mainnetKeys := upstreamNetwork(t, "mainnet").GenesisKeys
		server := newServer(t, &chaincfg.TestNetParams, settings.AlertSettings{
			GenesisKeys: []string{"custom_key", mainnetKeys[0], mainnetKeys[1]},
		})

		err := server.loadConfig(context.Background(), nil, true)
		require.True(t, errors.Is(err, errors.ErrConfiguration), "got %v", err)
		require.ErrorContains(t, err, "alert_genesis_keys[0]")
	})
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
	tSettings.Alert.GenesisKeys = upstreamNetwork(t, "mainnet").GenesisKeys[:3]
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

// TestServer_syncGenesisKeys verifies that a store which has accepted no alert past
// the genesis alert has its active keys reset to the configured genesis keys, so a
// node whose store was seeded with another network's keys can verify alerts again.
func TestServer_syncGenesisKeys(t *testing.T) {
	mainnet := upstreamNetwork(t, "mainnet")
	testnet := upstreamNetwork(t, "testnet")

	// loadServer loads the config for chainParams against the sqlite store in dataFolder
	loadServer := func(t *testing.T, dataFolder string, chainParams *chaincfg.Params) *Server {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.ChainCfgParams = chainParams
		tSettings.DataFolder = dataFolder
		tSettings.Alert.GenesisKeys = nil
		tSettings.Alert.ProtocolID = ""
		tSettings.Alert.TopicName = ""
		tSettings.Alert.P2PPort = 8080
		tSettings.Alert.P2PPrivateKey = "test_private_key"

		storeURL, err := url.Parse("sqlite:///alert")
		require.NoError(t, err)

		tSettings.Alert.StoreURL = storeURL

		server := New(ulogger.TestLogger{}, tSettings, nil, nil, nil, nil, nil)
		require.NoError(t, server.loadConfig(context.Background(), models.BaseModels, true))
		t.Cleanup(func() { server.appConfig.CloseAll(context.Background()) })

		return server
	}

	// startServer loads the server and ensures the genesis alert exists, without syncing its keys
	startServer := func(t *testing.T, dataFolder string, chainParams *chaincfg.Params) *Server {
		server := loadServer(t, dataFolder, chainParams)
		require.NoError(t, models.CreateGenesisAlert(context.Background(), model.WithAllDependencies(server.appConfig)))

		return server
	}

	// mainnetStore returns a data folder holding a store seeded with the mainnet genesis keys
	mainnetStore := func(t *testing.T) string {
		dataFolder := t.TempDir()
		startServer(t, dataFolder, &chaincfg.MainNetParams).appConfig.CloseAll(context.Background())

		return dataFolder
	}

	execSQL := func(t *testing.T, server *Server, query string) {
		require.NoError(t, server.appConfig.Services.Datastore.Execute(query).Error)
	}

	activeKeys := func(t *testing.T, server *Server) []string {
		keys, err := allActivePublicKeys(context.Background(), model.WithAllDependencies(server.appConfig))
		require.NoError(t, err)

		active := make([]string, 0, len(keys))
		for _, k := range keys {
			active = append(active, k.Key)
		}

		return active
	}

	t.Run("store seeded with other genesis keys is reset", func(t *testing.T) {
		server := startServer(t, mainnetStore(t), &chaincfg.TestNetParams)
		require.ElementsMatch(t, mainnet.GenesisKeys, activeKeys(t, server), "precondition: genesis alert keeps the first keys")

		active, err := server.syncGenesisKeys(context.Background())
		require.NoError(t, err)
		require.Equal(t, len(testnet.GenesisKeys), active)
		require.ElementsMatch(t, testnet.GenesisKeys, activeKeys(t, server))

		// a second start is a no-op
		active, err = server.syncGenesisKeys(context.Background())
		require.NoError(t, err)
		require.Equal(t, len(testnet.GenesisKeys), active)
		require.ElementsMatch(t, testnet.GenesisKeys, activeKeys(t, server))
	})

	t.Run("store with more active keys than a page is fully reset", func(t *testing.T) {
		server := startServer(t, mainnetStore(t), &chaincfg.TestNetParams)

		opts := model.WithAllDependencies(server.appConfig)
		for i := 0; i < 8; i++ {
			k := models.NewPublicKey(opts, model.New())
			k.Key = fmt.Sprintf("extra_key_%d", i)
			k.Active = true
			require.NoError(t, k.Save(context.Background()))
		}

		require.Len(t, activeKeys(t, server), len(mainnet.GenesisKeys)+8, "precondition: more than 10 active keys")

		active, err := server.syncGenesisKeys(context.Background())
		require.NoError(t, err)
		require.Equal(t, len(testnet.GenesisKeys), active)
		require.ElementsMatch(t, testnet.GenesisKeys, activeKeys(t, server))
	})

	t.Run("keys that were deactivated are reactivated", func(t *testing.T) {
		dataFolder := mainnetStore(t)

		testnetServer := startServer(t, dataFolder, &chaincfg.TestNetParams)
		_, err := testnetServer.syncGenesisKeys(context.Background())
		require.NoError(t, err)
		testnetServer.appConfig.CloseAll(context.Background())

		// the mainnet key rows exist but are inactive, so they are found and reactivated
		server := startServer(t, dataFolder, &chaincfg.MainNetParams)

		active, err := server.syncGenesisKeys(context.Background())
		require.NoError(t, err)
		require.Equal(t, len(mainnet.GenesisKeys), active)
		require.ElementsMatch(t, mainnet.GenesisKeys, activeKeys(t, server))
	})

	t.Run("matching store is left alone", func(t *testing.T) {
		server := startServer(t, t.TempDir(), &chaincfg.TestNetParams)

		active, err := server.syncGenesisKeys(context.Background())
		require.NoError(t, err)
		require.Equal(t, len(testnet.GenesisKeys), active)
		require.ElementsMatch(t, testnet.GenesisKeys, activeKeys(t, server))
	})

	t.Run("store past the genesis alert is left alone", func(t *testing.T) {
		dataFolder := t.TempDir()

		first := startServer(t, dataFolder, &chaincfg.MainNetParams)

		// a later alert means the active keys may have been rotated by SetKeys
		alert := models.NewAlertMessage(model.WithAllDependencies(first.appConfig), model.New())
		alert.SequenceNumber = 1
		alert.Hash = "0000000000000000000000000000000000000000000000000000000000000001"
		alert.Processed = true
		require.NoError(t, alert.Save(context.Background()))
		first.appConfig.CloseAll(context.Background())

		server := startServer(t, dataFolder, &chaincfg.TestNetParams)

		active, err := server.syncGenesisKeys(context.Background())
		require.NoError(t, err)
		require.Equal(t, len(mainnet.GenesisKeys), active)
		require.ElementsMatch(t, mainnet.GenesisKeys, activeKeys(t, server))
	})

	t.Run("initGenesisKeys creates the genesis alert and resets the keys", func(t *testing.T) {
		fresh := loadServer(t, t.TempDir(), &chaincfg.TestNetParams)
		require.NoError(t, fresh.initGenesisKeys(context.Background()))
		require.ElementsMatch(t, testnet.GenesisKeys, activeKeys(t, fresh))

		server := loadServer(t, mainnetStore(t), &chaincfg.TestNetParams)
		require.NoError(t, server.initGenesisKeys(context.Background()))
		require.ElementsMatch(t, testnet.GenesisKeys, activeKeys(t, server))
	})

	t.Run("initGenesisKeys fails when the genesis alert cannot be created", func(t *testing.T) {
		server := loadServer(t, t.TempDir(), &chaincfg.TestNetParams)
		execSQL(t, server, "DROP TABLE alert_alert_messages")

		require.ErrorContains(t, server.initGenesisKeys(context.Background()), "error creating genesis alert")
	})

	t.Run("initGenesisKeys fails when the keys cannot be synced", func(t *testing.T) {
		server := startServer(t, t.TempDir(), &chaincfg.TestNetParams)
		execSQL(t, server, "DROP TABLE alert_public_keys")

		require.ErrorContains(t, server.initGenesisKeys(context.Background()), "error syncing alert genesis keys")
	})

	t.Run("store without a genesis alert fails", func(t *testing.T) {
		server := loadServer(t, t.TempDir(), &chaincfg.TestNetParams)

		_, err := server.syncGenesisKeys(context.Background())
		require.ErrorContains(t, err, "error getting the latest alert")
	})

	t.Run("deactivating a stored key fails", func(t *testing.T) {
		server := startServer(t, mainnetStore(t), &chaincfg.TestNetParams)
		execSQL(t, server, "CREATE TRIGGER no_update BEFORE UPDATE ON alert_public_keys BEGIN SELECT RAISE(ABORT, 'read only'); END")

		_, err := server.syncGenesisKeys(context.Background())
		require.ErrorContains(t, err, "error deactivating public key")
	})

	t.Run("activating a genesis key fails", func(t *testing.T) {
		server := startServer(t, mainnetStore(t), &chaincfg.TestNetParams)
		execSQL(t, server, "CREATE TRIGGER no_insert BEFORE INSERT ON alert_public_keys BEGIN SELECT RAISE(ABORT, 'read only'); END")

		_, err := server.syncGenesisKeys(context.Background())
		require.ErrorContains(t, err, "error activating public key")
	})

}

// TestServer_Start_survivesOversizedSyncFrame sends the alert sync stream handler that
// Start registers length prefixes it must reject before allocating: a uint64-max VarInt,
// which panics make() in the stream's reader goroutine and kills the process on
// go-alert-system before v0.3.0, and one byte over the 2 MiB default frame cap. The
// server must close each stream and still answer a well-formed sync request afterwards.
func TestServer_Start_survivesOversizedSyncFrame(t *testing.T) {
	// initDHT dials the public IPFS bootstrap peers on top of the configured one; empty them
	// so the server bootstraps off the local attacker host only
	defaultBootstrapPeers := dht.DefaultBootstrapPeers
	dht.DefaultBootstrapPeers = nil
	t.Cleanup(func() { dht.DefaultBootstrapPeers = defaultBootstrapPeers })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	attacker, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = attacker.Close() })

	serverKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	serverKeyBytes, err := serverKey.Raw()
	require.NoError(t, err)
	serverID, err := peer.IDFromPrivateKey(serverKey)
	require.NoError(t, err)

	port := freeTCPPort(t)
	protocolID := protocol.ID("/alert/test/1.0.0")

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Alert.GenesisKeys = upstreamNetwork(t, "mainnet").GenesisKeys[:3]
	tSettings.Alert.ProtocolID = string(protocolID)
	tSettings.Alert.P2PPort = port
	tSettings.Alert.P2PPrivateKey = hex.EncodeToString(serverKeyBytes)
	tSettings.Alert.P2PBootstrapPeer = fmt.Sprintf("%s/p2p/%s", attacker.Addrs()[0], attacker.ID())
	tSettings.Alert.P2PAllowPrivateIPs = true

	storeURL, err := url.Parse("sqlitememory:///test_oversized_sync_frame")
	require.NoError(t, err)
	tSettings.Alert.StoreURL = storeURL

	// TestLogger, not ErrorTestLogger: the library logs each rejected frame at error level
	server := New(ulogger.TestLogger{}, tSettings, nil, nil, nil, nil, nil)
	require.NoError(t, server.loadConfig(ctx, models.BaseModels, true))

	startErr := make(chan error, 1)
	go func() { startErr <- server.Start(ctx, make(chan struct{})) }()
	t.Cleanup(func() {
		cancel()

		// the library's DHT bootstrap loop ignores ctx, so a server that never reached the
		// attacker host would block here forever
		select {
		case err := <-startErr:
			require.NoError(t, err)
		case <-time.After(30 * time.Second):
			t.Error("alert server did not stop")
		}

		require.NoError(t, server.Stop(context.Background()))
	})

	attacker.Peerstore().AddAddr(serverID, ma.StringCast(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port)), peerstore.PermanentAddrTTL)

	// the handler is only registered once the server's DHT has bootstrapped, so retry until
	// protocol negotiation succeeds
	openStream := func(t *testing.T) network.Stream {
		var stream network.Stream

		require.Eventually(t, func() bool {
			var streamErr error
			stream, streamErr = attacker.NewStream(ctx, serverID, protocolID)
			return streamErr == nil
		}, 30*time.Second, 100*time.Millisecond)

		t.Cleanup(func() { _ = stream.Reset() })

		return stream
	}

	oversizedFrames := map[string][]byte{
		// 0xff marker followed by uint64 max
		"uint64 max": {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		// 0xfe marker followed by 2 MiB + 1 as a little-endian uint32
		"one byte over the 2 MiB default": {0xfe, 0x01, 0x00, 0x20, 0x00},
	}

	for name, frame := range oversizedFrames {
		t.Run(name, func(t *testing.T) {
			stream := openStream(t)

			_, err := stream.Write(frame)
			require.NoError(t, err)

			// the server must close the stream without sending anything back, not leave it open
			// until the sync timeout: a clean EOF reads as a nil error, a hang as a yamux timeout
			require.NoError(t, stream.SetReadDeadline(time.Now().Add(10*time.Second)))
			reply, err := io.ReadAll(stream)
			require.NoError(t, err, "server did not close the stream")
			require.Empty(t, reply)
		})
	}

	t.Run("a well-formed sync request is still answered", func(t *testing.T) {
		stream := openStream(t)

		request := p2p.SyncMessage{Type: p2p.IWantLatest}
		writer := util.NewWriter()
		writer.WriteIntBytes(request.Serialize())
		_, err := stream.Write(writer.Buf)
		require.NoError(t, err)

		require.NoError(t, stream.SetReadDeadline(time.Now().Add(10*time.Second)))

		var length util.VarInt
		_, err = length.ReadFrom(stream)
		require.NoError(t, err)

		body := make([]byte, length)
		_, err = io.ReadFull(stream, body)
		require.NoError(t, err)

		reply, err := p2p.NewSyncMessageFromBytes(body)
		require.NoError(t, err)
		require.Equal(t, byte(p2p.IGotLatest), reply.Type)
	})
}

// freeTCPPort returns a TCP port that was free on localhost when it was checked.
func freeTCPPort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())

	return port
}
