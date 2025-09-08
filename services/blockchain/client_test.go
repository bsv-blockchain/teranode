package blockchain

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestNewClient_blackbox(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("NewClient returns config error when address empty", func(t *testing.T) {
		tSettings.BlockChain.GRPCAddress = ""
		client, err := NewClient(ctx, logger, tSettings, "src")
		require.Error(t, err)
		require.Nil(t, client)
		assert.Contains(t, err.Error(), "no blockchain_grpcAddress setting found")
	})

	t.Run("NewClientWithAddress fails when GetGRPCClient fails", func(t *testing.T) {
		badAddr := "bad-address"
		tSettings.BlockChain.MaxRetries = 0
		client, err := NewClientWithAddress(ctx, logger, tSettings, badAddr, "src")
		require.Error(t, err)
		require.Nil(t, client)
		assert.Contains(t, err.Error(), "rpc error")
	})

	t.Run("NewClientWithAddress fails HealthGRPC immediately", func(t *testing.T) {
		tSettings.BlockChain.MaxRetries = 0
		tSettings.BlockChain.GRPCAddress = "localhost:65535" //closed port
		client, err := NewClientWithAddress(ctx, logger, tSettings, tSettings.BlockChain.GRPCAddress, "src")
		require.Error(t, err)
		require.Nil(t, client)
	})
}

func TestNewClientWithAddress(t *testing.T) {
	// start fake gRPC server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	s := grpc.NewServer()
	fs := &fakeServer{subCh: make(chan *blockchain_api.Notification, 1)}
	blockchain_api.RegisterBlockchainAPIServer(s, fs)

	go s.Serve(lis) // nolint:errcheck
	defer s.Stop()

	// prepare settings
	tSettings := settings.NewSettings()
	tSettings.BlockChain.GRPCAddress = lis.Addr().String()
	tSettings.BlockChain.MaxRetries = 1
	tSettings.BlockChain.RetrySleep = 10

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	logger := ulogger.NewErrorTestLogger(t)

	// call NewClientWithAddress → should hit the branch
	client, err := NewClientWithAddress(ctx, logger, tSettings, lis.Addr().String(), "src")
	require.NoError(t, err)
	require.NotNil(t, client)

	// Wait a moment to let goroutine process FSM notification
	time.Sleep(300 * time.Millisecond)
}

func TestNewClientWithAddressNotificationLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockChain.GRPCAddress = "localhost:50052"
	tSettings.BlockChain.MaxRetries = 0

	lis, err := net.Listen("tcp", tSettings.BlockChain.GRPCAddress)
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	fakeSrv := &fakeServer{subCh: make(chan *blockchain_api.Notification, 1)}
	blockchain_api.RegisterBlockchainAPIServer(grpcServer, fakeSrv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	clientI, err := NewClientWithAddress(ctx, logger, tSettings, tSettings.BlockChain.GRPCAddress, "src")
	require.NoError(t, err)
	c := clientI.(*Client)

	dummyHash := make([]byte, 32)

	notifFSM := &blockchain_api.Notification{
		Type: model.NotificationType_FSMState,
		Hash: dummyHash,
		Metadata: &blockchain_api.NotificationMetadata{
			Metadata: map[string]string{"destination": "RUNNING"},
		},
	}
	fakeSrv.subCh <- notifFSM

	require.Eventually(t, func() bool {
		state := c.fmsState.Load()
		return state != nil && *state == FSMStateRUNNING
	}, 2*time.Second, 100*time.Millisecond)

	ch := make(chan *blockchain_api.Notification, 1)
	c.subscribersMu.Lock()
	c.subscribers = append(c.subscribers, clientSubscriber{id: "test", ch: ch})
	c.subscribersMu.Unlock()

	notifBlock := &blockchain_api.Notification{
		Type: model.NotificationType_Block,
		Hash: dummyHash,
	}
	fakeSrv.subCh <- notifBlock

	select {
	case got := <-ch:
		require.NotNil(t, got)
		require.Equal(t, model.NotificationType_Block, got.Type)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for block notification")
	}

	c.subscribersMu.Lock()
	last := c.lastBlockNotification
	c.subscribersMu.Unlock()
	require.NotNil(t, last)
}

func TestClientHealth(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("liveness check returns OK", func(t *testing.T) {
		c := &Client{
			client:   &mockHealthClient{},
			logger:   logger,
			settings: tSettings,
		}

		code, msg, err := c.Health(context.Background(), true)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, code)
		assert.Equal(t, "OK", msg)
	})

	t.Run("readiness check fails with error", func(t *testing.T) {
		c := &Client{
			client: &mockHealthClient{
				resp: &blockchain_api.HealthResponse{Ok: false, Details: "not ready"},
				err:  errors.NewProcessingError("grpc error"),
			},
			logger:   logger,
			settings: tSettings,
		}

		code, msg, err := c.Health(context.Background(), false)
		require.Error(t, err)
		assert.Equal(t, http.StatusFailedDependency, code)
		assert.Contains(t, msg, "not ready")
	})

	t.Run("readiness check returns OK", func(t *testing.T) {
		c := &Client{
			client: &mockHealthClient{
				resp: &blockchain_api.HealthResponse{Ok: true, Details: "all good"},
				err:  nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		code, msg, err := c.Health(context.Background(), false)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, code)
		assert.Equal(t, "all good", msg)
	})
}

func TestClientAddBlock(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Minimal valid block
	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.NBit{0x1d, 0x00, 0xff, 0xff},
		Nonce:          123,
	}
	coinbase := bt.NewTx()
	_ = coinbase.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	_ = coinbase.AddP2PKHOutputFromAddress("mrs6FYWPcb441b4qfcEPyvLvzj64WHtwCU", 5000000000)
	hashSubtree := &chainhash.Hash{1, 2, 3}

	block := &model.Block{
		Header:           header,
		CoinbaseTx:       coinbase,
		TransactionCount: 1,
		SizeInBytes:      1000,
		Subtrees:         []*chainhash.Hash{hashSubtree}, // at least one subtree
	}

	t.Run("success", func(t *testing.T) {
		c := &Client{
			client:   &mockBlockClient{err: nil},
			logger:   logger,
			settings: tSettings,
		}

		err := c.AddBlock(context.Background(), block, "peer1")
		require.NoError(t, err)
	})

	t.Run("failure", func(t *testing.T) {
		c := &Client{
			client:   &mockBlockClient{err: errors.NewProcessingError("grpc failure")},
			logger:   logger,
			settings: tSettings,
		}

		err := c.AddBlock(context.Background(), block, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "grpc failure")
	})
}
