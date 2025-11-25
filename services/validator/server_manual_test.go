//go:build manual_tests

package validator

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/validator/validator_api"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	sampleTx, _ = hex.DecodeString("01000000016ec78dc364a3a911b38b32e58e5c228030f172da7d550e699939f6f3771f7f63010000006b483045022100dc6378291c4f8e06a54b0b60701cff7719c985db20537cd16d0350abe2f749660220694d67764dd8501e32190e6209a48103e3c7b202bb8c7682fc4c86e890c58409412103c3e59e22c5a32e54183a43993e8f584f709785f440fbf6f960551cb32041c5a9ffffffff03d0070000000000001976a9149e10b4a781c5be9f67367c3994fb3419aafd358e88ac2a240c00000000001976a914c52f8797b57f0b0cfc5856a5dd4a6f491a41822c88ac00000000000000000a006a075354554b2e434f00000000")
)

func TestTransactionBatchWithErrors(t *testing.T) {
	t.Run("directly from server", func(t *testing.T) {
		ctx, _, server, _, err := setupServer(t)
		require.NoError(t, err)

		response, err := server.ValidateTransactionBatch(ctx, &validator_api.ValidateTransactionBatchRequest{
			Transactions: []*validator_api.ValidateTransactionRequest{
				{
					TransactionData: sampleTx,
				},
			},
		})
		require.NoError(t, err)

		assert.Len(t, response.Errors, 1)
		assert.Equal(t, response.Errors[0], "")

		require.Len(t, response.Metadata, 1)

		txMetaData := &meta.Data{}
		meta.NewMetaDataFromBytes(&response.Metadata[0], txMetaData)
		require.NotNil(t, txMetaData)

		assert.Equal(t, uint64(32279815860), txMetaData.Fee)
		assert.Equal(t, uint64(245), txMetaData.SizeInBytes)
		assert.Len(t, txMetaData.ParentTxHashes, 1)
	})

	t.Run("using client", func(t *testing.T) {
		ctx, _, _, client, err := setupServer(t)
		require.NoError(t, err)

		btTx, err := bt.NewTxFromBytes(sampleTx)
		require.NoError(t, err)

		txMetaData, err := client.Validate(ctx, btTx, 101)
		require.NoError(t, err)

		assert.Equal(t, uint64(32279815860), txMetaData.Fee)
		assert.Equal(t, uint64(245), txMetaData.SizeInBytes)
		assert.Len(t, txMetaData.ParentTxHashes, 1)
	})

	t.Run("using client with batching", func(t *testing.T) {
		ctx, _, _, client, err := setupServer(t, 1)
		require.NoError(t, err)

		btTx, err := bt.NewTxFromBytes(sampleTx)
		require.NoError(t, err)

		txMetaData, err := client.Validate(ctx, btTx, 101)
		require.NoError(t, err)

		client.TriggerBatcher()

		assert.Equal(t, uint64(32279815860), txMetaData.Fee)
		assert.Equal(t, uint64(245), txMetaData.SizeInBytes)
		assert.Len(t, txMetaData.ParentTxHashes, 1)
	})
}

func setupServer(t *testing.T, sendBatchSize ...int) (context.Context, *Server, validator_api.ValidatorAPIClient, *Client, error) {
	ctx, cancel := context.WithCancel(context.Background())

	t.Cleanup(func() {
		cancel() // Make sure context is canceled when test completes
	})

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Disable fee checking for tests
	tSettings.Policy.MinMiningTxFee = 0
	tSettings.BlockAssembly.Disabled = true

	// Set specific port ranges that are unlikely to be in use
	portBase := 50000 + (time.Now().Nanosecond() % 10000)
	tSettings.Validator.GRPCAddress = fmt.Sprintf("localhost:%d", portBase)
	tSettings.Validator.HTTPListenAddress = fmt.Sprintf("localhost:%d", portBase+1)
	// Make sure other ports are unique as well

	if len(sendBatchSize) > 0 {
		tSettings.Validator.SendBatchSize = sendBatchSize[0]
		tSettings.Validator.SendBatchTimeout = 1
	}

	utxoStore := &utxo.MockUtxostore{}
	utxoStore.On("GetBlockState").Return(utxo.BlockState{Height: 1000, MedianTime: uint32(time.Now().Unix())}) // nolint:gosec
	utxoStore.On("PreviousOutputsDecorate", mock.Anything, mock.Anything).Return(nil)

	blockchainClient := &blockchain.MockBlockchain{}

	// Create server instance with the mock validator
	server := NewServer(logger, tSettings, utxoStore, blockchainClient, nil, nil, nil)

	// Create a custom validator that always succeeds
	txid, _ := chainhash.NewHashFromStr("63f7f771376f9f9369e650d7a72d1f0328c2e5582eb3381b913a4a36dc78ec6e")
	mockValidator := &TestMockValidator{
		validateTxFunc: func(ctx context.Context, tx *bt.Tx) (*meta.Data, error) {
			return &meta.Data{
				Fee:            32279815860,
				SizeInBytes:    245,
				ParentTxHashes: []chainhash.Hash{*txid},
			}, nil
		},
	}

	// Replace the real validator with our mock
	server.validator = mockValidator

	err := server.Init(ctx)
	require.NoError(t, err)

	// Channel to signal that the server is ready
	readyCh := make(chan struct{})

	// Start the server in a goroutine
	go func() {
		err := server.Start(ctx, readyCh)
		// Only log errors that aren't due to context cancellation
		if err != nil && ctx.Err() == nil {
			t.Logf("Server error: %v", err)
		}
	}()

	// Wait for the ready signal or timeout
	select {
	case <-readyCh:
		// Server started successfully
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for server to start")
	}

	// Wait a bit longer to ensure the server is fully ready
	time.Sleep(100 * time.Millisecond)

	// Setup gRPC connection
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	address := tSettings.Validator.GRPCAddress
	require.NotEmpty(t, address, "[Validator] no setting GrpcAddress found")

	// opts = append(opts, grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
	// 	return clientConn, nil
	// }))

	conn, err := grpc.NewClient(address, opts...)
	require.NoError(t, err)

	apiClient := validator_api.NewValidatorAPIClient(conn)

	// Setup the client
	client, err := NewClient(ctx, logger, tSettings)
	require.NoError(t, err)

	client.client = apiClient

	return ctx, server, apiClient, client, err
}
