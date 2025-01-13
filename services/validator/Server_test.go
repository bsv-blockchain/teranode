//go:build manual_tests

package validator

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/chaincfg"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/validator/validator_api"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/libsv/go-bt/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	tx, _ = hex.DecodeString("01000000016ec78dc364a3a911b38b32e58e5c228030f172da7d550e699939f6f3771f7f63010000006b483045022100dc6378291c4f8e06a54b0b60701cff7719c985db20537cd16d0350abe2f749660220694d67764dd8501e32190e6209a48103e3c7b202bb8c7682fc4c86e890c58409412103c3e59e22c5a32e54183a43993e8f584f709785f440fbf6f960551cb32041c5a9ffffffff03d0070000000000001976a9149e10b4a781c5be9f67367c3994fb3419aafd358e88ac2a240c00000000001976a914c52f8797b57f0b0cfc5856a5dd4a6f491a41822c88ac00000000000000000a006a075354554b2e434f00000000")
)

func TestGRPCStreaming(t *testing.T) {
	ctx, validator, server, _, err := setupServer(t)
	require.NoError(t, err)

	stream, err := server.ValidateTransactionStream(ctx)
	require.NoError(t, err)

	err = stream.Send(&validator_api.ValidateTransactionRequest{
		TransactionData: tx[:50],
		BlockHeight:     chaincfg.GenesisActivationHeight,
	})
	require.NoError(t, err)

	err = stream.Send(&validator_api.ValidateTransactionRequest{
		TransactionData: tx[50:],
		BlockHeight:     chaincfg.GenesisActivationHeight,
	})
	require.NoError(t, err)

	res, err := stream.CloseAndRecv()
	require.NoError(t, err)

	t.Log(res)

	if err = validator.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestTransactionBatchWithErrors(t *testing.T) {
	t.Run("directly from server", func(t *testing.T) {
		ctx, _, server, _, err := setupServer(t)
		require.NoError(t, err)

		response, err := server.ValidateTransactionBatch(ctx, &validator_api.ValidateTransactionBatchRequest{
			Transactions: []*validator_api.ValidateTransactionRequest{
				{
					TransactionData: tx,
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

		btTx, err := bt.NewTxFromBytes(tx)
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

		btTx, err := bt.NewTxFromBytes(tx)
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
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings()
	tSettings.Policy.MinMiningTxFee = 0
	tSettings.BlockAssembly.Disabled = true

	if len(sendBatchSize) > 0 {
		tSettings.Validator.SendBatchSize = sendBatchSize[0]
		tSettings.Validator.SendBatchTimeout = 1
	}

	utxoStore := &utxo.MockUtxostore{}

	blockchainClient := &blockchain.MockBlockchain{}

	// serverConn, clientConn := net.Pipe()

	// Start the Server
	server := NewServer(logger, tSettings, utxoStore, blockchainClient, nil, nil, nil)

	err := server.Init(ctx)
	require.NoError(t, err)

	go func() {
		err := server.Start(context.Background())
		require.NoError(t, err)
	}()

	time.Sleep(20 * time.Millisecond)

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	address := tSettings.Validator.GRPCAddress
	if address == "" {
		t.Fatal("[Validator] no setting GrpcAddress found")
	}

	// opts = append(opts, grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
	// 	return clientConn, nil
	// }))

	conn, err := grpc.NewClient(address, opts...)
	require.NoError(t, err)

	apiClient := validator_api.NewValidatorAPIClient(conn)

	client, err := NewClient(ctx, logger, tSettings)
	require.NoError(t, err)

	client.client = apiClient

	return ctx, server, apiClient, client, err
}
