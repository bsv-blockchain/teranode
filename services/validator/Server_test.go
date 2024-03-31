//go:build manual_tests

package validator

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/bitcoin-sv/ubsv/services/validator/validator_api"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGRPCStreaming(t *testing.T) {
	tx, _ := hex.DecodeString("01000000016ec78dc364a3a911b38b32e58e5c228030f172da7d550e699939f6f3771f7f63010000006b483045022100dc6378291c4f8e06a54b0b60701cff7719c985db20537cd16d0350abe2f749660220694d67764dd8501e32190e6209a48103e3c7b202bb8c7682fc4c86e890c58409412103c3e59e22c5a32e54183a43993e8f584f709785f440fbf6f960551cb32041c5a9ffffffff03d0070000000000001976a9149e10b4a781c5be9f67367c3994fb3419aafd358e88ac2a240c00000000001976a914c52f8797b57f0b0cfc5856a5dd4a6f491a41822c88ac00000000000000000a006a075354554b2e434f00000000")

	logger := ulogger.TestLogger{}

	// Start the Server
	validator := NewServer(logger, nil, nil)

	go func() {
		err := validator.Start(context.Background())
		require.NoError(t, err)
	}()

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	serviceName := "validator"
	grpcAddress := fmt.Sprintf("%s_grpcAddress", serviceName)
	address, ok := gocore.Config().Get(grpcAddress)
	if !ok {
		t.Fatalf("[%s] no setting %s found", serviceName, grpcAddress)
	}

	conn, err := grpc.Dial(address, opts...)
	require.NoError(t, err)

	defer conn.Close()

	client := validator_api.NewValidatorAPIClient(conn)

	stream, err := client.ValidateTransactionStream(context.Background())
	require.NoError(t, err)

	err = stream.Send(&validator_api.ValidateTransactionRequest{
		TransactionData: tx[:50],
		BlockHeight:     GenesisActivationHeight,
	})
	require.NoError(t, err)

	err = stream.Send(&validator_api.ValidateTransactionRequest{
		TransactionData: tx[50:],
		BlockHeight:     GenesisActivationHeight,
	})
	require.NoError(t, err)

	res, err := stream.CloseAndRecv()
	require.NoError(t, err)

	t.Log(res)

	if err = validator.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}
