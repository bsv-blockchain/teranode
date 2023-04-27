//go:build manual_tests

package validator

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/TAAL-GmbH/ubsv/services/validator/validator_api"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type TestLogger struct {
	t *testing.T
}

func NewTestLogger(t *testing.T) *TestLogger {
	return &TestLogger{
		t: t,
	}
}

func (l *TestLogger) LogLevel() int {
	return 0
}

func (l *TestLogger) Debugf(format string, args ...interface{}) {
	l.t.Logf(format, args...)
}

func (l *TestLogger) Infof(format string, args ...interface{}) {
	l.t.Logf(format, args...)
}

func (l *TestLogger) Warnf(format string, args ...interface{}) {
	l.t.Logf(format, args...)
}

func (l *TestLogger) Errorf(format string, args ...interface{}) {
	l.t.Logf(format, args...)
}

func (l *TestLogger) Fatalf(format string, args ...interface{}) {
	l.t.Logf(format, args...)
}

func TestGRPCStreaming(t *testing.T) {
	tx, _ := hex.DecodeString("01000000016ec78dc364a3a911b38b32e58e5c228030f172da7d550e699939f6f3771f7f63010000006b483045022100dc6378291c4f8e06a54b0b60701cff7719c985db20537cd16d0350abe2f749660220694d67764dd8501e32190e6209a48103e3c7b202bb8c7682fc4c86e890c58409412103c3e59e22c5a32e54183a43993e8f584f709785f440fbf6f960551cb32041c5a9ffffffff03d0070000000000001976a9149e10b4a781c5be9f67367c3994fb3419aafd358e88ac2a240c00000000001976a914c52f8797b57f0b0cfc5856a5dd4a6f491a41822c88ac00000000000000000a006a075354554b2e434f00000000")

	logger := NewTestLogger(t)

	// Start the Server server
	validator := NewServer(logger)

	go func() {
		err := validator.Start()
		require.NoError(t, err)
	}()

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.Dial("localhost:8011", opts...)
	require.NoError(t, err)

	defer conn.Close()

	client := validator_api.NewValidatorAPIClient(conn)

	stream, err := client.ValidateTransactionStream(context.Background())
	require.NoError(t, err)

	err = stream.Send(&validator_api.ValidateTransactionRequest{
		TransactionData: tx[:50],
	})
	require.NoError(t, err)

	err = stream.Send(&validator_api.ValidateTransactionRequest{
		TransactionData: tx[50:],
	})
	require.NoError(t, err)

	res, err := stream.CloseAndRecv()
	require.NoError(t, err)

	t.Log(res)

	validator.Stop(context.Background())
}
