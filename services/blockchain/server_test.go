package blockchain

import (
	"context"
	"fmt"
	"testing"

	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/ubsv/ubsverrors"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/stretchr/testify/require"
)

func Test_GetBlock(t *testing.T) {
	// Create a new server instance
	logger := ulogger.New("blockchain")
	server, err := New(logger)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Start the server
	ctx := context.Background()
	if err := server.Start(ctx); err != nil {
		t.Errorf("Failed to start server: %v", err)
	}

	request := &blockchain_api.GetBlockRequest{
		Hash: []byte{1},
	}

	block, err := server.GetBlock(context.Background(), request)
	require.Empty(t, block)

	// unwrap the error
	unwrappedErr := ubsverrors.UnwrapGRPC(err)
	uErr, ok := unwrappedErr.(*ubsverrors.Error)
	if !ok {
		t.Fatalf("expected *ubsverrors.Error; got %T", unwrappedErr)
	}

	require.True(t, uErr.Is(ubsverrors.ErrBlockNotFound))
	fmt.Println(uErr)

	// Stop the server
	if err := server.Stop(ctx); err != nil {
		t.Fatalf("Failed to stop server: %v", err)
	}
}
