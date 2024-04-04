package blockchain

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/stretchr/testify/require"
)

func Test_GetBlock(t *testing.T) {
	// Create a new server instance
	logger := ulogger.New("blockchain")
	server, err := New(context.Background(), logger)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Start the server
	ctx := context.Background()
	go func() {
		if err := server.Start(ctx); err != nil {
			t.Errorf("Failed to start server: %v", err)
		}
	}()

	request := &blockchain_api.GetBlockRequest{
		Hash: []byte{1},
	}

	block, err := server.GetBlock(context.Background(), request)
	require.Empty(t, block)

	unwrappedErr := errors.UnwrapGRPC(err)
	require.ErrorIs(t, unwrappedErr, errors.ErrBlockNotFound)

	requestHeight := &blockchain_api.GetBlockByHeightRequest{
		Height: 1,
	}

	block, err = server.GetBlockByHeight(context.Background(), requestHeight)
	require.Empty(t, block)

	// unwrap the error
	unwrappedErr = errors.UnwrapGRPC(err)
	require.ErrorIs(t, unwrappedErr, errors.ErrBlockNotFound)
	var tErr *errors.Error
	assert.ErrorAs(t, unwrappedErr, &tErr)
	assert.Equal(t, tErr.Code, errors.ERR_BLOCK_NOT_FOUND)
	assert.Equal(t, tErr.Message, "block not found")

	// Stop the server
	if err := server.Stop(ctx); err != nil {
		t.Fatalf("Failed to stop server: %v", err)
	}
}
