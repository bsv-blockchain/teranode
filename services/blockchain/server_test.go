package blockchain

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/emptypb"

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

func Test_GetFSMCurrentState(t *testing.T) {
	// Create a new server instance
	logger := ulogger.New("blockchain")
	server, err := New(context.Background(), logger)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	require.NoError(t, server.Init(context.Background()))

	// Assuming that the setup or initialization of the server sets the FSM state
	// Start the server and the FSM in an initial state you control (not shown here)
	ctx := context.Background()
	go func() {
		if err := server.Start(ctx); err != nil {
			t.Errorf("Failed to start server: %v", err)
		}
	}()

	// Test the GetFSMCurrentState function
	response, err := server.GetFSMCurrentState(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.NotNil(t, response)
	// Check the actual response.State against an expected state.
	// This is just an example, you need to replace FSMEventType_RUN with the actual expected enum based on your server's FSM setup.
	assert.Equal(t, blockchain_api.FSMEventType_RUN, response.State, "Expected FSM state did not match")

	// Optionally, you can simulate an invalid state as well by directly setting an invalid state in your server's FSM,
	// then calling GetFSMCurrentState again to see if it handles the error as expected.
	// This part is commented out as it depends on the ability to set the FSM state, which might not be possible without a mock or additional method.

	// Stop the server
	if err := server.Stop(ctx); err != nil {
		t.Fatalf("Failed to stop server: %v", err)
	}
}
