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

	// var wg sync.WaitGroup
	// wg.Add(1)

	// Start the server
	// ctx := context.Background()
	// go func() {
	// 	//defer wg.Done()
	// 	if err := server.Start(ctx); err != nil {
	// 		t.Errorf("Failed to start server: %v", err)
	// 	}
	// }()

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

	//wg.Wait()
	// Stop the server
	// if err := server.Stop(ctx); err != nil {
	// 	t.Fatalf("Failed to stop server: %v", err)
	// }
}

func Test_GetFSMCurrentState(t *testing.T) {
	// Create a new server instance
	logger := ulogger.New("blockchain")
	server, err := New(context.Background(), logger)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	require.NoError(t, server.Init(context.Background()))

	// var wg sync.WaitGroup
	// wg.Add(1)

	// Assuming that the setup or initialization of the server sets the FSM state
	// Start the server and the FSM in an initial state you control (not shown here)

	//ctx := context.Background()
	// go func() {
	// 	defer wg.Done()
	// if err := server.Start(ctx); err != nil {
	// 	t.Errorf("Failed to start server: %v", err)
	// }
	//	}()

	// Test the GetFSMCurrentState function
	response, err := server.GetFSMCurrentState(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.NotNil(t, response)
	assert.Equal(t, blockchain_api.FSMStateType_STOPPED, response.State, "Expected FSM state did not match")

	//wg.Wait()
	// Stop the server
	// if err := server.Stop(ctx); err != nil {
	// 	t.Fatalf("Failed to stop server: %v", err)
	// }
}
