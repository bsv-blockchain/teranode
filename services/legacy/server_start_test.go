package legacy

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestStart_FSMContextCancellation verifies graceful shutdown handling when
// the context is cancelled during the FSM wait. The error must be returned
// (not swallowed) and must be a context error so the service manager can
// distinguish it from a real failure.
func TestStart_FSMContextCancellation(t *testing.T) {
	ctx := context.Background()

	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("WaitUntilFSMTransitionFromIdleState", mock.Anything).Return(context.Canceled)

	server := &Server{
		logger:           ulogger.TestLogger{},
		settings:         &settings.Settings{},
		blockchainClient: mockBlockchainClient,
	}

	readyCh := make(chan struct{})
	err := server.Start(ctx, readyCh)

	require.Error(t, err)
	require.True(t, errors.IsContextError(err), "expected context error, got %v", err)
	mockBlockchainClient.AssertExpectations(t)
}

// TestStop_NilInnerServer covers the issue #1032 crash path: when newServer
// fails (e.g. "no valid listen address"), Server.Init leaves the inner
// s.server nil. The daemon error path then calls Server.Stop, which must not
// dereference the nil inner server. Without the guard this panics with a
// SIGSEGV in a daemon goroutine and takes the whole test binary down.
func TestStop_NilInnerServer(t *testing.T) {
	s := &Server{
		logger:   ulogger.TestLogger{},
		settings: &settings.Settings{},
		// server intentionally left nil — mirrors the post-Init-failure state.
	}

	require.NotPanics(t, func() {
		err := s.Stop(context.Background())
		require.NoError(t, err)
	})
}
