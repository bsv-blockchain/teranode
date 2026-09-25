package blockchain

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAdmitCatchupWork_RequiresReadyAuthority(t *testing.T) {
	for _, state := range []FSMStateType{FSMStateRUNNING, FSMStateCATCHINGBLOCKS, FSMStateIDLE} {
		t.Run(state.String(), func(t *testing.T) {
			b, _ := newFSMPersistenceTestBlockchain(t, state)
			b.subscriptionManagerReady.Store(true)
			client := &Client{client: blockchain_api.NewBlockchainAPIClient(newFSMReadTestConnection(t, b))}
			cached := FSMStateIDLE
			client.fmsState.Store(&cached)
			if state == FSMStateIDLE {
				require.ErrorIs(t, client.AdmitCatchupWork(t.Context()), ErrCatchupPaused)
			} else {
				require.NoError(t, client.AdmitCatchupWork(t.Context()))
			}
			b.subscriptionManagerReady.Store(false)
			err := client.AdmitCatchupWork(t.Context())
			require.Equal(t, codes.Unavailable, status.Code(err))
			require.NotErrorIs(t, err, ErrCatchupPaused)
		})
	}
}

func TestCatchUpBlocks_ClassifiesAuthorityFailure(t *testing.T) {
	for _, scenario := range []struct {
		name                   string
		state                  FSMStateType
		ready                  bool
		failure                error
		wantPaused             bool
		wantCode               codes.Code
		wantStateError         bool
		wantConfigurationError bool
	}{
		{name: "confirmed pause", state: FSMStateIDLE, ready: true, wantPaused: true},
		{name: "unready is not pause", state: FSMStateIDLE, wantCode: codes.Unavailable},
		{name: "transport unavailable", state: FSMStateRUNNING, ready: true, failure: status.Error(codes.Unavailable, "restarting"), wantCode: codes.Unavailable},
		{name: "permanent transition error", state: FSMStateRUNNING, ready: true, failure: errors.WrapGRPC(errors.NewStateError("invalid transition")), wantStateError: true},
		{name: "permanent configuration error", state: FSMStateRUNNING, ready: true, failure: errors.WrapGRPC(errors.NewConfigurationError("invalid configuration")), wantConfigurationError: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			b, _ := newFSMPersistenceTestBlockchain(t, scenario.state)
			b.subscriptionManagerReady.Store(scenario.ready)
			intercept := grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
				if scenario.failure != nil && info.FullMethod == blockchain_api.BlockchainAPI_CatchUpBlocks_FullMethodName {
					return nil, scenario.failure
				}
				return handler(ctx, req)
			})
			client := &Client{client: blockchain_api.NewBlockchainAPIClient(newFSMReadTestConnection(t, b, intercept)), logger: ulogger.TestLogger{}}
			err := client.CatchUpBlocks(t.Context())
			require.Error(t, err)
			if scenario.wantPaused {
				require.ErrorIs(t, err, ErrCatchupPaused)
			} else {
				require.NotErrorIs(t, err, ErrCatchupPaused)
			}
			if scenario.wantCode != codes.OK {
				require.Equal(t, scenario.wantCode, status.Code(err))
			}
			if scenario.wantStateError {
				require.ErrorIs(t, err, errors.ErrStateError)
			}
			if scenario.wantConfigurationError {
				require.ErrorIs(t, err, errors.ErrConfiguration)
			}
		})
	}
}
