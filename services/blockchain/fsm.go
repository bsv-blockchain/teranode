package blockchain

import (
	"context"
	"net/http"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/looplab/fsm"
)

// NewFiniteStateMachine creates a new finite state machine for the blockchain service.
// The finite state machine has the following states:
// - Stopped
// - Running
// - Mining
// - CatchingBlocks
// - CatchingTxs
// - Restoring
// - ResourceUnavailable
// The finite state machine has the following events:
// - Run
// - Mine
// - CatchupBlocks
// - CatchupTxs
// - Restore
// - Unavailable
// - Stop
func (b *Blockchain) NewFiniteStateMachine(opts ...func(*fsm.FSM)) *fsm.FSM {
	// Define callbacks
	callbacks := fsm.Callbacks{
		"enter_state": func(_ context.Context, e *fsm.Event) {
			metadata := map[string]string{
				"event":       e.Event,
				"destination": e.Dst,
			}

			if _, err := b.SendNotification(context.Background(), &blockchain_api.Notification{
				Type:     model.NotificationType_FSMState,
				Hash:     (&chainhash.Hash{})[:], // not relevant for FSMEvent notifications
				Base_URL: "",                     // not relevant for FSMEvent notifications
				Metadata: &blockchain_api.NotificationMetadata{
					Metadata: metadata,
				},
			}); err != nil {
				b.logger.Errorf("[Blockchain][FiniteStateMachine] error sending notification: %s", err)
			}

			prometheusBlockchainFSMCurrentState.Set(float64(blockchain_api.FSMStateType_value[e.Dst]))
		},
	}

	// Create the finite state machine, with states and transitions
	finiteStateMachine := fsm.NewFSM(
		blockchain_api.FSMStateType_IDLE.String(),
		fsm.Events{
			{
				Name: blockchain_api.FSMEventType_RUN.String(),
				Src: []string{
					blockchain_api.FSMStateType_IDLE.String(),
					blockchain_api.FSMStateType_LEGACYSYNCING.String(),
					blockchain_api.FSMStateType_CATCHINGBLOCKS.String(),
				},
				Dst: blockchain_api.FSMStateType_RUNNING.String(),
			},
			{
				Name: blockchain_api.FSMEventType_LEGACYSYNC.String(),
				Src: []string{
					blockchain_api.FSMStateType_IDLE.String(),
				},
				Dst: blockchain_api.FSMStateType_LEGACYSYNCING.String(),
			},
			{
				Name: blockchain_api.FSMEventType_CATCHUPBLOCKS.String(),
				Src: []string{
					blockchain_api.FSMStateType_RUNNING.String(),
				},
				Dst: blockchain_api.FSMStateType_CATCHINGBLOCKS.String(),
			},
			{
				Name: blockchain_api.FSMEventType_STOP.String(),
				Src: []string{
					blockchain_api.FSMStateType_RUNNING.String(),
					blockchain_api.FSMStateType_CATCHINGBLOCKS.String(),
					blockchain_api.FSMStateType_LEGACYSYNCING.String(),
				},
				Dst: blockchain_api.FSMStateType_IDLE.String(),
			},
		},
		callbacks,
		// fsm.Callbacks{},
	)

	// apply options
	for _, opt := range opts {
		opt(finiteStateMachine)
	}

	return finiteStateMachine
}

func CheckFSM(blockchainClient ClientI) func(ctx context.Context, checkLiveness bool) (int, string, error) {
	return func(ctx context.Context, checkLiveness bool) (int, string, error) {
		state, err := blockchainClient.GetFSMCurrentState(ctx)
		if err != nil {
			return http.StatusServiceUnavailable, "failed to check FSM state", err
		}

		var (
			status int
		)

		switch *state {
		case blockchain_api.FSMStateType_CATCHINGBLOCKS:
			status = http.StatusOK
		case blockchain_api.FSMStateType_LEGACYSYNCING:
			status = http.StatusOK
		case blockchain_api.FSMStateType_RUNNING:
			status = http.StatusOK
		case blockchain_api.FSMStateType_IDLE:
			status = http.StatusServiceUnavailable
		default:
			status = http.StatusOK
		}

		return status, state.String(), nil
	}
}
