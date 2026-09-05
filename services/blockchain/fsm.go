// Package blockchain provides functionality for managing the Bitcoin blockchain.
package blockchain

import (
	"context"
	"net/http"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/looplab/fsm"
)

// FSMTransitions is the single source of truth for blockchain FSM transitions.
// Used by NewFiniteStateMachine and by AvailableEventsForState.
var FSMTransitions = fsm.Events{
	{
		Name: blockchain_api.FSMEventType_RUN.String(),
		Src: []string{
			blockchain_api.FSMStateType_IDLE.String(),
			blockchain_api.FSMStateType_CATCHINGBLOCKS.String(),
		},
		Dst: blockchain_api.FSMStateType_RUNNING.String(),
	},
	{
		Name: blockchain_api.FSMEventType_CATCHUPBLOCKS.String(),
		Src: []string{
			blockchain_api.FSMStateType_IDLE.String(),
			blockchain_api.FSMStateType_RUNNING.String(),
		},
		Dst: blockchain_api.FSMStateType_CATCHINGBLOCKS.String(),
	},
	{
		Name: blockchain_api.FSMEventType_STOP.String(),
		Src: []string{
			blockchain_api.FSMStateType_RUNNING.String(),
			blockchain_api.FSMStateType_CATCHINGBLOCKS.String(),
		},
		Dst: blockchain_api.FSMStateType_IDLE.String(),
	},
}

// AvailableEventsForState returns the event names valid from the given state,
// derived from FSMTransitions (single source of truth). Order follows the
// table's declaration order. Unknown state returns an empty (non-nil) slice.
func AvailableEventsForState(state string) []string {
	events := make([]string, 0)
	for _, e := range FSMTransitions {
		for _, src := range e.Src {
			if src == state {
				events = append(events, e.Name)
				break
			}
		}
	}
	return events
}

// NewFiniteStateMachine creates a new finite state machine for the blockchain service.
//
// States: IDLE, RUNNING, CATCHINGBLOCKS
// Events: RUN, CATCHUPBLOCKS, STOP
//
// Persists accepted transitions before changing state, sending notifications,
// and updating Prometheus metrics. Runtime events must use SendFSMEvent, which
// serializes admission and detaches the transition from caller cancellation.
func (b *Blockchain) NewFiniteStateMachine(opts ...func(*fsm.FSM)) *fsm.FSM {
	// Define callbacks
	callbacks := fsm.Callbacks{
		"before_event": func(ctx context.Context, e *fsm.Event) {
			storeCtx, cancel := b.fsmStoreContext(context.WithoutCancel(ctx))
			defer cancel()
			if err := b.store.SetFSMState(storeCtx, e.Dst); err != nil {
				// Cancel before looplab installs its pending transition or changes
				// state, so an explicit retry remains possible. A failed write may
				// have committed: never attempt an unsafe compensating rollback.
				e.Cancel(errors.NewStorageError("failed to persist FSM transition from %s to %s", e.Src, e.Dst, err))
			}
		},
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
		FSMTransitions,
		callbacks,
		// fsm.Callbacks{},
	)

	// apply options
	for _, opt := range opts {
		opt(finiteStateMachine)
	}

	return finiteStateMachine
}

// CheckFSM creates a health check function for the blockchain FSM.
// Returns a function that checks the current FSM state and returns appropriate
// HTTP status codes:
//   - StatusOK (200): For CATCHINGBLOCKS, RUNNING states
//   - StatusServiceUnavailable (503): For IDLE state
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
		case blockchain_api.FSMStateType_RUNNING:
			status = http.StatusOK
		case blockchain_api.FSMStateType_IDLE:
			status = http.StatusOK
		default:
			status = http.StatusServiceUnavailable
		}

		return status, state.String(), nil
	}
}
