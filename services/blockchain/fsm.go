package blockchain

import (
	"context"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/looplab/fsm"
)

// NewFiniteStateMachine creates a new finite state machine for the blockchain service.
// The finite state machine has the following states:
// - Stopped
// - Running
// - Mining
// The finite state machine has the following events:
// - Run
// - Mine
// - StopMining
// - Stop
func (b *Blockchain) NewFiniteStateMachine(logger ulogger.Logger, opts ...func(*fsm.FSM)) *fsm.FSM {

	// Define callbacks
	callbacks := fsm.Callbacks{
		"enter_state": func(_ context.Context, e *fsm.Event) {
			metadata := make(map[string]string)
			metadata["event"] = e.Event

			_, err := b.SendNotification(context.Background(), &blockchain_api.Notification{
				Type:    model.NotificationType_FSMEvent,
				Hash:    nil, // irrelevant
				BaseUrl: "",  // irrelevant
				Metadata: &blockchain_api.NotificationMetadata{
					Metadata: metadata,
				},
			})

			if err != nil {
				logger.Errorf("[Blockchain][FiniteStateMachine] error sending notification: %s", err)
			}
		},
	}

	// Create the finite state machine, with states and transitions
	finiteStateMachine := fsm.NewFSM(
		FiniteStateMachineState_Stopped,
		fsm.Events{
			{
				Name: FiniteStateMachineEvent_Run,
				Src: []string{
					FiniteStateMachineState_Stopped,
				},
				Dst: FiniteStateMachineState_Running,
			},
			{
				Name: FiniteStateMachineEvent_Mine,
				Src: []string{
					FiniteStateMachineState_Running,
				},
				Dst: FiniteStateMachineState_Mining,
			},
			{
				Name: FiniteStateMachineEvent_StopMining,
				Src: []string{
					FiniteStateMachineState_Mining,
				},
				Dst: FiniteStateMachineState_Running,
			},
			{
				Name: FiniteStateMachineEvent_Stop,
				Src: []string{
					FiniteStateMachineState_Running,
					FiniteStateMachineState_Mining,
				},
				Dst: FiniteStateMachineState_Stopped,
			},
		},
		callbacks,
	)

	// apply options
	for _, opt := range opts {
		opt(finiteStateMachine)
	}

	return finiteStateMachine
}
