package blockchain

import (
	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
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
func (b *Blockchain) NewFiniteStateMachine(opts ...func(*fsm.FSM)) *fsm.FSM {

	// // Define callbacks
	callbacks := fsm.Callbacks{
		// 	"enter_state": func(_ context.Context, e *fsm.Event) {
		// 		metadata := make(map[string]string)
		// 		metadata["event"] = e.Event

		// 		_, err := b.SendNotification(context.Background(), &blockchain_api.Notification{
		// 			Type:    model.NotificationType_FSMEvent,
		// 			Hash:    nil, // irrelevant
		// 			BaseUrl: "",  // irrelevant
		// 			Metadata: &blockchain_api.NotificationMetadata{
		// 				Metadata: metadata,
		// 			},
		// 		})

		// 		if err != nil {
		// 			b.logger.Errorf("[Blockchain][FiniteStateMachine] error sending notification: %s", err)
		// 		}
		// 	},
	}

	// Create the finite state machine, with states and transitions
	finiteStateMachine := fsm.NewFSM(
		// FiniteStateMachineState_Stopped,
		blockchain_api.FSMStateType_STOPPED.String(),
		fsm.Events{
			{
				Name: blockchain_api.FSMEventType_RUN.String(), //FiniteStateMachineEvent_Run,
				Src: []string{
					blockchain_api.FSMStateType_STOPPED.String(),
				},
				Dst: blockchain_api.FSMStateType_RUNNING.String(),
			},
			{
				Name: blockchain_api.FSMEventType_MINE.String(),
				Src: []string{
					blockchain_api.FSMStateType_RUNNING.String(),
				},
				Dst: blockchain_api.FSMStateType_MINING.String(),
			},
			{
				Name: blockchain_api.FSMEventType_STOPMINING.String(),
				Src: []string{
					blockchain_api.FSMStateType_MINING.String(),
				},
				Dst: blockchain_api.FSMStateType_RUNNING.String(),
			},
			{
				Name: blockchain_api.FSMEventType_STOP.String(),
				Src: []string{
					blockchain_api.FSMStateType_RUNNING.String(),
					blockchain_api.FSMStateType_MINING.String(),
				},
				Dst: blockchain_api.FSMStateType_STOPPED.String(),
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
