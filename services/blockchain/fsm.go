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
// - CatchingBlocks
// The finite state machine has the following events:
// - Run
// - Mine
// - CatchupBlocks
// - Stop
func (b *Blockchain) NewFiniteStateMachine(opts ...func(*fsm.FSM)) *fsm.FSM {
	// Create the finite state machine, with states and transitions
	finiteStateMachine := fsm.NewFSM(
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
					blockchain_api.FSMStateType_CATCHINGBLOCKS.String(),
				},
				Dst: blockchain_api.FSMStateType_MINING.String(),
			},
			{
				Name: blockchain_api.FSMEventType_CATCHUPBLOCKS.String(),
				Src: []string{
					blockchain_api.FSMStateType_MINING.String(),
				},
				Dst: blockchain_api.FSMStateType_CATCHINGBLOCKS.String(),
			},
			{
				Name: blockchain_api.FSMEventType_STOP.String(),
				Src: []string{
					blockchain_api.FSMStateType_RUNNING.String(),
					blockchain_api.FSMStateType_MINING.String(),
					blockchain_api.FSMStateType_CATCHINGBLOCKS.String(),
				},
				Dst: blockchain_api.FSMStateType_STOPPED.String(),
			},
		},
		fsm.Callbacks{},
	)

	// apply options
	for _, opt := range opts {
		opt(finiteStateMachine)
	}

	return finiteStateMachine
}
