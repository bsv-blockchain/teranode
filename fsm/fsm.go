package fsm

import (
	"github.com/looplab/fsm"
)

func NewFiniteStateMachine(callbacks *fsm.Callbacks, opts ...func(*fsm.FSM)) *fsm.FSM {
	var cb = fsm.Callbacks{}
	if callbacks != nil {
		cb = *callbacks
	}

	finiteStateMachine := fsm.NewFSM(
		FiniteStateMachineState_Stopped,
		fsm.Events{
			{},
			{},
		},
		cb,
	)

	// apply options
	for _, opt := range opts {
		opt(finiteStateMachine)
	}

	return finiteStateMachine
}
