package blockchain

type FiniteStateMachineEvent string

const (
	FiniteStateMachineEvent_Stop       = "Stop"
	FiniteStateMachineEvent_Run        = "Run"
	FiniteStateMachineEvent_Mine       = "Mine"
	FiniteStateMachineEvent_StopMining = "StopMining"
)
