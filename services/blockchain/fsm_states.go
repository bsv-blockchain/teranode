package blockchain

type FiniteStateMachineState string

const (
	FiniteStateMachineState_Stopped = "Stopped"
	FiniteStateMachineState_Running = "Running"
	FiniteStateMachineState_Mining  = "Mining"
)
