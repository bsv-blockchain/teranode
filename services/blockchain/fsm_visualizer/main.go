package main

import (
	"os"

	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/looplab/fsm"
)

func main() {
	var blockchainService *blockchain.Blockchain
	stateMachine := blockchainService.NewFiniteStateMachine()

	mermaidStateDiagram, err := fsm.VisualizeForMermaidWithGraphType(stateMachine, fsm.StateDiagram)
	if err != nil {
		panic(err)
	}

	header := "# State Machine\n\nThe mermaid diagram outlined below represents the various states and events that dictate the functionality of the node. To create and visualize the state machine diagram, you can use https://mermaid.live/. This tool allows you to generate the diagram visualization interactively.\n\n```mermaid\n"
	footer := "```"
	if err := os.WriteFile("docs/state-machine.diagram.md", []byte(header+mermaidStateDiagram+footer), 0o600); err != nil {
		panic(err)
	}
}
