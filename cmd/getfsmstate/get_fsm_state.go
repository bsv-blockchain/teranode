// Package getfsmstate provides utilities for retrieving and displaying the current state of the Finite State Machine (FSM)
// from the blockchain service. It is intended for debugging and inspection of FSM states and related blockchain information.
//
// Usage:
//
//	This package is typically used as a command-line tool to fetch and print the current FSM state
//	from the blockchain service, using the provided settings.
//
// Functions:
//   - FetchFSMState: Retrieves and prints the current FSM state from the blockchain service.
//
// Side effects:
//
//	Functions in this package may print to stdout and exit the process if an error occurs.
package getfsmstate

import (
	"context"
	"fmt"
	"os"

	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
)

// FetchFSMState retrieves and prints the current state of the FSM (Finite State Machine) from the blockchain service.
//
// Parameters:
//   - logger: ulogger.Logger for logging messages.
//   - settings: pointer to settings.Settings containing configuration details.
//
// Side effects:
//   - Prints the current FSM state to stdout.
//   - Exits the process with a fatal log message if an error occurs during execution.
func FetchFSMState(logger ulogger.Logger, settings *settings.Settings) {
	ctx := context.Background()

	// create a new blockchain client
	blockchainClient, err := blockchain.NewClient(ctx, logger, settings, "FetchFSMState CMD")
	if err != nil {
		logger.Fatalf("failed to create blockchain client: %v", err)
		os.Exit(1)
	}

	// get the current state
	var currentState *blockchain.FSMStateType

	currentState, err = blockchainClient.GetFSMCurrentState(ctx)
	if err != nil {
		logger.Fatalf("failed to get current state: %v", err)
		os.Exit(1)
	}

	// print the current state
	fmt.Println("Current state:", currentState)
}
