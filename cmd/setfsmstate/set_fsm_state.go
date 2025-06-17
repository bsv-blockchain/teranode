// Package setfsmstate provides utilities for managing and updating the Finite State Machine (FSM) state
// of the blockchain client.
//
// Usage:
//
//	This package is typically used as a command-line tool to set the FSM state of the blockchain
//	client to a desired state, such as "idle" or "running".
//
// Functions:
//   - UpdateFSMState: Updates the FSM state of the blockchain client based on the provided input.
//
// Side effects:
//
//	Functions in this package may print to stdout and terminate the process if an error occurs.
package setfsmstate

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
)

// UpdateFSMState updates the FSM (Finite State Machine) state of the blockchain client.
//
// Parameters:
//   - logger: ulogger.Logger for logging
//   - settings: pointer to settings.Settings
//   - targetFsmState: string representing the desired FSM state (e.g., "idle", "running")
//
// Behavior:
//   - Maps the input string to a corresponding FSM event type.
//   - Creates a blockchain client and retrieves the current FSM state.
//   - Exits the process with an error message if the input state is invalid or if the client fails to initialize.
//
// Side effects:
//   - Prints error messages to stdout and may terminate the process on failure.
func UpdateFSMState(logger ulogger.Logger, settings *settings.Settings, targetFsmState string) {
	ctx := context.Background()

	var targetEvent blockchain.FSMEventType

	inputLowerCase := strings.ToLower(targetFsmState)

	switch inputLowerCase {
	case "idle":
		targetEvent = blockchain.FSMEventIDLE
	case "running":
		targetEvent = blockchain.FSMEventRUN
	case "catchingblocks":
		targetEvent = blockchain.FSMEventCATCHUPBLOCKS
	case "legacysyncing":
		targetEvent = blockchain.FSMEventLEGACYSYNC
	default:
		fmt.Println("Error: invalid fsm state")
		fmt.Println("\nAccepted FSM States:")
		fmt.Println("  running         - The node is running normally.")
		fmt.Println("  idle            - The node is idle, awaiting instructions.")
		fmt.Println("  catchingblocks  - The node is catching up by processing incoming blocks.")
		fmt.Println("  legacysyncing   - The node is syncing using the legacy method.")
		os.Exit(1)
	}

	// create a new blockchain client
	blockchainClient, err := blockchain.NewClient(ctx, logger, settings, "SetFSMState CMD")
	if err != nil {
		logger.Fatalf("Failed to create blockchain client: %v", err)
		os.Exit(1)
	}

	// get the current FSM state
	var currentState *blockchain.FSMStateType

	currentState, err = blockchainClient.GetFSMCurrentState(ctx)
	if err != nil {
		logger.Fatalf("Failed to get current FSM state: %v", err)
		os.Exit(1)
	}

	fmt.Println("Current FSM state:", currentState, ", target FSM state:", targetFsmState, ", sending event:", targetEvent)

	// send the FSM event to update the state
	err = blockchainClient.SendFSMEvent(ctx, targetEvent)
	if err != nil {
		logger.Fatalf("Failed to send FSM event: %v", err)
		os.Exit(1)
	}

	// get the updated FSM state
	currentState, err = blockchainClient.GetFSMCurrentState(ctx)
	if err != nil {
		logger.Fatalf("Failed to get current FSM state: %v", err)
		os.Exit(1)
	}

	// print the updated FSM state
	fmt.Println("FSM state set to:", currentState)
}
