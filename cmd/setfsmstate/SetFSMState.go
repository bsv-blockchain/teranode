package setfsmstate

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
)

func SetFSMState(targetFsmState string) {
	ctx := context.Background()
	logger := ulogger.NewGoCoreLogger("Command Line Tool", ulogger.WithLevel("WARN"))
	tSettings := settings.NewSettings()

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
	blockchainClient, err := blockchain.NewClient(ctx, logger, tSettings, "SetFSMState CMD")
	if err != nil {
		log.Fatalf("Failed to create blockchain client: %v", err)
		os.Exit(1)
	}

	currentState, err := blockchainClient.GetFSMCurrentState(ctx)
	if err != nil {
		log.Fatalf("Failed to get current FSM state: %v", err)
		os.Exit(1)
	}

	fmt.Println("Current FSM state:", currentState, ", target FSM state:", targetFsmState, ", sending event:", targetEvent)

	err = blockchainClient.SendFSMEvent(ctx, targetEvent)
	if err != nil {
		log.Fatalf("Failed to send FSM event: %v", err)
		os.Exit(1)
	}

	currentState, err = blockchainClient.GetFSMCurrentState(ctx)
	if err != nil {
		log.Fatalf("Failed to get current FSM state: %v", err)
		os.Exit(1)
	}

	fmt.Println("FSM state set to:", currentState)
}
