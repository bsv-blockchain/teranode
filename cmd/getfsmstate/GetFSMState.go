package getfsmstate

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
)

func GetFSMState() {
	ctx := context.Background()

	logger := ulogger.NewGoCoreLogger("Command Line Tool", ulogger.WithLevel("WARN"))
	tSettings := settings.NewSettings()

	// create a new blockchain client
	blockchainClient, err := blockchain.NewClient(ctx, logger, tSettings, "GetFSMState CMD")
	if err != nil {
		log.Fatalf("Failed to create blockchain client: %v", err)
		os.Exit(1)
	}

	// get the current state
	currentState, err := blockchainClient.GetFSMCurrentState(ctx)
	if err != nil {
		log.Fatalf("Failed to get current state: %v", err)
	}

	fmt.Println("Current state:", currentState)
}
