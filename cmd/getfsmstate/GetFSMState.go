package getfsmstate

import (
	"context"
	"fmt"
	"log"

	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
)

func GetFSMState(logger ulogger.Logger, tSettings *settings.Settings) {
	ctx := context.Background()

	// create a new blockchain client
	blockchainClient, err := blockchain.NewClient(ctx, logger, tSettings, "GetFSMState CMD")
	if err != nil {
		log.Fatalf("Failed to create blockchain client: %v", err)
	}

	// get the current state
	currentState, err := blockchainClient.GetFSMCurrentState(ctx)
	if err != nil {
		log.Fatalf("Failed to get current state: %v", err)
	}

	fmt.Println("Current state:", currentState)
}
