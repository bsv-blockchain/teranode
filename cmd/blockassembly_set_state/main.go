package main

import (
	"context"
	"encoding/binary"
	"os"
	"strconv"

	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

func main() {
	ctx := context.Background()
	logger := ulogger.New("blockassembly_set_state")

	blockchainClient, err := blockchain.NewClient(ctx, logger, "cmd/blockassembly_set_state")
	if err != nil {
		panic(err)
	}

	// get the height to set from the command line
	heightStr := os.Args[1]

	height, err := strconv.ParseUint(heightStr, 10, 32)
	if err != nil {
		panic(err)
	}

	if height == 0 {
		panic("Height must be greater than 0")
	}

	// get the block at that height
	block, err := blockchainClient.GetBlockByHeight(ctx, uint32(height))
	if err != nil {
		panic(err)
	}

	logger.Infof("Setting state for block %d: %s", height, block.Header.Hash().String())

	// set the state to the block we just got
	blockHeaderBytes := block.Header.Bytes()
	state := make([]byte, 4+len(blockHeaderBytes))
	binary.LittleEndian.PutUint32(state[:4], uint32(height))
	state = append(state[:4], blockHeaderBytes...)

	err = blockchainClient.SetState(ctx, "BlockAssembler", state)
	if err != nil {
		panic(err)
	}

	logger.Infof("State set successfully")
}
