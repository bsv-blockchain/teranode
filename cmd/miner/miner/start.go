package miner

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ordishs/go-bitcoin"
)

func Start() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(interrupt)

	// Command Line Options (Flags)
	rpcconnect := flag.String("rpcconnect", "127.0.0.1", "Connect to a node to retrieve blockchain data")
	rpcport := flag.Int("rpcport", 9292, "Port to connect to the node")
	rpcuser := flag.String("rpcuser", "bitcoin", "Username for the node")
	rpcpassword := flag.String("rpcpassword", "bitcoin", "Password for the node")
	useSSL := flag.Bool("use-ssl", false, "Use SSL to connect to the node")
	cpus := flag.Int("cpus", 1, "Number of CPUs to use for mining")
	coinbaseAddr := flag.String("coinbase-addr", "", "Address to send the coinbase reward to")
	coinbaseSig := flag.String("coinbase-sig", "", "Signature for the coinbase reward")

	flag.Parse()

	if *coinbaseAddr == "" {
		fmt.Println("No coinbase address provided")
		return
	}

	if *coinbaseSig == "" {
		fmt.Println("No coinbase signature provided")
		return
	}

	// Start the miner
	rpcClient, err := bitcoin.New(*rpcconnect, *rpcport, *rpcuser, *rpcpassword, *useSSL)
	if err != nil {
		fmt.Printf("Error connecting to node (%s@%s:%d): %s\n", *rpcuser, *rpcconnect, *rpcport, err)
		return
	}

	ctx, ctxCancel := context.WithCancel(context.Background())

	miner := NewMiner(rpcClient, *cpus, *coinbaseAddr, *coinbaseSig)

	go miner.mine(ctx)

	select {
	case <-interrupt:
		ctxCancel()
		break
	case <-ctx.Done():
		fmt.Printf("context cancelled: %v\n", ctx.Err())
		ctxCancel()

		break
	}
}
