package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"os"

	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/ordishs/go-bitcoin"
)

// ExtendedBlock is a command line utility that extends all the transactions in a block and creates a binary file
// containing the extended transactions. This can then be read by the Go BT library.
func main() {
	// Command Line Options (Flags)
	rpcHost := flag.String("rpcHost", "localhost", "RPC host")     // RPC host
	rpcPort := flag.Int("rpcPort", 8332, "RPC port")               // RPC port
	username := flag.String("username", "bitcoin", "RPC username") // RPC username
	password := flag.String("password", "bitcoin", "RPC password") // RPC password

	flag.Usage = func() {
		fmt.Printf("Usage: extended_block [-rpcHost=...] [-rpcPort=...] [-username=...] [-password=...] <block hash>\n\n")
	}

	flag.Parse()

	blockHex := flag.Arg(0)

	if blockHex == "" {
		flag.Usage()
		os.Exit(0)
	}

	b, err := bitcoin.New(*rpcHost, *rpcPort, *username, *password, false)
	if err != nil {
		panic(err)
	}

	_, err = b.GetBlockchainInfo()
	if err != nil {
		panic(err)
	}

	block, err := b.GetBlock(blockHex)
	if err != nil {
		panic(err)
	}

	f, err := os.OpenFile(fmt.Sprintf("%s.extended.bin", blockHex), os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

	txLength := len(block.Tx) - 1

	// get the first transaction
	var tx *bt.Tx

	for idx, txID := range block.Tx {
		if idx == 0 {
			// skip the coinbase
			continue
		}

		fmt.Printf("Processing transaction %d of %d\r", idx, txLength)

		tx, err = fetchTransaction(b, txID)
		if err != nil {
			panic(err)
		}

		if err = EnrichStandard(b, tx); err != nil {
			panic(err)
		}

		_, err = f.Write(tx.ExtendedBytes())
		if err != nil {
			panic(err)
		}
	}

	if err = f.Close(); err != nil {
		panic(err)
	}
}

func EnrichStandard(b *bitcoin.Bitcoind, tx *bt.Tx) (err error) {
	parentTxs := make(map[string]*bt.Tx)

	for i, input := range tx.Inputs {
		parentTx, ok := parentTxs[input.PreviousTxIDChainHash().String()]
		if !ok {
			// get the parent tx and store in the map
			parentTxID := input.PreviousTxIDChainHash().String()

			parentTx, err = fetchTransaction(b, parentTxID)
			if err != nil {
				return err
			}

			parentTxs[parentTxID] = parentTx
		}

		// add the parent tx output to the input
		previousScript, err := hex.DecodeString(parentTx.Outputs[input.PreviousTxOutIndex].LockingScript.String())
		if err != nil {
			return err
		}

		tx.Inputs[i].PreviousTxScript = bscript.NewFromBytes(previousScript)
		tx.Inputs[i].PreviousTxSatoshis = parentTx.Outputs[input.PreviousTxOutIndex].Satoshis
	}

	return nil
}

func fetchTransaction(b *bitcoin.Bitcoind, txIDHex string) (*bt.Tx, error) {
	rawTx, err := b.GetRawTransaction(txIDHex)
	if err != nil {
		panic(err)
	}

	tx, err := bt.NewTxFromString(rawTx.Hex)
	if err != nil {
		panic(err)
	}

	return tx, nil
}
