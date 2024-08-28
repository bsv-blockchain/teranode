package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
)

func main() {
	// read command line arguments
	if len(os.Args) != 2 {
		fmt.Println("Usage: extend <txID | txHex | @<filename in hex>")
		os.Exit(1)
	}

	// read hex data from command line, or file from os if in the format @filename.hex
	var txHex string

	if os.Args[1][0] == '@' {
		txHexFile := os.Args[1][1:]

		txHexBytes, err := os.ReadFile(txHexFile)
		if err != nil {
			fmt.Println("Error reading file:", err)
			os.Exit(1)
		}

		txHex = string(txHexBytes)
	} else {
		txHex = os.Args[1]
	}

	if len(txHex) == 64 {
		// this was a tx ID, lookup the tx from whatsonchain
		txHexFromWoC, err := fetchTransactionHex(txHex)
		if err != nil {
			fmt.Println("Error fetching transaction:", err)
			os.Exit(1)
		}

		txHex = txHexFromWoC
	}

	tx, err := bt.NewTxFromString(txHex)
	if err != nil {
		fmt.Println("Error creating transaction:", err)
		os.Exit(1)
	}

	// enrich the transaction with the standard WOC format
	err = EnrichStandardWOC(tx)
	if err != nil {
		fmt.Println("Error enriching transaction:", err)
		os.Exit(1)
	}

	// print the enriched transaction
	fmt.Println(hex.EncodeToString(tx.ExtendedBytes()))
}

func EnrichStandardWOC(tx *bt.Tx) error {
	parentTxs := make(map[string]*bt.Tx)

	for i, input := range tx.Inputs {
		parentTx, ok := parentTxs[input.PreviousTxIDChainHash().String()]
		if !ok {
			// get the parent tx and store in the map
			parentTxID := input.PreviousTxIDChainHash().String()

			parentTxHex, err := fetchTransactionHex(parentTxID)
			if err != nil {
				return err
			}

			parentTx, err = bt.NewTxFromString(parentTxHex)
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

func fetchTransactionHex(txIDHex string) (string, error) {
	resp, err := http.Get(fmt.Sprintf("https://api.whatsonchain.com/v1/bsv/main/tx/%s/hex", txIDHex))
	if err != nil {
		return "", err
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}
