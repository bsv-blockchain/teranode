package main

import (
	"encoding/hex"
	"encoding/json"
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
		fmt.Println("Usage: extended <txHex | @tx.hex>")
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
	var err error

	parentTXs := make(map[string]*WocTxJSON)

	for i, input := range tx.Inputs {
		parentTX, ok := parentTXs[input.PreviousTxIDChainHash().String()]
		if !ok {
			// get the parent tx and store in the map
			parentTXHex := input.PreviousTxIDChainHash().String()

			parentTX, err = fetchTransaction(parentTXHex)
			if err != nil {
				return err
			}

			parentTXs[parentTXHex] = parentTX
		}

		// add the parent tx output to the input
		previousScript, err := hex.DecodeString(parentTX.Vout[input.PreviousTxOutIndex].ScriptPubKey.Hex)
		if err != nil {
			return err
		}

		tx.Inputs[i].PreviousTxScript = bscript.NewFromBytes(previousScript)
		tx.Inputs[i].PreviousTxSatoshis = uint64(parentTX.Vout[input.PreviousTxOutIndex].Value)
	}

	return nil
}

type Vout struct {
	Value        float64 `json:"value"`
	ScriptPubKey struct {
		Hex string `json:"hex"`
	} `json:"scriptPubKey"`
}

type WocTxJSON struct {
	Vout []Vout `json:"vout"`
}

func fetchTransaction(txIDHex string) (*WocTxJSON, error) {
	resp, err := http.Get(fmt.Sprintf("https://api.whatsonchain.com/v1/bsv/main/tx/hash/%s", txIDHex))
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var wocTxJSON WocTxJSON
	if err = json.Unmarshal(body, &wocTxJSON); err != nil {
		return nil, err
	}

	return &wocTxJSON, nil
}
