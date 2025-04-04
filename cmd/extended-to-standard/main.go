package main

import (
	"encoding/hex"
	"fmt"
	"os"

	"github.com/libsv/go-bt/v2"
)

func main() {
	// read command line arguments
	if len(os.Args) != 2 {
		fmt.Println("Usage: extended-to-standard <txHex>")
		os.Exit(1)
	}

	// read hex data from command line, or file from os if in the format @filename.hex
	txHex := os.Args[1]

	tx, err := bt.NewTxFromString(txHex)
	if err != nil {
		fmt.Println("Error creating transaction:", err)
		os.Exit(1)
	}

	// print the standard transaction
	fmt.Println(hex.EncodeToString(tx.Bytes()))
}
