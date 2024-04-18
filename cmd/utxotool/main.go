package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/bitcoin-sv/ubsv/services/blockpersister/utxoset/model"
)

func main() {
	// Read the filename from the first argument of the command line
	filename := os.Args[1]

	// Open the file for reading
	f, err := os.Open(filename)
	defer func() {
		_ = f.Close()
	}()

	// r := bufio.NewReader(f)

	// Check if there was an error opening the file
	if err != nil {
		// Print the error message
		fmt.Println("error opening file:", err)
		// Exit the program
		os.Exit(1)
	}

	if strings.HasSuffix(filename, ".utxodiff") {
		utxodiff, err := model.NewUTXODiffFromReader(f)
		if err != nil {
			fmt.Println("error reading utxodiff:", err)
			os.Exit(1)
		}

		fmt.Println("UTXODiff block hash:", utxodiff.BlockHash)
		fmt.Println("UTXODiff block height:", utxodiff.BlockHeight)

		fmt.Println("UTXODiff removed UTXOs:")
		utxodiff.Removed.Iter(func(uk model.UTXOKey, uv *model.UTXOValue) (stop bool) {
			fmt.Printf("%v %v\n", &uk, uv)
			return
		})

		fmt.Println("UTXODiff added UTXOs:")
		utxodiff.Added.Iter(func(uk model.UTXOKey, uv *model.UTXOValue) (stop bool) {
			fmt.Printf("%v %v\n", &uk, uv)
			return
		})
	}
}
