package utxotool

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/bitcoin-sv/ubsv/services/blockpersister/utxoset/model"
)

func Start() {
	// Read the filename from the first argument of the command line
	filename := os.Args[1]

	// Open the file for reading
	f, err := os.Open(filename)
	if err != nil {
		// Print the error message
		fmt.Println("error opening file:", err)
		// Exit the program
		os.Exit(1)
	}

	defer func() {
		_ = f.Close()
	}()

	r := bufio.NewReader(f)

	switch {
	case strings.HasSuffix(filename, ".utxodiff"):
		utxodiff, err := model.NewUTXODiffFromReader(r)
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

	case strings.HasSuffix(filename, ".utxoset"):
		utxoSet, err := model.NewUTXOSetFromReader(r)
		if err != nil {
			fmt.Println("error reading utxoSet:", err)
			os.Exit(1)
		}

		fmt.Println("UTXOSet block hash:", utxoSet.BlockHash)
		fmt.Println("UTXOSet block height:", utxoSet.BlockHeight)

		fmt.Println("UTXOSet UTXOs:")
		utxoSet.Current.Iter(func(uk model.UTXOKey, uv *model.UTXOValue) (stop bool) {
			fmt.Printf("%v %v\n", &uk, uv)
			return
		})

	default:
		fmt.Println("unknown file type")
		os.Exit(1)
	}
}
