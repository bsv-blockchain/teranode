package utxotool

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"strings"

	block_model "github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockpersister/utxoset/model"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

func Start() {
	logger := ulogger.NewGoCoreLogger("main")

	fmt.Println()

	if len(os.Args) < 2 {
		fmt.Printf("Usage: utxotool <filename | hash>.[block | utxoset | utxodiff]\n\n")
		return
	}

	filename := os.Args[1]

	// // Get the file extension
	// ext := strings.TrimPrefix(filename, strings.TrimSuffix(filename, "."))

	// // if ext == "" {
	// // 	fmt.Printf("Usage: utxotool <filename | hash>.[block | utxoset | utxodiff]\n\n")
	// // 	return
	// // }

	// filenameWithoutSuffix := strings.TrimSuffix(filename, "."+ext)

	// start := time.Now()
	// defer func() {
	// 	logger.Infof("Time taken: %s", time.Since(start))
	// }()

	// var r io.Reader
	// var err error

	// if hash, err := chainhash.NewHashFromStr(filenameWithoutSuffix); err == nil {
	// 	// This is a valid hash, so we'll assume it's a block hash and read it from the store
	// 	persistURL, err, ok := gocore.Config().GetURL("blockPersister_persistURL")
	// 	if err != nil || !ok {
	// 		logger.Fatalf("Error getting blockpersister_store URL: %v", err)
	// 	}

	// 	store, err := blob.NewStore(logger, persistURL)
	// 	if err != nil {
	// 		logger.Errorf("failed to open store at %s: %s", persistURL, err)
	// 		return
	// 	}

	// 	rc, err := store.GetIoReader(context.Background(), nil, options.WithFileName(hash.String()), options.WithSubDirectory("blocks"), options.WithFileExtension(ext))
	// 	if err != nil {
	// 		logger.Errorf("error getting reader from store: %s", err)
	// 		return
	// 	}

	// 	r = rc
	// 	defer rc.Close()

	// } else {
	// 	f, err := os.Open(filename)
	// 	if err != nil {
	// 		logger.Errorf("error opening file: %v", err)
	// 		return
	// 	}

	// 	r = f
	// 	defer f.Close()
	// }

	f, err := os.Open(filename)
	if err != nil {
		logger.Errorf("error opening file: %v", err)
		return
	}

	// Wrap the reader with a buffered reader
	r := bufio.NewReaderSize(f, 1024*1024)

	logger.Infof("Reading file %s", filename)

	switch {
	case strings.HasSuffix(filename, ".utxodiff"):
		utxodiff, err := model.NewUTXODiffFromReader(r)
		if err != nil {
			fmt.Println("error reading utxodiff:", err)
			os.Exit(1)
		}

		fmt.Println("UTXODiff block hash:", utxodiff.BlockHash)

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

		fmt.Println("UTXOSet UTXOs:")
		utxoSet.Current.Iter(func(uk model.UTXOKey, uv *model.UTXOValue) (stop bool) {
			fmt.Printf("%v %v\n", &uk, uv)
			return
		})

	case strings.HasSuffix(filename, ".block"):
		blockHeaderBytes := make([]byte, 80)
		// read the first 80 bytes as the block header
		_, err = io.ReadFull(r, blockHeaderBytes)
		if err != nil {
			fmt.Println("error reading block header:", err)
			os.Exit(1)
		}

		header, err := block_model.NewBlockHeaderFromBytes(blockHeaderBytes)
		if err != nil {
			fmt.Println("error creating block header:", err)
			os.Exit(1)
		}

		fmt.Println("Block header:", header.StringDump())

		// read the transaction count
		numTransactions, err := wire.ReadVarInt(r, 0)
		if err != nil {
			fmt.Println("error reading transaction count:", err)
			os.Exit(1)
		}

		fmt.Println("Number of transactions:", numTransactions)

	default:
		fmt.Println("unknown file type")
		os.Exit(1)
	}

	os.Exit(0)
}
