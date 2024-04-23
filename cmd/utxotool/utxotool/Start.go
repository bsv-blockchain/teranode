package utxotool

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"strings"

	block_model "github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockpersister/utxoset/model"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func Start() {
	logger := ulogger.NewGoCoreLogger("main")

	fmt.Println()

	if len(os.Args) < 2 {
		fmt.Printf("Usage: utxotool <filename | hash>.[block | utxoset | utxodiff]\n\n")
		return
	}

	path := os.Args[1]

	dir, file := filepath.Split(path)

	// Get the file extension
	ext := filepath.Ext(file)

	fileWithoutExtension := strings.TrimSuffix(file, ext)

	if ext == "" {
		fmt.Printf("Usage: utxotool <filename | hash>.[block | utxoset | utxodiff]\n\n")
		return
	}

	start := time.Now()
	defer func() {
		logger.Infof("Time taken: %s", time.Since(start))
	}()

	var r io.Reader
	var err error

	hash, err := chainhash.NewHashFromStr(fileWithoutExtension)

	if dir == "" && err == nil {
		// This is a valid hash, so we'll assume it's a block hash and read it from the store
		persistURL, err, ok := gocore.Config().GetURL("blockPersister_persistURL")
		if err != nil || !ok {
			logger.Fatalf("Error getting blockpersister_store URL: %v", err)
		}

		store, err := blob.NewStore(logger, persistURL)
		if err != nil {
			logger.Errorf("failed to open store at %s: %s", persistURL, err)
			return
		}

		rc, err := store.GetIoReader(context.Background(), hash[:], options.WithFileExtension(ext))
		if err != nil {
			logger.Errorf("error getting reader from store: %s", err)
			return
		}

		r = rc
		defer rc.Close()

	} else {
		f, err := os.Open(path)
		if err != nil {
			logger.Errorf("error opening file: %v", err)
			return
		}

		r = f
		defer f.Close()
	}

	// Wrap the reader with a buffered reader
	r = bufio.NewReaderSize(r, 1024*1024)

	logger.Infof("Reading file %s", path)

	switch ext {
	case ".utxodiff":
		utxodiff, err := model.NewUTXODiffFromReader(logger, r)
		if err != nil {
			logger.Errorf("error reading utxodiff:", err)
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

	case ".utxoset":
		utxoSet, err := model.NewUTXOSetFromReader(logger, r)
		if err != nil {
			logger.Errorf("error reading utxoSet:", err)
			os.Exit(1)
		}

		fmt.Println("UTXOSet block hash:", utxoSet.BlockHash)

		fmt.Println("UTXOSet UTXOs:")
		utxoSet.Current.Iter(func(uk model.UTXOKey, uv *model.UTXOValue) (stop bool) {
			fmt.Printf("%v %v\n", &uk, uv)
			return
		})

	case ".block":
		blockHeaderBytes := make([]byte, 80)
		// read the first 80 bytes as the block header
		_, err = io.ReadFull(r, blockHeaderBytes)
		if err != nil {
			logger.Errorf("error reading block header:", err)
			os.Exit(1)
		}

		header, err := block_model.NewBlockHeaderFromBytes(blockHeaderBytes)
		if err != nil {
			logger.Errorf("error creating block header:", err)
			os.Exit(1)
		}

		fmt.Println("Block header:", header.StringDump())

		// read the transaction count
		numTransactions, err := wire.ReadVarInt(r, 0)
		if err != nil {
			logger.Errorf("error reading transaction count:", err)
			os.Exit(1)
		}

		fmt.Println("Number of transactions:", numTransactions)

	default:
		logger.Errorf("unknown file type")
		os.Exit(1)
	}

	os.Exit(0)
}
