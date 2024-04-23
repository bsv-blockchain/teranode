package filereader

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"strings"

	block_model "github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockpersister/utxoset/model"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func Start() {
	logger := ulogger.TestLogger{}

	if len(os.Args) < 2 {
		fmt.Printf("Usage: filereader [-v] <filename | hash>.[block | subtree | utxoset | utxodiff]\n\n")
		return
	}

	var verbose bool
	var path string

	if os.Args[1] == "-v" {
		verbose = true
		if len(os.Args) < 3 {
			fmt.Printf("Usage: filereader [-v] <filename | hash>.[block | subtree | utxoset | utxodiff]\n\n")
			return
		}
		path = os.Args[2]
	} else {
		path = os.Args[1]
	}

	// Get the file extension
	dir, ext, r, shouldReturn := getReader(path, logger)
	if shouldReturn {
		return
	}

	// Wrap the reader with a buffered reader
	r = bufio.NewReaderSize(r, 1024*1024)

	fmt.Printf("Reading file %s\n", path)

	switch ext {
	case ".utxodiff":
		utxodiff, err := model.NewUTXODiffFromReader(logger, r)
		if err != nil {
			fmt.Printf("error reading utxodiff: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("UTXODiff block hash: %v\n", utxodiff.BlockHash)

		fmt.Printf("UTXODiff removed %d UTXOs", utxodiff.Removed.Length())
		if verbose {
			fmt.Println(":")
			utxodiff.Removed.Iter(func(uk model.UTXOKey, uv *model.UTXOValue) (stop bool) {
				fmt.Printf("%v %v\n", &uk, uv)
				return
			})
		} else {
			fmt.Println()
		}

		fmt.Printf("UTXODiff added %d UTXOs", utxodiff.Added.Length())
		if verbose {
			fmt.Println(":")
			utxodiff.Added.Iter(func(uk model.UTXOKey, uv *model.UTXOValue) (stop bool) {
				fmt.Printf("%v %v\n", &uk, uv)
				return
			})
		} else {
			fmt.Println()
		}

	case ".utxoset":
		utxoSet, err := model.NewUTXOSetFromReader(logger, r)
		if err != nil {
			fmt.Printf("error reading utxoSet: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("UTXOSet block hash: %v\n", utxoSet.BlockHash)

		fmt.Printf("UTXOSet with %d UTXOs", utxoSet.Current.Length())
		if verbose {
			fmt.Println(":")
			utxoSet.Current.Iter(func(uk model.UTXOKey, uv *model.UTXOValue) (stop bool) {
				fmt.Printf("%v %v\n", &uk, uv)
				return
			})
		} else {
			fmt.Println()
		}

	case ".subtree":
		// read the transaction count
		num := readSubtree(r, logger, verbose)
		fmt.Printf("Number of transactions: %d\n", num)

	case ".block":
		block, err := block_model.NewBlockFromReader(r)
		if err != nil {
			fmt.Printf("error reading block: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("Block hash: %s\n", block.Hash())
		fmt.Printf("%s", block.Header.StringDump())
		fmt.Printf("Number of transactions: %d\n", block.TransactionCount)

		for _, subtree := range block.Subtrees {

			fmt.Printf("Subtree %s\n", subtree)

			if verbose {
				filename := filepath.Join(dir, fmt.Sprintf("%s.subtree", subtree.String()))
				_, _, stReader, shouldReturn := getReader(filename, logger)
				if shouldReturn {
					return
				}
				readSubtree(stReader, logger, verbose)
			}
		}

	default:
		fmt.Printf("unknown file type")
		os.Exit(1)
	}

	os.Exit(0)
}

func getReader(path string, logger ulogger.Logger) (string, string, io.Reader, bool) {
	dir, file := filepath.Split(path)

	ext := filepath.Ext(file)

	fileWithoutExtension := strings.TrimSuffix(file, ext)

	if ext == "" {
		fmt.Printf("Usage: filereader [-v] <filename | hash>.[block | subtree | utxoset | utxodiff]\n\n")
		return "", "", nil, true
	}

	hash, err := chainhash.NewHashFromStr(fileWithoutExtension)

	if dir == "" && err == nil {
		store := getBlockStore(logger)

		r, err := store.GetIoReader(context.Background(), hash[:], options.WithFileExtension(ext))
		if err != nil {
			fmt.Printf("error getting reader from store: %s", err)
			return "", "", nil, true
		}

		return dir, ext, r, false
	}

	f, err := os.Open(path)
	if err != nil {
		fmt.Printf("error opening file: %v\n", err)
		return dir, "", nil, true
	}

	return dir, ext, f, false
}

func readSubtree(r io.Reader, logger ulogger.Logger, verbose bool) uint32 {
	var num uint32

	if err := binary.Read(r, binary.LittleEndian, &num); err != nil {
		fmt.Printf("error reading transaction count: %v\n", err)
		os.Exit(1)
	}

	if verbose {
		for i := uint32(0); i < num; i++ {
			var tx bt.Tx
			_, err := tx.ReadFrom(r)
			if err != nil {
				fmt.Printf("error reading transaction: %v\n", err)
				os.Exit(1)
			}

			if block_model.IsCoinbasePlaceHolderTx(&tx) {
				fmt.Printf("%10d: Coinbase Placeholder\n", i)
			} else {
				fmt.Printf("%10d: %v\n", i, tx.TxIDChainHash())
			}
		}
	}
	return num
}

func getBlockStore(logger ulogger.Logger) blob.Store {
	blockStoreUrl, err, found := gocore.Config().GetURL("blockstore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("blockstore config not found")
	}

	blockStore, err := blob.NewStore(logger, blockStoreUrl, options.WithPrefixDirectory(10))
	if err != nil {
		panic(err)
	}

	return blockStore
}
