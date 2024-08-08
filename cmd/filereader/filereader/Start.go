package filereader

import (
	"bufio"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"strings"

	"github.com/bitcoin-sv/ubsv/errors"
	block_model "github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/blockpersister/utxoset/model"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/services/utxopersister"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var verbose bool
var verify bool
var old bool

func Start() {
	logger := ulogger.TestLogger{}

	// Define command line arguments
	flag.BoolVar(&verbose, "verbose", false, "verbose output")
	flag.BoolVar(&verify, "verify", false, "verify all stored data")
	flag.BoolVar(&old, "old", false, "old format")

	flag.Parse()

	if verify {
		if err := verifyChain(); err != nil {
			fmt.Printf("error verifying: %v\n", err)
			os.Exit(1)
		}

		os.Exit(0)
	}

	if len(flag.Args()) != 1 {
		usage("filename or hash required")
	}

	path := flag.Arg(0)

	// Wrap the reader with a buffered reader
	// read the transaction count
	if err := ProcessFile(path, logger); err != nil {
		fmt.Printf("error processing file: %v\n", err)
		os.Exit(1)
	}

	os.Exit(0)
}

func ProcessFile(path string, logger ulogger.Logger) error {
	dir, filename, ext, r, err := getReader(path, logger)
	if err != nil {
		return errors.NewProcessingError("error getting reader", err)
	}

	r = bufio.NewReaderSize(r, 1024*1024)

	logger.Infof("Reading file %s\n", path)

	if err := readFile(filename, ext, logger, r, dir); err != nil {
		return errors.NewProcessingError("error reading file", err)
	}

	return nil
}

func verifyChain() error {
	logger := ulogger.NewZeroLogger("main")

	blockchain, err := blockchain.NewClient(context.Background(), logger, "cmd/filereader")
	if err != nil {
		return errors.NewServiceError("error creating blockchain client", err)
	}

	blockStore := getBlockStore(logger)

	var o []options.Options

	ext := "block"
	if old {
		ext = ""
	}

	if !old {
		o = append(o, options.WithFileExtension("block"))
	}

	// Verify all blocks in reverse order (as it's easier)
	header, meta, err := blockchain.GetBestBlockHeader(context.Background())
	if err != nil {
		return errors.NewBlockError("error getting best block header", err)
	}

	p := message.NewPrinter(language.English)

	for {
		r, err := blockStore.GetIoReader(context.Background(), header.Hash()[:], o...)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) {
				fmt.Printf("%s (%d): NOT FOUND\n", header.Hash(), meta.Height)
			} else {
				return errors.NewProcessingError("error getting block reader", err)
			}
		}

		if err == nil {
			if err := readFile(header.Hash().String(), ext, logger, r, ""); err != nil {
				return errors.NewBlockError("error reading block", err)
			} else {
				p.Printf("%s (%d): FOUND with %12d transactions\n", header.Hash(), meta.Height, meta.TxCount)
			}
		}

		header, meta, err = blockchain.GetBlockHeader(context.Background(), header.HashPrevBlock)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) {
				break
			}
			return errors.NewBlockError("error getting block header", err)
		}
	}

	return nil
}

func readFile(filename string, ext string, logger ulogger.Logger, r io.Reader, dir string) error {
	switch ext {
	case "utxodiff":
		utxodiff, err := model.NewUTXODiffFromReader(logger, r)
		if err != nil {
			return errors.NewProcessingError("error reading utxodiff", err)
		}

		fmt.Printf("UTXODiff block hash: %v\n", utxodiff.BlockHash)

		fmt.Printf("UTXODiff removed %d UTXOs", utxodiff.Removed.Length())
		if verbose {
			fmt.Println(":")
			utxodiff.Removed.Iter(func(uk model.UTXOKey, uv *model.UTXOValue) (stop bool) {
				fmt.Printf("%v %v\n", &uk, uv)
				return true
			})
		} else {
			fmt.Println()
		}

		fmt.Printf("UTXODiff added %d UTXOs", utxodiff.Added.Length())
		if verbose {
			fmt.Println(":")
			utxodiff.Added.Iter(func(uk model.UTXOKey, uv *model.UTXOValue) (stop bool) {
				fmt.Printf("%v %v\n", &uk, uv)
				return true
			})
		} else {
			fmt.Println()
		}

	case "utxo-set":
		var count int

		fmt.Printf("UTXOSet for block hash: %v\n", filename)

		for {
			ud, err := utxopersister.NewUTXOFromReader(r)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return errors.NewProcessingError("error reading utxo-additions", err)
			}

			if verbose {
				fmt.Printf("%v\n", ud)
			}

			count++
		}

		fmt.Printf("\tset contains %d UTXOs\n\n", count)

	case "utxo-additions":
		var count int

		fmt.Printf("UTXOAdditions for block hash: %v\n", filename)

		for {
			ud, err := utxopersister.NewUTXOFromReader(r)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return errors.NewProcessingError("error reading utxo-additions", err)
			}

			if verbose {
				fmt.Printf("%v\n", ud)
			}

			count++
		}

		fmt.Printf("\tadded	 %d UTXOs\n\n", count)

	case "utxo-deletions":
		var count int

		fmt.Printf("UTXODeletions for block hash: %v\n", filename)

		for {
			ud, err := utxopersister.NewUTXODeletionFromReader(r)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return errors.NewProcessingError("error reading utxo-deletions", err)
			}

			if verbose {
				fmt.Printf("%v\n", ud)
			}

			count++
		}

		fmt.Printf("\tremoved %d UTXOs\n\n", count)

	case "utxoset":
		utxoSet, err := model.NewUTXOSetFromReader(logger, r)
		if err != nil {
			return errors.NewProcessingError("error reading utxoSet: %v\n", err)
		}

		fmt.Printf("UTXOSet block hash: %v\n", utxoSet.BlockHash)

		fmt.Printf("UTXOSet with %d UTXOs", utxoSet.Current.Length())
		if verbose {
			fmt.Println(":")
			utxoSet.Current.Iter(func(uk model.UTXOKey, uv *model.UTXOValue) (stop bool) {
				fmt.Printf("%v %v\n", &uk, uv)
				return true
			})
		} else {
			fmt.Println()
		}

	case "subtree":
		num := readSubtree(r, logger, verbose)
		fmt.Printf("Number of transactions: %d\n", num)

	case "":
		blockHeaderBytes := make([]byte, 80)
		// read the first 80 bytes as the block header
		if _, err := io.ReadFull(r, blockHeaderBytes); err != nil {
			return errors.NewBlockInvalidError("error reading block header", err)
		}

		// read the transaction count
		txCount, err := wire.ReadVarInt(r, 0)
		if err != nil {
			return errors.NewBlockInvalidError("error reading transaction count", err)
		}

		fmt.Printf("\t%d transactions\n", txCount)

	case "block":
		block, err := block_model.NewBlockFromReader(r)
		if err != nil {
			return errors.NewBlockError("error reading block: %v\n", err)
		}

		if verify {
			return nil
		}

		fmt.Printf("Block hash: %s\n", block.Hash())
		fmt.Printf("%s", block.Header.StringDump())
		fmt.Printf("Number of transactions: %d\n", block.TransactionCount)

		for _, subtree := range block.Subtrees {
			fmt.Printf("Subtree %s\n", subtree)

			if verbose {
				filename := filepath.Join(dir, fmt.Sprintf("%s.subtree", subtree.String()))
				_, _, _, stReader, err := getReader(filename, logger)
				if err != nil {
					return err
				}
				readSubtree(stReader, logger, verbose)
			}
		}

	default:
		return errors.NewProcessingError("unknown file type")
	}

	return nil
}

func usage(msg string) {
	if msg != "" {
		fmt.Printf("Error: %s\n\n", msg)
	}
	fmt.Printf("Usage: filereader [-verbose] <filename | hash>.[block | subtree | utxoset | utxodiff] | -verify [-old]\n\n")
	os.Exit(1)
}

func getReader(path string, logger ulogger.Logger) (string, string, string, io.Reader, error) {
	dir, file := filepath.Split(path)

	ext := filepath.Ext(file)
	if ext == "" || ext == "." {
		usage("file extension missing")
	}

	fileWithoutExtension := strings.TrimSuffix(file, ext)

	if ext[0] == '.' {
		ext = ext[1:]
	}

	hash, err := chainhash.NewHashFromStr(fileWithoutExtension)

	if dir == "" && err == nil {
		store := getBlockStore(logger)

		r, err := store.GetIoReader(context.Background(), hash[:], options.WithFileExtension(ext))
		if err != nil {
			return "", "", "", nil, errors.NewProcessingError("error getting reader from store", err)
		}

		return dir, fileWithoutExtension, ext, r, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return "", "", "", nil, errors.NewProcessingError("error opening file", err)
	}

	return dir, fileWithoutExtension, ext, f, nil
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

	blockStore, err := blob.NewStore(logger, blockStoreUrl)
	if err != nil {
		panic(err)
	}

	return blockStore
}
