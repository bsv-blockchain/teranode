package filereader

import (
	"bufio"
	"bytes"
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

const stdin = "[stdin]"

var (
	verbose bool
	verify  bool
	old     bool
)

func Start() {
	logger := ulogger.TestLogger{}

	fmt.Println()

	// Define command line arguments
	flag.BoolVar(&verbose, "verbose", false, "verbose output")
	flag.BoolVar(&verify, "verify", false, "verify all stored data")
	flag.BoolVar(&old, "old", false, "old format")

	flag.Usage = func() { usage("") }

	flag.Parse()

	if verify {
		if err := verifyChain(); err != nil {
			fmt.Printf("error verifying: %v\n", err)
			os.Exit(1)
		}

		os.Exit(0)
	}

	var path string // If no path is provided, read from stdin
	if len(flag.Args()) == 1 {
		path = flag.Arg(0)
	}

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
	br := bufio.NewReaderSize(r, 1024*1024)

	if filename == stdin {
		magic, blockHash, blockHeight, err := utxopersister.GetHeaderFromReader(br)
		if err != nil {
			return errors.NewProcessingError("error reading header", err)
		}

		fmt.Printf("magic:                  %s\n", magic)
		fmt.Printf("block hash:             %s\n", blockHash)
		fmt.Printf("block height:           %d\n", blockHeight)

		switch magic {
		case "U-A-1.0":
			ext = "utxo-additions"
		case "U-D-1.0":
			ext = "utxo-deletions"
		case "U-H-1.0":
			ext = "utxo-headers"
		case "U-S-1.0":
			ext = "utxo-set"
			// Read the previous block hash
			b := make([]byte, 32)
			if _, err := io.ReadFull(br, b); err != nil {
				return errors.NewProcessingError("error reading previous block hash", err)
			}

			previousBlockHash, err := chainhash.NewHash(b)
			if err != nil {
				return errors.NewProcessingError("error parsing previous block hash", err)
			}

			fmt.Printf("previous block hash:    %s\n", previousBlockHash)

		default:
			return errors.NewProcessingError("unknown file type")
		}

		fmt.Println()
	}

	switch ext {
	case "utxo-set":
		var count int

		if filename != stdin {
			magic, blockHash, blockHeight, previousBlockHash, err := utxopersister.GetUTXOSetHeaderFromReader(br)

			if err != nil {
				return errors.NewProcessingError("error reading utxo-additions header", err)
			}

			fmt.Printf("magic:                  %s\n", magic)
			fmt.Printf("block hash:             %s\n", blockHash)
			fmt.Printf("block height:           %d\n", blockHeight)
			fmt.Printf("previous block hash:    %s\n", previousBlockHash)
			fmt.Println()
		}

		if verbose {
			for {
				ud, err := utxopersister.NewUTXOWrapperFromReader(br)
				if err != nil {
					if errors.Is(err, io.EOF) {
						if ud == nil {
							fmt.Printf("ERROR: EOF marker not found\n")
						} else {
							fmt.Printf("EOF marker found\n")
						}

						break
					}

					return errors.NewProcessingError("error reading utxo-additions", err)
				}

				fmt.Printf("%v\n", ud)

				count += len(ud.UTXOs)
			}

			fmt.Printf("\tset contains %d UTXO(s)\n\n", count)
		}

		if filename != stdin {
			if err := printFooter(r, "utxo"); err != nil {
				return errors.NewProcessingError("Couldn't read footer", err)
			}
		}

	case "utxo-additions":
		var count int

		if filename != stdin {
			magic, blockHash, blockHeight, err := utxopersister.GetHeaderFromReader(br)
			if err != nil {
				return errors.NewProcessingError("error reading utxo-additions header", err)
			}

			fmt.Printf("magic:                        %s\n", magic)
			fmt.Printf("block hash:                   %s\n", blockHash)
			fmt.Printf("block height:                 %d\n", blockHeight)
			fmt.Println()
		}

		if verbose {
			for {
				ud, err := utxopersister.NewUTXOWrapperFromReader(br)
				if err != nil {
					if errors.Is(err, io.EOF) {
						if ud == nil {
							fmt.Printf("ERROR: EOF marker not found\n")
						} else {
							fmt.Printf("EOF marker found\n")
						}

						break
					}

					return errors.NewProcessingError("error reading utxo-additions", err)
				}

				fmt.Printf("%v\n", ud)

				count += len(ud.UTXOs)
			}

			fmt.Printf("\tadded	 %d UTXO(s)\n\n", count)
		}

		if filename != stdin {
			if err := printFooter(r, "utxo"); err != nil {
				return errors.NewProcessingError("Couldn't read footer", err)
			}
		}

	case "utxo-headers":
		var count int

		if filename != stdin {
			magic, blockHash, blockHeight, err := utxopersister.GetHeaderFromReader(br)

			if err != nil {
				return errors.NewProcessingError("error reading utxo-headers header", err)
			}

			fmt.Printf("magic:                        %s\n", magic)
			fmt.Printf("block hash:                   %s\n", blockHash)
			fmt.Printf("block height:                 %d\n", blockHeight)
			fmt.Println()
		}

		var lastHeight uint32

		if verbose {
			for {
				uh, err := utxopersister.NewUTXOHeaderFromReader(br)
				if err != nil {
					if errors.Is(err, io.EOF) {
						if uh == nil {
							fmt.Printf("ERROR: EOF marker not found\n")
						} else {
							fmt.Printf("EOF marker found\n")
						}

						break
					}

					return errors.NewProcessingError("error reading utxo-headers", err)
				}

				fmt.Printf("%v\n", uh)

				if verify {
					if lastHeight != 0 && lastHeight != uh.Height-1 {
						fmt.Printf("height mismatch: %d -> %d\n", lastHeight, uh.Height)
					}

					lastHeight = uh.Height
				}

				count++
			}

			fmt.Printf("\tread %d Header(s)\n\n", count)
		}

		if filename != stdin {
			if err := printFooter(r, "tx"); err != nil {
				return errors.NewProcessingError("Couldn't read footer", err)
			}
		}

	case "utxo-deletions":
		var count int

		if filename != stdin {
			magic, blockHash, blockHeight, err := utxopersister.GetHeaderFromReader(br)

			if err != nil {
				return errors.NewProcessingError("error reading utxo-deletions header", err)
			}

			fmt.Printf("magic:                        %s\n", magic)
			fmt.Printf("block hash:                   %s\n", blockHash)
			fmt.Printf("block height:                 %d\n", blockHeight)
			fmt.Println()
		}

		if verbose {
			for {
				ud, err := utxopersister.NewUTXODeletionFromReader(br)
				if err != nil {
					if errors.Is(err, io.EOF) {
						if ud == nil {
							fmt.Printf("ERROR: EOF marker not found\n")
						} else {
							fmt.Printf("EOF marker found\n")
						}

						break
					}

					return errors.NewProcessingError("error reading utxo-deletions", err)
				}

				fmt.Printf("%v\n", ud)

				count++
			}

			fmt.Printf("\tremoved %d UTXO(s)\n\n", count)
		}

		if filename != stdin {
			if err := printFooter(r, "utxo"); err != nil {
				return errors.NewProcessingError("Couldn't read footer", err)
			}
		}

	case "utxoset":
		utxoSet, err := model.NewUTXOSetFromReader(logger, br)
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
		num := readSubtree(br, verbose)
		fmt.Printf("Number of transactions: %d\n", num)

	case "":
		blockHeaderBytes := make([]byte, 80)
		// read the first 80 bytes as the block header
		if _, err := io.ReadFull(br, blockHeaderBytes); err != nil {
			return errors.NewBlockInvalidError("error reading block header", err)
		}

		// read the transaction count
		txCount, err := wire.ReadVarInt(br, 0)
		if err != nil {
			return errors.NewBlockInvalidError("error reading transaction count", err)
		}

		fmt.Printf("\t%d transactions\n", txCount)

	case "block":
		block, err := block_model.NewBlockFromReader(br)
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

				readSubtree(stReader, verbose)
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
	if path == "" {
		// Handle stdin
		return "", stdin, "", os.Stdin, nil
	}

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

func readSubtree(r io.Reader, verbose bool) uint32 {
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
	blockStoreURL, err, found := gocore.Config().GetURL("blockstore")
	if err != nil {
		panic(err)
	}

	if !found {
		panic("blockstore config not found")
	}

	blockStore, err := blob.NewStore(logger, blockStoreURL)
	if err != nil {
		panic(err)
	}

	return blockStore
}

func printFooter(r io.Reader, label string) error {
	f, ok := r.(*os.File)
	if !ok {
		fmt.Printf("seek is not supported\n")
		return nil
	}

	// The end of the file should have the EOF marker at the end (32 bytes)
	// and the txCount uint64 and the utxoCount uint64 (each 8 bytes)
	_, err := f.Seek(-48, io.SeekEnd)
	if err != nil {
		return errors.NewProcessingError("error seeking to EOF marker", err)
	}

	b := make([]byte, 48)
	if _, err := io.ReadFull(f, b); err != nil {
		return errors.NewProcessingError("error reading EOF marker", err)
	}

	if !bytes.Equal(b[0:32], utxopersister.EOFMarker) {
		return errors.NewProcessingError("EOF marker not found")
	}

	fmt.Printf("EOF marker found\n")

	txCount := binary.LittleEndian.Uint64(b[32:40])
	utxoCount := binary.LittleEndian.Uint64(b[40:48])

	fmt.Printf("record count: %16s\n", formatNumber(txCount))
	fmt.Printf("%s count:   %16s\n", label, formatNumber(utxoCount))

	return nil
}

func formatNumber(n uint64) string {
	in := fmt.Sprintf("%d", n)
	out := make([]string, 0, len(in)+(len(in)-1)/3)

	for i, c := range in {
		if i > 0 && (len(in)-i)%3 == 0 {
			out = append(out, ",")
		}

		out = append(out, string(c))
	}

	return strings.Join(out, "")
}
