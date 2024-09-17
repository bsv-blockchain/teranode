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
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/services/utxopersister"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

const stdin = "[stdin]"

const (
	numTransactionsFormat     = "Number of transactions: %d\n"
	coinbasePlaceholderFormat = "%10d: Coinbase Placeholder\n"
	nodeFormat                = "%10d: %v\n"
	errEOFMarkerNotFound      = "ERROR: EOF marker not found\n"
	eofMarkerFound            = "EOF marker found\n"
	errCouldNotReadFooter     = "Could not read footer"
	magicFormat               = "magic:                        %s\n"
	blockHashFormat           = "block hash:                   %s\n"
	blockHeightFormat         = "block height:                 %d\n"
)

var (
	verbose      bool
	checkHeights bool
	useStore     bool
)

func usage(msg string) {
	if msg != "" {
		fmt.Printf("Error: %s\n\n", msg)
	}

	fmt.Printf("Usage: filereader [-verbose] [-checkHeights] [-useStore] <filename | hash>.[block | subtree | subtreeData | utxo-set | utxo-additions | utxo-deletions | utxo-headers]\n\n")

	os.Exit(1)
}

func Start() {
	logger := ulogger.TestLogger{}

	fmt.Println()

	// Define command line arguments
	flag.BoolVar(&verbose, "verbose", false, "verbose output")
	flag.BoolVar(&checkHeights, "checkHeights", false, "check heights in utxo headers")
	flag.BoolVar(&useStore, "useStore", false, "use store")

	flag.Usage = func() { usage("") }

	flag.Parse()

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

func readFile(filename string, ext string, logger ulogger.Logger, r io.Reader, dir string) error {
	br := bufio.NewReaderSize(r, 1024*1024)

	if filename == stdin {
		magic, blockHash, blockHeight, err := utxopersister.GetHeaderFromReader(br)
		if err != nil {
			return errors.NewProcessingError("error reading header", err)
		}

		fmt.Printf(magicFormat, magic)
		fmt.Printf(blockHashFormat, blockHash)
		fmt.Printf(blockHeightFormat, blockHeight)

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
				return errors.NewProcessingError("error reading utxo-set header", err)
			}

			fmt.Printf(magicFormat, magic)
			fmt.Printf(blockHashFormat, blockHash)
			fmt.Printf(blockHeightFormat, blockHeight)
			fmt.Printf("previous block hash:    %s\n", previousBlockHash)
			fmt.Println()
		}

		if verbose {
			for {
				ud, err := utxopersister.NewUTXOWrapperFromReader(br)
				if err != nil {
					if errors.Is(err, io.EOF) {
						if ud == nil {
							fmt.Print(errEOFMarkerNotFound)
						} else {
							fmt.Print(eofMarkerFound)
						}

						break
					}

					return errors.NewProcessingError("error reading utxo-set", err)
				}

				fmt.Printf("%v\n", ud)

				count += len(ud.UTXOs)
			}

			fmt.Printf("\tset contains %d UTXO(s)\n\n", count)
		}

		if filename != stdin {
			if err := printFooter(r, "utxo"); err != nil {
				return errors.NewProcessingError(errCouldNotReadFooter, err)
			}
		}

	case "utxo-additions":
		var count int

		if filename != stdin {
			magic, blockHash, blockHeight, err := utxopersister.GetHeaderFromReader(br)
			if err != nil {
				return errors.NewProcessingError("error reading utxo-additions header", err)
			}

			fmt.Printf(magicFormat, magic)
			fmt.Printf(blockHashFormat, blockHash)
			fmt.Printf(blockHeightFormat, blockHeight)
			fmt.Println()
		}

		if verbose {
			for {
				ud, err := utxopersister.NewUTXOWrapperFromReader(br)
				if err != nil {
					if errors.Is(err, io.EOF) {
						if ud == nil {
							fmt.Print(errEOFMarkerNotFound)
						} else {
							fmt.Print(eofMarkerFound)
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
				return errors.NewProcessingError(errCouldNotReadFooter, err)
			}
		}

	case "utxo-headers":
		var count int

		if filename != stdin {
			magic, blockHash, blockHeight, err := utxopersister.GetHeaderFromReader(br)

			if err != nil {
				return errors.NewProcessingError("error reading utxo-headers header", err)
			}

			fmt.Printf(magicFormat, magic)
			fmt.Printf(blockHashFormat, blockHash)
			fmt.Printf(blockHeightFormat, blockHeight)
			fmt.Println()
		}

		var lastHeight uint32

		if verbose {
			for {
				uh, err := utxopersister.NewUTXOHeaderFromReader(br)
				if err != nil {
					if errors.Is(err, io.EOF) {
						if uh == nil {
							fmt.Print(errEOFMarkerNotFound)
						} else {
							fmt.Print(eofMarkerFound)
						}

						break
					}

					return errors.NewProcessingError("error reading utxo-headers", err)
				}

				fmt.Printf("%v\n", uh)

				if checkHeights {
					if lastHeight != 0 && lastHeight != uh.Height-1 {
						fmt.Printf("height mismatch: %d -> %d\n", lastHeight, uh.Height)
					}

					lastHeight = uh.Height
				}

				count++
			}

			fmt.Printf("\tread %d header(s)\n\n", count)
		}

		if filename != stdin {
			if err := printFooter(r, "tx"); err != nil {
				return errors.NewProcessingError(errCouldNotReadFooter, err)
			}
		}

	case "utxo-deletions":
		var count int

		if filename != stdin {
			magic, blockHash, blockHeight, err := utxopersister.GetHeaderFromReader(br)

			if err != nil {
				return errors.NewProcessingError("error reading utxo-deletions header", err)
			}

			fmt.Printf(magicFormat, magic)
			fmt.Printf(blockHashFormat, blockHash)
			fmt.Printf(blockHeightFormat, blockHeight)
			fmt.Println()
		}

		if verbose {
			for {
				ud, err := utxopersister.NewUTXODeletionFromReader(br)
				if err != nil {
					if errors.Is(err, io.EOF) {
						if ud == nil {
							fmt.Print(errEOFMarkerNotFound)
						} else {
							fmt.Print(eofMarkerFound)
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
				return errors.NewProcessingError(errCouldNotReadFooter, err)
			}
		}

	case "subtree":
		st := &util.Subtree{}
		if err := st.DeserializeFromReader(br); err != nil {
			return errors.NewProcessingError("error reading subtree", err)
		}

		fmt.Printf("Subtree root hash: %s\n", st.RootHash())
		fmt.Printf("Subtree fees: %d\n", st.Fees)
		fmt.Printf("Subtree height: %d\n", st.Height)
		fmt.Printf("Subtree size: %d\n", st.SizeInBytes)
		fmt.Printf("Subtree conflicting nodes: %v\n", st.ConflictingNodes)

		fmt.Printf(numTransactionsFormat, st.Length())

		if verbose {
			for i, node := range st.Nodes {
				if block_model.CoinbasePlaceholderHash.IsEqual(&node.Hash) {
					fmt.Printf(coinbasePlaceholderFormat, i)
				} else {
					fmt.Printf(nodeFormat, i, node.Hash)
				}
			}
		}

	case "subtreeData":
		// The subtreeData deserialization needs the subtree first
		stPath := filepath.Join(dir, fmt.Sprintf("%s.subtree", filename))

		_, _, _, stReader, err := getReader(stPath, logger)
		if err != nil {
			return errors.NewProcessingError("Reading subtreeData files depends on subtree file", err)
		}

		st := &util.Subtree{}
		if err := st.DeserializeFromReader(stReader); err != nil {
			return errors.NewProcessingError("error reading subtree", err)
		}

		sd, err := util.NewSubtreeDataFromReader(st, br)
		if err != nil {
			return errors.NewProcessingError("error reading subtree data", err)
		}

		fmt.Printf("Subtree root hash: %s\n", st.RootHash())
		fmt.Printf("Subtree fees: %d\n", st.Fees)
		fmt.Printf("Subtree height: %d\n", st.Height)
		fmt.Printf("Subtree size: %d\n", st.SizeInBytes)
		fmt.Printf("Subtree conflicting nodes: %v\n", st.ConflictingNodes)

		fmt.Printf(numTransactionsFormat, st.Length())

		if verbose {
			for i, tx := range sd.Txs {
				switch {
				case tx == nil:
					fmt.Printf("%10d: <nil>\n", i)
				case block_model.IsCoinbasePlaceHolderTx(tx):
					fmt.Printf(coinbasePlaceholderFormat, i)
				default:
					fmt.Printf(nodeFormat, i, tx.TxIDChainHash())
				}
			}
		}

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
			return errors.NewBlockError("error reading block", err)
		}

		fmt.Printf("Block hash: %s\n", block.Hash())
		fmt.Printf("%s", block.Header.StringDump())
		fmt.Printf(numTransactionsFormat, block.TransactionCount)

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

	if useStore {
		hash, err := chainhash.NewHashFromStr(fileWithoutExtension)
		if err != nil {
			return "", "", "", nil, errors.NewProcessingError("error parsing hash", err)
		}

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
				fmt.Printf(coinbasePlaceholderFormat, i)
			} else {
				fmt.Printf(nodeFormat, i, tx.TxIDChainHash())
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
