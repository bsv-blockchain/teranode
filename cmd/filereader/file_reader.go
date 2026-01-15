// Package filereader provides utilities for reading and processing blockchain-related files.
// It is designed to handle various file formats, extract relevant data, and perform operations
// such as validation, transformation, and storage.
//
// Usage:
//
//    This package is typically used as a command-line tool to read and process blockchain files
//    for debugging, analysis, or integration with other services.
//
// Functions:
//   - ReadFile: Reads and processes a blockchain file, extracting transactions, and metadata.
//
// Side effects:
//
//    Functions in this package may print to stdout, log errors, and interact with external
//    storage systems or services.

package filereader

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	blockmodel "github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
)

const (
	blockHashFormat           = "block hash:                %s\n" // format for block hash
	blockHeightFormat         = "block height:              %d\n" // format for block height
	coinbasePlaceholderFormat = "%10d: Coinbase Placeholder\n"    // format for coinbase placeholder transactions
	fileTypeFormat            = "file type:                 %s\n" // format for showing a file type
	nodeFormat                = "%10d: %v\n"                      // format for node information
	numTransactionsFormat     = "Number of transactions: %d\n"    // format for number of transactions
	previousBlockHashFormat   = "previous block hash:       %s\n" // format for previous block hash
	stdin                     = "[stdin]"                         // constant for stdin input
)

var (
	checkHeights bool
	useStore     bool
	verbose      bool
)

// usage prints the usage message and exits the program with an error code.
func usage(msg string) {
	if msg != "" {
		fmt.Printf("Error: %s\n\n", msg)
	}

	fmt.Printf("Usage: filereader [-verbose] [-checkHeights] [-useStore] <filename | hash>.[block | subtree | subtreeData | utxo-set | utxo-additions | utxo-deletions | utxo-headers]\n\n")

	os.Exit(1)
}

// ReadAndProcessFile is the main entry point for reading and processing blockchain files.
//
// This function initializes the necessary settings and context, then processes the specified
// file by delegating to the appropriate handler based on the file type. It supports various
// blockchain-related file formats, such as UTXO sets, block headers, and subtree data.
//
// Parameters:
//   - logger: A logger instance for logging messages and errors.
//   - settings: Application settings for configuring file processing.
//   - verboseInput: A boolean flag to enable verbose output.
//   - checkHeightsInput: A boolean flag to enable height consistency checks.
//   - useStoreInput: A boolean flag to enable reading files from an external store.
//   - path: The file path or hash to be processed.
//
// Side effects:
//   - Prints file processing details to stdout.
//   - Logs errors and informational messages.
//   - Exits the process with a non-zero status code in case of errors.
func ReadAndProcessFile(logger ulogger.Logger, settings *settings.Settings, verboseInput bool, checkHeightsInput bool,
	useStoreInput bool, path string) {
	verbose = verboseInput
	checkHeights = checkHeightsInput
	useStore = useStoreInput

	ctx := context.Background()

	fmt.Println()

	// Wrap the reader with a buffered reader
	// read the transaction count
	if err := ProcessFile(ctx, path, logger, settings); err != nil {
		logger.Fatalf("Error processing file: %v\n", err)
		os.Exit(1)
	}

	os.Exit(0)
}

// ProcessFile processes the specified file, reading its contents and handling different file types.
//
// This function determines the file type based on its extension or header and delegates
// processing to the appropriate handler. It supports various blockchain-related file formats,
// such as UTXO sets, block headers, and subtree data.
//
// Parameters:
//   - ctx: The context for managing deadlines and cancellations.
//   - path: The file path or hash to be processed.
//   - logger: A logger instance for logging messages and errors.
//   - settings: Application settings for configuring file processing.
//
// Returns:
//   - An error if the file cannot be processed or if an issue occurs during processing.
//
// Side effects:
//   - Logs file processing details and errors.
//   - Reads file contents and interacts with external storage systems if necessary.
func ProcessFile(ctx context.Context, path string, logger ulogger.Logger, settings *settings.Settings) error {
	if path == "" {
		return errors.NewProcessingError("no file specified")
	}

	dir, filename, ext, r, err := getReader(path, logger, settings)
	if err != nil {
		return errors.NewProcessingError("error getting reader", err)
	}

	logger.Infof("Reading file %s\n", path)

	if err = readFile(ctx, filename, ext, logger, settings, r, dir); err != nil {
		return errors.NewProcessingError("error reading file", err)
	}

	return nil
}

// readFile reads the contents of a file and processes it based on its type.
func readFile(ctx context.Context, filename string, ext fileformat.FileType, logger ulogger.Logger,
	settings *settings.Settings, r io.Reader, dir string) error {
	br := bufio.NewReaderSize(r, 1024*128)

	// Read the header
	header, err := fileformat.ReadHeader(br)
	if err != nil {
		return errors.NewProcessingError("error reading header", err)
	}

	if filename != stdin {
		ext = header.FileType()
	}

	fmt.Printf(fileTypeFormat, header.FileType())

	fmt.Println()

	switch ext {
	case fileformat.FileTypeUtxoSet:
		return handleUtxoSet(ctx, br)

	case fileformat.FileTypeUtxoAdditions:
		return handleUtxoAdditions(ctx, br)

	case fileformat.FileTypeUtxoHeaders:
		return handleUtxoHeaders(header, br)

	case fileformat.FileTypeUtxoDeletions:
		return handleUtxoDeletions(br)

	case fileformat.FileTypeSubtree:
		return handleSubtree(br)

	case fileformat.FileTypeSubtreeToCheck:
		return handleSubtree(br)

	case fileformat.FileTypeSubtreeData:
		return handleSubtreeData(br, logger, settings, dir, filename)

	case fileformat.FileTypeSubtreeMeta:
		return handleSubtreeMeta(br, logger, settings, dir, filename)

	case "":
		return handleBlockHeader(br)

	case fileformat.FileTypeBlock:
		return handleBlock(br, logger, settings, dir)

	default:
		return errors.NewProcessingError("unknown file type")
	}
}

// handleSubtreeData processes FileTypeSubtreeData files.
func handleSubtreeData(br *bufio.Reader, logger ulogger.Logger, settings *settings.Settings, dir, filename string) error {
	stPath := filepath.Join(dir, fmt.Sprintf("%s.subtree", filename))

	_, _, _, stReader, err := getReader(stPath, logger, settings)
	if err != nil {
		return errors.NewProcessingError("reading subtreeData files depends on subtree file", err)
	}

	st := &subtree.Subtree{}
	if err = st.DeserializeFromReader(stReader); err != nil {
		return errors.NewProcessingError("error reading subtree", err)
	}

	var sd *subtree.Data

	sd, err = subtree.NewSubtreeDataFromReader(st, br)
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
			case subtree.IsCoinbasePlaceHolderTx(tx):
				fmt.Printf(coinbasePlaceholderFormat, i)
			default:
				fmt.Printf(nodeFormat, i, tx.TxIDChainHash())
			}
		}
	}

	return nil
}

// handleSubtreeMeta processes FileTypeSubtreeMeta files.
func handleSubtreeMeta(br *bufio.Reader, logger ulogger.Logger, settings *settings.Settings, dir, filename string) error {
	stPath := filepath.Join(dir, fmt.Sprintf("%s.%s", filename, fileformat.FileTypeSubtree))
	logger.Infof("Reading subtree from %s\n", stPath)

	_, _, _, stReader, err := getReader(stPath, logger, settings)
	if err != nil {
		return errors.NewProcessingError("reading subtreeData files depends on subtree file", err)
	}

	st := &subtree.Subtree{}

	if err = st.DeserializeFromReader(stReader); err != nil {
		return errors.NewProcessingError("error reading subtree", err)
	}

	var subtreeMeta *subtree.Meta

	subtreeMeta, err = subtree.NewSubtreeMetaFromReader(st, br)
	if err != nil {
		return errors.NewProcessingError("error reading subtree meta", err)
	}

	fmt.Printf("Subtree root hash: %s\n", st.RootHash())

	fmt.Printf(numTransactionsFormat, st.Length())

	if verbose {
		for i, txInpoint := range subtreeMeta.TxInpoints {
			switch {
			case txInpoint.ParentTxHashes == nil:
				fmt.Printf("%10d: <nil>\n", i)
			default:
				fmt.Printf(nodeFormat, i, txInpoint.ParentTxHashes)
			}
		}
	}

	return nil
}

// handleBlockHeader processes the unnamed block header case.
func handleBlockHeader(br *bufio.Reader) error {
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

	return nil
}

// handleBlock processes FileTypeBlock files.
func handleBlock(br *bufio.Reader, logger ulogger.Logger, settings *settings.Settings, dir string) error {
	block, err := blockmodel.NewBlockFromReader(br)
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

			var stReader io.Reader

			_, _, _, stReader, err = getReader(filename, logger, settings)
			if err != nil {
				return err
			}

			readSubtree(stReader, verbose)
		}
	}

	return nil
}

// handleUtxoSet processes FileTypeUtxoSet files.
func handleUtxoSet(ctx context.Context, br *bufio.Reader) error {
	// Read the previous block hash
	b := make([]byte, 32)

	if _, err := io.ReadFull(br, b); err != nil {
		return errors.NewProcessingError("error reading previous block hash", err)
	}

	blockHash, err := chainhash.NewHash(b)
	if err != nil {
		return errors.NewProcessingError("error parsing previous block hash", err)
	}

	fmt.Printf(blockHashFormat, blockHash)

	var blockHeight uint32

	if err = binary.Read(br, binary.LittleEndian, &blockHeight); err != nil {
		return errors.NewProcessingError("error reading header number", err)
	}

	fmt.Printf(blockHeightFormat, blockHeight)

	if _, err = io.ReadFull(br, b); err != nil {
		return errors.NewProcessingError("error reading previous block hash", err)
	}

	var previousBlockHash *chainhash.Hash

	previousBlockHash, err = chainhash.NewHash(b)
	if err != nil {
		return errors.NewProcessingError("error parsing previous block hash", err)
	}

	fmt.Printf(previousBlockHashFormat, previousBlockHash)

	var count int

	if verbose {
		for {
			var ud *utxopersister.UTXOWrapper

			ud, err = utxopersister.NewUTXOWrapperFromReader(ctx, br)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				return errors.NewProcessingError("error reading utxo-set", err)
			}

			fmt.Printf("%v\n", ud)

			count += len(ud.UTXOs)
		}

		fmt.Printf("\tset contains %d UTXO(s)\n\n", count)
	}

	return nil
}

// handleUtxoAdditions processes FileTypeUtxoAdditions files.
func handleUtxoAdditions(ctx context.Context, br *bufio.Reader) error {
	b := make([]byte, 32)
	if _, err := io.ReadFull(br, b); err != nil {
		return errors.NewProcessingError("error reading block hash", err)
	}

	blockHash, err := chainhash.NewHash(b)
	if err != nil {
		return errors.NewProcessingError("error parsing block hash", err)
	}

	fmt.Printf(blockHashFormat, blockHash)

	var blockHeight uint32
	if err = binary.Read(br, binary.LittleEndian, &blockHeight); err != nil {
		return errors.NewProcessingError("error reading header number", err)
	}

	fmt.Printf(blockHeightFormat, blockHeight)

	var count int

	if verbose {
		for {
			var ud *utxopersister.UTXOWrapper

			ud, err = utxopersister.NewUTXOWrapperFromReader(ctx, br)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				return errors.NewProcessingError("error reading utxo-additions", err)
			}

			fmt.Printf("%v\n", ud)

			count += len(ud.UTXOs)
		}

		fmt.Printf("\tadded\t %d UTXO(s)\n\n", count)
	}

	return nil
}

// handleUtxoHeaders processes FileTypeUtxoHeaders files.
func handleUtxoHeaders(header fileformat.Header, br *bufio.Reader) error {
	var (
		count      int
		lastHeight uint32
	)

	// Determine if this is V1 (without coinbase) or V2 (with coinbase)
	isV1 := header.IsUtxoHeadersV1()
	if isV1 {
		fmt.Printf("Reading V1 utxo-headers (without coinbase transactions)\n")
	} else {
		fmt.Printf("Reading V2 utxo-headers (with coinbase transactions)\n")
	}

	// Read the last block hash and height that's written after the file header
	var (
		hash   chainhash.Hash
		height uint32
	)

	if err := binary.Read(br, binary.LittleEndian, &hash); err != nil {
		return errors.NewProcessingError("failed to read last block hash", err)
	}

	if err := binary.Read(br, binary.LittleEndian, &height); err != nil {
		return errors.NewProcessingError("failed to read last block height", err)
	}

	if verbose {
		fmt.Printf("Last block hash: %s, height: %d\n", hash.String(), height)

		for {
			uh, err := utxopersister.NewUTXOHeaderFromReader(br, isV1)
			if err != nil {
				if errors.Is(err, io.EOF) {
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

	return nil
}

// handleUtxoDeletions processes FileTypeUtxoDeletions files.
func handleUtxoDeletions(br *bufio.Reader) error {
	b := make([]byte, 32)
	if _, err := io.ReadFull(br, b); err != nil {
		return errors.NewProcessingError("error reading block hash", err)
	}

	blockHash, err := chainhash.NewHash(b)
	if err != nil {
		return errors.NewProcessingError("error parsing block hash", err)
	}

	fmt.Printf(blockHashFormat, blockHash)

	var blockHeight uint32

	if err = binary.Read(br, binary.LittleEndian, &blockHeight); err != nil {
		return errors.NewProcessingError("error reading header number", err)
	}

	fmt.Printf(blockHeightFormat, blockHeight)

	var count int

	if verbose {
		for {
			var ud *utxopersister.UTXODeletion

			ud, err = utxopersister.NewUTXODeletionFromReader(br)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				return errors.NewProcessingError("error reading utxo-deletions", err)
			}

			fmt.Printf("%v\n", ud)

			count++
		}

		fmt.Printf("\tremoved %d UTXO(s)\n\n", count)
	}

	return nil
}

// handleSubtree processes FileTypeSubtree files.
func handleSubtree(br *bufio.Reader) error {
	st := &subtree.Subtree{}
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
			if subtree.CoinbasePlaceholderHash.IsEqual(&node.Hash) {
				fmt.Printf(coinbasePlaceholderFormat, i)
			} else {
				fmt.Printf(nodeFormat, i, node.Hash)
			}
		}
	}

	return nil
}

// getReader returns an io.Reader for the specified path, handling both file and stdin cases.
func getReader(path string, logger ulogger.Logger, settings *settings.Settings) (string, string,
	fileformat.FileType, io.Reader, error) {
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

		store := getBlockStore(logger, settings)

		var storeReader io.Reader

		storeReader, err = store.GetIoReader(context.Background(), hash[:], fileformat.FileType(ext))
		if err != nil {
			return "", "", "", nil, errors.NewProcessingError("error getting reader from store", err)
		}

		return dir, fileWithoutExtension, fileformat.FileType(ext), storeReader, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return "", "", "", nil, errors.NewProcessingError("error opening file", err)
	}

	return dir, fileWithoutExtension, fileformat.FileType(ext), f, nil
}

// readSubtree reads a subtree from the provided reader and prints its transactions if verbose is enabled.
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

			if subtree.IsCoinbasePlaceHolderTx(&tx) {
				fmt.Printf(coinbasePlaceholderFormat, i)
			} else {
				fmt.Printf(nodeFormat, i, tx.TxIDChainHash())
			}
		}
	}

	return num
}

// getBlockStore initializes and returns a blob store for block data.
func getBlockStore(logger ulogger.Logger, settings *settings.Settings) blob.Store {
	blockStoreURL := settings.Block.BlockStore
	if blockStoreURL == nil {
		panic("blockstore config not found")
	}

	var err error

	hashPrefix := -2

	if blockStoreURL.Query().Get("hashPrefix") != "" {
		hashPrefix, err = strconv.Atoi(blockStoreURL.Query().Get("hashPrefix"))
		if err != nil {
			panic(err)
		}
	}

	blockStore, err := blob.NewStore(logger, blockStoreURL, options.WithHashPrefix(hashPrefix))
	if err != nil {
		panic(err)
	}

	return blockStore
}
