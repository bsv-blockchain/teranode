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

	"github.com/bitcoin-sv/teranode/errors"
	block_model "github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/pkg/go-wire"
	"github.com/bitcoin-sv/teranode/services/utxopersister"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

const stdin = "[stdin]"

const (
	numTransactionsFormat     = "Number of transactions: %d\n"
	coinbasePlaceholderFormat = "%10d: Coinbase Placeholder\n"
	nodeFormat                = "%10d: %v\n"
	fileTypeFormat            = "file type:                 %s\n"
	blockHashFormat           = "block hash:                %s\n"
	blockHeightFormat         = "block height:              %d\n"
	previousBlockHashFormat   = "previous block hash:       %s\n"
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

func FileReader(logger ulogger.Logger, tSettings *settings.Settings, verboseInput bool, checkHeightsInput bool, useStoreInput bool, path string) {
	verbose = verboseInput
	checkHeights = checkHeightsInput
	useStore = useStoreInput

	ctx := context.Background()

	fmt.Println()

	// Wrap the reader with a buffered reader
	// read the transaction count
	if err := ProcessFile(ctx, path, logger, tSettings); err != nil {
		fmt.Printf("error processing file: %v\n", err)
		os.Exit(1)
	}

	os.Exit(0)
}

func ProcessFile(ctx context.Context, path string, logger ulogger.Logger, tSettings *settings.Settings) error {
	if path == "" {
		return errors.NewProcessingError("no file specified")
	}

	dir, filename, ext, r, err := getReader(path, logger, tSettings)
	if err != nil {
		return errors.NewProcessingError("error getting reader", err)
	}

	logger.Infof("Reading file %s\n", path)

	if err := readFile(ctx, filename, ext, logger, tSettings, r, dir); err != nil {
		return errors.NewProcessingError("error reading file", err)
	}

	return nil
}

func readFile(ctx context.Context, filename string, ext fileformat.FileType, logger ulogger.Logger,
	tSettings *settings.Settings, r io.Reader, dir string) error {
	br := bufio.NewReaderSize(r, 1024*1024)

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
		return handleUtxoHeaders(br)

	case fileformat.FileTypeUtxoDeletions:
		return handleUtxoDeletions(br)

	case fileformat.FileTypeSubtree:
		return handleSubtree(br)

	case fileformat.FileTypeSubtreeData:
		return handleSubtreeData(br, logger, tSettings, dir, filename)

	case fileformat.FileTypeSubtreeMeta:
		return handleSubtreeMeta(br, logger, tSettings, dir, filename)

	case "":
		return handleBlockHeader(br)

	case fileformat.FileTypeBlock:
		return handleBlock(br, logger, tSettings, dir)

	default:
		return errors.NewProcessingError("unknown file type")
	}
}

// handleSubtreeData processes FileTypeSubtreeData files.
func handleSubtreeData(br *bufio.Reader, logger ulogger.Logger, tSettings *settings.Settings, dir, filename string) error {
	stPath := filepath.Join(dir, fmt.Sprintf("%s.subtree", filename))

	_, _, _, stReader, err := getReader(stPath, logger, tSettings)
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
			case util.IsCoinbasePlaceHolderTx(tx):
				fmt.Printf(coinbasePlaceholderFormat, i)
			default:
				fmt.Printf(nodeFormat, i, tx.TxIDChainHash())
			}
		}
	}

	return nil
}

// handleSubtreeMeta processes FileTypeSubtreeMeta files.
func handleSubtreeMeta(br *bufio.Reader, logger ulogger.Logger, tSettings *settings.Settings, dir, filename string) error {
	stPath := filepath.Join(dir, fmt.Sprintf("%s.%s", filename, fileformat.FileTypeSubtree))
	logger.Infof("Reading subtree from %s\n", stPath)

	_, _, _, stReader, err := getReader(stPath, logger, tSettings)
	if err != nil {
		return errors.NewProcessingError("Reading subtreeData files depends on subtree file", err)
	}

	st := &util.Subtree{}
	if err := st.DeserializeFromReader(stReader); err != nil {
		return errors.NewProcessingError("error reading subtree", err)
	}

	subtreeMeta, err := util.NewSubtreeMetaFromReader(st, br)
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
func handleBlock(br *bufio.Reader, logger ulogger.Logger, tSettings *settings.Settings, dir string) error {
	block, err := block_model.NewBlockFromReader(br, nil)
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

			_, _, _, stReader, err := getReader(filename, logger, tSettings)
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

	if err := binary.Read(br, binary.LittleEndian, &blockHeight); err != nil {
		return errors.NewProcessingError("error reading header number", err)
	}

	fmt.Printf(blockHeightFormat, blockHeight)

	if _, err := io.ReadFull(br, b); err != nil {
		return errors.NewProcessingError("error reading previous block hash", err)
	}

	previousBlockHash, err := chainhash.NewHash(b)
	if err != nil {
		return errors.NewProcessingError("error parsing previous block hash", err)
	}

	fmt.Printf(previousBlockHashFormat, previousBlockHash)

	var count int

	if verbose {
		for {
			ud, err := utxopersister.NewUTXOWrapperFromReader(ctx, br)
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
	if err := binary.Read(br, binary.LittleEndian, &blockHeight); err != nil {
		return errors.NewProcessingError("error reading header number", err)
	}

	fmt.Printf(blockHeightFormat, blockHeight)

	var count int

	if verbose {
		for {
			ud, err := utxopersister.NewUTXOWrapperFromReader(ctx, br)
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
func handleUtxoHeaders(br *bufio.Reader) error {
	var (
		count      int
		lastHeight uint32
	)

	if verbose {
		for {
			uh, err := utxopersister.NewUTXOHeaderFromReader(br)
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

	if err := binary.Read(br, binary.LittleEndian, &blockHeight); err != nil {
		return errors.NewProcessingError("error reading header number", err)
	}

	fmt.Printf(blockHeightFormat, blockHeight)

	var count int

	if verbose {
		for {
			ud, err := utxopersister.NewUTXODeletionFromReader(br)
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
			if util.CoinbasePlaceholderHash.IsEqual(&node.Hash) {
				fmt.Printf(coinbasePlaceholderFormat, i)
			} else {
				fmt.Printf(nodeFormat, i, node.Hash)
			}
		}
	}

	return nil
}

func getReader(path string, logger ulogger.Logger, tSettings *settings.Settings) (string, string, fileformat.FileType, io.Reader, error) {
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

		store := getBlockStore(logger, tSettings)

		r, err := store.GetIoReader(context.Background(), hash[:], fileformat.FileType(ext))
		if err != nil {
			return "", "", "", nil, errors.NewProcessingError("error getting reader from store", err)
		}

		return dir, fileWithoutExtension, fileformat.FileType(ext), r, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return "", "", "", nil, errors.NewProcessingError("error opening file", err)
	}

	return dir, fileWithoutExtension, fileformat.FileType(ext), f, nil
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

			if util.IsCoinbasePlaceHolderTx(&tx) {
				fmt.Printf(coinbasePlaceholderFormat, i)
			} else {
				fmt.Printf(nodeFormat, i, tx.TxIDChainHash())
			}
		}
	}

	return num
}

func getBlockStore(logger ulogger.Logger, tSettings *settings.Settings) blob.Store {
	blockStoreURL := tSettings.Block.BlockStore
	if blockStoreURL == nil {
		panic("blockstore config not found")
	}

	blockStore, err := blob.NewStore(logger, blockStoreURL)
	if err != nil {
		panic(err)
	}

	return blockStore
}
