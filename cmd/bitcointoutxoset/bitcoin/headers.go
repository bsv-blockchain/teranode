package bitcoin

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/services/utxopersister"
	teranodeUtil "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/btcsuite/goleveldb/leveldb/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

// Block validation statuses
const (
	BlockValidReserved     = 1
	BlockValidTree         = 2
	BlockValidTransactions = 3
	BlockValidChain        = 4
	BlockValidScripts      = 5
	BlockValidMask         = BlockValidReserved | BlockValidTree | BlockValidTransactions | BlockValidChain | BlockValidScripts

	BlockHaveData = 8  // !< full block available in blk*.dat
	BlockHaveUndo = 16 // !< undo data available in rev*.dat
)

// DumpRecords prints the first `count` records from the index database.
//
// Usage:
//
// This function is used for debugging or inspection purposes to print a limited number of records
// from the index database. It iterates over the database and prints the key, hash, and value of
// each record.
//
// Parameters:
//   - count: The maximum number of records to print.
//
// Side effects:
//
// This function outputs data to the standard output, which may include sensitive information
// depending on the database contents.
func (in *IndexDB) DumpRecords(count int) {
	// Iterate over the block headers in the LevelDB
	iter := in.db.NewIterator(util.BytesPrefix([]byte("b")), nil)
	defer iter.Release()

	var i int

	for iter.Next() {
		key := iter.Key()
		value := iter.Value()

		var hashStr string

		hash, err := chainhash.NewHash(key[1:])
		if err != nil {
			hashStr = err.Error()
		} else {
			hashStr = hash.String()
		}

		fmt.Printf("Key %d (%d): %x\n", i, len(key), key)
		fmt.Printf("Hash: %s\n", hashStr)
		fmt.Printf("Value (%d): %x\n\n", len(value), value)

		i++

		if i == count {
			break
		}
	}
}

// WriteHeadersToFile writes the block headers from the index database to a file.
//
// Usage:
//
// This function is used to export block headers from the index database to a file in a specified
// output directory. It sorts the blocks by height and writes them along with metadata to the file.
//
// Parameters:
//   - outputDir: The directory where the output file will be created.
//   - heightHint: An estimated number of blocks to optimize memory allocation.
//
// Returns:
//   - A pointer to the highest BlockIndex written to the file.
//   - An error if the operation fails, such as issues with file creation or writing.
//
// Side effects:
//
// This function interacts with the file system, creating and writing to files. It also calculates
// a SHA256 hash of the output file and writes it to a separate file for verification.
func (in *IndexDB) WriteHeadersToFile(outputDir string, heightHint int) (*utxopersister.BlockIndex, error) {
	// Slice to store block information
	blocks := make([]*utxopersister.BlockIndex, 0, heightHint)

	// Iterate over the block headers in the LevelDB
	iter := in.db.NewIterator(util.BytesPrefix([]byte("b")), nil)
	defer iter.Release()

	for iter.Next() {
		key := iter.Key()
		value := iter.Value()

		blockHash, err := chainhash.NewHash(key[1:])
		if err != nil {
			log.Printf("failed to parse block hash: %v", err)
			continue
		}

		var blockIndex *utxopersister.BlockIndex
		blockIndex, err = DeserializeBlockIndex(value)
		if err != nil {
			if !errors.Is(err, errors.ErrBlockInvalid) {
				log.Printf("failed to parse block index: %v", err)
			}

			continue
		}

		if blockIndex.TxCount == 0 {
			continue
		}

		blockIndex.Hash = blockHash

		blocks = append(blocks, blockIndex)
	}

	if err := iter.Error(); err != nil {
		return nil, err
	}

	// Sort the slice by block height
	sort.SliceStable(blocks, func(i, j int) bool {
		return blocks[i].Height < blocks[j].Height
	})

	// Now we are sorted, the last block in the slice is the highest
	bestBlock := blocks[len(blocks)-1]

	safeOutputDir := filepath.Clean(outputDir)

	// Convert safeOutputDir to an absolute path if it's not already
	if !filepath.IsAbs(safeOutputDir) {
		absDir, err := filepath.Abs(safeOutputDir)
		if err != nil {
			return nil, errors.NewProcessingError("invalid output directory", err)
		}

		safeOutputDir = absDir
	}

	// Verify that safeOutputDir exists
	info, err := os.Stat(safeOutputDir)
	if err != nil {
		return nil, errors.NewProcessingError("failed to access output directory", err)
	}

	// Verify that safeOutputDir is a directory
	if !info.IsDir() {
		return nil, errors.NewProcessingError("output path is not a directory")
	}

	// Create the output directory if it doesn't exist
	outFile := filepath.Join(safeOutputDir, bestBlock.Hash.String()+".utxo-headers")

	var file *os.File

	file, err = os.Create(outFile)
	if err != nil {
		return nil, err
	}

	// Ensure the file is closed properly
	defer func() {
		_ = file.Close()
	}()

	// Create a new SHA256 hasher
	hasher := sha256.New()

	bufferedWriter := bufio.NewWriter(io.MultiWriter(file, hasher))
	defer func() {
		_ = bufferedWriter.Flush()
	}()

	header := fileformat.NewHeader(fileformat.FileTypeUtxoHeaders)

	if err = header.Write(bufferedWriter); err != nil {
		return nil, errors.NewProcessingError("couldn't write header to file", err)
	}

	if _, err = bufferedWriter.Write(bestBlock.Hash[:]); err != nil {
		return nil, errors.NewProcessingError("couldn't write block header to file", err)
	}

	if err = binary.Write(bufferedWriter, binary.LittleEndian, bestBlock.Height); err != nil {
		return nil, errors.NewProcessingError("error writing header number", err)
	}

	var (
		recordCount uint64
		txCount     uint64
	)

	// Write each block's header to the file
	for _, block := range blocks {
		if err = block.Serialise(bufferedWriter); err != nil {
			return nil, errors.NewProcessingError("couldn't write header to file", err)
		}

		recordCount++
		txCount += block.TxCount
	}

	// Write the number of txs and utxos written
	b := make([]byte, 8)

	binary.LittleEndian.PutUint64(b, recordCount)

	if _, err = bufferedWriter.Write(b); err != nil {
		return nil, errors.NewProcessingError("couldn't write tx count", err)
	}

	binary.LittleEndian.PutUint64(b, txCount)

	if _, err = bufferedWriter.Write(b); err != nil {
		return nil, errors.NewProcessingError("couldn't write tx count", err)
	}

	if err = bufferedWriter.Flush(); err != nil {
		return nil, errors.NewProcessingError("couldn't flush buffer", err)
	}

	hashData := fmt.Sprintf("%x  %s\n", hasher.Sum(nil), bestBlock.Hash.String()+".utxo-headers") // N.B. The 2 spaces is important for the hash to be valid

	//nolint:gosec // G306: Expect WriteFile permissions to be 0600 or less (gosec)go-golangci-lint
	if err = os.WriteFile(outFile+".sha256", []byte(hashData), 0600); err != nil {
		return nil, errors.NewProcessingError("Couldn't write hash file", err)
	}

	return bestBlock, nil
}

// DeserializeBlockIndex deserializes a block index from the given byte slice.
//
// Usage:
//
// This function is used to parse and extract block index information from a serialized byte slice.
// It validates the block's status and ensures the block header is properly deserialized.
//
// Parameters:
//   - data: A byte slice containing the serialized block index data.
//
// Returns:
//   - A pointer to a BlockIndex struct containing the deserialized block index information.
//   - An error if the deserialization fails or the block is invalid.
//
// Side effects:
//
// This function performs validation checks on the block's status and may return errors if the block
// is not in the active chain or if the block header is invalid.
func DeserializeBlockIndex(data []byte) (*utxopersister.BlockIndex, error) {
	var (
		pos int
	)

	val, i := DecodeVarIntForIndex(data[pos:])
	_ = val

	pos += i

	var height int
	height, i = DecodeVarIntForIndex(data[pos:])
	pos += i

	var status int
	status, i = DecodeVarIntForIndex(data[pos:])
	pos += i

	var txs int
	txs, i = DecodeVarIntForIndex(data[pos:])
	pos += i

	if (status & BlockValidMask) <= BlockValidTree {
		return nil, errors.NewBlockInvalidError(fmt.Sprintf("block %d is not in active chain, skip it", height))
	}

	if status&(BlockHaveData|BlockHaveUndo) != 0 {
		val, i = DecodeVarIntForIndex(data[pos:])
		_ = val
		pos += i
	}

	if status&BlockHaveData != 0 {
		val, i = DecodeVarIntForIndex(data[pos:])
		_ = val
		pos += i
	}

	if status&BlockHaveUndo != 0 {
		val, i = DecodeVarIntForIndex(data[pos:])
		_ = val
		pos += i
	}

	if len(data[pos:]) < 80 {
		return nil, errors.NewProcessingError("block header length is less than 80")
	}

	bh, err := model.NewBlockHeaderFromBytes(data[pos : pos+80])
	if err != nil {
		return nil, err
	}

	var txCountUint64 uint64

	txCountUint64, err = teranodeUtil.IntToUint64(txs)
	if err != nil {
		return nil, err
	}

	var heightUint32 uint32

	heightUint32, err = teranodeUtil.IntToUint32(height)
	if err != nil {
		return nil, err
	}

	return &utxopersister.BlockIndex{
		Height:      heightUint32,
		TxCount:     txCountUint64,
		BlockHeader: bh,
	}, nil
}
