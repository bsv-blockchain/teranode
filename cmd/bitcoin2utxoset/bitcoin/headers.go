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
	teranodeUtil "github.com/bitcoin-sv/teranode/util"
	"github.com/btcsuite/goleveldb/leveldb/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

const (
	BlockValidReserved     = 1
	BlockValidTree         = 2
	BlockValidTransactions = 3
	BlockValidChain        = 4
	BlockValidScripts      = 5
	BlockValidMask         = BlockValidReserved | BlockValidTree | BlockValidTransactions | BlockValidChain | BlockValidScripts

	BlockHaveData = 8  //!< full block available in blk*.dat
	BlockHaveUndo = 16 //!< undo data available in rev*.dat
)

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
			log.Printf("Failed to parse block hash: %v", err)
			continue
		}

		blockIndex, err := DeserializeBlockIndex(value)
		if err != nil {
			if !errors.Is(err, errors.ErrBlockInvalid) {
				log.Printf("Failed to parse block index: %v", err)
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

	outFile := filepath.Join(safeOutputDir, bestBlock.Hash.String()+".utxo-headers")

	file, err := os.Create(outFile)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	hasher := sha256.New()

	bufferedWriter := bufio.NewWriter(io.MultiWriter(file, hasher))
	defer bufferedWriter.Flush()

	header := fileformat.NewHeader(fileformat.FileTypeUtxoHeaders)

	if err := header.Write(bufferedWriter); err != nil {
		return nil, errors.NewProcessingError("Couldn't write header to file", err)
	}

	if _, err := bufferedWriter.Write(bestBlock.Hash[:]); err != nil {
		return nil, errors.NewProcessingError("Couldn't write block header to file", err)
	}

	if err := binary.Write(bufferedWriter, binary.LittleEndian, bestBlock.Height); err != nil {
		return nil, errors.NewProcessingError("error writing header number", err)
	}

	var (
		recordCount uint64
		txCount     uint64
	)

	for _, block := range blocks {
		if err := block.Serialise(bufferedWriter); err != nil {
			return nil, errors.NewProcessingError("Couldn't write header to file", err)
		}

		recordCount++
		txCount += block.TxCount
	}

	// Write the number of txs and utxos written
	b := make([]byte, 8)

	binary.LittleEndian.PutUint64(b, recordCount)

	if _, err := bufferedWriter.Write(b); err != nil {
		return nil, errors.NewProcessingError("Couldn't write tx count", err)
	}

	binary.LittleEndian.PutUint64(b, txCount)

	if _, err := bufferedWriter.Write(b); err != nil {
		return nil, errors.NewProcessingError("Couldn't write tx count", err)
	}

	if err := bufferedWriter.Flush(); err != nil {
		return nil, errors.NewProcessingError("Couldn't flush buffer", err)
	}

	hashData := fmt.Sprintf("%x  %s\n", hasher.Sum(nil), bestBlock.Hash.String()+".utxo-headers") // N.B. The 2 spaces is important for the hash to be valid

	//nolint:gosec // G306: Expect WriteFile permissions to be 0600 or less (gosec)go-golangci-lint
	if err := os.WriteFile(outFile+".sha256", []byte(hashData), 0644); err != nil {
		return nil, errors.NewProcessingError("Couldn't write hash file", err)
	}

	// logger.Infof("FINISHED  %16s transactions with %16s utxos, skipped %d", formatNumber(txWritten), formatNumber(utxosWritten), utxosSkipped)

	return bestBlock, nil
}

func DeserializeBlockIndex(data []byte) (*utxopersister.BlockIndex, error) {
	var (
		pos int
	)

	val, i := DecodeVarIntForIndex(data[pos:])
	_ = val
	// fmt.Printf("Val 1: %d\n", val)
	pos += i

	height, i := DecodeVarIntForIndex(data[pos:])
	pos += i

	status, i := DecodeVarIntForIndex(data[pos:])
	pos += i

	txs, i := DecodeVarIntForIndex(data[pos:])
	pos += i

	if (status & BlockValidMask) <= BlockValidTree {
		return nil, errors.NewBlockInvalidError(fmt.Sprintf("block %d is not in active chain, skip it", height))
	}

	if status&(BlockHaveData|BlockHaveUndo) != 0 {
		val, i = DecodeVarIntForIndex(data[pos:])
		_ = val
		// fmt.Printf("Val 2: %d\n", val)
		pos += i
	}

	if status&BlockHaveData != 0 {
		val, i = DecodeVarIntForIndex(data[pos:])
		_ = val
		// fmt.Printf("Val 3: %d\n", val)
		pos += i
	}

	if status&BlockHaveUndo != 0 {
		val, i = DecodeVarIntForIndex(data[pos:])
		_ = val
		// fmt.Printf("Val 4: %d\n", val)
		pos += i
	}

	if len(data[pos:]) < 80 {
		return nil, errors.NewProcessingError("block header length is less than 80")
	}

	// fmt.Printf("Height: %d\n", height)
	// fmt.Printf("Tx count: %d\n", txs)
	// fmt.Printf("Block header: %x\n", data[pos:pos+80])

	bh, err := model.NewBlockHeaderFromBytes(data[pos : pos+80])
	if err != nil {
		return nil, err
	}

	txCountUint64, err := teranodeUtil.SafeIntToUint64(txs)
	if err != nil {
		return nil, err
	}

	heightUint32, err := teranodeUtil.SafeIntToUint32(height)
	if err != nil {
		return nil, err
	}

	return &utxopersister.BlockIndex{
		Height:      heightUint32,
		TxCount:     txCountUint64,
		BlockHeader: bh,
	}, nil
}
