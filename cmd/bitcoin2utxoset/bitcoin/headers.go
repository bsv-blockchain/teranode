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

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/utxopersister"
	"github.com/btcsuite/goleveldb/leveldb/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

const (
	BlockHaveData = 8
	BlockHaveUndo = 16
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
			log.Printf("Failed to parse block index: %v", err)
			continue
		}

		blockIndex.Hash = blockHash

		blocks = append(blocks, blockIndex)
	}

	if err := iter.Error(); err != nil {
		return nil, err
	}

	// Sort the slice by block height
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Height < blocks[j].Height
	})

	// Now we are sorted, the last block in the slice is the highest
	bestBlock := blocks[len(blocks)-1]

	outFile := filepath.Join(outputDir, bestBlock.Hash.String()+".utxo-headers")

	file, err := os.Create(outFile)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	hasher := sha256.New()

	bufferedWriter := bufio.NewWriter(io.MultiWriter(file, hasher))
	defer bufferedWriter.Flush()

	header, err := utxopersister.BuildHeaderBytes("U-H-1.0", bestBlock.Hash, uint32(bestBlock.Height), bestBlock.BlockHeader.HashPrevBlock)
	if err != nil {
		return nil, errors.NewProcessingError("Couldn't build UTXO set header", err)
	}

	_, err = bufferedWriter.Write(header)
	if err != nil {
		return nil, errors.NewProcessingError("Couldn't write header to file", err)
	}

	var txWritten uint64

	for _, block := range blocks {
		if err := block.Serialise(bufferedWriter); err != nil {
			return nil, errors.NewProcessingError("Couldn't write header to file", err)
		}

		txWritten++
	}

	// Write the eof marker
	if _, err := bufferedWriter.Write(utxopersister.EOFMarker); err != nil {
		return nil, errors.NewProcessingError("Couldn't write EOF marker", err)
	}

	// Write the number of txs and utxos written
	b := make([]byte, 8)

	binary.LittleEndian.PutUint64(b, txWritten)

	if _, err := bufferedWriter.Write(b); err != nil {
		return nil, errors.NewProcessingError("Couldn't write tx count", err)
	}

	if err := bufferedWriter.Flush(); err != nil {
		return nil, errors.NewProcessingError("Couldn't flush buffer", err)
	}

	hashData := fmt.Sprintf("%x  %s\n", hasher.Sum(nil), bestBlock.Hash.String()+".utxo-headers") // N.B. The 2 spaces is important for the hash to be valid
	//nolint:gosec
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

	_, i := DecodeVarIntForIndex(data[pos:])
	pos += i

	height, i := DecodeVarIntForIndex(data[pos:])
	pos += i

	status, i := DecodeVarIntForIndex(data[pos:])
	pos += i

	txs, i := DecodeVarIntForIndex(data[pos:])
	pos += i

	if status&(BlockHaveData|BlockHaveUndo) != 0 {
		_, i = DecodeVarIntForIndex(data[pos:])
		pos += i
	}

	if status&BlockHaveData != 0 {
		_, i = DecodeVarIntForIndex(data[pos:])
		pos += i
	}

	if status&BlockHaveUndo != 0 {
		_, i = DecodeVarIntForIndex(data[pos:])
		pos += i
	}

	if len(data[pos:]) < 80 {
		return nil, errors.NewProcessingError("block header length is less than 80")
	}

	bh, err := model.NewBlockHeaderFromBytes(data[pos : pos+80])
	if err != nil {
		return nil, err
	}

	return &utxopersister.BlockIndex{
		//nolint:gosec
		Height:      uint32(height),
		TxCount:     uint64(txs),
		BlockHeader: bh,
	}, nil
}
