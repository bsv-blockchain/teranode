package test

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/bitcoin-sv/ubsv/errors"
	"os"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

func GenerateTestSubtrees(subtreeStore *TestSubtreeStore, config *TestConfig) (*TestSubtrees, error) {
	txMetastoreFile, err := os.Create(config.TxMetafileNameTemplate)
	if err != nil {
		return nil, err
	}

	txMetastoreWriter := bufio.NewWriter(txMetastoreFile)
	defer func() {
		_ = txMetastoreWriter.Flush() // Ensure all data is written to the underlying writer
		_ = txMetastoreFile.Close()
	}()

	var subtreeBytes []byte
	subtree, err := util.NewTreeByLeafCount(config.SubtreeSize)
	if err != nil {
		return nil, err
	}
	_ = subtree.AddNode(model.CoinbasePlaceholder, 0, 0)

	var subtreeFile *os.File
	//var subtreeFileMerkleHashes *os.File
	subtreeCount := 0

	// create the first files
	subtreeFile, err = os.Create(fmt.Sprintf(config.FileNameTemplate, subtreeCount))
	if err != nil {
		return nil, err
	}
	// subtreeFileMerkleHashes, err = os.Create(FileNameTemplateMerkleHashes)
	// if err != nil {
	// 	return nil, err
	// }

	subtreeHashes := make([]*chainhash.Hash, 0)

	txId := make([]byte, 32)
	var hash chainhash.Hash
	fees := uint64(0)
	var n int
	for i := 1; i < int(TxCount); i++ {
		binary.LittleEndian.PutUint64(txId, uint64(i))
		hash = chainhash.Hash(txId)

		if err = subtree.AddNode(hash, uint64(i), uint64(i)); err != nil {
			return nil, err
		}

		n, err = WriteTxMeta(txMetastoreWriter, hash, uint64(i), uint64(i))
		if err != nil {
			return nil, err
		}
		if n != 48 {
			return nil, errors.NewProcessingError("expected to write 48 bytes, wrote %d", n)
		}

		fees += uint64(i)

		if subtree.IsComplete() {
			// write subtree bytes to file
			if subtreeBytes, err = subtree.Serialize(); err != nil {
				return nil, err
			}

			if _, err = subtreeFile.Write(subtreeBytes); err != nil {
				return nil, err
			}

			subtreeHashes = append(subtreeHashes, subtree.RootHash())

			if err = subtreeFile.Close(); err != nil {
				return nil, err
			}

			subtreeCount++

			if subtreeFile, err = os.Create(fmt.Sprintf(FileNameTemplate, subtreeCount)); err != nil {
				return nil, err
			}

			// create new tree
			subtree, err = util.NewTreeByLeafCount(SubtreeSize)
			if err != nil {
				return nil, err
			}
		}
	}

	// write the last subtree
	if subtree.Length() > 0 {
		// write subtree bytes to file
		subtreeBytes, err = subtree.Serialize()
		if err != nil {
			return nil, err
		}

		if _, err = subtreeFile.Write(subtreeBytes); err != nil {
			return nil, err
		}

		subtreeHashes = append(subtreeHashes, subtree.RootHash())

		if err = subtreeFile.Close(); err != nil {
			return nil, err
		}
	}

	return &TestSubtrees{totalFees: fees, subtreeHashes: subtreeHashes}, nil
}
