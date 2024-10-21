package test

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

func GenerateTestBlock(subtreeStore *TestSubtreeStore, config *TestConfig) (*model.Block, error) {
	FileDir = config.FileDir
	FileNameTemplate = config.FileNameTemplate
	FileNameTemplateMerkleHashes = config.FileNameTemplateMerkleHashes
	FileNameTemplateBlock = config.FileNameTemplateBlock
	TxMetafileNameTemplate = config.TxMetafileNameTemplate
	SubtreeSize = config.SubtreeSize
	TxCount = config.TxCount
	GenerateNewTestData = config.GenerateNewTestData

	// create test dir of not exists
	if _, err := os.Stat(FileDir); os.IsNotExist(err) {
		if err = os.Mkdir(FileDir, 0755); err != nil {
			return nil, err
		}
	}

	// read block from file and return if exists
	blockFile, err := os.Open(FileNameTemplateBlock)
	if err == nil && !GenerateNewTestData {
		blockBytes, err := io.ReadAll(blockFile)
		if err != nil {
			return nil, err
		}
		_ = blockFile.Close()

		block, err := model.NewBlockFromBytes(blockBytes)
		if err != nil {
			return nil, err
		}

		return block, nil
	}

	/*
		txMetastoreFile, err := os.Create(TxMetafileNameTemplate)
		if err != nil {
			return nil, err
		}

		txMetastoreWriter := bufio.NewWriter(txMetastoreFile)
		defer func() {
			_ = txMetastoreWriter.Flush() // Ensure all data is written to the underlying writer
			_ = txMetastoreFile.Close()
		}()

			var subtreeBytes []byte
			subtree, err := util.NewTreeByLeafCount(SubtreeSize)
			if err != nil {
				return nil, err
			}
			_ = subtree.AddCoinbaseNode()

			var subtreeFile *os.File
			var subtreeFileMerkleHashes *os.File
			subtreeCount := 0

			// create the first files
			subtreeFile, err = os.Create(fmt.Sprintf(FileNameTemplate, subtreeCount))
			if err != nil {
				return nil, err
			}
			subtreeFileMerkleHashes, err = os.Create(FileNameTemplateMerkleHashes)
			if err != nil {
				return nil, err
			}

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
					return nil, fmt.Errorf("expected to write 48 bytes, wrote %d", n)
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
	*/
	// Generates
	testSubtrees, err := GenerateTestSubtrees(subtreeStore, config)
	if err != nil {
		return nil, err
	}

	subtreeHashes := testSubtrees.subtreeHashes
	fees := testSubtrees.totalFees

	coinbaseHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1703fb03002f6d322d75732f0cb6d7d459fb411ef3ac6d65ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000"
	coinbase, err := bt.NewTxFromString(coinbaseHex)
	if err != nil {
		return nil, err
	}
	coinbase.Outputs = nil
	_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000+fees)

	nBits, _ := model.NewNBitFromString("2000ffff")
	hashPrevBlock, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")

	var subtreeFile *os.File
	var subtreeFileMerkleHashes *os.File
	var subtreeBytes []byte
	var merkleRootsubtreeHashes []*chainhash.Hash

	subtreeFileMerkleHashes, err = os.Create(FileNameTemplateMerkleHashes)
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(subtreeHashes); i++ {
		subtreeStore.Files[*subtreeHashes[i]] = i

		if i == 0 {
			// read the first subtree into file, replace the coinbase placeholder with the coinbase txid and calculate the merkle root
			replacedCoinbaseSubtree, err := util.NewTreeByLeafCount(config.SubtreeSize)
			if err != nil {
				return nil, err
			}
			subtreeFile, err = os.Open(fmt.Sprintf(config.FileNameTemplate, i))
			if err != nil {
				return nil, err
			}

			subtreeBytes, err = io.ReadAll(subtreeFile)
			if err != nil {
				return nil, err
			}

			_ = subtreeFile.Close()

			err = replacedCoinbaseSubtree.Deserialize(subtreeBytes)
			if err != nil {
				return nil, err
			}

			replacedCoinbaseSubtree.ReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size()))

			rootHash := replacedCoinbaseSubtree.RootHash()
			merkleRootsubtreeHashes = append(merkleRootsubtreeHashes, rootHash)
		} else {
			merkleRootsubtreeHashes = append(merkleRootsubtreeHashes, subtreeHashes[i])
		}
	}

	for _, hash := range merkleRootsubtreeHashes {
		if _, err = subtreeFileMerkleHashes.Write(hash[:]); err != nil {
			return nil, err
		}
	}
	if err = subtreeFileMerkleHashes.Close(); err != nil {
		return nil, err
	}

	var calculatedMerkleRootHash *chainhash.Hash
	if calculatedMerkleRootHash, err = CalculateMerkleRoot(merkleRootsubtreeHashes); err != nil {
		return nil, err
	}

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: calculatedMerkleRootHash,
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           *nBits,
		Nonce:          0,
	}

	// mine to the target difficulty
	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}
		blockHeader.Nonce++

		if blockHeader.Nonce%1000000 == 0 {
			fmt.Printf("mining Nonce: %d, hash: %s\n", blockHeader.Nonce, blockHeader.Hash().String())
		}
	}

	// if subtreeCount != len(subtreeHashes) {
	// 	return nil, fmt.Errorf("subtree count %d does not match subtree hash count %d", subtreeCount, len(subtreeHashes))
	// }

	block := &model.Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbase,
		TransactionCount: TxCount,
		SizeInBytes:      123123,
		Subtrees:         subtreeHashes,
	}

	blockFile, err = os.Create(FileNameTemplateBlock)
	if err != nil {
		return nil, err
	}

	blockBytes, err := block.Bytes()
	if err != nil {
		return nil, err
	}

	_, err = blockFile.Write(blockBytes)
	if err != nil {
		return nil, err
	}
	if err = blockFile.Close(); err != nil {
		return nil, err
	}

	return block, nil
}
