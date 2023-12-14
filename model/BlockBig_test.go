package model

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	blob_memory "github.com/bitcoin-sv/ubsv/stores/blob/memory"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/txmeta/memory"
	"github.com/bitcoin-sv/ubsv/stores/txmetacache"
	"github.com/bitcoin-sv/ubsv/ulogger"

	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const subtreeSize int = 16

func TestBigBlock_Valid(t *testing.T) {
	txIdCount := 32
	var subtreeCount int

	if txIdCount > subtreeSize {
		subtreeCount = txIdCount / subtreeSize
	} else {
		subtreeCount = 1
	}

	generateTestSets(txIdCount)

	nBits := NewNBitFromString("1d00ffff")
	hashPrevBlock, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

	coinbaseHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1703fb03002f6d322d75732f0cb6d7d459fb411ef3ac6d65ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000"
	coinbase, err := bt.NewTxFromString(coinbaseHex)
	require.NoError(t, err)

	var subtree *util.Subtree

	var subtrees []*util.Subtree
	subtreeStore := blob_memory.New()
	var subtreeFile *os.File
	currentSubtreeCount := 0

	var calculatedMerkleRootHash *chainhash.Hash
	// read in txs of 32 bytes each
	for i, j := 0, 0; i < subtreeCount; i, j = i+1, j+subtreeSize {
		if (i > 0 && txIdCount > subtreeSize && j%subtreeSize == 0) || i == 0 {
			if i > 0 {
				currentSubtreeCount++
				assert.Equal(t, subtreeSize, subtree.Size())
				subtreeFile.Close()
			}
			subtree = util.NewTreeByLeafCount(subtreeSize)

			subtreeFile, err = os.Open(fmt.Sprintf("./testdata/subtree-%d.bin", currentSubtreeCount))
			if err != nil {
				fmt.Println("Error opening file:", err)
				return
			}
		}
		defer subtreeFile.Close()

		var txHash *chainhash.Hash
		for k := 0; k < subtreeSize; k++ {
			txId := make([]byte, 32)
			_, err := subtreeFile.Read(txId)
			if err != nil {
				if !errors.Is(err, io.EOF) {
					return
				}
			}
			txHash, err = chainhash.NewHash(txId)
			if err != nil {
				return
			}
			// fmt.Printf("txId coming out of subtree %s: %s\n", subtreeFile.Name(), txHash.String())

			if j == 0 && k == 0 {
				_ = subtree.AddNode(*CoinbasePlaceholderHash, 0, 0)
			} else {
				_ = subtree.AddNode(*txHash, 0, 0)
			}
		}
		subtreeBytes, _ := subtree.Serialize()
		// fmt.Printf("setting subtreeStore key %s\n", subtree.RootHash().String())
		subtreeStore.Set(context.Background(), subtree.RootHash()[:], subtreeBytes)
		subtrees = append(subtrees, subtree)
	}

	if len(subtrees) == 1 {
		// first subtree
		replacedCoinbaseSubtree := subtrees[0].Duplicate()
		replacedCoinbaseSubtree.ReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbaseTx.Size()))
		// fmt.Printf("replacedCoinbaseSubtree hash: %s\n", replacedCoinbaseSubtree.RootHash().String())
		calculatedMerkleRootHash, err = chainhash.NewHash(replacedCoinbaseSubtree.RootHash()[:])
	} else {
		calculatedMerkleRootHash, err = calculateMerkleRoot(subtrees, subtrees, coinbase)
		require.NoError(t, err)
	}
	require.NoError(t, err)

	blockHeader := &BlockHeader{
		Version:        1,
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: calculatedMerkleRootHash,
		Timestamp:      1231469650,
		Bits:           nBits,
		Nonce:          0,
	}

	var subtreeHashes []*chainhash.Hash
	for _, subtree := range subtrees {
		subtreeHashes = append(subtreeHashes, subtree.RootHash())
		// fmt.Printf("subtreeHash %d: %s\n", i, subtree.RootHash().String())
	}

	assert.Equal(t, subtreeCount, len(subtreeHashes))
	assert.Equal(t, subtreeCount, len(subtrees))
	// assert.Equal(t, "08c8efae0da039768e3dd6acfee122c1d941e53adb1634da81d7af3f4e8b186f", blockHeader.HashMerkleRoot.String())

	b := &Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbase,
		TransactionCount: 1,
		SizeInBytes:      123,
		Subtrees:         subtreeHashes,
		SubtreeSlices:    subtrees,
	}

	txMetaStore := memory.New(ulogger.TestLogger{}, true)
	cachedTxMetaStore := txmetacache.NewTxMetaCache(ulogger.TestLogger{}, txMetaStore)
	// create a reader from the txmetacache file
	file, err := os.Open("./testdata/txMeta.bin")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()
	err = ReadTxMeta(file, cachedTxMetaStore.(*txmetacache.TxMetaCache))
	require.NoError(t, err)

	reqTxId, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	data, err := cachedTxMetaStore.Get(context.Background(), reqTxId)
	require.NoError(t, err)
	require.Equal(t, &txmeta.Data{
		Fee:         0,
		SizeInBytes: 1,
	}, data)

	currentChain := make([]*BlockHeader, 11)
	for i := 0; i < 11; i++ {
		currentChain[i] = &BlockHeader{
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			// set the last 11 block header timestamps to be less than the current timestamps
			Timestamp: 1231469665 - uint32(i),
		}
	}
	currentChain[0].HashPrevBlock = &chainhash.Hash{}
	start := time.Now()
	runtime.SetCPUProfileRate(500)
	f, _ := os.Create("cpu.prof")
	defer f.Close()

	_ = pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	v, err := b.Valid(context.Background(), subtreeStore, txMetaStore, currentChain)
	require.NoError(t, err)
	t.Logf("Time taken: %s\n", time.Since(start))

	f, _ = os.Create("mem.prof")
	defer f.Close()
	_ = pprof.WriteHeapProfile(f)
	require.True(t, v)
}

func hasMetTargetDifficulty(bh *BlockHeader, target *big.Int) (bool, *chainhash.Hash, error) {

	var bn = big.NewInt(0)
	bn.SetString(bh.Hash().String(), 16)

	compare := bn.Cmp(target)
	if compare <= 0 {
		return true, bh.Hash(), nil
	}

	return false, nil, fmt.Errorf("block header does not meet target %d: %032x >? %032x", compare, target.Bytes(), bn.Bytes())
}

func generateTestSets(nrOfIds int) error {
	folder := "./testdata/"
	filename := "subtree"
	txMetastoreFilename := "txMeta"

	var subtreeFile *os.File
	var subtreeWriter *bufio.Writer
	var err error
	subtreeCount := 0

	txMetastoreFile, err := os.Create(fmt.Sprintf("%s/%s.bin", folder, txMetastoreFilename))
	if err != nil {
		return err
	}
	defer txMetastoreFile.Close()

	txMetastoreWriter := bufio.NewWriter(txMetastoreFile)
	defer txMetastoreWriter.Flush() // Ensure all data is written to the underlying writer

	for i := 0; i < nrOfIds; i++ {
		if (i > 0 && nrOfIds > subtreeSize && i%subtreeSize == 0) || i == 0 {
			if subtreeFile != nil {
				subtreeCount++
				subtreeWriter.Flush() // Flush any remaining data
				subtreeFile.Close()   // Close the current file
			}

			subtreeFile, err = os.Create(fmt.Sprintf("%s/%s-%d.bin", folder, filename, subtreeCount))
			if err != nil {
				return err
			}
			subtreeWriter = bufio.NewWriter(subtreeFile) // Update the writer to write to the new file
		}

		txId := make([]byte, 32)
		binary.LittleEndian.PutUint64(txId, uint64(i))
		hash := chainhash.Hash(txId)
		// fmt.Printf("txId going in subtree %s: %s\n", subtreeFile.Name(), hash.String())

		_, err = subtreeWriter.Write(hash[:])
		if err != nil {
			return err
		}
		WriteTxMeta(txMetastoreWriter, hash, uint64(0), uint64(i))
	}

	if subtreeFile != nil {
		subtreeWriter.Flush() // Flush any remaining data
		subtreeFile.Close()   // Close the last file
	}

	return nil
}

func ReadTxMeta(r io.Reader, txMetaStore *txmetacache.TxMetaCache) error {
	// read from the reader and add to txMeta store
	b := make([]byte, 48)
	for {
		_, err := r.Read(b)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		txHash := &chainhash.Hash{}
		copy(txHash[:], b[:32])
		fee := binary.LittleEndian.Uint64(b[32:40])
		sizeInBytes := binary.LittleEndian.Uint64(b[40:48])

		if err = txMetaStore.SetCache(txHash, &txmeta.Data{
			Fee:         fee,
			SizeInBytes: sizeInBytes,
		}); err != nil {
			return err
		}
	}
}

func WriteTxMeta(w io.Writer, txHash chainhash.Hash, fee, sizeInBytes uint64) (int, error) {
	b := make([]byte, 48)
	copy(b[:32], txHash[:])
	binary.LittleEndian.PutUint64(b[32:40], fee)
	binary.LittleEndian.PutUint64(b[40:48], sizeInBytes)

	return w.Write(b)
}

func calculateMerkleRoot(subtrees []*util.Subtree, subtreeSlices []*util.Subtree, coinbaseTx *bt.Tx) (*chainhash.Hash, error) {
	if len(subtrees) != len(subtreeSlices) {
		return nil, fmt.Errorf("number of subtrees does not match number of subtree slices")
	}

	hashes := make([]chainhash.Hash, len(subtrees))

	for i, subtree := range subtreeSlices {
		if i == 0 {
			// We need to inject the coinbase txid into the first position of the first subtree
			replacedSubtree := subtreeSlices[0].Duplicate()
			replacedSubtree.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size()))
			// fmt.Printf("replacedCoinbaseSubtree hash: %s\n", subtree.RootHash().String())
			hashes[i] = *replacedSubtree.RootHash()
		} else {
			hashes[i] = *subtree.RootHash()
		}
	}

	var calculatedMerkleRootHash *chainhash.Hash
	if len(hashes) == 1 {
		calculatedMerkleRootHash = &hashes[0]
	} else if len(hashes) > 0 {
		// Create a new subtree with the hashes of the subtrees
		st := util.NewTreeByLeafCount(util.CeilPowerOfTwo(len(subtrees)))
		for _, hash := range hashes {
			err := st.AddNode(hash, 1, 0)
			if err != nil {
				return nil, err
			}
		}

		calculatedMerkleRoot := st.RootHash()
		var err error
		calculatedMerkleRootHash, err = chainhash.NewHash(calculatedMerkleRoot[:])
		if err != nil {
			return nil, err
		}
	} else {
		calculatedMerkleRootHash = coinbaseTx.TxIDChainHash()
	}

	return calculatedMerkleRootHash, nil
}
