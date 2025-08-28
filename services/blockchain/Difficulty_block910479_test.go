package blockchain

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/big"
	"testing"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/stores/blockchain"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
)

// BlockInfo represents the JSON structure from WhatsOnChain API
type BlockInfo struct {
	Hash         string  `json:"hash"`
	Height       int     `json:"height"`
	Version      int     `json:"version"`
	VersionHex   string  `json:"versionHex"`
	MerkleRoot   string  `json:"merkleroot"`
	Time         int64   `json:"time"`
	MedianTime   int64   `json:"mediantime"`
	Nonce        uint32  `json:"nonce"`
	Bits         string  `json:"bits"`
	Difficulty   float64 `json:"difficulty"`
	ChainWork    string  `json:"chainwork"`
	PreviousHash string  `json:"previousblockhash"`
	NextHash     string  `json:"nextblockhash"`
}

// TestBlock910479Difficulty tests the difficulty calculation for the specific block 910479
// This block was mentioned in issue #3772 as having incorrect difficulty with the squaring bug
func TestBlock910479Difficulty(t *testing.T) {
	// Load block data from the JSON files we downloaded
	blocks := make(map[int]*BlockInfo)

	// Heights we need
	heights := []int{
		910333, 910334, 910335, 910336, 910337, 910338, // Around 144 blocks back
		910476, 910477, 910478, 910479, 910480, 910481, // Around target block
	}

	for _, height := range heights {
		data, err := ioutil.ReadFile(fmt.Sprintf("../../block_%d_info.json", height))
		if err != nil {
			t.Skipf("Skipping test - block data files not found. Run the curl commands to download them first.")
			return
		}

		var blockInfo BlockInfo
		err = json.Unmarshal(data, &blockInfo)
		require.NoError(t, err)
		blocks[height] = &blockInfo
	}

	// Create MockStore and populate it
	store := blockchain.NewMockStore()

	// Convert blocks to model.Block and store them
	for height, info := range blocks {
		// Parse the block header fields
		version := uint32(info.Version)

		// Parse previous block hash
		prevHash, err := chainhash.NewHashFromStr(info.PreviousHash)
		require.NoError(t, err)

		// Parse merkle root
		merkleHash, err := chainhash.NewHashFromStr(info.MerkleRoot)
		require.NoError(t, err)

		// Parse nBits
		nBits, err := model.NewNBitFromString(info.Bits)
		require.NoError(t, err)

		// Create block header
		header := &model.BlockHeader{
			Version:        version,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: merkleHash,
			Timestamp:      uint32(info.Time),
			Bits:           *nBits,
			Nonce:          info.Nonce,
		}

		// Parse block hash
		hash, err := chainhash.NewHashFromStr(info.Hash)
		require.NoError(t, err)

		// Create block
		block := &model.Block{
			Header: header,
			Height: uint32(height),
		}

		// Store in MockStore
		store.Blocks[*hash] = block
		store.BlockExists[*hash] = true
		store.BlockByHeight[uint32(height)] = block

		// Parse and store chainwork
		chainworkHex := info.ChainWork
		if len(chainworkHex) > 2 && chainworkHex[:2] == "0x" {
			chainworkHex = chainworkHex[2:]
		}
		chainworkBytes, err := hex.DecodeString(chainworkHex)
		require.NoError(t, err)

		// Ensure chainwork is 32 bytes
		paddedChainwork := make([]byte, 32)
		copy(paddedChainwork[32-len(chainworkBytes):], chainworkBytes)
		store.BlockChainWork[*hash] = paddedChainwork

		// Update best block if needed
		if store.BestBlock == nil || block.Height > store.BestBlock.Height {
			store.BestBlock = block
		}
	}

	// Create difficulty calculator
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams
	d, err := NewDifficulty(store, ulogger.TestLogger{}, tSettings)
	require.NoError(t, err)

	// Now calculate difficulty for block 910479
	// We use block 910478 as the reference (the previous block)
	block910478 := blocks[910478]
	prevBlockHash, err := chainhash.NewHashFromStr(block910478.Hash)
	require.NoError(t, err)

	ctx := context.Background()

	// For testing, we know that 144 blocks back from 910478 is 910334
	// Instead of traversing, we'll directly get that block
	ancestorHeight := uint32(910334)
	ancestorBlock := store.BlockByHeight[ancestorHeight]
	require.NotNil(t, ancestorBlock, "Ancestor block at height %d not found", ancestorHeight)
	ancestorHash := ancestorBlock.Header.Hash()

	// Get suitable blocks
	firstSuitableBlock, err := store.GetSuitableBlock(ctx, ancestorHash)
	require.NoError(t, err, "Failed to get first suitable block")

	lastSuitableBlock, err := store.GetSuitableBlock(ctx, prevBlockHash)
	require.NoError(t, err, "Failed to get last suitable block")

	// Log the blocks being used
	t.Logf("Calculating difficulty for block 910479")
	t.Logf("First suitable block: height=%d, time=%d, nBits=%s",
		firstSuitableBlock.Height, firstSuitableBlock.Time,
		hex.EncodeToString(firstSuitableBlock.NBits))
	t.Logf("Last suitable block: height=%d, time=%d, nBits=%s",
		lastSuitableBlock.Height, lastSuitableBlock.Time,
		hex.EncodeToString(lastSuitableBlock.NBits))

	// Show chainwork difference
	firstWork := new(big.Int).SetBytes(firstSuitableBlock.ChainWork)
	lastWork := new(big.Int).SetBytes(lastSuitableBlock.ChainWork)
	workDiff := new(big.Int).Sub(lastWork, firstWork)
	t.Logf("Chainwork difference: %s", workDiff.String())
	t.Logf("Time difference: %d seconds", lastSuitableBlock.Time-firstSuitableBlock.Time)

	// Calculate difficulty
	calculatedNBits, err := d.computeTarget(firstSuitableBlock, lastSuitableBlock)
	require.NoError(t, err, "Failed to compute target")

	// Get the actual nBits from block 910479
	actualBits, err := model.NewNBitFromString(blocks[910479].Bits)
	require.NoError(t, err)

	// Compare
	calculatedHex := hex.EncodeToString(calculatedNBits.CloneBytes())
	actualHex := hex.EncodeToString(actualBits.CloneBytes())

	t.Logf("Block 910479 difficulty comparison:")
	t.Logf("Calculated nBits: %s", calculatedHex)
	t.Logf("Actual nBits:     %s", actualHex)

	// They should match exactly
	require.Equal(t, actualHex, calculatedHex,
		"Difficulty calculation mismatch for block 910479")

	t.Logf("✅ Block 910479 difficulty calculation is CORRECT!")
}
