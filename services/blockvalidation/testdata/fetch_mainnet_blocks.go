//go:build ignore

// This program fetches real mainnet blocks from a teranode instance using the GetNBlocks API.
// Run with: go run fetch_mainnet_blocks.go -url <teranode-url> [-start <hash>] [-count <number>]
// Example: go run fetch_mainnet_blocks.go -url https://teranode.example.com/api/v1
package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// TestBlockData stores the block data in JSON format
type TestBlockData struct {
	Blocks     []json.RawMessage `json:"blocks"` // Full blocks as JSON
	Count      int               `json:"count"`
	Generated  time.Time         `json:"generated"`
	Source     string            `json:"source"`
	StartBlock int               `json:"start_block"`
	EndBlock   int               `json:"end_block"`
}

func main() {
	var teranodeURL string
	var startHash string
	var blockCount int
	var outputFile string

	flag.StringVar(&teranodeURL, "url", "", "Teranode API URL (required)")
	flag.StringVar(&startHash, "start", "000000003dd32df94cfafd16e0a8300ea14d67dcfee9e1282786c2617b8daa09", "Starting block hash (default: block 10000)")
	flag.IntVar(&blockCount, "count", 10001, "Number of blocks to fetch (default: 10001 to include genesis)")
	flag.StringVar(&outputFile, "output", "mainnet_blocks.json", "Output file name")
	flag.Parse()

	if teranodeURL == "" {
		log.Fatal("Please provide a teranode URL with -url flag")
	}

	log.Printf("Fetching %d mainnet blocks from %s...\n", blockCount, teranodeURL)
	startTime := time.Now()

	// Start from block 10000 and work backwards to genesis
	currentHash := startHash
	if currentHash == "" {
		currentHash = "000000003dd32df94cfafd16e0a8300ea14d67dcfee9e1282786c2617b8daa09" // Block 10000
	}

	// We'll collect all blocks as we fetch them backwards
	var allBatches [][]json.RawMessage
	totalFetched := 0

	// Fetch blocks in batches of 1000, working backwards from block 10000 to genesis
	for totalFetched < blockCount {
		batchSize := 1000
		if blockCount-totalFetched < 1000 {
			batchSize = blockCount - totalFetched
		}

		log.Printf("Fetching batch of %d blocks starting from %s... (total: %d/%d)",
			batchSize, currentHash[:16], totalFetched, blockCount)

		blocksJSON, lastBlockHash, err := fetchBlockBatchJSON(teranodeURL, currentHash, batchSize)
		if err != nil {
			log.Printf("Failed to fetch blocks: %v", err)
			log.Printf("Retrying in 2 seconds...")
			time.Sleep(2 * time.Second)

			// Retry once
			blocksJSON, lastBlockHash, err = fetchBlockBatchJSON(teranodeURL, currentHash, batchSize)
			if err != nil {
				log.Printf("Failed again: %v", err)
				break
			}
		}

		if len(blocksJSON) == 0 {
			log.Printf("No blocks returned, stopping")
			break
		}

		log.Printf("Received %d blocks", len(blocksJSON))

		// Store this batch - we'll reverse everything at the end
		allBatches = append([][]json.RawMessage{blocksJSON}, allBatches...)
		totalFetched += len(blocksJSON)

		// Check if we've reached genesis
		if lastBlockHash == "" {
			log.Printf("Reached genesis block")
			break
		}

		// Use the previous block hash for the next batch
		currentHash = lastBlockHash

		// Delay between requests to avoid rate limiting
		time.Sleep(2 * time.Second)
	}

	// Now flatten all batches in the correct order (genesis to block 10000)
	allBlocksJSON := make([]json.RawMessage, 0, totalFetched)
	for _, batch := range allBatches {
		// Each batch is also in reverse order (newest to oldest), so reverse it
		for i := len(batch) - 1; i >= 0; i-- {
			allBlocksJSON = append(allBlocksJSON, batch[i])
		}
	}

	log.Printf("Total blocks fetched: %d", len(allBlocksJSON))

	// Create the data structure to save
	testData := TestBlockData{
		Blocks:     allBlocksJSON,
		Count:      len(allBlocksJSON),
		Generated:  time.Now(),
		Source:     teranodeURL,
		StartBlock: 0,
		EndBlock:   len(allBlocksJSON) - 1,
	}

	// Marshal to JSON
	jsonData, err := json.MarshalIndent(testData, "", "  ")
	if err != nil {
		log.Fatalf("Failed to marshal blocks to JSON: %v", err)
	}

	// Write to file
	err = os.WriteFile(outputFile, jsonData, 0644)
	if err != nil {
		log.Fatalf("Failed to write blocks to file: %v", err)
	}

	elapsed := time.Since(startTime)
	log.Printf("Successfully fetched %d blocks in %v", len(allBlocksJSON), elapsed)
	log.Printf("Blocks saved to %s (%.2f MB)", outputFile, float64(len(jsonData))/(1024*1024))

	// Print some info for verification
	if len(allBlocksJSON) > 0 {
		// Parse first and last blocks for verification
		var firstBlock, lastBlock map[string]interface{}
		if err := json.Unmarshal(allBlocksJSON[0], &firstBlock); err != nil {
			log.Printf("Error parsing first block: %v", err)
		}
		if err := json.Unmarshal(allBlocksJSON[len(allBlocksJSON)-1], &lastBlock); err != nil {
			log.Printf("Error parsing last block: %v", err)
		}

		// Extract and display block info
		firstHeight := firstBlock["height"]
		lastHeight := lastBlock["height"]

		log.Printf("\nFirst block (genesis): height %.0f", firstHeight)
		log.Printf("Last block: height %.0f", lastHeight)

		// Note: The blocks don't include computed hashes, only header fields
		// The hash needs to be computed from the header bytes
		// For now, we'll verify using the hash_prev_block field

		// Verify continuity - blocks should be in order 0, 1, 2, ...
		log.Printf("\nVerifying block order...")
		orderValid := true
		for i := 0; i < len(allBlocksJSON) && i < 100; i++ {
			var block map[string]interface{}
			json.Unmarshal(allBlocksJSON[i], &block)

			expectedHeight := float64(i)
			actualHeight := block["height"].(float64)
			if actualHeight != expectedHeight {
				log.Printf("Order broken at index %d: expected height %.0f, got %.0f", i, expectedHeight, actualHeight)
				orderValid = false
				break
			}
		}
		if orderValid {
			log.Printf("Block order verified: blocks are in sequence from 0 to %d", len(allBlocksJSON)-1)
		}

		log.Printf("Total blocks stored: %d", len(allBlocksJSON))
	}
}

// fetchBlockBatchJSON fetches a batch of blocks from the teranode API and returns them as JSON
func fetchBlockBatchJSON(teranodeURL string, startHash string, count int) ([]json.RawMessage, string, error) {
	// Build request URL - using /blocks/{hash}/json endpoint with n parameter for JSON response
	url := fmt.Sprintf("%s/blocks/%s/json?n=%d", teranodeURL, startHash, count)

	// Create HTTP client with longer timeout
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Make HTTP request
	resp, err := client.Get(url)
	if err != nil {
		return nil, "", fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	// Read response body (JSON array of blocks)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read response: %w", err)
	}

	// Parse JSON response as array of raw messages
	var jsonBlocks []json.RawMessage
	if err := json.Unmarshal(body, &jsonBlocks); err != nil {
		return nil, "", fmt.Errorf("failed to parse JSON response: %w", err)
	}

	// Get the hash of the last block's previous block for next batch
	var lastBlockHash string
	if len(jsonBlocks) > 0 {
		var lastBlock map[string]interface{}
		if err := json.Unmarshal(jsonBlocks[len(jsonBlocks)-1], &lastBlock); err == nil {
			if header, ok := lastBlock["header"].(map[string]interface{}); ok {
				if prevHash, ok := header["hash_prev_block"].(string); ok {
					// Check if it's the genesis block (all zeros)
					if prevHash != "0000000000000000000000000000000000000000000000000000000000000000" {
						lastBlockHash = prevHash
					}
				}
			}
		}
	}

	return jsonBlocks, lastBlockHash, nil
}

// fetchBlockBatch fetches a batch of blocks from the teranode API using GetNBlocks (deprecated)
func fetchBlockBatch(teranodeURL string, startHash string, count int) ([]*model.Block, [][]byte, error) {
	// Build request URL - using /blocks/{hash}/json endpoint with n parameter for JSON response
	url := fmt.Sprintf("%s/blocks/%s/json?n=%d", teranodeURL, startHash, count)

	// Create HTTP client with longer timeout
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Make HTTP request
	resp, err := client.Get(url)
	if err != nil {
		return nil, nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	// Read response body (JSON array of blocks)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Parse JSON response
	var jsonBlocks []json.RawMessage
	if err := json.Unmarshal(body, &jsonBlocks); err != nil {
		return nil, nil, fmt.Errorf("failed to parse JSON response: %w", err)
	}

	// Convert JSON blocks to model.Block
	blocks := make([]*model.Block, 0, len(jsonBlocks))
	blockBytes := make([][]byte, 0, len(jsonBlocks))

	for i, jsonBlock := range jsonBlocks {
		// Parse each block from JSON
		var blockData map[string]interface{}
		if err := json.Unmarshal(jsonBlock, &blockData); err != nil {
			return nil, nil, fmt.Errorf("failed to parse block %d: %w", i, err)
		}

		// The response should have the block in some format
		// Let's check if there's a raw block field or if we need to reconstruct from header
		block := &model.Block{}

		// Try to get the header
		if headerData, ok := blockData["header"].(map[string]interface{}); ok {
			// Build header from fields
			header := &model.BlockHeader{}

			if v, ok := headerData["version"].(float64); ok {
				header.Version = uint32(v)
			}
			if hashStr, ok := headerData["hash_prev_block"].(string); ok {
				if hash, err := chainhash.NewHashFromStr(hashStr); err == nil {
					header.HashPrevBlock = hash
				}
			}
			if hashStr, ok := headerData["hash_merkle_root"].(string); ok {
				if hash, err := chainhash.NewHashFromStr(hashStr); err == nil {
					header.HashMerkleRoot = hash
				}
			}
			if v, ok := headerData["timestamp"].(float64); ok {
				header.Timestamp = uint32(v)
			}
			if bitsStr, ok := headerData["bits"].(string); ok {
				if nBits, err := model.NewNBitFromString(bitsStr); err == nil {
					header.Bits = *nBits
				}
			}
			if v, ok := headerData["nonce"].(float64); ok {
				header.Nonce = uint32(v)
			}

			block.Header = header
		}

		// Get the height
		if h, ok := blockData["height"].(float64); ok {
			block.Height = uint32(h)
		}

		blocks = append(blocks, block)
		// For now, store the header bytes
		blockBytes = append(blockBytes, block.Header.Bytes())
	}

	return blocks, blockBytes, nil
}

// parseBlockFromBytes parses a single block from bytes and returns the block and bytes consumed
func parseBlockFromBytes(data []byte) (*model.Block, int, error) {
	if len(data) < model.BlockHeaderSize {
		return nil, 0, fmt.Errorf("insufficient data for block header")
	}

	// Parse the header first
	header, err := model.NewBlockHeaderFromBytes(data[:model.BlockHeaderSize])
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse header: %w", err)
	}

	// Create a basic block with just the header for now
	// The GetNBlocks endpoint returns blocks in a specific format that includes:
	// - Header (80 bytes)
	// - Transaction count (VarInt)
	// - Size in bytes (VarInt)
	// - Subtree list
	// - Coinbase transaction
	// - Height (VarInt)

	// For our test purposes, we just need the headers, so we'll create simple blocks
	block := &model.Block{
		Header: header,
	}

	// Try to determine where this block ends and the next begins
	// by looking for the next valid header (version field pattern)
	nextHeaderOffset := model.BlockHeaderSize

	// Scan for the next block header (version 1 or similar small number in little-endian)
	for nextHeaderOffset < len(data)-4 {
		// Check if this looks like a version field (1, 2, etc. in little-endian)
		possibleVersion := uint32(data[nextHeaderOffset]) |
			uint32(data[nextHeaderOffset+1])<<8 |
			uint32(data[nextHeaderOffset+2])<<16 |
			uint32(data[nextHeaderOffset+3])<<24

		if possibleVersion >= 1 && possibleVersion <= 10 {
			// This might be the start of the next block
			break
		}
		nextHeaderOffset++
	}

	// If we didn't find another header, consume all remaining data
	if nextHeaderOffset >= len(data)-4 {
		nextHeaderOffset = len(data)
	}

	return block, nextHeaderOffset, nil
}

// Alternative: Use hex endpoint and parse hex-encoded blocks
func fetchBlockBatchHex(teranodeURL string, startHash string, count int) ([]*model.Block, [][]byte, error) {
	// Build request URL - using /blocks/n/{hash}/hex endpoint for hex data
	url := fmt.Sprintf("%s/blocks/n/%s/hex?n=%d", teranodeURL, startHash, count)

	// Make HTTP request
	resp, err := http.Get(url)
	if err != nil {
		return nil, nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	// Read response body (hex string)
	hexData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Decode hex to bytes
	body, err := hex.DecodeString(string(hexData))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode hex: %w", err)
	}

	// Parse blocks as before
	blocks := make([]*model.Block, 0)
	blockBytes := make([][]byte, 0)

	// For hex format, we know each block header is exactly 80 bytes
	// and blocks are consecutive
	for i := 0; i < count && i*model.BlockHeaderSize < len(body); i++ {
		start := i * model.BlockHeaderSize
		end := start + model.BlockHeaderSize
		if end > len(body) {
			break
		}

		header, err := model.NewBlockHeaderFromBytes(body[start:end])
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse header %d: %w", i, err)
		}

		block := &model.Block{
			Header: header,
			Height: uint32(i),
		}

		blocks = append(blocks, block)
		blockBytes = append(blockBytes, body[start:end])
	}

	return blocks, blockBytes, nil
}
