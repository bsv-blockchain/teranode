package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
)

// BlockHeader represents the JSON structure returned by the asset server
type BlockHeader struct {
	Hash           string `json:"hash"`
	Height         int    `json:"height"`
	Version        int    `json:"version"`
	HashPrevBlock  string `json:"previousblockhash"`
	HashMerkleRoot string `json:"merkleroot"`
	Timestamp      int64  `json:"time"`
	Bits           string `json:"bits"`
	Nonce          int64  `json:"nonce"`
}

func main() {
	// Parse command-line arguments
	var (
		baseURL        string
		blockHash      string
		validateBoth   bool
		validateLegacy bool
	)
	flag.StringVar(&baseURL, "url", "https://us-east-1.teranode.space", "Base URL of the teranode instance")
	flag.StringVar(&blockHash, "hash", "00000000151e54c0ef1c023bca89d3fe904b353dc9316979000eedfa5de08d31", "Block hash to validate")
	flag.BoolVar(&validateBoth, "validate-both", false, "Validate both /block and /block_legacy endpoints")
	flag.BoolVar(&validateLegacy, "validate-legacy", false, "Use only legacy block endpoint for validation")
	flag.Parse()

	log.Printf("Validating block %s from %s\n", blockHash, baseURL)

	if validateBoth {
		// Validate both endpoints and compare
		log.Printf("=== Validating both /block and /block_legacy endpoints ===\n")

		// First validate /block endpoint
		log.Printf("\n--- Validating /block endpoint ---\n")
		blockRoot, err := validateBlockEndpoint(baseURL, blockHash)
		if err != nil {
			log.Fatalf("/block validation failed: %v", err)
		}

		// Then validate /block_legacy endpoint
		log.Printf("\n--- Validating /block_legacy endpoint ---\n")
		legacyRoot, err := validateLegacyBlock(baseURL, blockHash)
		if err != nil {
			log.Fatalf("/block_legacy validation failed: %v", err)
		}

		// Compare the results
		log.Printf("\n=== Comparing results ===\n")
		log.Printf("/block endpoint merkle root:    %s\n", blockRoot.String())
		log.Printf("/block_legacy endpoint merkle root: %s\n", legacyRoot.String())

		if blockRoot.String() == legacyRoot.String() {
			log.Printf("✓ Both endpoints produce identical merkle roots!\n")
		} else {
			log.Printf("✗ MISMATCH: Endpoints produce different merkle roots!\n")
			os.Exit(1)
		}
		return
	}

	if validateLegacy {
		// Use legacy block endpoint only
		if _, err := validateLegacyBlock(baseURL, blockHash); err != nil {
			log.Fatalf("Legacy validation failed: %v", err)
		}
		return
	}

	// Default to /block endpoint validation
	if _, err := validateBlockEndpoint(baseURL, blockHash); err != nil {
		log.Fatalf("Block validation failed: %v", err)
	}
}

// validateBlockEndpoint validates merkle root using the /block endpoint
func validateBlockEndpoint(baseURL, blockHash string) (*chainhash.Hash, error) {
	// Fetch block header
	blockHeader, err := fetchBlockHeader(baseURL, blockHash)
	if err != nil {
		return nil, errors.NewError("failed to fetch block header: %v", err)
	}

	log.Printf("Block header merkle root: %s\n", blockHeader.HashMerkleRoot)

	// Fetch block data to get subtree hashes
	blockData, err := fetchBlockData(baseURL, blockHash)
	if err != nil {
		log.Fatalf("Failed to fetch block data: %v", err)
	}

	// Check if block has subtrees
	subtrees, ok := blockData["subtrees"].([]interface{})
	if !ok {
		log.Fatalf("Invalid subtrees format in block data")
	}

	log.Printf("Block has %d subtree(s)\n", len(subtrees))

	var calculatedRoot *chainhash.Hash

	// Handle coinbase-only blocks (no subtrees)
	if len(subtrees) == 0 {
		log.Printf("Block contains only coinbase transaction (no subtrees)\n")

		// For blocks with only a coinbase, the merkle root is the coinbase TX hash itself
		coinbaseTxID, err := fetchCoinbaseTxID(baseURL, blockHash)
		if err != nil {
			log.Fatalf("Failed to fetch coinbase transaction ID: %v", err)
		}

		log.Printf("Coinbase transaction ID: %s\n", coinbaseTxID)

		calculatedRoot, err = chainhash.NewHashFromStr(coinbaseTxID)
		if err != nil {
			log.Fatalf("Failed to parse coinbase transaction ID: %v", err)
		}

		log.Printf("For coinbase-only block, merkle root = coinbase TX hash\n")
	} else if len(subtrees) == 1 {
		// Single subtree case - the subtree root is the block merkle root
		subtreeHash, ok := subtrees[0].(string)
		if !ok {
			log.Fatalf("Invalid subtree hash format")
		}

		log.Printf("Processing single subtree: %s\n", subtreeHash)
		calculatedRoot = validateSingleSubtree(baseURL, blockHash, subtreeHash)
	} else {
		// Multiple subtrees - need to build top-level merkle tree
		log.Printf("Processing %d subtrees\n", len(subtrees))

		subtreeRoots := make([]*chainhash.Hash, 0, len(subtrees))

		for i, subtreeInterface := range subtrees {
			subtreeHash, ok := subtreeInterface.(string)
			if !ok {
				log.Fatalf("Invalid subtree hash format at index %d", i)
			}

			log.Printf("Processing subtree %d/%d: %s\n", i+1, len(subtrees), subtreeHash)

			// For the first subtree (contains coinbase), validate it fully
			if i == 0 {
				subtreeRoot := validateSingleSubtree(baseURL, blockHash, subtreeHash)
				subtreeRoots = append(subtreeRoots, subtreeRoot)
			} else {
				// For other subtrees, we can use the hash directly as they don't have coinbase
				hash, err := chainhash.NewHashFromStr(subtreeHash)
				if err != nil {
					log.Fatalf("Failed to parse subtree hash: %v", err)
				}
				subtreeRoots = append(subtreeRoots, hash)
			}
		}

		// Build top-level merkle tree from subtree roots
		height := int(math.Ceil(math.Log2(float64(len(subtreeRoots)))))
		topTree, err := subtree.NewTree(height)
		if err != nil {
			log.Fatalf("Failed to create top-level tree: %v", err)
		}

		for _, root := range subtreeRoots {
			if err := topTree.AddNode(*root, 0, 0); err != nil {
				log.Fatalf("Failed to add subtree root to top tree: %v", err)
			}
		}

		calculatedRoot = topTree.RootHash()
		log.Printf("Calculated top-level merkle root from %d subtrees: %s\n", len(subtrees), calculatedRoot.String())
	}

	log.Printf("Final calculated merkle root: %s\n", calculatedRoot.String())

	// Parse the expected merkle root from block header
	expectedRoot, err := chainhash.NewHashFromStr(blockHeader.HashMerkleRoot)
	if err != nil {
		log.Fatalf("Failed to parse block header merkle root: %v", err)
	}

	// Validate the merkle root
	if calculatedRoot.String() == expectedRoot.String() {
		log.Printf("✓ Merkle root validation PASSED\n")
		log.Printf("  Expected: %s\n", expectedRoot.String())
		log.Printf("  Calculated: %s\n", calculatedRoot.String())
		return calculatedRoot, nil
	}

	return nil, errors.NewError("merkle root validation failed - expected: %s, calculated: %s",
		expectedRoot.String(), calculatedRoot.String())
}

func fetch(url string) ([]byte, error) {
	// Fetch data from URL and return byte array
	client := http.DefaultClient
	res, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = res.Body.Close()
	}()

	if res.StatusCode != http.StatusOK {
		body, err := io.ReadAll(res.Body)
		if err != nil {
			return nil, errors.NewError("HTTP %d: failed to read response body", res.StatusCode)
		}
		return nil, errors.NewError("HTTP %d: %s", res.StatusCode, string(body))
	}

	return io.ReadAll(res.Body)
}

func fetchBlockHeader(baseURL, blockHash string) (*BlockHeader, error) {
	url := fmt.Sprintf("%s/api/v1/header/%s/json", baseURL, blockHash)
	log.Printf("Fetching block header from: %s\n", url)

	body, err := fetch(url)
	if err != nil {
		return nil, errors.NewError("failed to fetch block header: %v", err)
	}

	var header BlockHeader
	if err := json.Unmarshal(body, &header); err != nil {
		return nil, errors.NewError("failed to parse block header JSON: %v", err)
	}

	return &header, nil
}

func fetchBlockData(baseURL, blockHash string) (map[string]interface{}, error) {
	blockURL := fmt.Sprintf("%s/api/v1/block/%s/json", baseURL, blockHash)
	log.Printf("Fetching block data from: %s\n", blockURL)

	body, err := fetch(blockURL)
	if err != nil {
		return nil, errors.NewError("failed to fetch block data: %v", err)
	}

	var blockData map[string]interface{}
	if err := json.Unmarshal(body, &blockData); err != nil {
		return nil, errors.NewError("failed to parse block JSON: %v", err)
	}

	return blockData, nil
}

func fetchSubtree(baseURL, subtreeHash string) ([]byte, error) {
	subtreeURL := fmt.Sprintf("%s/api/v1/subtree/%s", baseURL, subtreeHash)
	log.Printf("Fetching subtree from: %s\n", subtreeURL)

	return fetch(subtreeURL)
}

func fetchCoinbaseTxID(baseURL, blockHash string) (string, error) {
	// First, try to get block info to find coinbase tx
	blockData, err := fetchBlockData(baseURL, blockHash)
	if err != nil {
		return "", err
	}

	// Look for coinbase_tx field
	if coinbaseTx, ok := blockData["coinbase_tx"].(map[string]interface{}); ok {
		// Check for txid field (this is the correct field name)
		if txIDStr, ok := coinbaseTx["txid"].(string); ok {
			return txIDStr, nil
		}

		// Fallback to tx_id if txid not found
		if txIDStr, ok := coinbaseTx["tx_id"].(string); ok {
			return txIDStr, nil
		}

		// If neither found, calculate from hex if available
		if hexStr, ok := coinbaseTx["hex"].(string); ok {
			// Parse the transaction from hex and calculate its ID
			txBytes, err := hex.DecodeString(hexStr)
			if err == nil {
				tx, err := bt.NewTxFromBytes(txBytes)
				if err == nil {
					return tx.TxID(), nil
				}
			}
		}
	}

	// If not found in block data, try to fetch directly (assuming first tx is coinbase)
	// This is a fallback approach
	log.Printf("Warning: Coinbase not found in block data, using fallback method\n")
	return "", errors.NewError("coinbase transaction not found in block data")
}

func validateSingleSubtree(baseURL, blockHash, subtreeHash string) *chainhash.Hash {
	// Fetch subtree data
	subtreeNodeBytes, err := fetchSubtree(baseURL, subtreeHash)
	if err != nil {
		log.Fatalf("Failed to fetch subtree data: %v", err)
	}

	// Confirm bytes are multiple of 32
	subtreeNodeLen := len(subtreeNodeBytes)
	if subtreeNodeLen%32 != 0 {
		log.Fatalf("Subtree node bytes length is not multiple of 32: %d", subtreeNodeLen)
	}

	numTxs := subtreeNodeLen / 32
	log.Printf("Subtree has %d transactions\n", numTxs)

	// Find the coinbase transaction hash position
	coinbaseIndex := -1
	for i := 0; i < numTxs; i++ {
		start := i * 32
		end := start + 32
		txHashBytes := subtreeNodeBytes[start:end]
		txChainHash, err := chainhash.NewHash(txHashBytes)
		if err != nil {
			log.Printf("Warning: Failed to create chainhash from bytes at index %d: %v", i, err)
			continue
		}
		if txChainHash.String() == subtree.CoinbasePlaceholderHash.String() {
			coinbaseIndex = i
			log.Printf("Found coinbase placeholder at index %d\n", i)
			break
		}
	}

	// Create a copy of the subtree bytes for modification
	modifiedSubtreeBytes := make([]byte, len(subtreeNodeBytes))
	copy(modifiedSubtreeBytes, subtreeNodeBytes)

	// If this subtree has a coinbase, replace the placeholder
	if coinbaseIndex != -1 {
		// Fetch the actual coinbase transaction to get its hash
		coinbaseTxID, err := fetchCoinbaseTxID(baseURL, blockHash)
		if err != nil {
			log.Fatalf("Failed to fetch coinbase transaction ID: %v", err)
		}

		log.Printf("Coinbase transaction ID: %s\n", coinbaseTxID)

		// Replace placeholder with actual coinbase hash
		coinbaseHash, err := chainhash.NewHashFromStr(coinbaseTxID)
		if err != nil {
			log.Fatalf("Failed to parse coinbase transaction ID: %v", err)
		}

		copy(modifiedSubtreeBytes[coinbaseIndex*32:(coinbaseIndex+1)*32], coinbaseHash[:])
	}

	// Build the subtree with actual transaction hashes
	height := int(math.Ceil(math.Log2(float64(numTxs))))
	newtree, err := subtree.NewTree(height)
	if err != nil {
		log.Fatalf("Failed to create subtree: %v", err)
	}

	for i := 0; i < numTxs; i++ {
		start := i * 32
		end := start + 32
		txHashBytes := modifiedSubtreeBytes[start:end]
		txChainHash, err := chainhash.NewHash(txHashBytes)
		if err != nil {
			log.Printf("Warning: Failed to create chainhash from bytes at index %d: %v", i, err)
			continue
		}
		err = newtree.AddNode(*txChainHash, 0, 0)
		if err != nil {
			log.Printf("Warning: Failed to add node for tx %d: %v", i, err)
			continue
		}
	}

	log.Printf("Calculated subtree merkle root: %s\n", newtree.RootHash().String())
	return newtree.RootHash()
}

func validateLegacyBlock(baseURL, blockHash string) (*chainhash.Hash, error) {
	log.Printf("Using legacy block endpoint for validation\n")

	// Fetch the legacy block data with wire=1 parameter
	legacyURL := fmt.Sprintf("%s/api/v1/block_legacy/%s?wire=1", baseURL, blockHash)
	log.Printf("Fetching legacy block from: %s\n", legacyURL)

	blockData, err := fetch(legacyURL)
	if err != nil {
		return nil, errors.NewError("failed to fetch legacy block: %v", err)
	}

	log.Printf("Fetched %d bytes of legacy block data\n", len(blockData))

	// Parse the wire format block
	reader := bytes.NewReader(blockData)

	// Parse block using wire.MsgBlock
	msgBlock := &wire.MsgBlock{}
	if err := msgBlock.Deserialize(reader); err != nil {
		return nil, errors.NewError("failed to deserialize block: %v", err)
	}

	log.Printf("Parsed block with %d transactions\n", len(msgBlock.Transactions))

	// Debug: Print first few transaction hashes
	if len(msgBlock.Transactions) > 0 {
		for i := 0; i < 5 && i < len(msgBlock.Transactions); i++ {
			txHash := msgBlock.Transactions[i].TxHash()
			log.Printf("  Tx[%d]: %s\n", i, txHash.String())
		}
		if len(msgBlock.Transactions) > 5 {
			// Also print last transaction
			lastIdx := len(msgBlock.Transactions) - 1
			txHash := msgBlock.Transactions[lastIdx].TxHash()
			log.Printf("  Tx[%d]: %s\n", lastIdx, txHash.String())
		}
	}

	// Extract merkle root from header
	expectedRoot := msgBlock.Header.MerkleRoot
	log.Printf("Expected merkle root from header: %s\n", expectedRoot.String())

	// Calculate merkle root from transactions
	var calculatedRoot *chainhash.Hash

	if len(msgBlock.Transactions) == 0 {
		return nil, errors.NewError("block has no transactions")
	} else if len(msgBlock.Transactions) == 1 {
		// Single transaction - merkle root is the transaction hash
		txHash := msgBlock.Transactions[0].TxHash()
		calculatedRoot = &txHash
		log.Printf("Single transaction block, merkle root = tx hash: %s\n", calculatedRoot.String())
	} else {
		// Multiple transactions - build merkle tree
		txHashes := make([]*chainhash.Hash, 0, len(msgBlock.Transactions))

		for _, tx := range msgBlock.Transactions {
			txHash := tx.TxHash()
			txHashes = append(txHashes, &txHash)
		}

		log.Printf("Total transactions: %d\n", len(txHashes))

		// Try both methods: flat and hierarchical
		// Method 1: Flat merkle tree (traditional Bitcoin)
		flatRoot := buildMerkleRoot(txHashes)
		log.Printf("Flat merkle root from %d transactions: %s\n", len(txHashes), flatRoot.String())

		// Method 2: Hierarchical (teranode-style with subtrees)
		// Split into subtrees of 2048 transactions each
		subtreeSize := 2048
		var subtreeRoots []*chainhash.Hash

		for i := 0; i < len(txHashes); i += subtreeSize {
			end := i + subtreeSize
			if end > len(txHashes) {
				end = len(txHashes)
			}

			subtreeTxs := txHashes[i:end]
			subtreeRoot := buildMerkleRoot(subtreeTxs)
			subtreeRoots = append(subtreeRoots, subtreeRoot)
		}

		log.Printf("Created %d subtree roots from groups of %d transactions\n", len(subtreeRoots), subtreeSize)
		hierarchicalRoot := buildMerkleRoot(subtreeRoots)
		log.Printf("Hierarchical merkle root from %d subtree roots: %s\n", len(subtreeRoots), hierarchicalRoot.String())

		// Check which one matches
		if hierarchicalRoot.String() == expectedRoot.String() {
			log.Printf("✓ Using hierarchical (teranode) merkle calculation\n")
			calculatedRoot = hierarchicalRoot
		} else {
			log.Printf("Using flat merkle calculation\n")
			calculatedRoot = flatRoot
		}
	}

	// Validate the merkle root
	if calculatedRoot.String() == expectedRoot.String() {
		log.Printf("✓ Legacy merkle root validation PASSED\n")
		log.Printf("  Expected: %s\n", expectedRoot.String())
		log.Printf("  Calculated: %s\n", calculatedRoot.String())
		return calculatedRoot, nil
	}

	return nil, errors.NewError("legacy merkle root validation failed - expected: %s, calculated: %s",
		expectedRoot.String(), calculatedRoot.String())
}

// buildMerkleRoot calculates the merkle root from a list of transaction hashes
// using the Bitcoin merkle tree algorithm
func buildMerkleRoot(txHashes []*chainhash.Hash) *chainhash.Hash {
	if len(txHashes) == 0 {
		return &chainhash.Hash{}
	}
	if len(txHashes) == 1 {
		return txHashes[0]
	}

	// Build merkle tree level by level
	currentLevel := make([]*chainhash.Hash, len(txHashes))
	copy(currentLevel, txHashes)

	for len(currentLevel) > 1 {
		// If odd number, duplicate last hash
		if len(currentLevel)%2 != 0 {
			currentLevel = append(currentLevel, currentLevel[len(currentLevel)-1])
		}

		nextLevel := make([]*chainhash.Hash, 0, len(currentLevel)/2)
		for i := 0; i < len(currentLevel); i += 2 {
			// Concatenate and hash pairs
			combined := make([]byte, 64)
			copy(combined[:32], currentLevel[i][:])
			copy(combined[32:], currentLevel[i+1][:])

			hash := chainhash.DoubleHashH(combined)
			nextLevel = append(nextLevel, &hash)
		}
		currentLevel = nextLevel
	}

	return currentLevel[0]
}
