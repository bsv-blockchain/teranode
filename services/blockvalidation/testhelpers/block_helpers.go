package testhelpers

import (
	"fmt"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockvalidation/catchup"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// CreateTestBlocks creates a chain of test block headers
func CreateTestBlocks(t *testing.T, count int) []*model.Block {
	return CreateTestBlocksWithPrev(t, count, nil)
}

// CreateTestBlocksWithPrev creates a chain of test blocks with a previous hash
func CreateTestBlocksWithPrev(t *testing.T, count int, prevHash *chainhash.Hash) []*model.Block {
	blocks := make([]*model.Block, count)
	if prevHash == nil {
		prevHash = &chainhash.Hash{}
	}

	nBits, err := model.NewNBitFromString("207fffff") // minimum difficulty
	if err != nil {
		t.Fatalf("Failed to create NBit: %v", err)
	}

	for i := 0; i < count; i++ {
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: GenerateMerkleRoot(i),
			Timestamp:      uint32(1600000000 + i*600), // 10 minutes apart
			Bits:           *nBits,
			Nonce:          0,
		}
		MineHeader(header)

		coinbaseTx := CreateSimpleCoinbaseTx(uint32(i)) // golint:nolint

		blocks[i] = &model.Block{
			Header:     header,
			CoinbaseTx: coinbaseTx,
			Height:     uint32(i), // goling:nolint
			// Add minimal valid block data - can be expanded as needed
		}

		hash := header.Hash()
		prevHash = hash
	}

	return blocks
}

// GenerateMerkleRoot creates a deterministic merkle root for testing
func GenerateMerkleRoot(index int) *chainhash.Hash {
	data := fmt.Sprintf("merkle_root_%d", index)
	hash := chainhash.HashH([]byte(data))
	return &hash
}

// MineHeader performs simple mining to get valid proof of work
func MineHeader(header *model.BlockHeader) {
	// Mine to meet target difficulty like other tests do
	for {
		if ok, _, _ := header.HasMetTargetDifficulty(); ok {
			break
		}
		header.Nonce++
	}
}

// HeadersToBytes converts headers to byte representation
func HeadersToBytes(headers []*model.BlockHeader) []byte {
	headerBytes := make([]byte, 0, len(headers)*model.BlockHeaderSize)
	for _, h := range headers {
		headerBytes = append(headerBytes, h.Bytes()...)
	}
	return headerBytes
}

// CreateAdversarialFork creates a fork chain that branches from a valid chain
// This is used for testing secret mining and malicious fork scenarios
func CreateAdversarialFork(validChain []*model.BlockHeader, forkPoint int, forkLength int) ([]*model.BlockHeader, error) {
	if forkPoint >= len(validChain) {
		return nil, errors.ErrInvalidArgument
	}

	// Start the fork from the specified point in the valid chain
	forkHeaders := make([]*model.BlockHeader, forkLength)
	nBits, _ := model.NewNBitFromString("207fffff") // Minimum difficulty

	// add the fork point header as the first
	if forkPoint < 0 || forkPoint >= len(validChain) {
		return nil, errors.NewInvalidArgumentError("fork point %d is out of bounds for valid chain length %d", forkPoint, len(validChain))
	}
	forkHeaders[0] = validChain[forkPoint]

	for i := 1; i < forkLength; i++ {
		header := &model.BlockHeader{
			Version: 1,
			HashPrevBlock: func() *chainhash.Hash {
				if i == 0 {
					// Fork from the valid chain at forkPoint
					return validChain[forkPoint].Hash()
				}
				return forkHeaders[i-1].Hash()
			}(),
			HashMerkleRoot: GenerateMerkleRoot(forkPoint + i + 1),
			Timestamp:      uint32(1600000000 + int64((forkPoint+i+1)*600)), // Past timestamps, progressing forward
			Bits:           *nBits,
			Nonce:          0,
		}

		// Mine the header
		MineHeader(header)
		forkHeaders[i] = header
	}

	return forkHeaders, nil
}

// CreateTestHeaders This version accepts testing.T for compatibility with existing tests
func CreateTestHeaders(t *testing.T, count int) []*model.BlockHeader {
	blocks := CreateTestBlocks(t, count)
	headers := make([]*model.BlockHeader, count)
	for i, block := range blocks {
		headers[i] = block.Header
	}
	return headers
}

// CreateTestBlockChain creates a simple test blockchain - now returns blocks
func CreateTestBlockChain(t *testing.T, length int) []*model.Block {
	return CreateTestBlocks(t, length)
}

// CreateChainWithWork creates a chain with specific work values
// The startHeight parameter allows creating chains at different heights
func CreateChainWithWork(t *testing.T, length int, startHeight int, workPerBlock int64) []*model.Block {
	blocks := CreateTestBlocks(t, length)
	// In a real implementation, we'd adjust the difficulty/nBits based on work
	// For testing, the blocks already have valid PoW
	return blocks
}

// CreateMaliciousChain creates a chain with suspicious characteristics
func CreateMaliciousChain(length int) []*model.BlockHeader {
	headers := make([]*model.BlockHeader, length)
	prevHash := &chainhash.Hash{}
	nBits, _ := model.NewNBitFromString("207fffff")

	for i := 0; i < length; i++ {
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: GenerateMerkleRoot(i + 10000),
			Timestamp:      uint32(time.Now().Unix()), // Suspicious: same timestamp
			Bits:           *nBits,
			Nonce:          0,
		}
		MineHeader(header)
		headers[i] = header
		prevHash = header.Hash()
	}

	return headers
}

// CreateMaliciousChainFromValid creates a malicious fork from a valid chain
func CreateMaliciousChainFromValid(t *testing.T, validChain []*model.BlockHeader, forkPoint int, forkLength int) []*model.BlockHeader {
	if forkPoint >= len(validChain) {
		t.Fatalf("Fork point %d beyond chain length %d", forkPoint, len(validChain))
	}

	forkHeaders := make([]*model.BlockHeader, forkLength)
	prevHash := validChain[forkPoint].Hash()
	nBits, _ := model.NewNBitFromString("207fffff")

	for i := 0; i < forkLength; i++ {
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: GenerateMerkleRoot(forkPoint + i + 1000),
			Timestamp:      uint32(time.Now().Unix()), // Suspicious: same timestamp
			Bits:           *nBits,
			Nonce:          0,
		}
		MineHeader(header)
		forkHeaders[i] = header
		prevHash = header.Hash()
	}

	return forkHeaders
}

// BlocksToHeaderBytes converts blocks to header bytes for wire protocol
func BlocksToHeaderBytes(blocks []*model.Block) []byte {
	headers := make([]*model.BlockHeader, len(blocks))
	for i, block := range blocks {
		headers[i] = block.Header
	}
	return HeadersToBytes(headers)
}

// CreateTestHeaderAtHeight creates a single test header at a specific height
func CreateTestHeaderAtHeight(height int) *model.BlockHeader {
	nBits, _ := model.NewNBitFromString("207fffff")
	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: GenerateMerkleRoot(height),
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           *nBits,
		Nonce:          0,
	}
	MineHeader(header)
	return header
}

// CreateSyntheticChainFrom creates a synthetic chain starting from a given header
func CreateSyntheticChainFrom(prevHeader *model.BlockHeader, length int) []*model.BlockHeader {
	headers := make([]*model.BlockHeader, length)
	prevHash := prevHeader.Hash()
	nBits, _ := model.NewNBitFromString("207fffff")

	for i := 0; i < length; i++ {
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: GenerateMerkleRoot(i),
			Timestamp:      uint32(time.Now().Unix() + int64(i*600)),
			Bits:           *nBits,
			Nonce:          0,
		}
		MineHeader(header)
		headers[i] = header
		prevHash = header.Hash()
	}

	return headers
}

// NewChainBuilder creates a new test chain builder
func NewChainBuilder(t *testing.T) *TestChainBuilder {
	return NewTestChainBuilder(t)
}

// CreateSimpleCoinbaseTx creates a simple coinbase transaction for testing
func CreateSimpleCoinbaseTx(height uint32) *bt.Tx {
	coinbaseTx := bt.NewTx()
	_ = coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)

	// Add proper coinbase script with minimum required length
	// Format: block height (variable length integer) + extra data
	heightBytes := make([]byte, 4)
	heightBytes[0] = byte(height)
	heightBytes[1] = byte(height >> 8)
	heightBytes[2] = byte(height >> 16)
	heightBytes[3] = byte(height >> 24)

	// Create unique coinbase script with height
	scriptData := []byte{0x04} // 4 bytes of height data
	scriptData = append(scriptData, heightBytes...)
	scriptData = append(scriptData, '/', 'T', 'e', 's', 't')
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(scriptData)

	// Add a simple P2PKH output
	_ = coinbaseTx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000) // 50 BSV to first Bitcoin address

	return coinbaseTx
}

// ============================================================================
// Test Server Setup
// ============================================================================

// TestServerConfig contains configuration for test server setup
type TestServerConfig struct {
	SecretMiningThreshold int
	MaxRetries            int
	IterationTimeout      int
	OperationTimeout      int
	CoinbaseMaturity      uint16
	CircuitBreakerConfig  *catchup.CircuitBreakerConfig
}

// DefaultTestServerConfig returns default test server configuration
func DefaultTestServerConfig() *TestServerConfig {
	return &TestServerConfig{
		SecretMiningThreshold: 200,
		MaxRetries:            3,
		IterationTimeout:      5,
		OperationTimeout:      30,
		CoinbaseMaturity:      100,
		CircuitBreakerConfig: &catchup.CircuitBreakerConfig{
			FailureThreshold:    3,
			SuccessThreshold:    2,
			Timeout:             time.Minute,
			MaxHalfOpenRequests: 1,
		},
	}
}
