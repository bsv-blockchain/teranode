package testhelpers

import (
	"math/big"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// TestChainBuilder provides a unified interface for creating test chains
// This consolidates the following functions:
// - createTestHeaders()
// - createTestBlockChain()
// - createChainWithWork()
// - createHonestChain()
// - createMaliciousChain()
// - createAdversarialFork()
type TestChainBuilder struct {
	t             *testing.T
	length        int
	startHeight   uint32
	work          *big.Int
	forkPoint     int
	forkLength    int
	useMainnet    bool
	baseChain     []*model.BlockHeader
	nBits         *model.NBit
	timestampFunc func(int) uint32
	merkleFunc    func(int) *chainhash.Hash
}

// NewTestChainBuilder creates a new chain builder
func NewTestChainBuilder(t *testing.T) *TestChainBuilder {
	nBits, _ := model.NewNBitFromString("207fffff") // minimum difficulty
	return &TestChainBuilder{
		t:           t,
		length:      10,   // default length
		startHeight: 1000, // default start height
		work:        big.NewInt(1),
		nBits:       nBits,
		timestampFunc: func(i int) uint32 {
			// Default: timestamps progressing forward in time
			return uint32(time.Now().Unix() - int64((10-i)*600))
		},
		merkleFunc: func(i int) *chainhash.Hash {
			// Default: use GenerateMerkleRoot
			return GenerateMerkleRoot(i)
		},
	}
}

// WithLength sets the chain length
func (b *TestChainBuilder) WithLength(n int) *TestChainBuilder {
	b.length = n
	return b
}

// WithStartHeight sets the starting block height
func (b *TestChainBuilder) WithStartHeight(height uint32) *TestChainBuilder {
	b.startHeight = height
	return b
}

// WithWork sets the total work for the chain
func (b *TestChainBuilder) WithWork(work int64) *TestChainBuilder {
	b.work = big.NewInt(work)
	return b
}

// WithDifficulty sets the difficulty (nBits) for the chain
func (b *TestChainBuilder) WithDifficulty(nBitsStr string) *TestChainBuilder {
	nBits, err := model.NewNBitFromString(nBitsStr)
	if err != nil {
		b.t.Fatalf("Invalid nBits: %s", nBitsStr)
	}
	b.nBits = nBits
	return b
}

// WithFork adds a fork at specified point with given length
func (b *TestChainBuilder) WithFork(at, length int) *TestChainBuilder {
	b.forkPoint = at
	b.forkLength = length
	return b
}

// WithMainnetHeaders uses real mainnet headers
func (b *TestChainBuilder) WithMainnetHeaders() *TestChainBuilder {
	b.useMainnet = true
	return b
}

// WithBaseChain sets a base chain to build upon
func (b *TestChainBuilder) WithBaseChain(chain []*model.BlockHeader) *TestChainBuilder {
	b.baseChain = chain
	return b
}

// WithTimestampFunction sets a custom timestamp generation function
func (b *TestChainBuilder) WithTimestampFunction(fn func(int) uint32) *TestChainBuilder {
	b.timestampFunc = fn
	return b
}

// WithMerkleRootFunction sets a custom merkle root generation function
func (b *TestChainBuilder) WithMerkleRootFunction(fn func(int) *chainhash.Hash) *TestChainBuilder {
	b.merkleFunc = fn
	return b
}

// Build creates the chain according to specifications
func (b *TestChainBuilder) Build() []*model.BlockHeader {
	if b.useMainnet {
		return b.buildMainnetChain()
	}

	if b.forkLength > 0 {
		return b.buildForkedChain()
	}

	return b.buildSyntheticChain()
}

// GetHeaders returns the built chain headers
func (b *TestChainBuilder) GetHeaders() []*model.BlockHeader {
	return b.baseChain
}

// buildMainnetChain returns real mainnet headers
func (b *TestChainBuilder) buildMainnetChain() []*model.BlockHeader {
	return GetMainnetHeadersRange(b.t, 0, b.length)
}

// buildSyntheticChain creates a synthetic chain with valid PoW
func (b *TestChainBuilder) buildSyntheticChain() []*model.BlockHeader {
	headers := make([]*model.BlockHeader, b.length)

	var prevHash *chainhash.Hash
	if b.baseChain != nil && len(b.baseChain) > 0 {
		prevHash = b.baseChain[len(b.baseChain)-1].Hash()
	} else {
		// Start with genesis if no base chain
		genesis := GetMainnetHeaders(b.t, 1)[0]
		prevHash = genesis.Hash()
	}

	for i := 0; i < b.length; i++ {
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: b.merkleFunc(i),
			Timestamp:      b.timestampFunc(i),
			Bits:           *b.nBits,
			Nonce:          0,
		}

		// Mine the header for valid PoW
		MineHeader(header)
		headers[i] = header
		prevHash = header.Hash()
	}

	return headers
}

// buildForkedChain creates a chain with a fork
func (b *TestChainBuilder) buildForkedChain() []*model.BlockHeader {
	// Build main chain first
	mainChain := b.buildSyntheticChain()

	if b.forkPoint >= len(mainChain) {
		b.t.Fatalf("Fork point %d beyond chain length %d", b.forkPoint, len(mainChain))
	}

	// Create fork from specified point
	forkHeaders := make([]*model.BlockHeader, b.forkLength)
	prevHash := mainChain[b.forkPoint].Hash()

	for i := 0; i < b.forkLength; i++ {
		header := &model.BlockHeader{
			Version:       1,
			HashPrevBlock: prevHash,
			// Use different merkle roots to ensure fork is different
			HashMerkleRoot: b.merkleFunc(b.forkPoint + i + 1000),
			Timestamp:      b.timestampFunc(b.forkLength - i),
			Bits:           *b.nBits,
			Nonce:          0,
		}

		MineHeader(header)
		forkHeaders[i] = header
		prevHash = header.Hash()
	}

	// Return main chain up to fork point plus fork
	result := make([]*model.BlockHeader, 0, b.forkPoint+1+b.forkLength)
	result = append(result, mainChain[:b.forkPoint+1]...)
	result = append(result, forkHeaders...)

	return result
}

// BuildMultiple creates multiple independent chains with the same parameters
func (b *TestChainBuilder) BuildMultiple(count int) [][]*model.BlockHeader {
	chains := make([][]*model.BlockHeader, count)
	for i := 0; i < count; i++ {
		// Modify merkle root function for each chain to ensure uniqueness
		originalMerkleFunc := b.merkleFunc
		b.merkleFunc = func(idx int) *chainhash.Hash {
			// Add chain index to ensure different merkle roots
			return GenerateMerkleRoot(idx + i*1000)
		}
		chains[i] = b.Build()
		b.merkleFunc = originalMerkleFunc
	}
	return chains
}

// BuildWithCustomWork creates a chain with specific work values per block
func (b *TestChainBuilder) BuildWithCustomWork(workValues []int64) []*model.BlockHeader {
	if len(workValues) != b.length {
		b.t.Fatalf("Work values count %d doesn't match chain length %d", len(workValues), b.length)
	}

	headers := make([]*model.BlockHeader, b.length)
	var prevHash *chainhash.Hash

	if b.baseChain != nil && len(b.baseChain) > 0 {
		prevHash = b.baseChain[len(b.baseChain)-1].Hash()
	} else {
		genesis := GetMainnetHeaders(b.t, 1)[0]
		prevHash = genesis.Hash()
	}

	for i := 0; i < b.length; i++ {
		// Adjust difficulty based on work value
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: b.merkleFunc(i),
			Timestamp:      b.timestampFunc(i),
			Bits:           b.calculateNBitsForWork(workValues[i]),
			Nonce:          0,
		}

		MineHeader(header)
		headers[i] = header
		prevHash = header.Hash()
	}

	return headers
}

// calculateNBitsForWork calculates appropriate nBits for desired work
func (b *TestChainBuilder) calculateNBitsForWork(work int64) model.NBit {
	// Simple approximation - in reality this would be more complex
	if work > 1000000 {
		nBits, _ := model.NewNBitFromString("1d00ffff") // Higher difficulty
		return *nBits
	} else if work > 10000 {
		nBits, _ := model.NewNBitFromString("1f00ffff") // Medium difficulty
		return *nBits
	}
	return *b.nBits // Default minimum difficulty
}

// ChainType represents different types of test chains
type ChainType string

const (
	ChainTypeHonest      ChainType = "honest"
	ChainTypeMalicious   ChainType = "malicious"
	ChainTypeAdversarial ChainType = "adversarial"
	ChainTypeTimewarp    ChainType = "timewarp"
	ChainTypeHighWork    ChainType = "high_work"
)

// BuildType creates a specific type of chain
func (b *TestChainBuilder) BuildType(chainType ChainType) []*model.BlockHeader {
	switch chainType {
	case ChainTypeHonest:
		// Normal honest chain
		return b.Build()

	case ChainTypeMalicious:
		// Chain with suspicious characteristics
		b.WithTimestampFunction(func(i int) uint32 {
			// All timestamps the same (suspicious)
			return uint32(time.Now().Unix())
		})
		return b.Build()

	case ChainTypeAdversarial:
		// Chain designed to cause issues
		b.WithFork(b.length/2, b.length/3)
		return b.Build()

	case ChainTypeTimewarp:
		// Chain with time warp attack characteristics
		b.WithTimestampFunction(func(i int) uint32 {
			if i%2 == 0 {
				// Alternating future/past timestamps
				return uint32(time.Now().Unix() + 7200)
			}
			return uint32(time.Now().Unix() - 7200)
		})
		return b.Build()

	case ChainTypeHighWork:
		// Chain with very high cumulative work
		workValues := make([]int64, b.length)
		for i := range workValues {
			workValues[i] = int64(1000000 * (i + 1))
		}
		return b.BuildWithCustomWork(workValues)

	default:
		b.t.Fatalf("Unknown chain type: %s", chainType)
		return nil
	}
}

// CreateTestBlocks creates test blocks from headers
func (b *TestChainBuilder) CreateTestBlocks(headers []*model.BlockHeader, startHeight uint32) []*model.Block {
	blocks := make([]*model.Block, len(headers))
	for i, header := range headers {
		blocks[i] = &model.Block{
			Header: header,
			Height: startHeight + uint32(i),
		}
	}
	return blocks
}

// Validate ensures the chain is valid
func (b *TestChainBuilder) Validate(headers []*model.BlockHeader) error {
	if len(headers) == 0 {
		return nil
	}

	// Check chain linkage
	for i := 1; i < len(headers); i++ {
		if !headers[i].HashPrevBlock.IsEqual(headers[i-1].Hash()) {
			b.t.Errorf("Chain broken at index %d: prev hash mismatch", i)
		}
	}

	// Check timestamps are ordered (allowing some deviation)
	for i := 1; i < len(headers); i++ {
		if headers[i].Timestamp < headers[i-1].Timestamp-7200 {
			b.t.Logf("Warning: Timestamp goes backwards at index %d", i)
		}
	}

	return nil
}
