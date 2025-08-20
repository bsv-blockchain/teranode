package testhelpers

import (
	"testing"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/mock"
)

// MockBuilder helps set up mocks for tests
type MockBuilder struct {
	suite         *CatchupTestSuite
	bestBlock     *model.Block
	bestHeight    uint32
	targetBlock   *model.Block
	targetHeight  uint32
	validatorMock bool
}

// NewMockBuilder creates a new mock builder for the test suite
func (s *CatchupTestSuite) NewMockBuilder() *MockBuilder {
	return &MockBuilder{
		suite: s,
	}
}

// WithBestBlock sets the best block for the mock
func (b *MockBuilder) WithBestBlock(header *model.BlockHeader, height uint32) *MockBuilder {
	b.bestBlock = &model.Block{
		Header: header,
		Height: height,
	}
	b.bestHeight = height
	return b
}

// WithTargetBlock sets the target block for the mock
func (b *MockBuilder) WithTargetBlock(block *model.Block) *MockBuilder {
	b.targetBlock = block
	b.targetHeight = block.Height
	return b
}

// WithValidator enables validator mock setup
func (b *MockBuilder) WithValidator() *MockBuilder {
	b.validatorMock = true
	return b
}

// BuildWithDefaults builds the mocks with default expectations
func (b *MockBuilder) BuildWithDefaults(t *testing.T) {
	// Setup blockchain mock
	if b.suite.MockBlockchain != nil {
		// Setup best block
		if b.bestBlock != nil {
			hash := b.bestBlock.Header.Hash()
			b.suite.MockBlockchain.On("BestHeader").Return(&model.Block{
				Header: b.bestBlock.Header,
				Height: b.bestHeight,
			}, nil).Maybe()

			b.suite.MockBlockchain.On("GetBlockHeader", mock.Anything, hash).Return(b.bestBlock.Header, nil).Maybe()
		}

		// Setup target block if provided
		if b.targetBlock != nil {
			targetHash := b.targetBlock.Header.Hash()
			b.suite.MockBlockchain.On("GetBlockHeader", mock.Anything, targetHash).Return(b.targetBlock.Header, nil).Maybe()
		}

		// Setup common blockchain expectations
		b.suite.MockBlockchain.On("GetCheckpoint").Return(uint32(0), &chainhash.Hash{}, nil).Maybe()
		b.suite.MockBlockchain.On("GetChainWork", mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	}

	// Setup UTXO store mock
	if b.suite.MockUTXOStore != nil {
		// Add common UTXO store expectations if needed
	}
}

// Build builds the mocks without defaults
func (b *MockBuilder) Build() {
	// This can be extended for custom mock setups
}
