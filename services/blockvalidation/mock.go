package blockvalidation

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/stretchr/testify/mock"
)

// Mock implements a mock version of the block validation client for testing.
type Mock struct {
	mock.Mock
}

// NewMock creates a new mock client.
//
// Returns:
//   - *Mock: Mock client implementation
func NewMock() *Mock {
	return &Mock{}
}

// Health performs a mock health check.
func (m *Mock) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	args := m.Called(ctx, checkLiveness)

	if args.Error(2) != nil {
		return 0, "", args.Error(2)
	}

	return args.Int(0), args.String(1), args.Error(2)
}

// BlockFound performs a mock block found notification.
func (m *Mock) BlockFound(ctx context.Context, blockHash *chainhash.Hash, baseURL string, waitToComplete bool) error {
	args := m.Called(ctx, blockHash, baseURL, waitToComplete)
	return args.Error(0)
}

// ProcessBlock performs a mock block processing.
func (m *Mock) ProcessBlock(ctx context.Context, block *model.Block, blockHeight uint32, peerID, baseURL string) error {
	args := m.Called(ctx, block, blockHeight)
	return args.Error(0)
}

// ValidateBlock performs a mock block validation.
func (m *Mock) ValidateBlock(ctx context.Context, block *model.Block, options *ValidateBlockOptions) error {
	args := m.Called(ctx, block, options)
	return args.Error(0)
}

func (m *Mock) RevalidateBlock(ctx context.Context, blockHash chainhash.Hash) error {
	args := m.Called(ctx, blockHash)
	return args.Error(0)
}

// GetCatchupStatus performs a mock catchup status retrieval.
func (m *Mock) GetCatchupStatus(ctx context.Context) (*CatchupStatus, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*CatchupStatus), args.Error(1)
}

// mockKafkaConsumer implements kafka.KafkaConsumerGroupI for testing
type mockKafkaConsumer struct {
	mock.Mock
}

func (m *mockKafkaConsumer) Start(ctx context.Context, consumerFn func(message *kafka.KafkaMessage) error, opts ...kafka.ConsumerOption) {
	m.Called(ctx, consumerFn, opts)
}

func (m *mockKafkaConsumer) BrokersURL() []string {
	args := m.Called()
	return args.Get(0).([]string)
}

func (m *mockKafkaConsumer) Close() error {
	args := m.Called()
	return args.Error(0)
}
