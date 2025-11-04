package subtreevalidation

import (
	"context"
	"os"
	"testing"

	"github.com/IBM/sarama"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"
)

func TestMain(m *testing.M) {
	InitPrometheusMetrics()
	exitCode := m.Run()
	os.Exit(exitCode)
}

type mockLogger struct {
	mock.Mock
}

func (m *mockLogger) LogLevel() int {
	return 0
}

func (m *mockLogger) SetLogLevel(level string) {}

func (m *mockLogger) Debugf(format string, args ...interface{}) {
	m.Called(format, args)
}

func (m *mockLogger) Infof(format string, args ...interface{}) {
	m.Called(format, args)
}

func (m *mockLogger) Warnf(format string, args ...interface{}) {
	m.Called(format, args)
}

func (m *mockLogger) Errorf(format string, args ...interface{}) {
	m.Called(format, args)
}

func (m *mockLogger) Fatalf(format string, args ...interface{}) {
	m.Called(format, args)
}

func (m *mockLogger) New(service string, options ...ulogger.Option) ulogger.Logger {
	return m
}

func (m *mockLogger) Duplicate(options ...ulogger.Option) ulogger.Logger {
	return m
}

type mockCache struct {
	mock.Mock
	txmetacache.TxMetaCache
}

func (m *mockCache) Delete(ctx context.Context, hash *chainhash.Hash) error {
	args := m.Called(ctx, hash)
	return args.Error(0)
}

func (m *mockCache) SetCacheFromBytes(key, txMetaBytes []byte) error {
	args := m.Called(key, txMetaBytes)
	return args.Error(0)
}

func (m *mockCache) BatchDecorate(ctx context.Context, txs []*utxo.UnresolvedMetaData, fields ...fields.FieldName) error {
	args := m.Called(ctx, txs, fields)
	return args.Error(0)
}

func (m *mockCache) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	args := m.Called(ctx, tx, blockHeight, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*meta.Data), args.Error(1)
}

func (m *mockCache) Get(ctx context.Context, hash *chainhash.Hash, fields ...fields.FieldName) (*meta.Data, error) {
	args := m.Called(ctx, hash, fields)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*meta.Data), args.Error(1)
}

func (m *mockCache) GetMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error) {
	args := m.Called(ctx, hash)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*meta.Data), args.Error(1)
}

func (m *mockCache) GetSpend(ctx context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	args := m.Called(ctx, spend)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*utxo.SpendResponse), args.Error(1)
}

func (m *mockCache) Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	args := m.Called(ctx, tx, blockHeight, ignoreFlags)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]*utxo.Spend), args.Error(1)
}

func (m *mockCache) UnSpend(ctx context.Context, spends []*utxo.Spend) error {
	args := m.Called(ctx, spends)
	return args.Error(0)
}

func (m *mockCache) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	args := m.Called(ctx, hashes, minedBlockInfo)
	return args.Get(0).(map[chainhash.Hash][]uint32), args.Error(1)
}

func (m *mockCache) PreviousOutputsDecorate(ctx context.Context, tx *bt.Tx) error {
	args := m.Called(ctx, tx)
	return args.Error(0)
}

func (m *mockCache) FreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	args := m.Called(ctx, spends, tSettings)
	return args.Error(0)
}

func (m *mockCache) UnFreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	args := m.Called(ctx, spends, tSettings)
	return args.Error(0)
}

func (m *mockCache) ReAssignUTXO(ctx context.Context, utxo *utxo.Spend, newUtxo *utxo.Spend, tSettings *settings.Settings) error {
	args := m.Called(ctx, utxo, newUtxo, tSettings)
	return args.Error(0)
}

func (m *mockCache) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	args := m.Called(ctx, checkLiveness)
	return args.Int(0), args.String(1), args.Error(2)
}

func (m *mockCache) GetBlockHeight() uint32 {
	args := m.Called()
	return args.Get(0).(uint32)
}

func (m *mockCache) SetBlockHeight(blockHeight uint32) error {
	args := m.Called(blockHeight)
	return args.Error(0)
}

func (m *mockCache) GetMedianBlockTime() uint32 {
	args := m.Called()
	return args.Get(0).(uint32)
}

func (m *mockCache) SetMedianBlockTime(medianTime uint32) error {
	args := m.Called(medianTime)
	return args.Error(0)
}

func createKafkaMessage(t *testing.T, delete bool, content []byte) *kafka.KafkaMessage {
	t.Helper()

	hash := chainhash.Hash{1, 2, 3}
	action := kafkamessage.KafkaTxMetaActionType_ADD

	if delete {
		action = kafkamessage.KafkaTxMetaActionType_DELETE
	}

	kafkaMsg := &kafkamessage.KafkaTxMetaTopicMessage{
		TxHash:  hash.String(),
		Action:  action,
		Content: content,
	}

	data, err := proto.Marshal(kafkaMsg)
	if err != nil {
		t.Fatal(err)
	}

	consumerMsg := sarama.ConsumerMessage{
		Value: data,
	}

	return &kafka.KafkaMessage{
		ConsumerMessage: consumerMsg,
	}
}

func TestServer_txmetaHandler(t *testing.T) {
	tests := []struct {
		name          string
		setupMocks    func(*mockLogger, *mockCache)
		input         *kafka.KafkaMessage
		expectedError bool
	}{
		{
			name:          "nil message",
			setupMocks:    func(l *mockLogger, c *mockCache) {},
			input:         nil,
			expectedError: false,
		},
		{
			name:          "message too short",
			setupMocks:    func(l *mockLogger, c *mockCache) {},
			input:         &kafka.KafkaMessage{ConsumerMessage: sarama.ConsumerMessage{Value: make([]byte, chainhash.HashSize-1)}},
			expectedError: false,
		},
		{
			name:          "invalid protobuf",
			setupMocks:    func(l *mockLogger, c *mockCache) {},
			input:         &kafka.KafkaMessage{ConsumerMessage: sarama.ConsumerMessage{Value: make([]byte, chainhash.HashSize+1)}},
			expectedError: true,
		},
		{
			name: "successful delete operation",
			setupMocks: func(l *mockLogger, c *mockCache) {
				c.On("Delete", mock.Anything, mock.AnythingOfType("*chainhash.Hash")).Return(nil)
			},
			input:         createKafkaMessage(t, true, []byte{}),
			expectedError: false,
		},
		{
			name: "failed delete operation with processing error",
			setupMocks: func(l *mockLogger, c *mockCache) {
				c.On("Delete", mock.Anything, mock.AnythingOfType("*chainhash.Hash")).Return(errors.ErrProcessing)
				l.On("Warnf", mock.Anything, mock.Anything).Return()
			},
			input:         createKafkaMessage(t, true, []byte{}),
			expectedError: false,
		},
		{
			name: "failed delete operation with other error",
			setupMocks: func(l *mockLogger, c *mockCache) {
				c.On("Delete", mock.Anything, mock.AnythingOfType("*chainhash.Hash")).Return(errors.NewServiceError("other error"))
			},
			input:         createKafkaMessage(t, true, []byte{}),
			expectedError: true,
		},
		{
			name: "successful set operation",
			setupMocks: func(l *mockLogger, c *mockCache) {
				c.On("SetCacheFromBytes", mock.Anything, mock.Anything).Return(nil)
			},
			input:         createKafkaMessage(t, false, []byte("test data")),
			expectedError: false,
		},
		{
			name: "failed set operation with processing error",
			setupMocks: func(l *mockLogger, c *mockCache) {
				c.On("SetCacheFromBytes", mock.Anything, mock.Anything).Return(errors.ErrProcessing)
				l.On("Debugf", mock.Anything, mock.Anything).Return()
			},
			input:         createKafkaMessage(t, false, []byte("test data")),
			expectedError: false,
		},
		{
			name: "failed set operation with other error",
			setupMocks: func(l *mockLogger, c *mockCache) {
				c.On("SetCacheFromBytes", mock.Anything, mock.Anything).Return(errors.NewServiceError("other error"))
			},
			input:         createKafkaMessage(t, false, []byte("test data")),
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockLogger := &mockLogger{}
			mockCache := &mockCache{}
			tt.setupMocks(mockLogger, mockCache)

			server := &Server{
				logger:    mockLogger,
				utxoStore: mockCache,
			}

			err := server.txmetaHandler(context.Background(), tt.input)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockLogger.AssertExpectations(t)
			mockCache.AssertExpectations(t)
		})
	}
}
