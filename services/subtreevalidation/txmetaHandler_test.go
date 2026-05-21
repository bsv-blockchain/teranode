package subtreevalidation

import (
	"context"
	"encoding/binary"
	"os"
	"sync"
	"testing"
	"time"

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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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

func (m *mockLogger) WithTraceContext(_ context.Context) ulogger.Logger {
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

func (m *mockCache) SetCacheMulti(keys, values [][]byte) error {
	args := m.Called(keys, values)
	return args.Error(0)
}

func (m *mockCache) SetCacheMultiSequential(keys, values [][]byte) error {
	args := m.Called(keys, values)
	return args.Error(0)
}

func (m *mockCache) SetCacheMultiSequentialWithHashes(keys, values [][]byte, hashes []uint64) error {
	args := m.Called(keys, values, hashes)
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

func (m *mockCache) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	args := m.Called(ctx, hash, data)
	if result := args.Get(0); result != nil {
		*data = *result.(*meta.Data)
	}

	return args.Error(1)
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

func (m *mockCache) BatchPreviousOutputsDecorate(ctx context.Context, txs []*bt.Tx) error {
	args := m.Called(ctx, txs)
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

// createKafkaMessage creates a binary batch format Kafka message for testing.
// Format: [4 bytes entry count] + for each entry: [32 bytes hash][1 byte action][4 bytes length][N bytes content]
func createKafkaMessage(t *testing.T, delete bool, content []byte) *kafka.KafkaMessage {
	t.Helper()

	hash := chainhash.Hash{1, 2, 3}
	action := txmetaActionADD
	if delete {
		action = txmetaActionDELETE
	}

	// Calculate total size: 4 (count) + 32 (hash) + 1 (action) + 4 (length) + len(content)
	contentLen := uint32(0)
	if !delete {
		contentLen = uint32(len(content))
	}
	dataSize := 4 + 32 + 1 + 4 + int(contentLen)
	data := make([]byte, dataSize)
	offset := 0

	// Write entry count (1 entry)
	binary.LittleEndian.PutUint32(data[offset:], 1)
	offset += 4

	// Write hash (32 bytes)
	copy(data[offset:], hash[:])
	offset += 32

	// Write action (1 byte)
	data[offset] = action
	offset++

	// Write content length (4 bytes)
	binary.LittleEndian.PutUint32(data[offset:], contentLen)
	offset += 4

	// Write content (only for ADD)
	if !delete && len(content) > 0 {
		copy(data[offset:], content)
	}

	return &kafka.KafkaMessage{
		Value: data,
	}
}

func createKafkaMessageForHash(t *testing.T, hash chainhash.Hash, action byte, content []byte) *kafka.KafkaMessage {
	t.Helper()

	contentLen := uint32(len(content))
	if action == txmetaActionDELETE {
		contentLen = 0
	}

	dataSize := 4 + 32 + 1 + 4 + int(contentLen)
	data := make([]byte, dataSize)
	offset := 0

	binary.LittleEndian.PutUint32(data[offset:], 1)
	offset += 4

	copy(data[offset:], hash[:])
	offset += 32

	data[offset] = action
	offset++

	binary.LittleEndian.PutUint32(data[offset:], contentLen)
	offset += 4

	if contentLen > 0 {
		copy(data[offset:], content[:int(contentLen)])
	}

	return &kafka.KafkaMessage{Value: data}
}

func TestServer_txmetaHandler(t *testing.T) {
	// Note: The handler dispatches work to bounded shard workers and may return an error if a queue is full.
	// Tests verify proper parsing of the binary batch format.
	tests := []struct {
		name       string
		setupMocks func(*mockLogger, *mockCache)
		input      *kafka.KafkaMessage
	}{
		{
			name:       "nil message",
			setupMocks: func(l *mockLogger, c *mockCache) {},
			input:      nil,
		},
		{
			name:       "message too short for entry count",
			setupMocks: func(l *mockLogger, c *mockCache) {},
			input:      &kafka.KafkaMessage{Value: make([]byte, 3)},
		},
		{
			name: "successful delete operation",
			setupMocks: func(l *mockLogger, c *mockCache) {
				c.On("Delete", mock.Anything, mock.AnythingOfType("*chainhash.Hash")).Return(nil)
			},
			input: createKafkaMessage(t, true, []byte{}),
		},
		{
			name: "failed delete operation logs error",
			setupMocks: func(l *mockLogger, c *mockCache) {
				c.On("Delete", mock.Anything, mock.AnythingOfType("*chainhash.Hash")).Return(errors.ErrProcessing)
				l.On("Errorf", mock.Anything, mock.Anything).Return()
			},
			input: createKafkaMessage(t, true, []byte{}),
		},
		{
			name: "successful set operation",
			setupMocks: func(l *mockLogger, c *mockCache) {
				c.On("SetCacheMultiSequential", mock.Anything, mock.Anything).Return(nil)
			},
			input: createKafkaMessage(t, false, []byte("test data")),
		},
		{
			name: "failed set operation logs debug",
			setupMocks: func(l *mockLogger, c *mockCache) {
				c.On("SetCacheMultiSequential", mock.Anything, mock.Anything).Return(errors.ErrProcessing)
				l.On("Debugf", mock.Anything, mock.Anything).Return()
			},
			input: createKafkaMessage(t, false, []byte("test data")),
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

			// The handler always returns nil (async processing)
			err := server.txmetaHandler(context.Background(), tt.input)
			assert.NoError(t, err)

			// Wait briefly for async goroutine to complete
			// This is a bit awkward but necessary since processing is async
			<-time.After(10 * time.Millisecond)

			mockCache.AssertExpectations(t)
		})
	}
}

func TestServer_txmetaHandler_PreservesPerKeyOrdering(t *testing.T) {
	mockLogger := &mockLogger{}
	mockCache := &mockCache{}

	var (
		operationMu sync.Mutex
		operations  []string
	)

	mockCache.On("SetCacheMultiSequential", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		operationMu.Lock()
		defer operationMu.Unlock()
		operations = append(operations, "add")
	})

	mockCache.On("Delete", mock.Anything, mock.AnythingOfType("*chainhash.Hash")).Return(nil).Run(func(args mock.Arguments) {
		operationMu.Lock()
		defer operationMu.Unlock()
		operations = append(operations, "delete")
	})

	server := &Server{
		logger:    mockLogger,
		utxoStore: mockCache,
	}

	hash := chainhash.Hash{42}
	addMessage := createKafkaMessageForHash(t, hash, txmetaActionADD, []byte("payload"))
	deleteMessage := createKafkaMessageForHash(t, hash, txmetaActionDELETE, nil)

	err := server.txmetaHandler(context.Background(), addMessage)
	assert.NoError(t, err)

	err = server.txmetaHandler(context.Background(), deleteMessage)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		operationMu.Lock()
		defer operationMu.Unlock()
		return len(operations) == 2
	}, 2*time.Second, 10*time.Millisecond)

	operationMu.Lock()
	defer operationMu.Unlock()
	assert.Equal(t, []string{"add", "delete"}, operations)
}

// TestServer_txmetaHandler_V2_Parses verifies the receiver correctly handles
// the v2 wire format (magic byte 0xFF, 8-byte hash per entry).
func TestServer_txmetaHandler_V2_Parses(t *testing.T) {
	mLogger := &mockLogger{}
	mCache := &mockCache{}
	mCache.On("SetCacheMultiSequentialWithHashes", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	server := &Server{
		logger:    mLogger,
		utxoStore: mCache,
	}

	payload := []byte("test-meta")
	totalSize := 8 + 8 + 32 + 1 + 4 + len(payload)
	data := make([]byte, totalSize)
	data[0] = txmetaWireV2Magic
	data[1] = txmetaWireV2Version
	binary.LittleEndian.PutUint32(data[4:], 1)

	off := 8
	binary.LittleEndian.PutUint64(data[off:], 0xCAFEBABE)
	off += 8
	hash := chainhash.Hash{42}
	copy(data[off:], hash[:])
	off += 32
	data[off] = txmetaActionADD
	off++
	binary.LittleEndian.PutUint32(data[off:], uint32(len(payload)))
	off += 4
	copy(data[off:], payload)

	err := server.txmetaHandler(context.Background(), &kafka.KafkaMessage{Value: data})
	assert.NoError(t, err)
	mCache.AssertExpectations(t)
}

// TestServer_txmetaHandler_V2_UnknownVersion confirms that an unknown v2
// sub-version is logged and acked rather than triggering a redelivery loop.
func TestServer_txmetaHandler_V2_UnknownVersion(t *testing.T) {
	mLogger := &mockLogger{}
	mLogger.On("Errorf", mock.Anything, mock.Anything).Return()

	server := &Server{logger: mLogger}

	data := make([]byte, 8)
	data[0] = txmetaWireV2Magic
	data[1] = 0x99

	err := server.txmetaHandler(context.Background(), &kafka.KafkaMessage{Value: data})
	assert.NoError(t, err)
	mLogger.AssertCalled(t, "Errorf", mock.Anything, mock.Anything)
}

// TestServer_txmetaHandler_V2_MixedAddDelete verifies the receiver handles a
// v2 message containing both ADDs and DELETEs in any order, mirroring what
// the validator's serializeTxMetaBatchV2 can emit when a partition has a
// mixed batch.
func TestServer_txmetaHandler_V2_MixedAddDelete(t *testing.T) {
	mLogger := &mockLogger{}
	mCache := &mockCache{}
	// Two ADDs (single SetCacheMultiSequential call) + one DELETE.
	mCache.On("SetCacheMultiSequentialWithHashes", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mCache.On("Delete", mock.Anything, mock.AnythingOfType("*chainhash.Hash")).Return(nil)

	server := &Server{
		logger:    mLogger,
		utxoStore: mCache,
	}

	hashes := []chainhash.Hash{{1}, {2}, {3}}
	payloads := [][]byte{[]byte("a"), nil, []byte("ccc")}
	actions := []byte{txmetaActionADD, txmetaActionDELETE, txmetaActionADD}
	pseudoHashes := []uint64{0x11, 0x22, 0x33}

	size := 8 // header
	for i := range hashes {
		size += 8 + 32 + 1 + 4 + len(payloads[i])
	}
	data := make([]byte, size)
	data[0] = txmetaWireV2Magic
	data[1] = txmetaWireV2Version
	binary.LittleEndian.PutUint32(data[4:], uint32(len(hashes)))
	off := 8

	for i := range hashes {
		binary.LittleEndian.PutUint64(data[off:], pseudoHashes[i])
		off += 8
		copy(data[off:], hashes[i][:])
		off += 32
		data[off] = actions[i]
		off++
		binary.LittleEndian.PutUint32(data[off:], uint32(len(payloads[i])))
		off += 4
		copy(data[off:], payloads[i])
		off += len(payloads[i])
	}

	err := server.txmetaHandler(context.Background(), &kafka.KafkaMessage{Value: data})
	assert.NoError(t, err)
	mCache.AssertExpectations(t)
}
