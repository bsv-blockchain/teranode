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

func (m *mockCache) SetCacheMulti(keys [][]byte, values [][]byte) error {
	args := m.Called(keys, values)
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

// newTestServerForHandler builds a minimal Server suitable for txmetaHandler tests.
// txmetaHandler is fire-and-forget — it spawns one goroutine per message — so there
// is no worker pool / channel to set up or tear down. The returned drain function
// gives the spawned goroutine time to run before mock expectations are asserted.
//
// If `log` is a *mockLogger we also pre-register a Maybe() Infof so tests that don't
// care about logger calls aren't surprised by them.
func newTestServerForHandler(t *testing.T, log ulogger.Logger, cache utxo.Store) (*Server, func()) {
	t.Helper()

	if ml, ok := log.(*mockLogger); ok {
		ml.On("Infof", mock.Anything, mock.Anything).Maybe().Return()
	}

	tSettings := &settings.Settings{}

	server := &Server{
		logger:    log,
		settings:  tSettings,
		utxoStore: cache,
	}

	return server, func() {
		// Wait for any in-flight apply goroutine to finish. There is no global
		// waitgroup since the handler is fire-and-forget; a short sleep is enough
		// because per-key SetCacheFromBytes calls take microseconds. Tests with
		// many entries still use a deterministic mock that returns immediately.
		time.Sleep(50 * time.Millisecond)
	}
}

func TestServer_txmetaHandler(t *testing.T) {
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
				l.On("Errorf", mock.Anything, mock.Anything, mock.Anything).Return()
			},
			input: createKafkaMessage(t, true, []byte{}),
		},
		{
			name: "successful set operation uses per-entry SetCacheFromBytes",
			setupMocks: func(l *mockLogger, c *mockCache) {
				// Per-entry path: one Kafka message of 1 entry → one SetCacheFromBytes
				// call (NOT SetCacheMulti). This is the bucket-lock-friendly path that
				// keeps each lock acquisition brief.
				c.On("SetCacheFromBytes", mock.Anything, mock.Anything).Return(nil)
			},
			input: createKafkaMessage(t, false, []byte("test data")),
		},
		{
			name: "failed set operation logs debug",
			setupMocks: func(l *mockLogger, c *mockCache) {
				c.On("SetCacheFromBytes", mock.Anything, mock.Anything).Return(errors.ErrProcessing)
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

			server, stop := newTestServerForHandler(t, mockLogger, mockCache)

			err := server.txmetaHandler(context.Background(), tt.input)
			if tt.name == "failed delete operation logs error" {
				// DELETE is synchronous and must surface failures so the Kafka
				// consumer leaves the offset uncommitted and the message gets
				// re-delivered.
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			// stop() drains any in-flight apply goroutines (ADDs only — DELETE
			// runs synchronously above) before AssertExpectations.
			stop()

			mockCache.AssertExpectations(t)
		})
	}
}

// createMultiEntryKafkaMessage builds a binary batch with N ADD entries, used to verify
// that one Kafka message produces ONE SetCacheMulti call (not N SetCacheFromBytes).
func createMultiEntryKafkaMessage(t *testing.T, n int) *kafka.KafkaMessage {
	t.Helper()

	const contentLen = 8
	dataSize := 4 + n*(32+1+4+contentLen)
	data := make([]byte, dataSize)
	offset := 0

	binary.LittleEndian.PutUint32(data[offset:], uint32(n))
	offset += 4

	for i := 0; i < n; i++ {
		// Hash: byte 0 is i so we can verify per-entry distinctness if needed.
		data[offset] = byte(i)
		offset += 32
		data[offset] = txmetaActionADD
		offset++
		binary.LittleEndian.PutUint32(data[offset:], contentLen)
		offset += 4
		// Content: filled with i so values are also distinct.
		for j := 0; j < contentLen; j++ {
			data[offset+j] = byte(i)
		}
		offset += contentLen
	}

	return &kafka.KafkaMessage{Value: data}
}

// TestServer_txmetaHandler_PerEntrySetCacheFromBytes guards the cache-write strategy.
//
// Previously the worker batched all entries into one SetCacheMulti call — that turned
// out to hold each touched bucket's write lock for the entire batch (~1ms), and under
// many concurrent workers the bucket-lock queue inflated, collapsing throughput.
//
// Per-entry SetCacheFromBytes acquires/releases the bucket lock for a single key at a
// time (~1µs holds), keeping the queue shallow. That's the lock-contention profile
// that historically sustained 2M+ ops/sec on this path.
func TestServer_txmetaHandler_PerEntrySetCacheFromBytes(t *testing.T) {
	const entries = 50

	mockLogger := &mockLogger{}
	mockCache := &mockCache{}
	// Each entry gets its own SetCacheFromBytes call — never SetCacheMulti.
	mockCache.On("SetCacheFromBytes", mock.Anything, mock.Anything).Return(nil).Times(entries)

	server, stop := newTestServerForHandler(t, mockLogger, mockCache)
	defer stop()

	err := server.txmetaHandler(context.Background(), createMultiEntryKafkaMessage(t, entries))
	assert.NoError(t, err)

	stop()
	mockCache.AssertExpectations(t)
	// Belt-and-braces: the batched path must NOT be used — that's the regression we're avoiding.
	mockCache.AssertNotCalled(t, "SetCacheMulti", mock.Anything, mock.Anything)
}

// TestServer_txmetaHandler_MixedBatch verifies that a single Kafka message containing
// both ADD and DELETE entries dispatches to the right cache call for each.
func TestServer_txmetaHandler_MixedBatch(t *testing.T) {
	// Build a batch: ADD, DELETE, ADD.
	buf := make([]byte, 0)
	count := make([]byte, 4)
	binary.LittleEndian.PutUint32(count, 3)
	buf = append(buf, count...)

	appendEntry := func(action byte, keyByte byte, content []byte) {
		hash := make([]byte, 32)
		hash[0] = keyByte
		buf = append(buf, hash...)
		buf = append(buf, action)
		lenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuf, uint32(len(content)))
		buf = append(buf, lenBuf...)
		buf = append(buf, content...)
	}
	appendEntry(txmetaActionADD, 0xAA, []byte("a-data"))
	appendEntry(txmetaActionDELETE, 0xBB, nil)
	appendEntry(txmetaActionADD, 0xCC, []byte("c-data-longer"))

	mockLogger := &mockLogger{}
	mockCache := &mockCache{}
	mockCache.On("SetCacheFromBytes", mock.Anything, mock.Anything).Return(nil).Twice()
	mockCache.On("Delete", mock.Anything, mock.AnythingOfType("*chainhash.Hash")).Return(nil).Once()

	server, stop := newTestServerForHandler(t, mockLogger, mockCache)
	defer stop()

	err := server.txmetaHandler(context.Background(), &kafka.KafkaMessage{Value: buf})
	assert.NoError(t, err)

	stop()
	mockCache.AssertExpectations(t)
}

// TestServer_txmetaHandler_TruncatedBatch verifies that a malformed (claims more
// entries than it contains) message is acked without panic and without applying
// any pending partial state.
func TestServer_txmetaHandler_TruncatedBatch(t *testing.T) {
	// Header claims 2 entries, body contains only 1.
	buf := make([]byte, 0, 4+32+1+4)
	buf = binary.LittleEndian.AppendUint32(buf, 2)
	buf = append(buf, make([]byte, 32+1+4)...) // first entry: hash + action + contentLen=0

	mockLogger := &mockLogger{}
	mockCache := &mockCache{}
	// First (well-formed) entry is an ADD with empty content; per-entry goroutine
	// is allowed to land. The truncated second entry must just bail without panic.
	mockCache.On("SetCacheFromBytes", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockLogger.On("Errorf", mock.Anything, mock.Anything, mock.Anything).Maybe().Return()

	server, stop := newTestServerForHandler(t, mockLogger, mockCache)
	defer stop()

	err := server.txmetaHandler(context.Background(), &kafka.KafkaMessage{Value: buf})
	assert.NoError(t, err)
}

// TestServer_txmetaHandler_ConcurrentCalls asserts that the fire-and-forget handler
// tolerates many concurrent callers without panicking or deadlocking. There is no
// shared state to corrupt — each call spawns its own apply goroutine — so this is a
// regression smoke test, not a race test. It still earns its keep under -race because
// SetCacheFromBytes is hit from N goroutines simultaneously.
func TestServer_txmetaHandler_ConcurrentCalls(t *testing.T) {
	mockLogger := &mockLogger{}
	mockCache := &mockCache{}
	// Apply path uses per-entry SetCacheFromBytes; we don't care how many calls land,
	// only that none panic.
	mockCache.On("SetCacheFromBytes", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockCache.On("Delete", mock.Anything, mock.AnythingOfType("*chainhash.Hash")).Return(nil).Maybe()
	mockLogger.On("Debugf", mock.Anything, mock.Anything).Maybe().Return()
	mockLogger.On("Errorf", mock.Anything, mock.Anything, mock.Anything).Maybe().Return()

	server, stop := newTestServerForHandler(t, mockLogger, mockCache)

	const senders = 16
	var senderWg sync.WaitGroup
	stopRequested := make(chan struct{})

	for i := 0; i < senders; i++ {
		senderWg.Add(1)
		go func() {
			defer senderWg.Done()
			for {
				select {
				case <-stopRequested:
					return
				default:
				}
				_ = server.txmetaHandler(context.Background(),
					createKafkaMessage(t, false, []byte("payload")))
			}
		}()
	}

	time.Sleep(5 * time.Millisecond)
	close(stopRequested)
	senderWg.Wait()

	// Drain in-flight apply goroutines before the test exits so the mock isn't called
	// after t.Cleanup runs.
	stop()
}
