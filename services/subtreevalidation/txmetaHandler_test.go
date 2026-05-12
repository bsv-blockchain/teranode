package subtreevalidation

import (
	"context"
	"encoding/binary"
	"os"
	"sync"
	"testing"

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
	"github.com/stretchr/testify/require"
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

// newTestServerWithWorker builds a Server wired up with the txmeta worker-pool plumbing
// and a single worker goroutine running. Returns a stop function that closes the work
// channel and waits for the worker to drain — call it after enqueueing to deterministically
// observe the cache-write side effects.
//
// If `log` is a *mockLogger, this helper pre-registers the Infof startup log so individual
// tests don't have to. Tests that care about specific Infof calls can still add their own
// expectations on top.
func newTestServerWithWorker(t *testing.T, log ulogger.Logger, cache utxo.Store) (*Server, func()) {
	t.Helper()

	if ml, ok := log.(*mockLogger); ok {
		// startTxmetaCacheWorkers logs an "Infof" startup line; allow it implicitly.
		ml.On("Infof", mock.Anything, mock.Anything).Maybe().Return()
	}

	tSettings := &settings.Settings{}
	tSettings.SubtreeValidation.TxmetaCacheKafkaWorkers = 1
	tSettings.SubtreeValidation.TxmetaCacheKafkaQueueSize = 16

	server := &Server{
		logger:    log,
		settings:  tSettings,
		utxoStore: cache,
	}
	server.initTxmetaCacheWorkerPool()

	ctx, cancel := context.WithCancel(context.Background())
	server.startTxmetaCacheWorkers(ctx)

	return server, func() {
		cancel()
		server.stopTxmetaCacheWorkers()
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
			name: "successful set operation uses SetCacheMulti",
			setupMocks: func(l *mockLogger, c *mockCache) {
				// One Kafka message → one SetCacheMulti call (not N SetCacheFromBytes).
				c.On("SetCacheMulti", mock.Anything, mock.Anything).Return(nil)
			},
			input: createKafkaMessage(t, false, []byte("test data")),
		},
		{
			name: "failed set operation logs debug",
			setupMocks: func(l *mockLogger, c *mockCache) {
				c.On("SetCacheMulti", mock.Anything, mock.Anything).Return(errors.ErrProcessing)
				l.On("Debugf", mock.Anything, mock.Anything, mock.Anything).Return()
			},
			input: createKafkaMessage(t, false, []byte("test data")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockLogger := &mockLogger{}
			mockCache := &mockCache{}
			tt.setupMocks(mockLogger, mockCache)

			server, stop := newTestServerWithWorker(t, mockLogger, mockCache)

			err := server.txmetaHandler(context.Background(), tt.input)
			assert.NoError(t, err)

			// stop() closes the work channel and waits for the worker to finish
			// processing any pending jobs — deterministic alternative to time.Sleep.
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

func TestServer_txmetaHandler_BatchesIntoSingleSetCacheMulti(t *testing.T) {
	const entries = 50

	mockLogger := &mockLogger{}
	mockCache := &mockCache{}
	// The whole regression this test guards against: one Kafka message of N entries
	// must result in ONE SetCacheMulti call carrying all N keys, not N individual calls.
	mockCache.On("SetCacheMulti",
		mock.MatchedBy(func(keys [][]byte) bool { return len(keys) == entries }),
		mock.MatchedBy(func(values [][]byte) bool { return len(values) == entries }),
	).Return(nil).Once()

	server, stop := newTestServerWithWorker(t, mockLogger, mockCache)
	defer stop()

	err := server.txmetaHandler(context.Background(), createMultiEntryKafkaMessage(t, entries))
	assert.NoError(t, err)

	stop()
	mockCache.AssertExpectations(t)
	// Belt-and-braces: explicitly assert the singular path is NOT used.
	mockCache.AssertNotCalled(t, "SetCacheFromBytes", mock.Anything, mock.Anything)
}

func TestParseTxmetaBatch(t *testing.T) {
	t.Run("empty data is rejected", func(t *testing.T) {
		_, ok := parseTxmetaBatch(&silentLogger{}, []byte{})
		assert.False(t, ok)
	})

	t.Run("zero-entry batch parses to empty job", func(t *testing.T) {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, 0)
		job, ok := parseTxmetaBatch(&silentLogger{}, buf)
		require.True(t, ok)
		assert.Empty(t, job.addKeys)
		assert.Empty(t, job.delHashes)
	})

	t.Run("multi-entry batch with mix of ADD/DELETE", func(t *testing.T) {
		// 3 entries: ADD, DELETE, ADD
		buf := []byte{}
		// count
		count := make([]byte, 4)
		binary.LittleEndian.PutUint32(count, 3)
		buf = append(buf, count...)

		appendEntry := func(action byte, key byte, content []byte) {
			hash := make([]byte, 32)
			hash[0] = key
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

		job, ok := parseTxmetaBatch(&silentLogger{}, buf)
		require.True(t, ok)
		assert.Len(t, job.addKeys, 2, "two ADD entries")
		assert.Len(t, job.addValues, 2)
		assert.Len(t, job.delHashes, 1, "one DELETE entry")
		assert.Equal(t, byte(0xAA), job.addKeys[0][0])
		assert.Equal(t, byte(0xCC), job.addKeys[1][0])
		assert.Equal(t, []byte("a-data"), job.addValues[0])
		assert.Equal(t, []byte("c-data-longer"), job.addValues[1])
		assert.Equal(t, byte(0xBB), job.delHashes[0][0])
	})

	t.Run("truncated batch returns ok=false", func(t *testing.T) {
		// Claims 2 entries but only contains data for 1.
		buf := make([]byte, 0, 4+32+1+4)
		buf = binary.LittleEndian.AppendUint32(buf, 2)
		// First entry: hash + action + contentLen=0
		buf = append(buf, make([]byte, 32+1+4)...)
		// Second entry header is missing entirely.

		_, ok := parseTxmetaBatch(&silentLogger{}, buf)
		assert.False(t, ok, "truncated batch must be rejected, not partially applied")
	})

	t.Run("buffer reuse safety: keys/values are deep-copied", func(t *testing.T) {
		// Build a batch in a single backing buffer.
		buf := make([]byte, 0, 4+32+1+4+4)
		buf = binary.LittleEndian.AppendUint32(buf, 1)
		hash := make([]byte, 32)
		hash[0] = 0x42
		buf = append(buf, hash...)
		buf = append(buf, txmetaActionADD)
		buf = binary.LittleEndian.AppendUint32(buf, 4)
		buf = append(buf, 'a', 'b', 'c', 'd')

		job, ok := parseTxmetaBatch(&silentLogger{}, buf)
		require.True(t, ok)
		require.Len(t, job.addKeys, 1)
		require.Len(t, job.addValues, 1)

		// Mutate the source buffer — the parsed job must not see the change.
		// (The handler keeps the message on the consumer side; the worker reads later.)
		for i := range buf {
			buf[i] = 0xFF
		}
		assert.Equal(t, byte(0x42), job.addKeys[0][0], "key was not deep-copied")
		assert.Equal(t, []byte("abcd"), job.addValues[0], "value was not deep-copied")
	})
}

// silentLogger satisfies the small logger interface that parseTxmetaBatch needs without
// pulling in the full mockLogger machinery.
type silentLogger struct{}

func (silentLogger) Errorf(format string, args ...interface{}) {}

// TestServer_txmetaHandler_ShutdownDuringSendIsSafe is a regression test for the
// "panic: send on closed channel" failure that the in-memory Kafka consumer triggered
// on shutdown: txmetaConsumerClient.Close() returns without joining its claim-pump
// goroutine, so subsequent close-of-work-channel from stopTxmetaCacheWorkers races
// with txmetaHandler still calling send. Closing a done signal (rather than the work
// channel) makes the send-vs-close race safe — this test exercises that scenario
// concurrently many times. Pre-fix, this panics; post-fix, it completes cleanly.
func TestServer_txmetaHandler_ShutdownDuringSendIsSafe(t *testing.T) {
	const senders = 32
	const messagesPerSender = 200

	for trial := 0; trial < 20; trial++ {
		ml := &mockLogger{}
		ml.On("Infof", mock.Anything, mock.Anything).Maybe().Return()
		ml.On("Debugf", mock.Anything, mock.Anything, mock.Anything).Maybe().Return()
		mc := &mockCache{}
		mc.On("SetCacheMulti", mock.Anything, mock.Anything).Maybe().Return(nil)

		// Tiny queue so senders block often enough to overlap shutdown.
		tSettings := &settings.Settings{}
		tSettings.SubtreeValidation.TxmetaCacheKafkaWorkers = 2
		tSettings.SubtreeValidation.TxmetaCacheKafkaQueueSize = 2

		server := &Server{
			logger:    ml,
			settings:  tSettings,
			utxoStore: mc,
		}
		server.initTxmetaCacheWorkerPool()

		ctx, cancel := context.WithCancel(context.Background())
		server.startTxmetaCacheWorkers(ctx)

		var senderWg sync.WaitGroup
		senderWg.Add(senders)
		for i := 0; i < senders; i++ {
			go func() {
				defer senderWg.Done()
				for j := 0; j < messagesPerSender; j++ {
					_ = server.txmetaHandler(ctx, createKafkaMessage(t, false, []byte("data")))
				}
			}()
		}

		// Shut workers down while senders are still firing. With the buggy
		// close-the-work-channel design, this is where the runtime panics.
		server.stopTxmetaCacheWorkers()
		senderWg.Wait()
		cancel()
	}
}
