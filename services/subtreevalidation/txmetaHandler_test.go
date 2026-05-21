package subtreevalidation

import (
	"context"
	"encoding/binary"
	"os"
	"sync"
	"sync/atomic"
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

func (m *mockCache) SetCacheMulti(keys, values [][]byte) error {
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
	// setupMocks takes a `bumpDone` callback that the mock's Run hook invokes
	// once per cache operation. Driving sync off a goroutine-safe atomic is
	// race-free, unlike polling mockCache.Calls (testify's internal mutex
	// is unexported).
	tests := []struct {
		name               string
		setupMocks         func(l *mockLogger, c *mockCache, bumpDone func())
		input              *kafka.KafkaMessage
		expectedCacheCalls int
	}{
		{
			name:       "nil message",
			setupMocks: func(_ *mockLogger, _ *mockCache, _ func()) {},
			input:      nil,
		},
		{
			name:       "message too short for entry count",
			setupMocks: func(_ *mockLogger, _ *mockCache, _ func()) {},
			input:      &kafka.KafkaMessage{Value: make([]byte, 3)},
		},
		{
			name: "successful delete operation",
			setupMocks: func(_ *mockLogger, c *mockCache, bumpDone func()) {
				c.On("Delete", mock.Anything, mock.AnythingOfType("*chainhash.Hash")).
					Return(nil).
					Run(func(_ mock.Arguments) { bumpDone() })
			},
			input:              createKafkaMessage(t, true, []byte{}),
			expectedCacheCalls: 1,
		},
		{
			name: "failed delete operation logs error",
			setupMocks: func(l *mockLogger, c *mockCache, bumpDone func()) {
				c.On("Delete", mock.Anything, mock.AnythingOfType("*chainhash.Hash")).
					Return(errors.ErrProcessing).
					Run(func(_ mock.Arguments) { bumpDone() })
				l.On("Errorf", mock.Anything, mock.Anything).Return()
			},
			input:              createKafkaMessage(t, true, []byte{}),
			expectedCacheCalls: 1,
		},
		{
			name: "successful set operation",
			setupMocks: func(_ *mockLogger, c *mockCache, bumpDone func()) {
				c.On("SetCacheMulti", mock.Anything, mock.Anything).
					Return(nil).
					Run(func(_ mock.Arguments) { bumpDone() })
			},
			input:              createKafkaMessage(t, false, []byte("test data")),
			expectedCacheCalls: 1,
		},
		{
			name: "failed set operation logs debug",
			setupMocks: func(l *mockLogger, c *mockCache, bumpDone func()) {
				c.On("SetCacheMulti", mock.Anything, mock.Anything).
					Return(errors.ErrProcessing).
					Run(func(_ mock.Arguments) { bumpDone() })
				l.On("Debugf", mock.Anything, mock.Anything).Return()
			},
			input:              createKafkaMessage(t, false, []byte("test data")),
			expectedCacheCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockLogger := &mockLogger{}
			mockCache := &mockCache{}
			var done atomic.Int32
			tt.setupMocks(mockLogger, mockCache, func() { done.Add(1) })

			server := &Server{
				logger:    mockLogger,
				utxoStore: mockCache,
			}

			// The handler always returns nil (async processing)
			err := server.txmetaHandler(context.Background(), tt.input)
			require.NoError(t, err)

			// Sync on the atomic counter (race-free) instead of a fixed
			// time.After sleep, so the test is robust on loaded CI.
			if tt.expectedCacheCalls > 0 {
				require.Eventually(t, func() bool {
					return int(done.Load()) >= tt.expectedCacheCalls
				}, 2*time.Second, time.Millisecond, "expected at least %d mock cache calls", tt.expectedCacheCalls)
			}

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

	mockCache.On("SetCacheMulti", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
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

// TestServer_txmetaHandler_CaughtUpModeDropsOnFullQueue verifies that once the
// caught-up latch is set, a full shard queue causes the batch to be silently
// abandoned (no error returned to the kafka consumer) so the failure mode is
// logged at Warn level instead of Error.
func TestServer_txmetaHandler_CaughtUpModeDropsOnFullQueue(t *testing.T) {
	server := &Server{
		logger: ulogger.TestLogger{},
	}
	server.txmetaCaughtUp.Store(true)

	// Pretend workers are already initialized; an unbuffered channel with no
	// reader is always "full" for a non-blocking send.
	server.txmetaWorkerInitOnce.Do(func() {})
	server.txmetaWorkerQueues = []chan *txmetaShardBatch{
		make(chan *txmetaShardBatch),
	}

	hash := chainhash.Hash{0}
	message := createKafkaMessageForHash(t, hash, txmetaActionADD, []byte("payload"))

	err := server.txmetaHandler(context.Background(), message)
	assert.NoError(t, err)
}

// TestServer_txmetaHandler_StartupModeBlocksUntilDrained verifies the startup
// mode applies backpressure: when the shard queue is full and the latch is not
// set, the handler waits for the worker to drain instead of dropping.
func TestServer_txmetaHandler_StartupModeBlocksUntilDrained(t *testing.T) {
	server := &Server{
		logger: ulogger.TestLogger{},
	}
	// Latch defaults to false (startup mode).

	server.txmetaWorkerInitOnce.Do(func() {})
	ch := make(chan *txmetaShardBatch, 1)
	server.txmetaWorkerQueues = []chan *txmetaShardBatch{ch}

	// Pre-fill the queue. Any further send blocks until a reader appears.
	ch <- &txmetaShardBatch{}

	// Drain a single item after a short delay; this should unblock the handler.
	go func() {
		time.Sleep(50 * time.Millisecond)
		<-ch // drain the prefill
	}()

	hash := chainhash.Hash{0}
	message := createKafkaMessageForHash(t, hash, txmetaActionADD, []byte("payload"))

	start := time.Now()
	err := server.txmetaHandler(context.Background(), message)
	elapsed := time.Since(start)

	assert.NoError(t, err)
	assert.GreaterOrEqual(t, elapsed, 40*time.Millisecond, "handler should have blocked until drained")
	// The new item should now be sitting in the queue.
	assert.Len(t, ch, 1)
}

// TestServer_txmetaHandler_StartupModeUnblocksOnContextCancel verifies that a
// startup-mode blocking send unblocks when the context is cancelled, so the
// service shutdown is not blocked by a stuck worker.
func TestServer_txmetaHandler_StartupModeUnblocksOnContextCancel(t *testing.T) {
	server := &Server{
		logger: ulogger.TestLogger{},
	}

	server.txmetaWorkerInitOnce.Do(func() {})
	ch := make(chan *txmetaShardBatch) // unbuffered, no reader -> always blocks
	server.txmetaWorkerQueues = []chan *txmetaShardBatch{ch}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	hash := chainhash.Hash{0}
	message := createKafkaMessageForHash(t, hash, txmetaActionADD, []byte("payload"))

	err := server.txmetaHandler(ctx, message)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled))
}

// TestServer_txmetaHandler_LatchFlipsAtHighWaterMark verifies that observing a
// message at the partition tail (msg.Offset+1 == HighWaterMark) flips the
// caught-up latch.
func TestServer_txmetaHandler_LatchFlipsAtHighWaterMark(t *testing.T) {
	mockLogger := &mockLogger{}
	mockCache := &mockCache{}
	mockCache.On("SetCacheMulti", mock.Anything, mock.Anything).Return(nil)
	mockLogger.On("Infof", mock.Anything, mock.Anything).Return()

	server := &Server{
		logger:    mockLogger,
		utxoStore: mockCache,
	}

	hash := chainhash.Hash{7}
	msg := createKafkaMessageForHash(t, hash, txmetaActionADD, []byte("payload"))
	msg.Topic = "txmeta"
	msg.Partition = 0
	msg.Offset = 99
	msg.HighWaterMark = 100 // Offset+1 == HWM => tail

	require.False(t, server.txmetaCaughtUp.Load())

	err := server.txmetaHandler(context.Background(), msg)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		return server.txmetaCaughtUp.Load()
	}, 2*time.Second, 10*time.Millisecond, "latch should flip on tail message")
}

// TestServer_txmetaHandler_LatchIgnoredWhenHighWaterMarkUnset verifies that an
// unpopulated HighWaterMark (zero value) does NOT prematurely flip the latch.
// This protects callers that wire a KafkaMessage by hand without HWM info.
func TestServer_txmetaHandler_LatchIgnoredWhenHighWaterMarkUnset(t *testing.T) {
	mockLogger := &mockLogger{}
	mockCache := &mockCache{}
	mockCache.On("SetCacheMulti", mock.Anything, mock.Anything).Return(nil)

	server := &Server{
		logger:    mockLogger,
		utxoStore: mockCache,
	}

	hash := chainhash.Hash{7}
	msg := createKafkaMessageForHash(t, hash, txmetaActionADD, []byte("payload"))
	msg.Offset = 0
	msg.HighWaterMark = 0 // unset

	err := server.txmetaHandler(context.Background(), msg)
	assert.NoError(t, err)

	// Give the worker a moment in case the latch were going to flip async.
	time.Sleep(20 * time.Millisecond)
	assert.False(t, server.txmetaCaughtUp.Load(), "latch must not flip when HWM is unset")
}

// buildMultiOpKafkaMessage encodes N entries into a single Kafka message in
// the txmetaHandler binary format. Used by the bucketing and ordering tests
// that need more than one entry per message.
func buildMultiOpKafkaMessage(t *testing.T, entries []struct {
	hash    chainhash.Hash
	action  byte
	content []byte
},
) *kafka.KafkaMessage {
	t.Helper()

	size := 4
	for _, e := range entries {
		size += 32 + 1 + 4
		if e.action == txmetaActionADD {
			size += len(e.content)
		}
	}
	buf := make([]byte, size)
	off := 0

	binary.LittleEndian.PutUint32(buf[off:], uint32(len(entries)))
	off += 4

	for _, e := range entries {
		copy(buf[off:], e.hash[:])
		off += 32
		buf[off] = e.action
		off++
		if e.action == txmetaActionADD {
			binary.LittleEndian.PutUint32(buf[off:], uint32(len(e.content)))
			off += 4
			copy(buf[off:], e.content)
			off += len(e.content)
		} else {
			binary.LittleEndian.PutUint32(buf[off:], 0)
			off += 4
		}
	}

	return &kafka.KafkaMessage{Value: buf}
}

// TestServer_txmetaHandler_ShardBucketingByHashByte verifies the central
// invariant of the per-shard dispatch model: entries from a single Kafka
// message are bucketed across shards by hash[0], and each shard batch
// contains only entries whose hash[0] matches its shard index.
func TestServer_txmetaHandler_ShardBucketingByHashByte(t *testing.T) {
	server := &Server{logger: ulogger.TestLogger{}}
	// Caught-up mode so the dispatch is non-blocking and we don't need a
	// reader on every channel.
	server.txmetaCaughtUp.Store(true)

	// Pre-create all shard channels and skip starting real workers; we
	// inspect what landed in each queue directly.
	server.txmetaWorkerInitOnce.Do(func() {})
	queues := make([]chan *txmetaShardBatch, txmetaWorkerShardCount)
	for i := range queues {
		queues[i] = make(chan *txmetaShardBatch, 1)
	}
	server.txmetaWorkerQueues = queues

	// Two entries per shard: one ADD and one DELETE, with hash[0] = shard
	// and hash[1] = 0 or 1 to keep entries distinct.
	type entry = struct {
		hash    chainhash.Hash
		action  byte
		content []byte
	}
	entries := make([]entry, 0, 2*txmetaWorkerShardCount)
	for shard := 0; shard < txmetaWorkerShardCount; shard++ {
		var h1, h2 chainhash.Hash
		h1[0] = byte(shard)
		h1[1] = 0
		h2[0] = byte(shard)
		h2[1] = 1
		entries = append(entries,
			entry{hash: h1, action: txmetaActionADD, content: []byte{byte(shard)}},
			entry{hash: h2, action: txmetaActionDELETE},
		)
	}

	msg := buildMultiOpKafkaMessage(t, entries)
	require.NoError(t, server.txmetaHandler(context.Background(), msg))

	for shard := 0; shard < txmetaWorkerShardCount; shard++ {
		select {
		case b := <-queues[shard]:
			require.Len(t, b.ops, 2, "shard %d should have 2 ops", shard)
			for _, op := range b.ops {
				require.Equal(t, byte(shard), op.hash[0], "shard %d received op for hash[0]=%d", shard, op.hash[0])
			}
		default:
			t.Fatalf("shard %d received no batch", shard)
		}
	}
}

// TestServer_txmetaHandler_PreservesOrderWithinKafkaMessage verifies that when
// a single Kafka message contains interleaved ADD/DELETE ops for the same hash
// (and therefore the same shard), the worker applies them in arrival order:
// ADD → DELETE → ADD must result in the cache holding the second ADD's value,
// not the first.
func TestServer_txmetaHandler_PreservesOrderWithinKafkaMessage(t *testing.T) {
	mockLogger := &mockLogger{}
	mockCache := &mockCache{}

	var (
		mu  sync.Mutex
		ops []string
	)
	mockCache.On("SetCacheMulti", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		mu.Lock()
		defer mu.Unlock()
		ops = append(ops, "add")
	})
	mockCache.On("Delete", mock.Anything, mock.AnythingOfType("*chainhash.Hash")).Return(nil).Run(func(args mock.Arguments) {
		mu.Lock()
		defer mu.Unlock()
		ops = append(ops, "delete")
	})

	server := &Server{
		logger:    mockLogger,
		utxoStore: mockCache,
	}

	hash := chainhash.Hash{42}
	msg := buildMultiOpKafkaMessage(t, []struct {
		hash    chainhash.Hash
		action  byte
		content []byte
	}{
		{hash: hash, action: txmetaActionADD, content: []byte("v1")},
		{hash: hash, action: txmetaActionDELETE},
		{hash: hash, action: txmetaActionADD, content: []byte("v2")},
	})

	require.NoError(t, server.txmetaHandler(context.Background(), msg))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(ops) == 3
	}, 2*time.Second, time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"add", "delete", "add"}, ops)
}

// TestServer_txmetaHandler_TruncatedMessageDoesNotLatch verifies that a
// truncated Kafka message neither dispatches partial work nor flips the
// caught-up latch even when the message claims to be at the partition's tail.
func TestServer_txmetaHandler_TruncatedMessageDoesNotLatch(t *testing.T) {
	mockLogger := &mockLogger{}
	mockCache := &mockCache{}
	mockLogger.On("Errorf", mock.Anything, mock.Anything).Return()

	server := &Server{
		logger:    mockLogger,
		utxoStore: mockCache,
	}

	// Claim 2 entries but only encode header for 1 entry's worth of bytes
	// (no content); the second entry's header trips the truncation check.
	buf := make([]byte, 4+32+1+4)
	binary.LittleEndian.PutUint32(buf, 2)
	// Leave the rest zeroed; entry-1 header is read, then the loop's next
	// iteration sees offset+32+1+4 > len(buf) and bails.

	msg := &kafka.KafkaMessage{
		Value:         buf,
		Offset:        99,
		HighWaterMark: 100, // would normally flip the latch
	}

	require.NoError(t, server.txmetaHandler(context.Background(), msg))

	// Give the async worker plenty of time to NOT do anything.
	time.Sleep(20 * time.Millisecond)

	require.False(t, server.txmetaCaughtUp.Load(), "latch must not flip on truncated message")
	mockCache.AssertNotCalled(t, "SetCacheMulti", mock.Anything, mock.Anything)
	mockCache.AssertNotCalled(t, "Delete", mock.Anything, mock.Anything)
}

// TestServer_txmetaHandler_LatchIsOneWay verifies that once the latch is set,
// subsequent messages that look like they are lagging (Offset+1 < HWM) do not
// revert the latch.
func TestServer_txmetaHandler_LatchIsOneWay(t *testing.T) {
	mockLogger := &mockLogger{}
	mockCache := &mockCache{}
	mockCache.On("SetCacheMulti", mock.Anything, mock.Anything).Return(nil)
	mockLogger.On("Infof", mock.Anything, mock.Anything).Return()

	server := &Server{
		logger:    mockLogger,
		utxoStore: mockCache,
	}

	hash := chainhash.Hash{9}

	// First message at the tail -> flips latch.
	first := createKafkaMessageForHash(t, hash, txmetaActionADD, []byte("a"))
	first.Offset = 10
	first.HighWaterMark = 11
	require.NoError(t, server.txmetaHandler(context.Background(), first))

	assert.Eventually(t, func() bool {
		return server.txmetaCaughtUp.Load()
	}, 2*time.Second, 10*time.Millisecond)

	// Second message lagging far behind a moved-on HWM. Latch must remain set.
	second := createKafkaMessageForHash(t, hash, txmetaActionADD, []byte("b"))
	second.Offset = 12
	second.HighWaterMark = 5000
	require.NoError(t, server.txmetaHandler(context.Background(), second))

	assert.True(t, server.txmetaCaughtUp.Load(), "latch is one-way; must not revert")
}
