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
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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

// DeleteDemoted shadows the embedded zero TxMetaCache's, which has no backend.
func (m *mockCache) DeleteDemoted(hashes []chainhash.Hash) {
	for i := range hashes {
		_ = m.Delete(context.Background(), &hashes[i])
	}
}

func (m *mockCache) Delete(ctx context.Context, hash *chainhash.Hash) error {
	args := m.Called(ctx, hash)
	return args.Error(0)
}

func (m *mockCache) DeleteComplete(ctx context.Context, hash *chainhash.Hash) error {
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

func (m *mockCache) SupportsOutpointOnlySpend() bool { return false }

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
	action := txmetacache.WireActionADD
	if delete {
		action = txmetacache.WireActionDELETE
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
	if action == txmetacache.WireActionDELETE {
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
	addMessage := createKafkaMessageForHash(t, hash, txmetacache.WireActionADD, []byte("payload"))
	deleteMessage := createKafkaMessageForHash(t, hash, txmetacache.WireActionDELETE, nil)

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
	data[0] = txmetacache.WireV2Magic
	data[1] = txmetacache.WireV2Version
	binary.LittleEndian.PutUint32(data[4:], 1)

	off := 8
	binary.LittleEndian.PutUint64(data[off:], 0xCAFEBABE)
	off += 8
	hash := chainhash.Hash{42}
	copy(data[off:], hash[:])
	off += 32
	data[off] = txmetacache.WireActionADD
	off++
	binary.LittleEndian.PutUint32(data[off:], uint32(len(payload)))
	off += 4
	copy(data[off:], payload)

	err := server.txmetaHandler(context.Background(), &kafka.KafkaMessage{Value: data})
	assert.NoError(t, err)
	mCache.AssertExpectations(t)
}

// TestServer_txmetaHandler_V2Shaped_UnknownVersion_FallsBackToV1 confirms
// that a v2-shaped message with an unknown sub-version is treated as v1
// (rather than dropped silently), so a legitimate v1 message with a count
// whose LE low byte is 0xFF is never misclassified. The 8-byte buffer is
// too small to hold a single v1 entry, so the v1 path logs a truncation
// error and acks — but crucially it does NOT call the v2 dispatch path.
func TestServer_txmetaHandler_V2Shaped_UnknownVersion_FallsBackToV1(t *testing.T) {
	mLogger := &mockLogger{}
	mLogger.On("Errorf", mock.Anything, mock.Anything).Return()
	mCache := &mockCache{}

	server := &Server{logger: mLogger, utxoStore: mCache}

	data := make([]byte, 8)
	data[0] = txmetacache.WireV2Magic
	data[1] = 0x99

	err := server.txmetaHandler(context.Background(), &kafka.KafkaMessage{Value: data})
	assert.NoError(t, err)
	// Must NOT take the v2 dispatch path — the v2-shaped-but-invalid
	// header must fall through to v1 parsing.
	mCache.AssertNotCalled(t, "SetCacheMultiSequentialWithHashes",
		mock.Anything, mock.Anything, mock.Anything)
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
	actions := []byte{txmetacache.WireActionADD, txmetacache.WireActionDELETE, txmetacache.WireActionADD}
	pseudoHashes := []uint64{0x11, 0x22, 0x33}

	size := 8 // header
	for i := range hashes {
		size += 8 + 32 + 1 + 4 + len(payloads[i])
	}
	data := make([]byte, size)
	data[0] = txmetacache.WireV2Magic
	data[1] = txmetacache.WireV2Version
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

// buildV1TxmetaMessage builds a v1-format batch message with the requested
// number of entries: one ADD followed by (count-1) DELETEs. Used to exercise
// the v1/v2 discriminator collision cases below.
func buildV1TxmetaMessage(t *testing.T, count int, addHash chainhash.Hash, addPayload []byte) []byte {
	t.Helper()

	size := 4 + 32 + 1 + 4 + len(addPayload) + (count-1)*(32+1+4)
	buf := make([]byte, 0, size)

	hdr := make([]byte, 4)
	binary.LittleEndian.PutUint32(hdr, uint32(count))
	buf = append(buf, hdr...)

	// ADD entry.
	buf = append(buf, addHash[:]...)
	buf = append(buf, txmetacache.WireActionADD)
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(addPayload)))
	buf = append(buf, lenBuf...)
	buf = append(buf, addPayload...)

	// DELETE entries.
	deleteHash := chainhash.Hash{99}
	for i := 0; i < count-1; i++ {
		buf = append(buf, deleteHash[:]...)
		buf = append(buf, txmetacache.WireActionDELETE)
		buf = append(buf, 0, 0, 0, 0)
	}

	return buf
}

// TestServer_txmetaHandler_V1_ImplausibleEntryCount_IsRejected verifies
// that a v1 message claiming far more entries than the buffer can hold is
// logged and acked rather than triggering an oversized pre-loop allocation.
// The wire claims max-uint32 entries (~4B) with only enough buffer for a
// few; without the plausibility bound, the pool would try to allocate
// ~128GB for keysBuf alone.
func TestServer_txmetaHandler_V1_ImplausibleEntryCount_IsRejected(t *testing.T) {
	mLogger := &mockLogger{}
	mLogger.On("Errorf", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()
	mCache := &mockCache{}

	server := &Server{logger: mLogger, utxoStore: mCache}

	// 4-byte header claiming 0xFFFFFFFE entries (just under max-uint32,
	// chosen so the first byte is 0xFE — not a v2 magic alias — to force
	// the v1 detection path), with no entry payload at all.
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, 0xFFFFFFFE)

	err := server.txmetaHandler(context.Background(), &kafka.KafkaMessage{Value: data})
	assert.NoError(t, err)
	// Neither dispatch path should be reached — the message is rejected
	// before any entries are parsed.
	mCache.AssertNotCalled(t, "SetCacheMultiSequential", mock.Anything, mock.Anything)
	mCache.AssertNotCalled(t, "SetCacheMultiSequentialWithHashes",
		mock.Anything, mock.Anything, mock.Anything)
	mLogger.AssertCalled(t, "Errorf",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

// TestServer_txmetaHandler_V1_CollidesWithV2Magic verifies that v1 messages
// whose entry count has a low byte of 0xFF (counts 255, 511, 767, ...) are
// still parsed as v1 instead of being misclassified as v2 and dropped/garbled.
func TestServer_txmetaHandler_V1_CollidesWithV2Magic(t *testing.T) {
	cases := []struct {
		name         string
		count        int
		headerPrefix []byte
	}{
		{
			name:         "count 255 (LE low byte 0xFF, version byte 0x00)",
			count:        255,
			headerPrefix: []byte{0xFF, 0x00, 0x00, 0x00},
		},
		{
			name:         "count 767 (full v2 header alias 0xFF 0x02 0x00 0x00)",
			count:        767,
			headerPrefix: []byte{0xFF, 0x02, 0x00, 0x00},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mLogger := &mockLogger{}
			mLogger.On("Errorf", mock.Anything, mock.Anything).Maybe().Return()
			mCache := &mockCache{}
			// Must take the v1-shaped Set path (no on-wire xxhash).
			// SetCacheMultiSequential takes (keys, values); WithHashes takes (keys, values, hashes).
			mCache.On("SetCacheMultiSequential", mock.Anything, mock.Anything).Return(nil)
			mCache.On("Delete", mock.Anything, mock.AnythingOfType("*chainhash.Hash")).Return(nil).Maybe()

			server := &Server{logger: mLogger, utxoStore: mCache}

			addHash := chainhash.Hash{1, 2, 3}
			payload := []byte("v1-payload")
			data := buildV1TxmetaMessage(t, tc.count, addHash, payload)
			assert.Equal(t, tc.headerPrefix, data[:4],
				"v1 length-prefix must alias the v2 header bytes for this regression")

			err := server.txmetaHandler(context.Background(), &kafka.KafkaMessage{Value: data})
			assert.NoError(t, err)
			mCache.AssertCalled(t, "SetCacheMultiSequential",
				mock.Anything, mock.Anything)
			mCache.AssertNotCalled(t, "SetCacheMultiSequentialWithHashes",
				mock.Anything, mock.Anything, mock.Anything)
		})
	}
}

// v1LaneMessage is a one-entry v1 txmeta message whose entry has the ADDLane action.
func v1LaneMessage(hash chainhash.Hash, content []byte) *kafka.KafkaMessage {
	data := make([]byte, 4+32+1+4+len(content))
	binary.LittleEndian.PutUint32(data, 1)
	copy(data[4:], hash[:])
	data[36] = txmetacache.WireActionADDLane
	binary.LittleEndian.PutUint32(data[37:], uint32(len(content))) //nolint:gosec // test content is small
	copy(data[41:], content)

	return &kafka.KafkaMessage{Value: data}
}

// An entry in the lane encoding, which the validator sends at utxostore_laneMigration shadow, goes
// into the cache as the MetaBytes a plain entry carries; one that does not decode is dropped.
func TestServer_txmetaHandler_LaneEntries(t *testing.T) {
	tx := bt.NewTx()
	in := &bt.Input{}
	require.NoError(t, in.PreviousTxIDAdd(&chainhash.Hash{5}))
	tx.Inputs = append(tx.Inputs, in)

	inpoints, err := subtreepkg.NewTxInpointsFromTx(tx)
	require.NoError(t, err)

	d := &meta.Data{TxInpoints: inpoints, Fee: 423, SizeInBytes: 250, LanePosition: lane.Pack(9, 1)}

	content, err := d.LaneMetaBytesInto(nil, []uint64{lane.Pack(9, 2)})
	require.NoError(t, err)

	legacy, err := d.MetaBytes()
	require.NoError(t, err)

	t.Run("decodes into the cache", func(t *testing.T) {
		mCache := &mockCache{}
		mCache.On("SetCacheMultiSequential", mock.Anything, [][]byte{legacy}).Return(nil)

		server := &Server{logger: &mockLogger{}, utxoStore: mCache}

		require.NoError(t, server.txmetaHandler(context.Background(), v1LaneMessage(chainhash.Hash{1}, content)))
		mCache.AssertExpectations(t)
	})

	t.Run("one that does not decode is dropped", func(t *testing.T) {
		mCache := &mockCache{}
		mLogger := &mockLogger{}
		mLogger.On("Debugf", mock.Anything, mock.Anything).Return()

		server := &Server{logger: mLogger, utxoStore: mCache}

		require.NoError(t, server.txmetaHandler(context.Background(), v1LaneMessage(chainhash.Hash{1}, legacy)))
		mCache.AssertNotCalled(t, "SetCacheMultiSequential", mock.Anything, mock.Anything)
	})
}

// txmetaEntry is one entry of a txmeta message built by txmetaMessage.
type txmetaEntry struct {
	hash      chainhash.Hash
	entryHash uint64
	action    byte
	content   []byte
}

// txmetaMessage builds a v1 or v2 txmeta message holding entries, in order.
func txmetaMessage(v2 bool, entries ...txmetaEntry) *kafka.KafkaMessage {
	var data []byte

	if v2 {
		data = append(data, txmetacache.WireV2Magic, txmetacache.WireV2Version, 0, 0)
	}

	data = binary.LittleEndian.AppendUint32(data, uint32(len(entries))) //nolint:gosec // a few test entries

	for _, e := range entries {
		if v2 {
			data = binary.LittleEndian.AppendUint64(data, e.entryHash)
		}

		data = append(data, e.hash[:]...)
		data = append(data, e.action)
		data = binary.LittleEndian.AppendUint32(data, uint32(len(e.content))) //nolint:gosec // small test content
		data = append(data, e.content...)
	}

	return &kafka.KafkaMessage{Value: data}
}

// laneTxMetaReceivedCount reads teranode_lane_txmeta_messages_total{side="subtreevalidation", format}.
func laneTxMetaReceivedCount(t *testing.T, format string) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, family := range families {
		if family.GetName() != "teranode_lane_txmeta_messages_total" {
			continue
		}

		for _, m := range family.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}

			if labels["side"] == "subtreevalidation" && labels["format"] == format {
				return m.GetCounter().GetValue()
			}
		}
	}

	return 0
}

// A lane entry in a v2 message reaches the cache with its own key, the MetaBytes it holds and its
// entry hash. A malformed lane entry is dropped without the entries after it, and counted: the
// lane format whatever this consumer's setting, the error counter, and a plain entry only at shadow.
func TestServer_txmetaHandler_LaneEntriesKeepTheirPlace(t *testing.T) {
	tx := bt.NewTx()
	in := &bt.Input{}
	require.NoError(t, in.PreviousTxIDAdd(&chainhash.Hash{5}))
	tx.Inputs = append(tx.Inputs, in)

	inpoints, err := subtreepkg.NewTxInpointsFromTx(tx)
	require.NoError(t, err)

	d := &meta.Data{TxInpoints: inpoints, Fee: 423, SizeInBytes: 250, LanePosition: lane.Pack(9, 1)}

	content, err := d.LaneMetaBytesInto(nil, []uint64{lane.Pack(9, 2)})
	require.NoError(t, err)

	legacy, err := d.MetaBytes()
	require.NoError(t, err)

	// At shadow the cache keeps the own position after the MetaBytes, for the block gate.
	cached, err := d.CacheBytes()
	require.NoError(t, err)
	require.Equal(t, append(append([]byte(nil), legacy...), lane.AppendPosition(nil, d.LanePosition)...), cached)

	shadow := &settings.Settings{UtxoStore: settings.UtxoStoreSettings{LaneMigration: settings.LaneMigrationShadow}}

	for _, tc := range []struct {
		name     string
		settings *settings.Settings
		value    []byte
	}{
		{name: "off, the MetaBytes", settings: &settings.Settings{}, value: legacy},
		{name: "shadow, the MetaBytes and the own position", settings: shadow, value: cached},
	} {
		t.Run("v2 at "+tc.name+": the key, the value and the entry hash", func(t *testing.T) {
			key := chainhash.Hash{7}

			mCache := &mockCache{}
			mCache.On("SetCacheMultiSequentialWithHashes", [][]byte{key[:]}, [][]byte{tc.value}, []uint64{0xBEEF}).Return(nil)

			server := &Server{logger: &mockLogger{}, utxoStore: mCache, settings: tc.settings}

			msg := txmetaMessage(true, txmetaEntry{hash: key, entryHash: 0xBEEF, action: txmetacache.WireActionADDLane, content: content})
			require.NoError(t, server.txmetaHandler(context.Background(), msg))
			mCache.AssertExpectations(t)
		})
	}

	t.Run("shadow: several lane entries each keep their own value in one message", func(t *testing.T) {
		k1, k2 := chainhash.Hash{8}, chainhash.Hash{9}

		other := &meta.Data{TxInpoints: inpoints, Fee: 1, SizeInBytes: 2, LanePosition: lane.Pack(3, 77)}

		otherContent, err := other.LaneMetaBytesInto(nil, []uint64{lane.Pack(9, 2)})
		require.NoError(t, err)

		otherCached, err := other.CacheBytes()
		require.NoError(t, err)

		mCache := &mockCache{}
		mCache.On("SetCacheMultiSequentialWithHashes", [][]byte{k1[:], k2[:]}, [][]byte{cached, otherCached}, []uint64{1, 2}).Return(nil)

		server := &Server{logger: &mockLogger{}, utxoStore: mCache, settings: shadow}

		msg := txmetaMessage(true,
			txmetaEntry{hash: k1, entryHash: 1, action: txmetacache.WireActionADDLane, content: content},
			txmetaEntry{hash: k2, entryHash: 2, action: txmetacache.WireActionADDLane, content: otherContent},
		)
		require.NoError(t, server.txmetaHandler(context.Background(), msg))
		mCache.AssertExpectations(t)
	})

	for _, tc := range []struct {
		name     string
		settings *settings.Settings
		legacy   float64
	}{
		{name: "off", settings: &settings.Settings{}, legacy: 0},
		{name: "shadow", settings: shadow, legacy: 1},
	} {
		t.Run("v1 at "+tc.name+": a malformed lane entry drops only itself", func(t *testing.T) {
			bad, good := chainhash.Hash{1}, chainhash.Hash{2}

			mCache := &mockCache{}
			mCache.On("SetCacheMultiSequential", [][]byte{good[:]}, [][]byte{legacy}).Return(nil)

			mLogger := &mockLogger{}
			mLogger.On("Debugf", mock.Anything, mock.Anything).Return()

			server := &Server{logger: mLogger, utxoStore: mCache, settings: tc.settings}

			lanes, plain := laneTxMetaReceivedCount(t, "lane"), laneTxMetaReceivedCount(t, "legacy")
			errs := testutil.ToFloat64(prometheusSubtreeValidationSetTXMetaCacheKafkaErrors)

			msg := txmetaMessage(false,
				txmetaEntry{hash: bad, action: txmetacache.WireActionADDLane, content: legacy},
				txmetaEntry{hash: good, action: txmetacache.WireActionADD, content: legacy},
			)
			require.NoError(t, server.txmetaHandler(context.Background(), msg))
			mCache.AssertExpectations(t)

			require.InDelta(t, lanes+1, laneTxMetaReceivedCount(t, "lane"), 0, "the malformed lane entry still counts as lane")
			require.InDelta(t, plain+tc.legacy, laneTxMetaReceivedCount(t, "legacy"), 0, "a plain entry counts only at shadow")
			require.InDelta(t, errs+1, testutil.ToFloat64(prometheusSubtreeValidationSetTXMetaCacheKafkaErrors), 0)
		})
	}
}

// An own-position entry, sent when the validator knows the transaction's position but not every
// parent's, reaches the cache whole at shadow, so subtree validation finds the position when it
// builds the subtree's lane bitmaps; with lane positions off the cache keeps its MetaBytes. A
// malformed one is dropped without the entries after it, and counted.
func TestServer_txmetaHandler_OwnPositionEntries(t *testing.T) {
	tx := bt.NewTx()
	in := &bt.Input{}
	require.NoError(t, in.PreviousTxIDAdd(&chainhash.Hash{5}))
	tx.Inputs = append(tx.Inputs, in)

	inpoints, err := subtreepkg.NewTxInpointsFromTx(tx)
	require.NoError(t, err)

	d := &meta.Data{TxInpoints: inpoints, Fee: 423, SizeInBytes: 250, LanePosition: lane.Pack(8, 14)}

	content, err := d.CacheBytes()
	require.NoError(t, err)

	legacy, err := d.MetaBytes()
	require.NoError(t, err)

	shadow := &settings.Settings{UtxoStore: settings.UtxoStoreSettings{LaneMigration: settings.LaneMigrationShadow}}

	for _, tc := range []struct {
		name     string
		settings *settings.Settings
		value    []byte
	}{
		{name: "off, the MetaBytes", settings: &settings.Settings{}, value: legacy},
		{name: "shadow, the MetaBytes and the own position", settings: shadow, value: content},
	} {
		t.Run("v2 at "+tc.name, func(t *testing.T) {
			key := chainhash.Hash{7}

			mCache := &mockCache{}
			mCache.On("SetCacheMultiSequentialWithHashes", [][]byte{key[:]}, [][]byte{tc.value}, []uint64{0xBEEF}).Return(nil)

			server := &Server{logger: &mockLogger{}, utxoStore: mCache, settings: tc.settings}

			own := laneTxMetaReceivedCount(t, "own")

			msg := txmetaMessage(true, txmetaEntry{hash: key, entryHash: 0xBEEF, action: txmetacache.WireActionADDOwn, content: content})
			require.NoError(t, server.txmetaHandler(context.Background(), msg))
			mCache.AssertExpectations(t)

			require.InDelta(t, own+1, laneTxMetaReceivedCount(t, "own"), 0, "counted whatever the setting")
		})
	}

	t.Run("v1 at shadow: a malformed own entry drops only itself", func(t *testing.T) {
		bad, good := chainhash.Hash{1}, chainhash.Hash{2}

		mCache := &mockCache{}
		mCache.On("SetCacheMultiSequential", [][]byte{good[:]}, [][]byte{legacy}).Return(nil)

		mLogger := &mockLogger{}
		mLogger.On("Debugf", mock.Anything, mock.Anything).Return()

		server := &Server{logger: mLogger, utxoStore: mCache, settings: shadow}

		errs := testutil.ToFloat64(prometheusSubtreeValidationSetTXMetaCacheKafkaErrors)

		msg := txmetaMessage(false,
			txmetaEntry{hash: bad, action: txmetacache.WireActionADDOwn, content: legacy},
			txmetaEntry{hash: good, action: txmetacache.WireActionADD, content: legacy},
		)
		require.NoError(t, server.txmetaHandler(context.Background(), msg))
		mCache.AssertExpectations(t)

		require.InDelta(t, errs+1, testutil.ToFloat64(prometheusSubtreeValidationSetTXMetaCacheKafkaErrors), 0)
	})
}
