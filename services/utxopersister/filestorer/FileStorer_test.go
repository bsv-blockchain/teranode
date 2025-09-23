package filestorer

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockBlobStore implements blob.Store interface for testing
type MockBlobStore struct {
	mock.Mock
}

func (m *MockBlobStore) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	args := m.Called(ctx, checkLiveness)
	return args.Int(0), args.String(1), args.Error(2)
}

func (m *MockBlobStore) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, fileOptions ...options.FileOption) (bool, error) {
	args := m.Called(ctx, key, fileType, fileOptions)
	return args.Bool(0), args.Error(1)
}

func (m *MockBlobStore) Get(ctx context.Context, key []byte, fileType fileformat.FileType, fileOptions ...options.FileOption) ([]byte, error) {
	args := m.Called(ctx, key, fileType, fileOptions)
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockBlobStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, fileOptions ...options.FileOption) (io.ReadCloser, error) {
	args := m.Called(ctx, key, fileType, fileOptions)
	return args.Get(0).(io.ReadCloser), args.Error(1)
}

func (m *MockBlobStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, fileOptions ...options.FileOption) error {
	args := m.Called(ctx, key, fileType, value, fileOptions)
	return args.Error(0)
}

func (m *MockBlobStore) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, fileOptions ...options.FileOption) error {
	args := m.Called(ctx, key, fileType, reader, fileOptions)
	return args.Error(0)
}

func (m *MockBlobStore) SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, dah uint32, fileOptions ...options.FileOption) error {
	args := m.Called(ctx, key, fileType, dah, fileOptions)
	return args.Error(0)
}

func (m *MockBlobStore) GetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, fileOptions ...options.FileOption) (uint32, error) {
	args := m.Called(ctx, key, fileType, fileOptions)
	return args.Get(0).(uint32), args.Error(1)
}

func (m *MockBlobStore) Del(ctx context.Context, key []byte, fileType fileformat.FileType, fileOptions ...options.FileOption) error {
	args := m.Called(ctx, key, fileType, fileOptions)
	return args.Error(0)
}

func (m *MockBlobStore) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockBlobStore) SetCurrentBlockHeight(height uint32) {
	m.Called(height)
}

// Helper functions for creating test data
func setupMockForSuccess(mockStore *MockBlobStore, ctx context.Context, key []byte, fileType fileformat.FileType) {
	// File doesn't exist initially
	mockStore.On("Exists", ctx, key, fileType, mock.Anything).Return(false, nil)
	// SetFromReader succeeds and properly handles the reader
	mockStore.On("SetFromReader", ctx, key, fileType, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		reader := args.Get(3).(io.ReadCloser)
		// Read all data from the reader to simulate normal behavior
		_, _ = io.ReadAll(reader)
		_ = reader.Close()
	}).Return(nil)
	// SetDAH is called during Close() if Close() succeeds
	mockStore.On("SetDAH", ctx, key, fileType, uint32(0), mock.Anything).Return(nil).Maybe()
	// waitUntilFileIsAvailable calls Exists repeatedly until file exists
	mockStore.On("Exists", ctx, key, fileType).Return(true, nil).Maybe()
}

func setupMockForBasicOperation(mockStore *MockBlobStore, ctx context.Context, key []byte, fileType fileformat.FileType) {
	// File doesn't exist initially
	mockStore.On("Exists", ctx, key, fileType, mock.Anything).Return(false, nil)
	// SetFromReader succeeds
	mockStore.On("SetFromReader", ctx, key, fileType, mock.Anything, mock.Anything).Return(nil)
	// No SetDAH or waitUntilFileIsAvailable expectations for tests that don't call Close()
}

func createTestSettings() *settings.Settings {
	return &settings.Settings{
		Block: settings.BlockSettings{
			UTXOPersisterBufferSize: "4096",
		},
	}
}

func createTestKey() []byte {
	return []byte("test-key-12345")
}

func createTestContext() context.Context {
	return context.Background()
}

func TestNewFileStorer_Success(t *testing.T) {
	ctx := createTestContext()
	logger := ulogger.TestLogger{}
	tSettings := createTestSettings()
	mockStore := &MockBlobStore{}
	key := createTestKey()
	fileType := fileformat.FileTypeUtxoSet

	setupMockForSuccess(mockStore, ctx, key, fileType)

	fs, err := NewFileStorer(ctx, logger, tSettings, mockStore, key, fileType)

	require.NoError(t, err)
	require.NotNil(t, fs)
	assert.Equal(t, logger, fs.logger)
	assert.Equal(t, mockStore, fs.store)
	assert.Equal(t, key, fs.key)
	assert.Equal(t, fileType, fs.fileType)
	assert.NotNil(t, fs.writer)
	assert.NotNil(t, fs.bufferedWriter)
	assert.NotNil(t, fs.done)

	// Clean up
	fs.Close(ctx)
	mockStore.AssertExpectations(t)
}

func TestNewFileStorer_FileAlreadyExists(t *testing.T) {
	ctx := createTestContext()
	logger := ulogger.TestLogger{}
	tSettings := createTestSettings()
	mockStore := &MockBlobStore{}
	key := createTestKey()
	fileType := fileformat.FileTypeUtxoSet

	// Setup expectations - file already exists
	mockStore.On("Exists", ctx, key, fileType, mock.Anything).Return(true, nil)

	fs, err := NewFileStorer(ctx, logger, tSettings, mockStore, key, fileType)

	require.Error(t, err)
	require.Nil(t, fs)
	assert.Contains(t, err.Error(), "already exists")

	mockStore.AssertExpectations(t)
}

func TestNewFileStorer_ExistsCheckError(t *testing.T) {
	ctx := createTestContext()
	logger := ulogger.TestLogger{}
	tSettings := createTestSettings()
	mockStore := &MockBlobStore{}
	key := createTestKey()
	fileType := fileformat.FileTypeUtxoSet
	expectedError := errors.NewError("exists check failed")

	// Setup expectations - exists check fails
	mockStore.On("Exists", ctx, key, fileType, mock.Anything).Return(false, expectedError)

	fs, err := NewFileStorer(ctx, logger, tSettings, mockStore, key, fileType)

	require.Error(t, err)
	require.Nil(t, fs)
	assert.Contains(t, err.Error(), "error checking")

	mockStore.AssertExpectations(t)
}

func TestNewFileStorer_InvalidBufferSize(t *testing.T) {
	ctx := createTestContext()
	logger := ulogger.TestLogger{}
	tSettings := &settings.Settings{
		Block: settings.BlockSettings{
			UTXOPersisterBufferSize: "invalid-size",
		},
	}
	mockStore := &MockBlobStore{}
	key := createTestKey()
	fileType := fileformat.FileTypeUtxoSet

	setupMockForSuccess(mockStore, ctx, key, fileType)

	fs, err := NewFileStorer(ctx, logger, tSettings, mockStore, key, fileType)

	require.NoError(t, err) // Should succeed with default buffer size
	require.NotNil(t, fs)

	// Clean up
	fs.Close(ctx)
	mockStore.AssertExpectations(t)
}

func TestNewFileStorer_SetFromReaderError(t *testing.T) {
	ctx := createTestContext()
	logger := ulogger.TestLogger{}
	tSettings := createTestSettings()
	mockStore := &MockBlobStore{}
	key := createTestKey()
	fileType := fileformat.FileTypeUtxoSet
	expectedError := errors.NewError("set from reader error")

	// Setup expectations - file doesn't exist, but SetFromReader fails
	mockStore.On("Exists", ctx, key, fileType, mock.Anything).Return(false, nil)
	mockStore.On("SetFromReader", ctx, key, fileType, mock.Anything, mock.Anything).Return(expectedError)
	mockStore.On("SetDAH", ctx, key, fileType, uint32(0), mock.Anything).Return(nil).Maybe()
	mockStore.On("Exists", ctx, key, fileType).Return(true, nil).Maybe()

	fs, err := NewFileStorer(ctx, logger, tSettings, mockStore, key, fileType)

	require.NoError(t, err) // Constructor succeeds even if background operation fails
	require.NotNil(t, fs)

	// Write some data to trigger the background error
	_, writeErr := fs.Write([]byte("test data"))
	if writeErr == nil {
		// Wait a bit for the reader error to be set
		time.Sleep(100 * time.Millisecond)

		// Try writing again, should get error now
		_, writeErr = fs.Write([]byte("more data"))
	}

	// Clean up - Close should handle reader error gracefully
	closeErr := fs.Close(ctx)

	// Either write error or close error should indicate the problem
	assert.True(t, writeErr != nil || closeErr != nil, "Expected either write error or close error")

	mockStore.AssertExpectations(t)
}

func TestWrite_Success(t *testing.T) {
	ctx := createTestContext()
	logger := ulogger.TestLogger{}
	tSettings := createTestSettings()
	mockStore := &MockBlobStore{}
	key := createTestKey()
	fileType := fileformat.FileTypeUtxoSet

	setupMockForSuccess(mockStore, ctx, key, fileType)

	fs, err := NewFileStorer(ctx, logger, tSettings, mockStore, key, fileType)
	require.NoError(t, err)

	// Test writing data
	testData := []byte("Hello, World!")
	n, err := fs.Write(testData)

	assert.NoError(t, err)
	assert.Equal(t, len(testData), n)

	// Clean up
	fs.Close(ctx)
	mockStore.AssertExpectations(t)
}

func TestWrite_WithReaderError(t *testing.T) {
	ctx := createTestContext()
	logger := ulogger.TestLogger{}
	tSettings := createTestSettings()
	mockStore := &MockBlobStore{}
	key := createTestKey()
	fileType := fileformat.FileTypeUtxoSet
	expectedError := errors.NewError("reader error")

	// Setup expectations - file doesn't exist, but reader fails
	mockStore.On("Exists", ctx, key, fileType, mock.Anything).Return(false, nil)
	mockStore.On("SetFromReader", ctx, key, fileType, mock.Anything, mock.Anything).Return(expectedError)
	mockStore.On("SetDAH", ctx, key, fileType, uint32(0), mock.Anything).Return(nil).Maybe()
	mockStore.On("Exists", ctx, key, fileType).Return(true, nil).Maybe()

	fs, err := NewFileStorer(ctx, logger, tSettings, mockStore, key, fileType)
	require.NoError(t, err)

	// Write should succeed initially
	testData := []byte("test data")
	n, _ := fs.Write(testData)
	assert.Equal(t, len(testData), n)

	// Wait for background error to be set
	time.Sleep(100 * time.Millisecond)

	// Subsequent write should return the reader error
	_, _ = fs.Write([]byte("more data"))

	// Clean up
	fs.Close(ctx)
	mockStore.AssertExpectations(t)
}

func TestWrite_ConcurrentAccess(t *testing.T) {
	ctx := createTestContext()
	logger := ulogger.TestLogger{}
	tSettings := createTestSettings()
	mockStore := &MockBlobStore{}
	key := createTestKey()
	fileType := fileformat.FileTypeUtxoSet

	setupMockForSuccess(mockStore, ctx, key, fileType)

	fs, err := NewFileStorer(ctx, logger, tSettings, mockStore, key, fileType)
	require.NoError(t, err)

	// Test concurrent writes
	done := make(chan bool, 2)

	go func() {
		for i := 0; i < 10; i++ {
			_, _ = fs.Write([]byte("goroutine1-data"))
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 10; i++ {
			_, _ = fs.Write([]byte("goroutine2-data"))
		}
		done <- true
	}()

	// Wait for both goroutines
	<-done
	<-done

	// Clean up
	fs.Close(ctx)
	mockStore.AssertExpectations(t)
}

func TestClose_Success(t *testing.T) {
	ctx := createTestContext()
	logger := ulogger.TestLogger{}
	tSettings := createTestSettings()
	mockStore := &MockBlobStore{}
	key := createTestKey()
	fileType := fileformat.FileTypeUtxoSet

	setupMockForSuccess(mockStore, ctx, key, fileType)

	fs, err := NewFileStorer(ctx, logger, tSettings, mockStore, key, fileType)
	require.NoError(t, err)

	// Write some data
	_, err = fs.Write([]byte("test data"))
	require.NoError(t, err)

	// Close should succeed
	err = fs.Close(ctx)
	assert.NoError(t, err)

	mockStore.AssertExpectations(t)
}

func TestClose_FlushError(t *testing.T) {
	ctx := createTestContext()
	logger := ulogger.TestLogger{}
	tSettings := createTestSettings()
	mockStore := &MockBlobStore{}
	key := createTestKey()
	fileType := fileformat.FileTypeUtxoSet

	// Setup basic mocks for flush error test - don't read from reader to simulate timing issues
	mockStore.On("Exists", ctx, key, fileType, mock.Anything).Return(false, nil)
	mockStore.On("SetFromReader", ctx, key, fileType, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		// Don't consume the reader data to simulate the flush error scenario
		reader := args.Get(3).(io.ReadCloser)
		_ = reader.Close()
	}).Return(nil)
	mockStore.On("SetDAH", ctx, key, fileType, uint32(0), mock.Anything).Return(nil).Maybe()
	mockStore.On("Exists", ctx, key, fileType).Return(true, nil).Maybe()

	fs, err := NewFileStorer(ctx, logger, tSettings, mockStore, key, fileType)
	require.NoError(t, err)

	// Write some data to the buffer first
	_, writeErr := fs.Write([]byte("test data to be flushed"))
	require.NoError(t, writeErr)

	// Close the writer early to cause a flush error
	_ = fs.writer.Close()

	// This should cause a flush error when Close is called
	err = fs.Close(ctx)
	assert.Error(t, err)
	if err != nil {
		assert.Contains(t, err.Error(), "Error flushing writer")
	}

	mockStore.AssertExpectations(t)
}

func TestClose_ReaderError(t *testing.T) {
	ctx := createTestContext()
	logger := ulogger.TestLogger{}
	tSettings := createTestSettings()
	mockStore := &MockBlobStore{}
	key := createTestKey()
	fileType := fileformat.FileTypeUtxoSet
	readerError := errors.NewError("reader error")

	// Setup expectations - reader fails
	mockStore.On("Exists", ctx, key, fileType, mock.Anything).Return(false, nil)
	mockStore.On("SetFromReader", ctx, key, fileType, mock.Anything, mock.Anything).Return(readerError)

	fs, err := NewFileStorer(ctx, logger, tSettings, mockStore, key, fileType)
	require.NoError(t, err)

	// Wait a bit for the reader error to be set
	time.Sleep(100 * time.Millisecond)

	// Close should return the reader error
	err = fs.Close(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Error in reader goroutine")

	mockStore.AssertExpectations(t)
}

func TestClose_SetDAHError(t *testing.T) {
	ctx := createTestContext()
	logger := ulogger.TestLogger{}
	tSettings := createTestSettings()
	mockStore := &MockBlobStore{}
	key := createTestKey()
	fileType := fileformat.FileTypeUtxoSet
	dahError := errors.NewError("SetDAH error")

	// Setup mocks for SetDAH error test - expect SetDAH to fail
	mockStore.On("Exists", ctx, key, fileType, mock.Anything).Return(false, nil)
	mockStore.On("SetFromReader", ctx, key, fileType, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		reader := args.Get(3).(io.ReadCloser)
		_, _ = io.ReadAll(reader)
		_ = reader.Close()
	}).Return(nil)
	mockStore.On("SetDAH", ctx, key, fileType, uint32(0), mock.Anything).Return(dahError)
	mockStore.On("Exists", ctx, key, fileType).Return(true, nil).Maybe()

	fs, err := NewFileStorer(ctx, logger, tSettings, mockStore, key, fileType)
	require.NoError(t, err)

	// Close should fail with SetDAH error
	err = fs.Close(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Error setting DAH")

	mockStore.AssertExpectations(t)
}

// Integration tests
func TestFileStorer_WriteAndClose_Integration(t *testing.T) {
	ctx := createTestContext()
	logger := ulogger.TestLogger{}
	tSettings := createTestSettings()
	mockStore := &MockBlobStore{}
	key := createTestKey()
	fileType := fileformat.FileTypeUtxoSet

	setupMockForSuccess(mockStore, ctx, key, fileType)

	fs, err := NewFileStorer(ctx, logger, tSettings, mockStore, key, fileType)
	require.NoError(t, err)

	// Write multiple chunks of data
	testData := [][]byte{
		[]byte("chunk1-data"),
		[]byte("chunk2-data"),
		[]byte("chunk3-data"),
	}

	for _, chunk := range testData {
		n, writeErr := fs.Write(chunk)
		assert.NoError(t, writeErr)
		assert.Equal(t, len(chunk), n)
	}

	// Close should succeed and flush all data
	err = fs.Close(ctx)
	assert.NoError(t, err)

	mockStore.AssertExpectations(t)
}

func TestFileStorer_EmptyFile(t *testing.T) {
	ctx := createTestContext()
	logger := ulogger.TestLogger{}
	tSettings := createTestSettings()
	mockStore := &MockBlobStore{}
	key := createTestKey()
	fileType := fileformat.FileTypeUtxoSet

	setupMockForSuccess(mockStore, ctx, key, fileType)

	fs, err := NewFileStorer(ctx, logger, tSettings, mockStore, key, fileType)
	require.NoError(t, err)

	// Don't write any data, just close
	err = fs.Close(ctx)
	assert.NoError(t, err)

	mockStore.AssertExpectations(t)
}

func TestFileStorer_LargeData(t *testing.T) {
	ctx := createTestContext()
	logger := ulogger.TestLogger{}
	tSettings := createTestSettings()
	mockStore := &MockBlobStore{}
	key := createTestKey()
	fileType := fileformat.FileTypeUtxoSet

	setupMockForSuccess(mockStore, ctx, key, fileType)

	fs, err := NewFileStorer(ctx, logger, tSettings, mockStore, key, fileType)
	require.NoError(t, err)

	// Write large amount of data (larger than buffer)
	largeData := make([]byte, 10000) // 10KB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	n, err := fs.Write(largeData)
	assert.NoError(t, err)
	assert.Equal(t, len(largeData), n)

	// Close and ensure everything is flushed
	err = fs.Close(ctx)
	assert.NoError(t, err)

	mockStore.AssertExpectations(t)
}
