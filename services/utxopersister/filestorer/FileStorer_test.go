package filestorer

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockBlobStore implements blob.Store interface for testing
type MockBlobStore struct {
	mock.Mock
	setFromReaderHandler func(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, fileOptions ...options.FileOption) error
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
	if m.setFromReaderHandler != nil {
		return m.setFromReaderHandler(ctx, key, fileType, reader, fileOptions...)
	}
	args := m.Called(ctx, key, fileType, "reader", fileOptions)
	return args.Error(0)
}

func (m *MockBlobStore) SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, dah uint32, fileOptions ...options.FileOption) error {
	args := m.Called(ctx, key, fileType, dah, fileOptions)
	return args.Error(0)
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
	// SetFromReader succeeds and properly handles the reader - use custom handler to avoid reflection race
	mockStore.setFromReaderHandler = func(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, fileOptions ...options.FileOption) error {
		// Read all data from the reader to simulate normal behavior
		_, _ = io.ReadAll(reader)
		_ = reader.Close()
		return nil
	}
	// waitUntilFileIsAvailable calls Exists repeatedly until file exists
	mockStore.On("Exists", ctx, key, fileType).Return(true, nil).Maybe()
}

func setupMockForBasicOperation(mockStore *MockBlobStore, ctx context.Context, key []byte, fileType fileformat.FileType) {
	// File doesn't exist initially
	mockStore.On("Exists", ctx, key, fileType, mock.Anything).Return(false, nil)
	// SetFromReader succeeds - use custom handler to avoid reflection race
	mockStore.setFromReaderHandler = func(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, fileOptions ...options.FileOption) error {
		_, _ = io.ReadAll(reader)
		_ = reader.Close()
		return nil
	}
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
	mockStore.setFromReaderHandler = func(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, fileOptions ...options.FileOption) error {
		// Drain the pipe to prevent blocking, even though we're returning an error
		_, _ = io.ReadAll(reader)
		_ = reader.Close()
		return expectedError
	}
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
	// Use a very small buffer (1 byte) to force immediate flushing
	tSettings := &settings.Settings{
		Block: settings.BlockSettings{
			UTXOPersisterBufferSize: "1",
		},
	}
	mockStore := &MockBlobStore{}
	key := createTestKey()
	fileType := fileformat.FileTypeUtxoSet
	expectedError := errors.NewError("reader error")

	// Setup expectations - file doesn't exist, but reader fails during reading
	mockStore.On("Exists", ctx, key, fileType, mock.Anything).Return(false, nil)
	mockStore.setFromReaderHandler = func(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, fileOptions ...options.FileOption) error {
		// Read only a small amount before returning error to simulate failure during read
		buf := make([]byte, 5)
		_, _ = reader.Read(buf)
		_ = reader.Close()
		return expectedError
	}

	fs, err := NewFileStorer(ctx, logger, tSettings, mockStore, key, fileType)
	require.NoError(t, err)

	// First write may succeed, partially succeed with pipe error, or fail with reader error
	// depending on race conditions with the background goroutine
	testData := []byte("test data")
	n, writeErr := fs.Write(testData)

	// Three possible outcomes due to the race between write and background error:
	// 1. Write fully succeeds (n == len(testData), err == nil)
	// 2. Write partially succeeds with pipe error (0 < n < len(testData), err != nil)
	// 3. Write fails with reader error already set (n == 0, err == readerError)
	if writeErr != nil {
		// Either a partial write with pipe error, or reader error already set
		assert.GreaterOrEqual(t, n, 0, "Bytes written should be non-negative")
		assert.LessOrEqual(t, n, len(testData), "Bytes written should not exceed data length")
	} else {
		assert.Equal(t, len(testData), n, "If write succeeds, it should write all bytes")
	}

	// Wait for background error to be set (if not already set)
	time.Sleep(100 * time.Millisecond)

	// Subsequent write should definitely return the reader error now
	// The reader error should be set after the background goroutine completes
	n2, err2 := fs.Write([]byte("more data"))
	assert.Equal(t, 0, n2, "Subsequent write should return 0 bytes")
	assert.True(t, err2 != nil, "Subsequent write should return an error")
	assert.True(t, errors.Is(err2, expectedError), "Subsequent write should return the reader error")

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
	mockStore.setFromReaderHandler = func(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, fileOptions ...options.FileOption) error {
		// Don't consume the reader data to simulate the flush error scenario
		_ = reader.Close()
		return nil
	}
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
	mockStore.setFromReaderHandler = func(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, fileOptions ...options.FileOption) error {
		_, _ = io.ReadAll(reader)
		_ = reader.Close()
		return readerError
	}

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

func TestAbort_DoesNotCallSetDAH(t *testing.T) {
	ctx := createTestContext()
	logger := ulogger.TestLogger{}
	tSettings := createTestSettings()
	mockStore := &MockBlobStore{}
	key := createTestKey()
	fileType := fileformat.FileTypeUtxoSet

	// Setup - SetFromReader should receive an error via CloseWithError
	mockStore.On("Exists", ctx, key, fileType, mock.Anything).Return(false, nil)

	var readerReceivedError error
	mockStore.setFromReaderHandler = func(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, fileOptions ...options.FileOption) error {
		// Read from the reader until we get an error
		buf := make([]byte, 1024)
		for {
			_, err := reader.Read(buf)
			if err != nil {
				readerReceivedError = err
				break
			}
		}
		_ = reader.Close()
		return readerReceivedError
	}
	// NOTE: We do NOT expect SetDAH to be called since Abort() should prevent it

	fs, err := NewFileStorer(ctx, logger, tSettings, mockStore, key, fileType)
	require.NoError(t, err)

	// Write some data
	_, err = fs.Write([]byte("test data"))
	require.NoError(t, err)

	// Abort the storer - this should close the pipe with an error
	abortErr := errors.NewProcessingError("intentional abort")
	fs.Abort(abortErr)

	// Verify the reader received an error (not EOF)
	assert.NotNil(t, readerReceivedError, "Reader should have received an error from CloseWithError")
	assert.NotEqual(t, io.EOF, readerReceivedError, "Reader should receive abort error, not EOF")

	// SetDAH should NOT have been called (this is verified by mockStore.AssertExpectations
	// since we didn't set up an expectation for it)
	mockStore.AssertExpectations(t)
}

func TestAbort_WithNilError(t *testing.T) {
	ctx := createTestContext()
	logger := ulogger.TestLogger{}
	tSettings := createTestSettings()
	mockStore := &MockBlobStore{}
	key := createTestKey()
	fileType := fileformat.FileTypeUtxoSet

	mockStore.On("Exists", ctx, key, fileType, mock.Anything).Return(false, nil)

	var readerReceivedError error
	mockStore.setFromReaderHandler = func(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, fileOptions ...options.FileOption) error {
		buf := make([]byte, 1024)
		for {
			_, err := reader.Read(buf)
			if err != nil {
				readerReceivedError = err
				break
			}
		}
		_ = reader.Close()
		return readerReceivedError
	}

	fs, err := NewFileStorer(ctx, logger, tSettings, mockStore, key, fileType)
	require.NoError(t, err)

	_, err = fs.Write([]byte("test data"))
	require.NoError(t, err)

	// Abort with nil error - should use default error
	fs.Abort(nil)

	// Verify the reader received an error
	assert.NotNil(t, readerReceivedError, "Reader should have received an error from CloseWithError")
	assert.NotEqual(t, io.EOF, readerReceivedError, "Reader should receive abort error, not EOF")

	mockStore.AssertExpectations(t)
}

func TestAbort_SafeToCallMultipleTimes(t *testing.T) {
	ctx := createTestContext()
	logger := ulogger.TestLogger{}
	tSettings := createTestSettings()
	mockStore := &MockBlobStore{}
	key := createTestKey()
	fileType := fileformat.FileTypeUtxoSet

	mockStore.On("Exists", ctx, key, fileType, mock.Anything).Return(false, nil)
	mockStore.setFromReaderHandler = func(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, fileOptions ...options.FileOption) error {
		buf := make([]byte, 1024)
		for {
			_, err := reader.Read(buf)
			if err != nil {
				break
			}
		}
		_ = reader.Close()
		return errors.NewProcessingError("aborted")
	}

	fs, err := NewFileStorer(ctx, logger, tSettings, mockStore, key, fileType)
	require.NoError(t, err)

	_, err = fs.Write([]byte("test data"))
	require.NoError(t, err)

	// Call Abort multiple times - should not panic
	fs.Abort(errors.NewProcessingError("first abort"))
	fs.Abort(errors.NewProcessingError("second abort"))
	fs.Abort(nil)

	mockStore.AssertExpectations(t)
}

// failingWriter records everything offered to it and fails every write, so a bufio.Writer
// over it ends up holding both unflushed bytes and a sticky error.
type failingWriter struct {
	offered []byte
	err     error
}

func (w *failingWriter) Write(p []byte) (int, error) {
	w.offered = append(w.offered, p...)
	return 0, w.err
}

// TestSequentialStorersProduceIndependentBlobs pins the invariant the writer pool must not
// break: a storer handed a recycled buffer writes its own bytes and nothing else.
func TestSequentialStorersProduceIndependentBlobs(t *testing.T) {
	ctx := createTestContext()
	logger := ulogger.TestLogger{}
	tSettings := createTestSettings()
	store := memory.New()

	first := bytes.Repeat([]byte("a"), 3000)
	second := bytes.Repeat([]byte("b"), 40)

	writeBlob := func(key []byte, payload []byte) {
		fs, err := NewFileStorer(ctx, logger, tSettings, store, key, fileformat.FileTypeUtxoSet)
		require.NoError(t, err)

		n, err := fs.Write(payload)
		require.NoError(t, err)
		require.Equal(t, len(payload), n)
		require.NoError(t, fs.Close(ctx))
	}

	writeBlob([]byte("blob-one"), first)
	writeBlob([]byte("blob-two"), second)

	gotFirst, err := store.Get(ctx, []byte("blob-one"), fileformat.FileTypeUtxoSet)
	require.NoError(t, err)
	require.Equal(t, first, gotFirst)

	gotSecond, err := store.Get(ctx, []byte("blob-two"), fileformat.FileTypeUtxoSet)
	require.NoError(t, err)
	require.Equal(t, second, gotSecond)
	require.NotContains(t, string(gotSecond), "a", "the second blob must carry none of the first blob's bytes")
}

// TestWriteAfterTerminalReturnsError pins that a write issued once the buffer has gone back
// to the pool is an error rather than a nil dereference.
func TestWriteAfterTerminalReturnsError(t *testing.T) {
	ctx := createTestContext()
	logger := ulogger.TestLogger{}
	tSettings := createTestSettings()
	fileType := fileformat.FileTypeUtxoSet

	t.Run("after Close", func(t *testing.T) {
		store := memory.New()

		fs, err := NewFileStorer(ctx, logger, tSettings, store, []byte("write-after-close"), fileType)
		require.NoError(t, err)
		require.NoError(t, fs.Close(ctx))

		n, err := fs.Write([]byte("after close"))
		require.Error(t, err)
		require.Zero(t, n)
	})

	t.Run("after Abort", func(t *testing.T) {
		mockStore := &MockBlobStore{}
		key := createTestKey()

		// The handler returns nil, so no reader error is recorded and the released
		// buffer is the only thing that can make the write below fail.
		mockStore.On("Exists", ctx, key, fileType, mock.Anything).Return(false, nil)
		mockStore.setFromReaderHandler = func(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, fileOptions ...options.FileOption) error {
			buf := make([]byte, 1024)
			for {
				if _, readErr := reader.Read(buf); readErr != nil {
					break
				}
			}

			return reader.Close()
		}

		fs, err := NewFileStorer(ctx, logger, tSettings, mockStore, key, fileType)
		require.NoError(t, err)

		fs.Abort(errors.NewProcessingError("aborted by test"))

		n, err := fs.Write([]byte("after abort"))
		require.Error(t, err)
		require.Zero(t, n)
	})
}

// TestTerminalOrderingOutcomes asserts each terminal ordering individually rather than
// claiming repeated calls are stable: they are not, and the differences are intended.
func TestTerminalOrderingOutcomes(t *testing.T) {
	ctx := createTestContext()
	logger := ulogger.TestLogger{}
	tSettings := createTestSettings()
	fileType := fileformat.FileTypeUtxoSet

	t.Run("close then close", func(t *testing.T) {
		store := memory.New()

		fs, err := NewFileStorer(ctx, logger, tSettings, store, []byte("close-close"), fileType)
		require.NoError(t, err)

		_, err = fs.Write([]byte("payload"))
		require.NoError(t, err)

		require.NoError(t, fs.Close(ctx))
		require.NoError(t, fs.Close(ctx))
	})

	t.Run("close after flush error", func(t *testing.T) {
		store := memory.New()

		fs, err := NewFileStorer(ctx, logger, tSettings, store, []byte("flush-error-close"), fileType)
		require.NoError(t, err)

		_, err = fs.Write([]byte("payload"))
		require.NoError(t, err)

		// Closing the pipe's write side makes the flush inside Close fail.
		_ = fs.writer.Close()

		err = fs.Close(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Error flushing writer")

		// The second Close has no buffer to flush and finds no reader error, so it
		// returns nil - the same outcome as before, by a different route.
		require.NoError(t, fs.Close(ctx))
	})

	t.Run("close after abort", func(t *testing.T) {
		store := memory.New()

		fs, err := NewFileStorer(ctx, logger, tSettings, store, []byte("abort-close"), fileType)
		require.NoError(t, err)

		_, err = fs.Write([]byte("payload"))
		require.NoError(t, err)

		fs.Abort(errors.NewProcessingError("aborted by test"))

		err = fs.Close(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Error in reader goroutine")
	})

	t.Run("close then abort twice", func(t *testing.T) {
		store := memory.New()

		fs, err := NewFileStorer(ctx, logger, tSettings, store, []byte("close-abort-abort"), fileType)
		require.NoError(t, err)

		_, err = fs.Write([]byte("payload"))
		require.NoError(t, err)

		require.NoError(t, fs.Close(ctx))

		fs.Abort(errors.NewProcessingError("first abort after close"))
		fs.Abort(nil)
	})
}

// TestCloseBlockedInFlushIsUnblockedByAbort is the regression for the design in which Close
// and Abort shared a sync.Once: Close would sit in the flush holding the mutex while Abort
// waited on the Once, never reaching the CloseWithError that is the only thing able to
// release it.
func TestCloseBlockedInFlushIsUnblockedByAbort(t *testing.T) {
	ctx := createTestContext()
	logger := ulogger.TestLogger{}
	tSettings := createTestSettings()
	mockStore := &MockBlobStore{}
	key := createTestKey()
	fileType := fileformat.FileTypeUtxoSet

	entered := make(chan struct{})
	unblock := make(chan struct{})

	mockStore.On("Exists", ctx, key, fileType, mock.Anything).Return(false, nil)
	mockStore.setFromReaderHandler = func(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, fileOptions ...options.FileOption) error {
		close(entered)

		// Deliberately never read: the pipe cannot drain, so the flush inside Close
		// blocks until something closes the pipe.
		<-unblock

		return reader.Close()
	}

	fs, err := NewFileStorer(ctx, logger, tSettings, mockStore, key, fileType)
	require.NoError(t, err)

	<-entered

	_, err = fs.Write([]byte("0123456789"))
	require.NoError(t, err)

	closeCh := make(chan error, 1)
	abortCh := make(chan struct{}, 1)

	go func() { closeCh <- fs.Close(ctx) }()

	// Bias the ordering so Close reaches the flush first. The assertions below hold
	// whichever goroutine gets to the pipe first.
	time.Sleep(50 * time.Millisecond)

	go func() {
		fs.Abort(errors.NewProcessingError("aborted while Close was flushing"))
		abortCh <- struct{}{}
	}()

	select {
	case closeErr := <-closeCh:
		require.Error(t, closeErr, "Close must surface the flush failure that Abort caused")
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after Abort closed the pipe")
	}

	close(unblock)

	select {
	case <-abortCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Abort did not return after the background reader finished")
	}
}

// TestAbortAfterFlushErrorWaitsForBackgroundReader pins the wait that Abort supplies and the
// flush-error return in Close skips.
func TestAbortAfterFlushErrorWaitsForBackgroundReader(t *testing.T) {
	ctx := createTestContext()
	logger := ulogger.TestLogger{}
	tSettings := createTestSettings()
	mockStore := &MockBlobStore{}
	key := createTestKey()
	fileType := fileformat.FileTypeUtxoSet

	unblock := make(chan struct{})

	var exited atomic.Bool

	mockStore.On("Exists", ctx, key, fileType, mock.Anything).Return(false, nil)
	mockStore.setFromReaderHandler = func(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, fileOptions ...options.FileOption) error {
		<-unblock

		// The sleep is the point: without it an Abort that failed to wait could still
		// be ordered after the store by luck, and this test could not fail.
		time.Sleep(50 * time.Millisecond)
		exited.Store(true)

		return reader.Close()
	}

	fs, err := NewFileStorer(ctx, logger, tSettings, mockStore, key, fileType)
	require.NoError(t, err)

	_, err = fs.Write([]byte("payload"))
	require.NoError(t, err)

	// Force the flush to fail, so Close returns on the path that does not wait.
	_ = fs.writer.Close()

	err = fs.Close(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Error flushing writer")
	require.False(t, exited.Load(), "Close must have returned without waiting for the background reader")

	close(unblock)

	fs.Abort(errors.NewProcessingError("abort after the flush error"))
	require.True(t, exited.Load(), "Abort must wait for the background reader to finish")
}

// TestBlockedWriteRacingAbort pins that Abort releases a Write blocked in the pipe, which it
// can only do by signalling before it takes any lock or waits on anything.
func TestBlockedWriteRacingAbort(t *testing.T) {
	ctx := createTestContext()
	logger := ulogger.TestLogger{}
	tSettings := &settings.Settings{
		Block: settings.BlockSettings{
			// One byte, so the very first Write goes straight into the pipe and blocks.
			UTXOPersisterBufferSize: "1",
		},
	}
	mockStore := &MockBlobStore{}
	key := createTestKey()
	fileType := fileformat.FileTypeUtxoSet

	entered := make(chan struct{})
	unblock := make(chan struct{})

	mockStore.On("Exists", ctx, key, fileType, mock.Anything).Return(false, nil)
	mockStore.setFromReaderHandler = func(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, fileOptions ...options.FileOption) error {
		one := make([]byte, 1)
		_, _ = reader.Read(one)

		close(entered)
		<-unblock

		_ = reader.Close()

		return errors.NewProcessingError("reader stopped")
	}

	fs, err := NewFileStorer(ctx, logger, tSettings, mockStore, key, fileType)
	require.NoError(t, err)

	writeCh := make(chan error, 1)
	abortCh := make(chan struct{}, 1)

	go func() {
		_, writeErr := fs.Write([]byte("a payload longer than the buffer"))
		writeCh <- writeErr
	}()

	<-entered

	go func() {
		fs.Abort(errors.NewProcessingError("aborted while Write was blocked"))
		abortCh <- struct{}{}
	}()

	select {
	case writeErr := <-writeCh:
		require.Error(t, writeErr, "a Write blocked in the pipe must fail once Abort closes it")
	case <-time.After(2 * time.Second):
		t.Fatal("Write did not return after Abort closed the pipe")
	}

	close(unblock)

	select {
	case <-abortCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Abort did not return after the background reader finished")
	}
}

// TestFlushErrorPathReleasesWriter checks the white-box fact rather than the behaviour of a
// second storer, which would pass even if the first buffer were never released.
func TestFlushErrorPathReleasesWriter(t *testing.T) {
	ctx := createTestContext()
	logger := ulogger.TestLogger{}
	tSettings := createTestSettings()
	store := memory.New()

	fs, err := NewFileStorer(ctx, logger, tSettings, store, []byte("flush-error-release"), fileformat.FileTypeUtxoSet)
	require.NoError(t, err)

	_, err = fs.Write([]byte("payload"))
	require.NoError(t, err)

	_ = fs.writer.Close()

	err = fs.Close(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Error flushing writer")

	fs.mu.Lock()
	defer fs.mu.Unlock()

	require.Nil(t, fs.bufferedWriter, "the flush-error path must still hand the buffer back")
}

// TestResetForPoolClearsWriter tests the release-time reset directly, and checks the buffer
// state before anything else touches the writer: a later Reset would clear it anyway, so a
// no-op helper would pass a test that looked afterwards.
func TestResetForPoolClearsWriter(t *testing.T) {
	dest := &failingWriter{err: errors.NewProcessingError("destination is down")}
	bw := bufio.NewWriterSize(dest, 16)

	n, err := bw.Write([]byte("12345678"))
	require.NoError(t, err)
	require.Equal(t, 8, n)
	require.Positive(t, bw.Buffered())

	require.Error(t, bw.Flush())
	require.Positive(t, bw.Buffered(), "the failed flush must leave the bytes buffered")

	offeredBeforeRelease := len(dest.offered)

	resetForPool(bw)

	require.Zero(t, bw.Buffered(), "release must drop the previous writer's buffered bytes")
	require.Equal(t, bw.Size(), bw.Available(), "release must leave the whole buffer free")

	other := &bytes.Buffer{}
	bw.Reset(other)

	_, err = bw.Write([]byte("xyz"))
	require.NoError(t, err)
	require.NoError(t, bw.Flush())
	require.Equal(t, "xyz", other.String())
	require.Equal(t, offeredBeforeRelease, len(dest.offered), "no byte may reach the previous destination after release")
}

// TestSmallBufferIsNotSatisfiedFromPool pins that the pool never widens a caller's buffer,
// which is what keeps a one-byte configuration meaningful.
func TestSmallBufferIsNotSatisfiedFromPool(t *testing.T) {
	ctx := createTestContext()
	logger := ulogger.TestLogger{}
	store := memory.New()

	large := &settings.Settings{Block: settings.BlockSettings{UTXOPersisterBufferSize: "256KB"}}
	small := &settings.Settings{Block: settings.BlockSettings{UTXOPersisterBufferSize: "1"}}

	first, err := NewFileStorer(ctx, logger, large, store, []byte("pool-size-large"), fileformat.FileTypeUtxoSet)
	require.NoError(t, err)
	require.Equal(t, 256*1024, first.bufferedWriter.Size())
	require.NoError(t, first.Close(ctx))

	second, err := NewFileStorer(ctx, logger, small, store, []byte("pool-size-small"), fileformat.FileTypeUtxoSet)
	require.NoError(t, err)
	require.Equal(t, 1, second.bufferedWriter.Size(), "a one-byte configuration must not be served a recycled 256KB buffer")
	require.NoError(t, second.Close(ctx))
}
