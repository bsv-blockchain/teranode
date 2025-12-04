package logger

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/util/debugflags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	debugflags.Init(debugflags.Flags{All: true, File: true, Blobstore: true})
}

func Test_New(t *testing.T) {
	mockLogger := &MockLogger{}
	mockStore := &MockBlobStore{}

	logger := New(mockLogger, mockStore)

	assert.NotNil(t, logger)
	loggerImpl, ok := logger.(*Logger)
	require.True(t, ok)
	assert.Equal(t, mockLogger, loggerImpl.logger)
	assert.Equal(t, mockStore, loggerImpl.store)
}

func Test_LoggerHealth(t *testing.T) {
	t.Run("SuccessfulHealth", func(t *testing.T) {
		var loggedMessage string
		mockLogger := &MockLogger{
			DebugfFunc: func(format string, args ...interface{}) {
				loggedMessage = format
			},
		}
		mockStore := &MockBlobStore{
			HealthFunc: func(ctx context.Context, checkLiveness bool) (int, string, error) {
				return 200, "OK", nil
			},
		}

		logger := New(mockLogger, mockStore).(*Logger)
		status, message, err := logger.Health(context.Background(), true)

		assert.NoError(t, err)
		assert.Equal(t, 200, status)
		assert.Equal(t, "OK", message)
		assert.Contains(t, loggedMessage, "[BlobStore][logger][Health]")
	})

	t.Run("HealthWithError", func(t *testing.T) {
		expectedErr := errors.NewError("health check failed")
		mockLogger := &MockLogger{
			DebugfFunc: func(format string, args ...interface{}) {},
		}
		mockStore := &MockBlobStore{
			HealthFunc: func(ctx context.Context, checkLiveness bool) (int, string, error) {
				return 500, "Error", expectedErr
			},
		}

		logger := New(mockLogger, mockStore).(*Logger)
		status, message, err := logger.Health(context.Background(), false)

		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.Equal(t, 500, status)
		assert.Equal(t, "Error", message)
	})
}

func Test_LoggerExists(t *testing.T) {
	testKey := []byte{1, 2, 3, 4}
	fileType := fileformat.FileTypeTx

	t.Run("KeyExists", func(t *testing.T) {
		var loggedFormat string
		var loggedArgs []interface{}
		mockLogger := &MockLogger{
			DebugfFunc: func(format string, args ...interface{}) {
				loggedFormat = format
				loggedArgs = args
			},
		}
		mockStore := &MockBlobStore{
			ExistsFunc: func(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
				assert.Equal(t, testKey, key)
				assert.Equal(t, fileformat.FileTypeTx, fileType)
				return true, nil
			},
		}

		logger := New(mockLogger, mockStore).(*Logger)
		exists, err := logger.Exists(context.Background(), testKey, fileType)

		assert.NoError(t, err)
		assert.True(t, exists)
		assert.Contains(t, loggedFormat, "[BlobStore][logger][Exists]")
		assert.Contains(t, loggedFormat, "key %x")
		assert.Contains(t, loggedFormat, "fileType %s")
		assert.Contains(t, loggedFormat, "exists %t")
		assert.Equal(t, []byte{4, 3, 2, 1}, loggedArgs[0]) // Reversed key
		assert.Equal(t, fileType, loggedArgs[1])
		assert.Equal(t, true, loggedArgs[2])
		assert.Nil(t, loggedArgs[3]) // err should be nil
	})

	t.Run("KeyDoesNotExist", func(t *testing.T) {
		mockLogger := &MockLogger{
			DebugfFunc: func(format string, args ...interface{}) {},
		}
		mockStore := &MockBlobStore{
			ExistsFunc: func(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
				return false, nil
			},
		}

		logger := New(mockLogger, mockStore).(*Logger)
		exists, err := logger.Exists(context.Background(), testKey, fileType)

		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("ExistsWithError", func(t *testing.T) {
		expectedErr := errors.NewError("exists check failed")
		mockLogger := &MockLogger{
			DebugfFunc: func(format string, args ...interface{}) {},
		}
		mockStore := &MockBlobStore{
			ExistsFunc: func(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
				return false, expectedErr
			},
		}

		logger := New(mockLogger, mockStore).(*Logger)
		exists, err := logger.Exists(context.Background(), testKey, fileType)

		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.False(t, exists)
	})
}

func Test_LoggerGet(t *testing.T) {
	testKey := []byte{1, 2, 3, 4}
	testData := []byte("test data")
	fileType := fileformat.FileTypeBlock

	t.Run("SuccessfulGet", func(t *testing.T) {
		var loggedFormat string
		var loggedArgs []interface{}
		mockLogger := &MockLogger{
			DebugfFunc: func(format string, args ...interface{}) {
				loggedFormat = format
				loggedArgs = args
			},
		}
		mockStore := &MockBlobStore{
			GetFunc: func(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
				assert.Equal(t, testKey, key)
				return testData, nil
			},
		}

		logger := New(mockLogger, mockStore).(*Logger)
		data, err := logger.Get(context.Background(), testKey, fileType)

		assert.NoError(t, err)
		assert.Equal(t, testData, data)
		assert.Contains(t, loggedFormat, "[BlobStore][logger][Get]")
		assert.Equal(t, []byte{4, 3, 2, 1}, loggedArgs[0]) // Reversed key
		assert.Equal(t, fileType, loggedArgs[1])
		assert.Nil(t, loggedArgs[2]) // err should be nil
	})

	t.Run("GetWithError", func(t *testing.T) {
		expectedErr := errors.NewError("get failed")
		mockLogger := &MockLogger{
			DebugfFunc: func(format string, args ...interface{}) {},
		}
		mockStore := &MockBlobStore{
			GetFunc: func(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
				return nil, expectedErr
			},
		}

		logger := New(mockLogger, mockStore).(*Logger)
		data, err := logger.Get(context.Background(), testKey, fileType)

		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.Nil(t, data)
	})
}

func Test_LoggerGetIoReader(t *testing.T) {
	testKey := []byte{5, 6, 7, 8}
	fileType := fileformat.FileTypeSubtree

	t.Run("SuccessfulGetIoReader", func(t *testing.T) {
		var loggedFormat string
		mockLogger := &MockLogger{
			DebugfFunc: func(format string, args ...interface{}) {
				loggedFormat = format
			},
		}
		testReader := io.NopCloser(strings.NewReader("test data"))
		mockStore := &MockBlobStore{
			GetIoReaderFunc: func(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
				return testReader, nil
			},
		}

		logger := New(mockLogger, mockStore).(*Logger)
		reader, err := logger.GetIoReader(context.Background(), testKey, fileType)

		assert.NoError(t, err)
		assert.Equal(t, testReader, reader)
		assert.Contains(t, loggedFormat, "[BlobStore][logger][GetIoReader]")
	})
}

func TestLogger_Set(t *testing.T) {
	testKey := []byte{9, 10, 11, 12}
	testData := []byte("data to set")
	fileType := fileformat.FileTypeUtxoSet

	t.Run("SuccessfulSet", func(t *testing.T) {
		var loggedFormat string
		var loggedArgs []interface{}
		mockLogger := &MockLogger{
			DebugfFunc: func(format string, args ...interface{}) {
				loggedFormat = format
				loggedArgs = args
			},
		}
		mockStore := &MockBlobStore{
			SetFunc: func(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
				assert.Equal(t, testKey, key)
				assert.Equal(t, testData, value)
				return nil
			},
		}

		logger := New(mockLogger, mockStore).(*Logger)
		err := logger.Set(context.Background(), testKey, fileType, testData)

		assert.NoError(t, err)
		assert.Contains(t, loggedFormat, "[BlobStore][logger][Set]")
		assert.Equal(t, []byte{12, 11, 10, 9}, loggedArgs[0]) // Reversed key
		assert.Equal(t, fileType, loggedArgs[1])
		assert.Nil(t, loggedArgs[2]) // err should be nil
	})

	t.Run("SetWithError", func(t *testing.T) {
		expectedErr := errors.NewError("set failed")
		mockLogger := &MockLogger{
			DebugfFunc: func(format string, args ...interface{}) {},
		}
		mockStore := &MockBlobStore{
			SetFunc: func(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
				return expectedErr
			},
		}

		logger := New(mockLogger, mockStore).(*Logger)
		err := logger.Set(context.Background(), testKey, fileType, testData)

		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})
}

func Test_LoggerSetFromReader(t *testing.T) {
	testKey := []byte{13, 14, 15, 16}
	testReader := io.NopCloser(strings.NewReader("reader data"))
	fileType := fileformat.FileTypeDat

	t.Run("SuccessfulSetFromReader", func(t *testing.T) {
		var loggedFormat string
		mockLogger := &MockLogger{
			DebugfFunc: func(format string, args ...interface{}) {
				loggedFormat = format
			},
		}
		mockStore := &MockBlobStore{
			SetFromReaderFunc: func(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
				assert.Equal(t, testKey, key)
				return nil
			},
		}

		logger := New(mockLogger, mockStore).(*Logger)
		err := logger.SetFromReader(context.Background(), testKey, fileType, testReader)

		assert.NoError(t, err)
		assert.Contains(t, loggedFormat, "[BlobStore][logger][SetFromReader]")
	})
}

func Test_LoggerSetDAH(t *testing.T) {
	testKey := []byte{17, 18, 19, 20}
	testDAH := uint32(100000)
	fileType := fileformat.FileTypeBlock

	t.Run("SuccessfulSetDAH", func(t *testing.T) {
		var loggedFormat string
		var loggedArgs []interface{}
		mockLogger := &MockLogger{
			DebugfFunc: func(format string, args ...interface{}) {
				loggedFormat = format
				loggedArgs = args
			},
		}
		mockStore := &MockBlobStore{
			SetDAHFunc: func(ctx context.Context, key []byte, fileType fileformat.FileType, dah uint32, opts ...options.FileOption) error {
				assert.Equal(t, testKey, key)
				assert.Equal(t, testDAH, dah)
				return nil
			},
		}

		logger := New(mockLogger, mockStore).(*Logger)
		err := logger.SetDAH(context.Background(), testKey, fileType, testDAH)

		assert.NoError(t, err)
		assert.Contains(t, loggedFormat, "[BlobStore][logger][SetDAH]")
		assert.Contains(t, loggedFormat, "dah %d")
		assert.Equal(t, []byte{20, 19, 18, 17}, loggedArgs[0]) // Reversed key
		assert.Equal(t, fileType, loggedArgs[1])
		assert.Equal(t, testDAH, loggedArgs[2])
		assert.Nil(t, loggedArgs[3]) // err should be nil
	})
}

func Test_LoggerGetDAH(t *testing.T) {
	testKey := []byte{21, 22, 23, 24}
	testDAH := uint32(200000)
	fileType := fileformat.FileTypeTx

	t.Run("SuccessfulGetDAH", func(t *testing.T) {
		var loggedFormat string
		var loggedArgs []interface{}
		mockLogger := &MockLogger{
			DebugfFunc: func(format string, args ...interface{}) {
				loggedFormat = format
				loggedArgs = args
			},
		}
		mockStore := &MockBlobStore{
			GetDAHFunc: func(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (uint32, error) {
				assert.Equal(t, testKey, key)
				return testDAH, nil
			},
		}

		logger := New(mockLogger, mockStore).(*Logger)
		dah, err := logger.GetDAH(context.Background(), testKey, fileType)

		assert.NoError(t, err)
		assert.Equal(t, testDAH, dah)
		assert.Contains(t, loggedFormat, "[BlobStore][logger][GetDAH]")
		assert.Contains(t, loggedFormat, "dah %d")
		assert.Equal(t, []byte{24, 23, 22, 21}, loggedArgs[0]) // Reversed key
		assert.Equal(t, fileType, loggedArgs[1])
		assert.Equal(t, testDAH, loggedArgs[2])
		assert.Nil(t, loggedArgs[3]) // err should be nil
	})
}

func Test_LoggerDel(t *testing.T) {
	testKey := []byte{25, 26, 27, 28}
	fileType := fileformat.FileTypeSubtree

	t.Run("SuccessfulDel", func(t *testing.T) {
		var loggedFormat string
		mockLogger := &MockLogger{
			DebugfFunc: func(format string, args ...interface{}) {
				loggedFormat = format
			},
		}
		mockStore := &MockBlobStore{
			DelFunc: func(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
				assert.Equal(t, testKey, key)
				return nil
			},
		}

		logger := New(mockLogger, mockStore).(*Logger)
		err := logger.Del(context.Background(), testKey, fileType)

		assert.NoError(t, err)
		assert.Contains(t, loggedFormat, "[BlobStore][logger][Del]")
	})

	t.Run("DelWithError", func(t *testing.T) {
		expectedErr := errors.NewError("delete failed")
		mockLogger := &MockLogger{
			DebugfFunc: func(format string, args ...interface{}) {},
		}
		mockStore := &MockBlobStore{
			DelFunc: func(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
				return expectedErr
			},
		}

		logger := New(mockLogger, mockStore).(*Logger)
		err := logger.Del(context.Background(), testKey, fileType)

		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})
}

func Test_LoggerClose(t *testing.T) {
	t.Run("SuccessfulClose", func(t *testing.T) {
		var loggedFormat string
		mockLogger := &MockLogger{
			DebugfFunc: func(format string, args ...interface{}) {
				loggedFormat = format
			},
		}
		mockStore := &MockBlobStore{
			CloseFunc: func(ctx context.Context) error {
				return nil
			},
		}

		logger := New(mockLogger, mockStore).(*Logger)
		err := logger.Close(context.Background())

		assert.NoError(t, err)
		assert.Contains(t, loggedFormat, "[BlobStore][logger][Close]")
	})

	t.Run("CloseWithError", func(t *testing.T) {
		expectedErr := errors.NewError("close failed")
		mockLogger := &MockLogger{
			DebugfFunc: func(format string, args ...interface{}) {},
		}
		mockStore := &MockBlobStore{
			CloseFunc: func(ctx context.Context) error {
				return expectedErr
			},
		}

		logger := New(mockLogger, mockStore).(*Logger)
		err := logger.Close(context.Background())

		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})
}

func Test_LoggerSetCurrentBlockHeight(t *testing.T) {
	testHeight := uint32(123456)

	t.Run("SuccessfulSetCurrentBlockHeight", func(t *testing.T) {
		var loggedFormat string
		var loggedArgs []interface{}
		var receivedHeight uint32
		mockLogger := &MockLogger{
			DebugfFunc: func(format string, args ...interface{}) {
				loggedFormat = format
				loggedArgs = args
			},
		}
		mockStore := &MockBlobStore{
			SetCurrentBlockHeightFunc: func(height uint32) {
				receivedHeight = height
			},
		}

		logger := New(mockLogger, mockStore).(*Logger)
		logger.SetCurrentBlockHeight(testHeight)

		assert.Equal(t, testHeight, receivedHeight)
		assert.Contains(t, loggedFormat, "[BlobStore][logger][SetCurrentBlockHeight]")
		assert.Contains(t, loggedFormat, "height %d")
		assert.Equal(t, testHeight, loggedArgs[0])
	})
}

func Test_LoggerWithOptions(t *testing.T) {
	testKey := []byte{1, 2, 3}
	fileType := fileformat.FileTypeTx

	t.Run("ExistsWithOptions", func(t *testing.T) {
		mockLogger := &MockLogger{
			DebugfFunc: func(format string, args ...interface{}) {},
		}
		var receivedOpts []options.FileOption
		mockStore := &MockBlobStore{
			ExistsFunc: func(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
				receivedOpts = opts
				return true, nil
			},
		}

		logger := New(mockLogger, mockStore).(*Logger)
		opt1 := options.WithFilename("test.txt")
		opt2 := options.WithDeleteAt(12345)

		exists, err := logger.Exists(context.Background(), testKey, fileType, opt1, opt2)

		assert.NoError(t, err)
		assert.True(t, exists)
		assert.Len(t, receivedOpts, 2)
	})
}

func Test_caller(t *testing.T) {
	t.Run("CallerReturnsStackTrace", func(t *testing.T) {
		result := caller()

		assert.NotEmpty(t, result)
		assert.Contains(t, result, "called from")

		// Should contain function names, file paths, and line numbers
		parts := strings.Split(result, ",")
		assert.Greater(t, len(parts), 0)

		// Each part should have the format "called from funcName: file:line"
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part != "" {
				assert.Contains(t, part, "called from")
				assert.Contains(t, part, ":")
			}
		}
	})

	t.Run("CallerPathSimplification", func(t *testing.T) {
		result := caller()

		// Should not contain full paths starting with github.com/bsv-blockchain/teranode
		assert.NotContains(t, result, "github.com/bsv-blockchain/teranode")

		// Should contain runtime information when called from test context
		assert.Contains(t, result, "testing.tRunner")
	})
}

func Test_LoggerEdgeCases(t *testing.T) {
	t.Run("NilKey", func(t *testing.T) {
		mockLogger := &MockLogger{
			DebugfFunc: func(format string, args ...interface{}) {},
		}
		mockStore := &MockBlobStore{}

		logger := New(mockLogger, mockStore).(*Logger)

		// Should handle nil key gracefully (bt.ReverseBytes should handle nil)
		exists, err := logger.Exists(context.Background(), nil, fileformat.FileTypeTx)
		assert.NoError(t, err)
		assert.True(t, exists) // Mock returns true by default
	})

	t.Run("EmptyKey", func(t *testing.T) {
		mockLogger := &MockLogger{
			DebugfFunc: func(format string, args ...interface{}) {},
		}
		mockStore := &MockBlobStore{}

		logger := New(mockLogger, mockStore).(*Logger)

		exists, err := logger.Exists(context.Background(), []byte{}, fileformat.FileTypeTx)
		assert.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("LargeKey", func(t *testing.T) {
		largeKey := make([]byte, 1000)
		for i := range largeKey {
			largeKey[i] = byte(i % 256)
		}

		mockLogger := &MockLogger{
			DebugfFunc: func(format string, args ...interface{}) {},
		}
		mockStore := &MockBlobStore{}

		logger := New(mockLogger, mockStore).(*Logger)

		exists, err := logger.Exists(context.Background(), largeKey, fileformat.FileTypeTx)
		assert.NoError(t, err)
		assert.True(t, exists)
	})
}

func Test_LoggerDebugToggle(t *testing.T) {
	debugflags.Init(debugflags.Flags{})
	t.Cleanup(func() {
		debugflags.Init(debugflags.Flags{All: true, File: true, Blobstore: true})
	})

	calls := 0
	mockLogger := &MockLogger{
		DebugfFunc: func(format string, args ...interface{}) {
			calls++
		},
		LogLevelFunc: func() int {
			return 0
		},
	}

	mockStore := &MockBlobStore{}
	logger := New(mockLogger, mockStore).(*Logger)

	_, err := logger.Exists(context.Background(), []byte{1, 2, 3, 4}, fileformat.FileTypeTx)
	require.NoError(t, err)
	assert.Equal(t, 0, calls, "debug log should be suppressed when flags are disabled")

	debugflags.Init(debugflags.Flags{Blobstore: true})

	_, err = logger.Exists(context.Background(), []byte{1, 2, 3, 4}, fileformat.FileTypeTx)
	require.NoError(t, err)
	assert.Equal(t, 1, calls, "debug log should emit when flag enabled")
}
