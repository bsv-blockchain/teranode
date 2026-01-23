package logger

import (
	"context"
	"io"

	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// MockBlobStore is a mock implementation of blobStore interface for testing
type MockBlobStore struct {
	HealthFunc                func(ctx context.Context, checkLiveness bool) (int, string, error)
	ExistsFunc                func(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error)
	GetFunc                   func(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error)
	GetIoReaderFunc           func(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error)
	SetFunc                   func(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error
	SetFromReaderFunc         func(ctx context.Context, key []byte, fileType fileformat.FileType, value io.ReadCloser, opts ...options.FileOption) error
	SetDAHFunc                func(ctx context.Context, key []byte, fileType fileformat.FileType, newDAH uint32, opts ...options.FileOption) error
	GetDAHFunc                func(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (uint32, error)
	DelFunc                   func(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error
	CloseFunc                 func(ctx context.Context) error
	SetCurrentBlockHeightFunc func(height uint32)
}

func (m *MockBlobStore) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if m.HealthFunc != nil {
		return m.HealthFunc(ctx, checkLiveness)
	}
	return 200, "OK", nil
}

func (m *MockBlobStore) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
	if m.ExistsFunc != nil {
		return m.ExistsFunc(ctx, key, fileType, opts...)
	}
	return true, nil
}

func (m *MockBlobStore) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	if m.GetFunc != nil {
		return m.GetFunc(ctx, key, fileType, opts...)
	}
	return []byte("test data"), nil
}

func (m *MockBlobStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	if m.GetIoReaderFunc != nil {
		return m.GetIoReaderFunc(ctx, key, fileType, opts...)
	}
	return io.NopCloser(io.Reader(nil)), nil
}

func (m *MockBlobStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	if m.SetFunc != nil {
		return m.SetFunc(ctx, key, fileType, value, opts...)
	}
	return nil
}

func (m *MockBlobStore) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, value io.ReadCloser, opts ...options.FileOption) error {
	if m.SetFromReaderFunc != nil {
		return m.SetFromReaderFunc(ctx, key, fileType, value, opts...)
	}
	return nil
}

func (m *MockBlobStore) SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, newDAH uint32, opts ...options.FileOption) error {
	if m.SetDAHFunc != nil {
		return m.SetDAHFunc(ctx, key, fileType, newDAH, opts...)
	}
	return nil
}

func (m *MockBlobStore) GetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (uint32, error) {
	if m.GetDAHFunc != nil {
		return m.GetDAHFunc(ctx, key, fileType, opts...)
	}
	return 123456, nil
}

func (m *MockBlobStore) Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
	if m.DelFunc != nil {
		return m.DelFunc(ctx, key, fileType, opts...)
	}
	return nil
}

func (m *MockBlobStore) Close(ctx context.Context) error {
	if m.CloseFunc != nil {
		return m.CloseFunc(ctx)
	}
	return nil
}

func (m *MockBlobStore) SetCurrentBlockHeight(height uint32) {
	if m.SetCurrentBlockHeightFunc != nil {
		m.SetCurrentBlockHeightFunc(height)
	}
}

// MockLogger is a mock implementation of ulogger.Logger interface for testing
type MockLogger struct {
	DebugfFunc      func(format string, args ...interface{})
	InfofFunc       func(format string, args ...interface{})
	WarnfFunc       func(format string, args ...interface{})
	ErrorfFunc      func(format string, args ...interface{})
	FatalfFunc      func(format string, args ...interface{})
	LogLevelFunc    func() int
	SetLogLevelFunc func(level string)
	NewFunc         func(service string, options ...ulogger.Option) ulogger.Logger
	DuplicateFunc   func(options ...ulogger.Option) ulogger.Logger
}

func (m *MockLogger) Debugf(format string, args ...interface{}) {
	if m.DebugfFunc != nil {
		m.DebugfFunc(format, args...)
	}
}

func (m *MockLogger) Infof(format string, args ...interface{}) {
	if m.InfofFunc != nil {
		m.InfofFunc(format, args...)
	}
}

func (m *MockLogger) Warnf(format string, args ...interface{}) {
	if m.WarnfFunc != nil {
		m.WarnfFunc(format, args...)
	}
}

func (m *MockLogger) Errorf(format string, args ...interface{}) {
	if m.ErrorfFunc != nil {
		m.ErrorfFunc(format, args...)
	}
}

func (m *MockLogger) Fatalf(format string, args ...interface{}) {
	if m.FatalfFunc != nil {
		m.FatalfFunc(format, args...)
	}
}

func (m *MockLogger) LogLevel() int {
	if m.LogLevelFunc != nil {
		return m.LogLevelFunc()
	}
	return 0 // DEBUG
}

func (m *MockLogger) SetLogLevel(level string) {
	if m.SetLogLevelFunc != nil {
		m.SetLogLevelFunc(level)
	}
}

func (m *MockLogger) New(service string, options ...ulogger.Option) ulogger.Logger {
	if m.NewFunc != nil {
		return m.NewFunc(service, options...)
	}
	return &MockLogger{}
}

func (m *MockLogger) Duplicate(options ...ulogger.Option) ulogger.Logger {
	if m.DuplicateFunc != nil {
		return m.DuplicateFunc(options...)
	}
	return &MockLogger{}
}
