package blob

import (
	"context"
	"io"

	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/stretchr/testify/mock"
)

// MockStore is a mock implementation of the Store interface using testify/mock.
// It can be used in tests to mock blob storage operations.
type MockStore struct {
	mock.Mock
}

// Health checks the health status of the blob store.
func (m *MockStore) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	args := m.Called(ctx, checkLiveness)
	return args.Int(0), args.String(1), args.Error(2)
}

// Exists checks if a blob exists in the store.
func (m *MockStore) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
	args := m.Called(ctx, key, fileType, opts)
	return args.Bool(0), args.Error(1)
}

// Get retrieves a blob from the store.
func (m *MockStore) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	args := m.Called(ctx, key, fileType, opts)
	return args.Get(0).([]byte), args.Error(1)
}

// GetIoReader returns an io.ReadCloser for streaming blob data.
func (m *MockStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	args := m.Called(ctx, key, fileType, opts)
	return args.Get(0).(io.ReadCloser), args.Error(1)
}

// Set stores a blob in the store.
func (m *MockStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	args := m.Called(ctx, key, fileType, value, opts)
	return args.Error(0)
}

// SetFromReader stores a blob from an io.ReadCloser.
func (m *MockStore) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	args := m.Called(ctx, key, fileType, reader, opts)
	return args.Error(0)
}

// SetDAH sets the delete at height for a blob.
func (m *MockStore) SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, dah uint32, opts ...options.FileOption) error {
	args := m.Called(ctx, key, fileType, dah, opts)
	return args.Error(0)
}

// Del deletes a blob from the store.
func (m *MockStore) Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
	args := m.Called(ctx, key, fileType, opts)
	return args.Error(0)
}

// Close closes the blob store and releases any resources.
func (m *MockStore) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// SetCurrentBlockHeight sets the current block height for the store.
func (m *MockStore) SetCurrentBlockHeight(height uint32) {
	m.Called(height)
}
