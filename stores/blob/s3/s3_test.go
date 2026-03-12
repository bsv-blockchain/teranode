package s3

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestS3(_ *testing.T) (*S3, *mockS3Client) {
	mock := newMockS3Client().(*mockS3Client)
	logger := ulogger.TestLogger{}

	s3Store := &S3{
		client:  mock,
		bucket:  "test-bucket",
		logger:  logger,
		options: options.NewStoreOptions(),
	}

	return s3Store, mock
}

func TestS3_SetAndGet(t *testing.T) {
	s3Store, mock := setupTestS3(t)
	ctx := context.Background()

	tests := []struct {
		name    string
		key     []byte
		value   []byte
		wantErr bool
	}{
		{
			name:    "basic set and get",
			key:     []byte("test-key"),
			value:   []byte("test-value"),
			wantErr: false,
		},
		{
			name:    "with header and footer",
			key:     []byte("test-key-2"),
			value:   []byte("test-value-2"),
			wantErr: false,
		},
		{
			name:    "empty value",
			key:     []byte("empty-key"),
			value:   []byte{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var opts []options.FileOption

			// Test Set
			err := s3Store.Set(ctx, tt.key, fileformat.FileTypeTesting, tt.value, opts...)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			// Verify raw data in mock includes headers and footers
			objectKey := aws.ToString(s3Store.getObjectKey(tt.key, fileformat.FileTypeTesting, options.MergeOptions(s3Store.options, opts)))

			rawData := mock.store[objectKey]
			magicBytes := fileformat.FileTypeTesting.ToMagicBytes()

			assert.True(t, bytes.HasPrefix(rawData, magicBytes[:]), "Raw data should start with header")

			// Test Get - should return data without headers/footers
			got, err := s3Store.Get(ctx, tt.key, fileformat.FileTypeTesting, opts...)
			require.NoError(t, err)
			assert.Equal(t, tt.value, got)

			// Test GetIoReader - should return data without headers/footers
			reader, err := s3Store.GetIoReader(ctx, tt.key, fileformat.FileTypeTesting, opts...)
			require.NoError(t, err)

			gotBytes, err := io.ReadAll(reader)
			require.NoError(t, err)
			assert.Equal(t, tt.value, gotBytes)
			reader.Close()
		})
	}

	err := s3Store.Close(ctx)
	require.NoError(t, err)
}

func TestS3_SetFromReader(t *testing.T) {
	s3Store, mock := setupTestS3(t)
	ctx := context.Background()

	tests := []struct {
		name    string
		key     []byte
		value   []byte
		wantErr bool
	}{
		{
			name:    "basic set from reader",
			key:     []byte("test-key"),
			value:   []byte("test-value"),
			wantErr: false,
		},
		{
			name:    "with header and footer",
			key:     []byte("test-key-2"),
			value:   []byte("test-value-2"),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var opts []options.FileOption

			reader := io.NopCloser(bytes.NewReader(tt.value))

			err := s3Store.SetFromReader(ctx, tt.key, fileformat.FileTypeTesting, reader, opts...)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			// Verify raw data in mock includes headers and footers
			objectKey := aws.ToString(s3Store.getObjectKey(tt.key, fileformat.FileTypeTesting, options.MergeOptions(s3Store.options, opts)))

			rawData := mock.store[objectKey]
			magicBytes := fileformat.FileTypeTesting.ToMagicBytes()
			assert.True(t, bytes.HasPrefix(rawData, magicBytes[:]), "Raw data should start with header")

			// Verify content - should return data without headers/footers
			got, err := s3Store.Get(ctx, tt.key, fileformat.FileTypeTesting, opts...)
			require.NoError(t, err)
			assert.Equal(t, tt.value, got)
		})
	}
}

func TestS3_Exists(t *testing.T) {
	s3Store, _ := setupTestS3(t)
	ctx := context.Background()

	key := []byte("test-key-exists")
	value := []byte("test-value")

	// Test non-existent key
	exists, err := s3Store.Exists(ctx, key, fileformat.FileTypeTesting)
	require.NoError(t, err)
	assert.False(t, exists)

	// Set value
	err = s3Store.Set(ctx, key, fileformat.FileTypeTesting, value)
	require.NoError(t, err)

	// Test existing key
	exists, err = s3Store.Exists(ctx, key, fileformat.FileTypeTesting)
	require.NoError(t, err)
	assert.True(t, exists)

	// Delete value
	err = s3Store.Del(ctx, key, fileformat.FileTypeTesting)
	require.NoError(t, err)

	// Test after deletion
	exists, err = s3Store.Exists(ctx, key, fileformat.FileTypeTesting)
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestS3_New(t *testing.T) {
	tests := []struct {
		name    string
		urlStr  string
		wantErr bool
	}{
		{
			name:    "valid configuration",
			urlStr:  "s3://bucket-name?region=us-west-2",
			wantErr: false,
		},
		{
			name:    "with subdirectory",
			urlStr:  "s3://bucket-name?region=us-west-2&subDirectory=test",
			wantErr: false,
		},
		{
			name:    "with connection parameters",
			urlStr:  "s3://bucket-name?region=us-west-2&MaxIdleConns=50&MaxIdleConnsPerHost=50",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.urlStr)
			require.NoError(t, err)

			logger := ulogger.TestLogger{}

			s3Store, err := New(logger, u)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.NotNil(t, s3Store)
		})
	}
}

func TestS3WithURLHeaderFooter(t *testing.T) {
	t.Run("with header and footer in URL", func(t *testing.T) {
		// Setup mock S3 client first
		mock := newMockS3Client().(*mockS3Client)

		// Create store with mock client
		s3Store := &S3{
			client:  mock,
			bucket:  "test-bucket",
			logger:  ulogger.TestLogger{},
			options: options.NewStoreOptions(),
		}

		key := []byte("test-key")
		content := "test content"

		// Test Set
		err := s3Store.Set(context.Background(), key, fileformat.FileTypeTesting, []byte(content))
		require.NoError(t, err)

		// Verify raw data in mock includes header and footer
		objectKey := aws.ToString(s3Store.getObjectKey(key, fileformat.FileTypeTesting, s3Store.options))
		rawData := mock.store[objectKey]

		// Verify header and footer are present in raw data
		expectedData := append([]byte("TESTING "), []byte(content)...)
		require.Equal(t, expectedData, rawData)

		// Test Get - should return content without header/footer
		value, err := s3Store.Get(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.Equal(t, content, string(value))

		// Test GetIoReader - should return content without header/footer
		reader, err := s3Store.GetIoReader(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		defer reader.Close()

		readContent, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, content, string(readContent))

		// Clean up
		err = s3Store.Del(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
	})
}

func TestS3_Health(t *testing.T) {
	logger := ulogger.TestLogger{}

	t.Run("successful health check", func(t *testing.T) {
		mockClient := newMockS3Client().(*mockS3Client)

		store := &S3{
			client:  mockClient,
			bucket:  "test-bucket",
			logger:  logger,
			options: options.NewStoreOptions(),
		}

		status, msg, err := store.Health(context.Background(), true)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, "S3 Store available", msg)
	})

	t.Run("failed health check", func(t *testing.T) {
		mockClient := newMockS3Client().(*mockS3Client)
		mockClient.SetHeadObjectError(errors.NewBlobError("connection failed"))

		store := &S3{
			client:  mockClient,
			bucket:  "test-bucket",
			logger:  logger,
			options: options.NewStoreOptions(),
		}

		status, msg, err := store.Health(context.Background(), true)

		assert.Error(t, err)
		assert.Equal(t, http.StatusServiceUnavailable, status)
		assert.Equal(t, "S3 Store unavailable", msg)
	})
}

func TestS3_TTL(t *testing.T) {
	s3Store, _ := setupTestS3(t)
	ctx := context.Background()
	key := []byte("test-key")

	t.Run("GetDAH removed", func(t *testing.T) {
		// GetDAH has been removed from the blob.Store interface
		// DAH functionality is now centralized in the pruner service
		t.Skip("GetDAH removed from interface - see e2e pruner tests")
	})

	t.Run("SetTTL is no-op", func(t *testing.T) {
		err := s3Store.SetDAH(ctx, key, fileformat.FileTypeTesting, 1)

		assert.NoError(t, err)

		// DAH verification now done via pruner service in e2e tests
	})
}

func TestS3_GetCacheMiss(t *testing.T) {
	s3Store, mock := setupTestS3(t)
	ctx := context.Background()
	key := []byte("test-key")
	value := []byte("test-value")

	// Set up test data directly in mock store to simulate existing S3 data without cache
	objectKey := aws.ToString(s3Store.getObjectKey(key, fileformat.FileTypeTesting, s3Store.options))

	ft := fileformat.FileTypeTesting.ToMagicBytes()
	mock.store[objectKey] = append(ft[:], value...)

	// Clear the cache to ensure cache miss
	cache.Delete(objectKey)

	// Get should still work by fetching from S3
	got, err := s3Store.Get(ctx, key, fileformat.FileTypeTesting)
	assert.NoError(t, err)
	assert.Equal(t, value, got)

	// Verify it's now in cache
	cached, ok := cache.Get(objectKey)
	assert.True(t, ok, "Value should be cached after Get")
	assert.Equal(t, value, cached)
}
