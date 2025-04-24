package s3

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/ulogger"
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
		header  []byte
		footer  []byte
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
			header:  []byte("header-"),
			footer:  []byte("-footer"),
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
			if tt.header != nil {
				opts = append(opts, options.FileOption(options.WithHeader(tt.header)))
			}

			if tt.footer != nil {
				opts = append(opts, options.FileOption(options.WithFooter(options.NewFooter(len(tt.footer), tt.footer, nil))))
			}

			// Test Set
			err := s3Store.Set(ctx, tt.key, tt.value, opts...)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			// Verify raw data in mock includes headers and footers
			objectKey := aws.ToString(s3Store.getObjectKey(tt.key, options.MergeOptions(s3Store.options, opts)))

			rawData := mock.store[objectKey]
			if tt.header != nil {
				assert.True(t, bytes.HasPrefix(rawData, tt.header), "Raw data should start with header")
			}

			if tt.footer != nil {
				assert.True(t, bytes.HasSuffix(rawData, tt.footer), "Raw data should end with footer")
			}

			// Test Get - should return data without headers/footers
			got, err := s3Store.Get(ctx, tt.key, opts...)
			require.NoError(t, err)
			assert.Equal(t, tt.value, got)

			// Test GetIoReader - should return data without headers/footers
			reader, err := s3Store.GetIoReader(ctx, tt.key, opts...)
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
		header  []byte
		footer  []byte
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
			header:  []byte("header-"),
			footer:  []byte("-footer"),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var opts []options.FileOption
			if tt.header != nil {
				opts = append(opts, options.FileOption(options.WithHeader(tt.header)))
			}

			if tt.footer != nil {
				opts = append(opts, options.FileOption(options.WithFooter(options.NewFooter(len(tt.footer), tt.footer, nil))))
			}

			reader := io.NopCloser(bytes.NewReader(tt.value))

			err := s3Store.SetFromReader(ctx, tt.key, reader, opts...)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			// Verify raw data in mock includes headers and footers
			objectKey := aws.ToString(s3Store.getObjectKey(tt.key, options.MergeOptions(s3Store.options, opts)))

			rawData := mock.store[objectKey]
			if tt.header != nil {
				assert.True(t, bytes.HasPrefix(rawData, tt.header), "Raw data should start with header")
			}

			if tt.footer != nil {
				assert.True(t, bytes.HasSuffix(rawData, tt.footer), "Raw data should end with footer")
			}

			// Verify content - should return data without headers/footers
			got, err := s3Store.Get(ctx, tt.key, opts...)
			require.NoError(t, err)
			assert.Equal(t, tt.value, got)
		})
	}
}

func TestS3_GetHead(t *testing.T) {
	s3Store, _ := setupTestS3(t)
	ctx := context.Background()

	value := []byte("test-value-for-head")
	key := []byte("test-key-head")
	header := []byte("header-")

	// Set the value first
	err := s3Store.Set(ctx, key, value, options.FileOption(options.WithHeader(header)))
	require.NoError(t, err)

	tests := []struct {
		name      string
		nrOfBytes int
		want      []byte
		wantErr   bool
	}{
		{
			name:      "get first 4 bytes",
			nrOfBytes: 4,
			want:      []byte("test"),
			wantErr:   false,
		},
		{
			name:      "get more bytes than available",
			nrOfBytes: 100,
			want:      value,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := s3Store.GetHead(ctx, key, tt.nrOfBytes, options.FileOption(options.WithHeader(header)))
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want[:min(len(tt.want), tt.nrOfBytes)], got)
		})
	}
}

func TestS3_Exists(t *testing.T) {
	s3Store, _ := setupTestS3(t)
	ctx := context.Background()

	key := []byte("test-key-exists")
	value := []byte("test-value")

	// Test non-existent key
	exists, err := s3Store.Exists(ctx, key)
	require.NoError(t, err)
	assert.False(t, exists)

	// Set value
	err = s3Store.Set(ctx, key, value)
	require.NoError(t, err)

	// Test existing key
	exists, err = s3Store.Exists(ctx, key)
	require.NoError(t, err)
	assert.True(t, exists)

	// Delete value
	err = s3Store.Del(ctx, key)
	require.NoError(t, err)

	// Test after deletion
	exists, err = s3Store.Exists(ctx, key)
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
			client: mock,
			bucket: "test-bucket",
			logger: ulogger.TestLogger{},
			options: options.NewStoreOptions(
				options.WithHeader([]byte("START")),
				options.WithFooter(options.NewFooter(len([]byte("END")), []byte("END"), nil)),
			),
		}

		key := []byte("test-key")
		content := "test content"

		// Test Set
		err := s3Store.Set(context.Background(), key, []byte(content))
		require.NoError(t, err)

		// Verify raw data in mock includes header and footer
		objectKey := aws.ToString(s3Store.getObjectKey(key, s3Store.options))
		rawData := mock.store[objectKey]

		// Verify header and footer are present in raw data
		expectedData := append([]byte("START"), []byte(content)...)
		expectedData = append(expectedData, []byte("END")...)
		require.Equal(t, expectedData, rawData)

		// Test Get - should return content without header/footer
		value, err := s3Store.Get(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, content, string(value))

		// Test GetIoReader - should return content without header/footer
		reader, err := s3Store.GetIoReader(context.Background(), key)
		require.NoError(t, err)
		defer reader.Close()

		readContent, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, content, string(readContent))

		// Clean up
		err = s3Store.Del(context.Background(), key)
		require.NoError(t, err)
	})

	// Add a separate test for URL parsing
	t.Run("parse URL with header and footer", func(t *testing.T) {
		u, err := url.Parse("s3://test-bucket?header=START&eofmarker=END&region=us-west-2")
		require.NoError(t, err)

		logger := ulogger.TestLogger{}
		store, err := New(logger, u)
		require.NoError(t, err)

		// Verify options were set correctly
		require.Equal(t, []byte("START"), store.options.Header)
		footer, err := store.options.Footer.GetFooter()
		require.NoError(t, err)
		require.Equal(t, []byte("END"), footer)
	})
}

func TestS3_GetHeader(t *testing.T) {
	s3Store, mock := setupTestS3(t)
	ctx := context.Background()
	key := []byte("test-key")
	value := []byte("test-value")
	header := []byte("header-bytes")

	tests := []struct {
		name    string
		key     []byte
		value   []byte
		header  []byte
		setup   func()
		wantErr bool
	}{
		{
			name:    "successful header retrieval",
			key:     key,
			value:   value,
			header:  header,
			wantErr: false,
			setup: func() {
				s3Store.options = options.NewStoreOptions(options.WithHeader(header))
				objectKey := aws.ToString(s3Store.getObjectKey(key, s3Store.options))
				mock.store[objectKey] = append(header, value...)
			},
		},
		{
			name:  "no header configured",
			key:   key,
			value: value,
			setup: func() {
				s3Store.options = options.NewStoreOptions() // Reset options
				objectKey := aws.ToString(s3Store.getObjectKey(key, s3Store.options))
				mock.store[objectKey] = value
			},
			wantErr: false,
		},
		{
			name:   "non-existent key",
			key:    []byte("non-existent"),
			header: header,
			setup: func() {
				s3Store.options = options.NewStoreOptions(options.WithHeader(header))
				mock.store = make(map[string][]byte) // Clear mock store
			},
			wantErr: true,
		},
		{
			name:    "header mismatch",
			key:     key,
			value:   value,
			header:  header,
			wantErr: true,
			setup: func() {
				// Set up data with different header
				s3Store.options = options.NewStoreOptions(options.WithHeader(header))
				objectKey := aws.ToString(s3Store.getObjectKey(key, s3Store.options))
				mock.store[objectKey] = append([]byte("different-header"), value...)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()

			got, err := s3Store.GetHeader(ctx, tt.key)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.header == nil {
				assert.Nil(t, got)
			} else {
				assert.Equal(t, tt.header, got)
			}
		})
	}
}

func TestS3_GetMetaData(t *testing.T) {
	s3Store, mock := setupTestS3(t)
	ctx := context.Background()
	key := []byte("test-key")
	value := []byte("test-value")
	footer := []byte("footer-bytes")
	metadata := []byte("metadata-content")
	footerObj := options.NewFooter(len(footer)+len(metadata), footer, func() []byte { return metadata })

	tests := []struct {
		name     string
		key      []byte
		value    []byte
		footer   []byte
		metadata []byte
		setup    func()
		wantErr  bool
	}{
		{
			name:     "successful metadata retrieval",
			key:      key,
			value:    value,
			footer:   footer,
			metadata: metadata,
			wantErr:  false,
			setup: func() {
				s3Store.options = options.NewStoreOptions(options.WithFooter(footerObj)) // Reset options
				objectKey := aws.ToString(s3Store.getObjectKey(key, s3Store.options))
				data := append(value, footer...) // nolint:gocritic
				data = append(data, metadata...)
				mock.store[objectKey] = data
			},
		},

		{
			name:  "no footer configured",
			key:   key,
			value: value,
			setup: func() {
				s3Store.options = options.NewStoreOptions() // Reset options
				objectKey := aws.ToString(s3Store.getObjectKey(key, s3Store.options))
				mock.store[objectKey] = value
			},
			wantErr: false,
		},
		{
			name:     "non-existent key",
			key:      []byte("non-existent"),
			footer:   []byte("footer-bytes"),
			metadata: []byte("metadata-content"),
			setup: func() {
				s3Store.options = options.NewStoreOptions(options.WithFooter(footerObj)) // Reset options
				mock.store = make(map[string][]byte)                                     // Clear mock store
			},
			wantErr: true,
		},
		{
			name:     "invalid footer",
			key:      []byte("test-key-invalid"),
			value:    []byte("test-value"),
			footer:   []byte("footer-bytes"),
			metadata: []byte("metadata-content"),
			setup: func() {
				// Set up data with invalid footer
				s3Store.options = options.NewStoreOptions(options.WithFooter(footerObj)) // Reset options
				objectKey := aws.ToString(s3Store.getObjectKey(key, s3Store.options))
				data := append(value, []byte("invalid-footer")...) // nolint:gocritic
				mock.store[objectKey] = data
			},
			wantErr: true,
		},
		{
			name:     "empty file",
			key:      []byte("empty-file"),
			footer:   []byte("footer-bytes"),
			metadata: []byte("metadata-content"),
			setup: func() {
				s3Store.options = options.NewStoreOptions(options.WithFooter(footerObj)) // Reset options
				objectKey := aws.ToString(s3Store.getObjectKey(key, s3Store.options))
				mock.store[objectKey] = []byte{}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				tt.setup()
			} else {
				// Default setup with footer and metadata
				footer := options.NewFooter(len(tt.footer), tt.footer, func() []byte { return tt.metadata })
				s3Store.options = options.NewStoreOptions(options.WithFooter(footer))

				if len(tt.value) > 0 {
					err := s3Store.Set(ctx, tt.key, tt.value)
					require.NoError(t, err)
				}
			}

			got, err := s3Store.GetFooterMetaData(ctx, tt.key)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.metadata == nil {
				assert.Nil(t, got)
			} else {
				assert.Equal(t, tt.metadata, got)
			}
		})
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}

	return b
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

	t.Run("GetDAH always returns 0", func(t *testing.T) {
		dah, err := s3Store.GetDAH(ctx, key)

		assert.NoError(t, err)
		assert.Equal(t, uint32(0), dah)
	})

	t.Run("SetTTL is no-op", func(t *testing.T) {
		err := s3Store.SetDAH(ctx, key, 1)

		assert.NoError(t, err)

		// Verify GetDAH still returns 0
		dah, err := s3Store.GetDAH(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, uint32(0), dah)
	})
}

func TestS3_GetCacheMiss(t *testing.T) {
	s3Store, mock := setupTestS3(t)
	ctx := context.Background()
	key := []byte("test-key")
	value := []byte("test-value")

	// Set up test data directly in mock store to simulate existing S3 data without cache
	objectKey := aws.ToString(s3Store.getObjectKey(key, s3Store.options))
	mock.store[objectKey] = value

	// Clear the cache to ensure cache miss
	cache.Delete(objectKey)

	// Get should still work by fetching from S3
	got, err := s3Store.Get(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, value, got)

	// Verify it's now in cache
	cached, ok := cache.Get(objectKey)
	assert.True(t, ok, "Value should be cached after Get")
	assert.Equal(t, value, cached)
}

func TestS3_GetHeadCacheMiss(t *testing.T) {
	s3Store, mock := setupTestS3(t)
	ctx := context.Background()
	key := []byte("test-key")
	value := []byte("test-value-for-head-request")

	// Set up test data with header directly in mock store
	objectKey := aws.ToString(s3Store.getObjectKey(key, s3Store.options))

	mock.store[objectKey] = value

	// Clear the cache to ensure cache miss
	cache.Delete(objectKey)

	// Test cases for different byte lengths
	tests := []struct {
		name      string
		nrOfBytes int
		want      []byte
	}{
		{
			name:      "get first 4 bytes",
			nrOfBytes: 4,
			want:      []byte("test"),
		},
		{
			name:      "get more bytes than content",
			nrOfBytes: 100,
			want:      value,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Configure store with header
			s3Store.options = options.NewStoreOptions()

			// GetHead should work by fetching from S3
			got, err := s3Store.GetHead(ctx, key, tt.nrOfBytes)
			assert.NoError(t, err)
			assert.Equal(t, tt.want[:min(len(tt.want), tt.nrOfBytes)], got)

			// Verify it's still not cached
			_, ok := cache.Get(objectKey)
			assert.False(t, ok, "Value should not be cached after GetHead")
		})
	}
}
