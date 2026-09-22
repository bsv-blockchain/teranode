package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
			objectKeyPtr, err := s3Store.getObjectKey(tt.key, fileformat.FileTypeTesting, options.MergeOptions(s3Store.options, opts))
			require.NoError(t, err)

			objectKey := aws.ToString(objectKeyPtr)

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
			objectKeyPtr, err := s3Store.getObjectKey(tt.key, fileformat.FileTypeTesting, options.MergeOptions(s3Store.options, opts))
			require.NoError(t, err)

			objectKey := aws.ToString(objectKeyPtr)

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
		objectKeyPtr, err := s3Store.getObjectKey(key, fileformat.FileTypeTesting, s3Store.options)
		require.NoError(t, err)

		objectKey := aws.ToString(objectKeyPtr)
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
	objectKeyPtr, err := s3Store.getObjectKey(key, fileformat.FileTypeTesting, s3Store.options)
	require.NoError(t, err)

	objectKey := aws.ToString(objectKeyPtr)

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

// trackingReadCloser wraps a Reader and records Close invocations so tests
// can assert that consumers release the underlying HTTP response body.
type trackingReadCloser struct {
	io.Reader
	closeCount int
}

func (t *trackingReadCloser) Close() error { t.closeCount++; return nil }

// trackingS3Client is a mock S3 client that returns a caller-supplied Body
// from GetObject and panics on any other method (we only test the
// GetIoReader path here). The Body is a trackingReadCloser so the test can
// assert Close on the header-validation error paths.
type trackingS3Client struct {
	body *trackingReadCloser
}

func (t *trackingS3Client) PutObject(context.Context, *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	panic("trackingS3Client.PutObject not implemented")
}

func (t *trackingS3Client) GetObject(context.Context, *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	return &s3.GetObjectOutput{Body: t.body, ContentLength: aws.Int64(int64(64))}, nil
}

func (t *trackingS3Client) HeadObject(context.Context, *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	panic("trackingS3Client.HeadObject not implemented")
}

func (t *trackingS3Client) DeleteObject(context.Context, *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	panic("trackingS3Client.DeleteObject not implemented")
}

func (t *trackingS3Client) CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput) (*s3.CreateMultipartUploadOutput, error) {
	panic("trackingS3Client.CreateMultipartUpload not implemented")
}

func (t *trackingS3Client) UploadPart(context.Context, *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	panic("trackingS3Client.UploadPart not implemented")
}

func (t *trackingS3Client) CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput) (*s3.CompleteMultipartUploadOutput, error) {
	panic("trackingS3Client.CompleteMultipartUpload not implemented")
}

func (t *trackingS3Client) AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput) (*s3.AbortMultipartUploadOutput, error) {
	panic("trackingS3Client.AbortMultipartUpload not implemented")
}

func (t *trackingS3Client) Download(context.Context, *s3.GetObjectInput) ([]byte, error) {
	panic("trackingS3Client.Download not implemented")
}

func (t *trackingS3Client) Upload(context.Context, *s3.PutObjectInput) error {
	panic("trackingS3Client.Upload not implemented")
}

// TestS3_GetIoReader_ClosesBodyOnHeaderReadError pins the close contract on
// S3.GetIoReader. result.Body is the AWS SDK's HTTP response body and holds
// a network connection until Close - if the fileformat header read fails
// (corrupted blob, truncated download) the function must Close before
// returning, otherwise the connection is held for the lifetime of the
// process. Mirrors the file store's pattern at file.go:996-1002.
func TestS3_GetIoReader_ClosesBodyOnHeaderReadError(t *testing.T) {
	// 0-byte body: header.Read will EOF on the first attempt to consume
	// the 8-byte magic, which is the corrupted-header failure mode.
	body := &trackingReadCloser{Reader: bytes.NewReader(nil)}
	s3Store := &S3{
		client:  &trackingS3Client{body: body},
		bucket:  "test-bucket",
		logger:  ulogger.TestLogger{},
		options: options.NewStoreOptions(),
	}

	_, err := s3Store.GetIoReader(context.Background(), []byte("k"), fileformat.FileTypeTesting)
	require.Error(t, err, "header read on empty body must fail")
	require.Equal(t, 1, body.closeCount, "result.Body must be Closed exactly once when header read fails - otherwise the S3 HTTP connection leaks")
}

// TestS3_GetIoReader_ClosesBodyOnFileTypeMismatch pins the same contract on
// the header file-type mismatch branch (a corrupted-or-wrong-typed blob in
// S3 that opens fine but identifies as a different fileformat).
func TestS3_GetIoReader_ClosesBodyOnFileTypeMismatch(t *testing.T) {
	// Build a body whose magic header is valid but for a DIFFERENT file
	// type than the caller will ask for, so header.Read succeeds but
	// header.FileType() != requested type.
	wrongMagic := fileformat.FileTypeBlock.ToMagicBytes()
	body := &trackingReadCloser{Reader: bytes.NewReader(wrongMagic[:])}
	s3Store := &S3{
		client:  &trackingS3Client{body: body},
		bucket:  "test-bucket",
		logger:  ulogger.TestLogger{},
		options: options.NewStoreOptions(),
	}

	_, err := s3Store.GetIoReader(context.Background(), []byte("k"), fileformat.FileTypeTesting)
	require.Error(t, err, "file-type mismatch must surface as an error")
	require.Equal(t, 1, body.closeCount, "result.Body must be Closed exactly once when file-type mismatches - otherwise the S3 HTTP connection leaks")
}

// TestS3_GetObjectKey_PrefixIsolation is the S3 half of the cross-backend regression table for
// caller-controlled filenames: a filename received over HTTP must never resolve to an object
// key outside the store's configured subDirectory, and the key must be built with object-key
// (slash) semantics rather than host filepath semantics.
func TestS3_GetObjectKey_PrefixIsolation(t *testing.T) {
	const subDir = "blocks/tenant-a"

	s3Store, _ := setupTestS3(t)
	s3Store.options = options.NewStoreOptions(options.WithDefaultSubDirectory(subDir))

	hash := []byte{0x01, 0x02}

	cases := []struct {
		name     string
		filename string
		valid    bool
	}{
		{"plain basename", "lastProcessed.dat", true},
		{"dots inside name", "a.b.c", true},
		{"parent traversal slash", "../../tenant-b/private.block", false},
		{"parent traversal backslash", `..\..\tenant-b\private.block`, false},
		{"single parent", "../private.block", false},
		{"absolute unix", "/tenant-b/private.block", false},
		{"absolute windows", `C:\tenant-b\private.block`, false},
		{"nested relative", "tenant-b/private.block", false},
		{"repeated separators", "a//b", false},
		{"nul byte", "name\x00.block", false},
		{"percent-encoded slash", "..%2F..%2Ftenant-b%2Fprivate.block", false},
		{"percent-encoded backslash", "..%5C..%5Ctenant-b", false},
		{"percent-encoded dot", "%2e%2e", false},
		{"literal percent in name", "a%b", true},
		{"dot", ".", false},
		{"dotdot", "..", false},
		{"too long", strings.Repeat("a", options.MaxFilenameLength+1), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			merged := options.MergeOptions(s3Store.options, []options.FileOption{options.WithFilename(tc.filename)})

			key, err := s3Store.getObjectKey(hash, fileformat.FileTypeBlock, merged)
			if !tc.valid {
				require.Error(t, err)
				require.True(t, errors.Is(err, errors.ErrInvalidArgument), "expected invalid-argument error, got %v", err)
				require.Nil(t, key)

				return
			}

			require.NoError(t, err)
			require.Equal(t, subDir+"/"+tc.filename, aws.ToString(key))
		})
	}

	t.Run("hash-derived key stays under prefix", func(t *testing.T) {
		key, err := s3Store.getObjectKey(hash, fileformat.FileTypeBlock, s3Store.options)
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(aws.ToString(key), subDir+"/"), "key %q not under %q", aws.ToString(key), subDir)
	})

	t.Run("no subdirectory, filename only", func(t *testing.T) {
		plain, _ := setupTestS3(t)
		merged := options.MergeOptions(plain.options, []options.FileOption{options.WithFilename("lastProcessed.dat")})

		key, err := plain.getObjectKey(hash, fileformat.FileTypeDat, merged)
		require.NoError(t, err)
		require.Equal(t, "lastProcessed.dat", aws.ToString(key))
	})

	t.Run("subdirectory with traversal is rejected", func(t *testing.T) {
		bad, _ := setupTestS3(t)
		bad.options = options.NewStoreOptions(options.WithDefaultSubDirectory("blocks/../tenant-b"))

		key, err := bad.getObjectKey(hash, fileformat.FileTypeBlock, bad.options)
		require.Error(t, err)
		require.Nil(t, key)
	})
}

// TestS3_CRUDRejectsTraversalFilename proves every S3 CRUD method refuses before touching the
// client when the filename would escape the prefix, so no cross-prefix read, probe, write or
// delete reaches the bucket.
func TestS3_CRUDRejectsTraversalFilename(t *testing.T) {
	s3Store, mock := setupTestS3(t)
	s3Store.options = options.NewStoreOptions(options.WithDefaultSubDirectory("blocks/tenant-a"))

	ctx := context.Background()
	hash := []byte{0x01}
	escape := options.WithFilename("../../tenant-b/private.block")

	// Seed the object the attacker is aiming at, so a bypass would be observable.
	mock.store["tenant-b/private.block"] = []byte("victim")

	err := s3Store.Set(ctx, hash, fileformat.FileTypeBlock, []byte("x"), escape)
	requireInvalidArgument(t, err)

	err = s3Store.SetFromReader(ctx, hash, fileformat.FileTypeBlock, io.NopCloser(bytes.NewReader([]byte("x"))), escape)
	requireInvalidArgument(t, err)

	_, err = s3Store.Get(ctx, hash, fileformat.FileTypeBlock, escape)
	requireInvalidArgument(t, err)

	_, err = s3Store.GetIoReader(ctx, hash, fileformat.FileTypeBlock, escape)
	requireInvalidArgument(t, err)

	_, err = s3Store.Exists(ctx, hash, fileformat.FileTypeBlock, escape)
	requireInvalidArgument(t, err)

	err = s3Store.Del(ctx, hash, fileformat.FileTypeBlock, escape)
	requireInvalidArgument(t, err)

	require.Equal(t, []byte("victim"), mock.store["tenant-b/private.block"], "target object must be untouched")
}

func requireInvalidArgument(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrInvalidArgument), "expected invalid-argument error, got %v", err)
}

// TestS3_GetObjectKey_SubDirectoryShapes pins the object key produced for every subDirectory
// spelling an operator might configure, so the prefix normalisation never silently re-keys an
// existing bucket. Expected values equal what filepath.Join produced before validation existed.
func TestS3_GetObjectKey_SubDirectoryShapes(t *testing.T) {
	hash := []byte{0x01}
	hashKey := "01." + fileformat.FileTypeBlock.String()

	cases := []struct {
		subDir string
		want   string
	}{
		{"", hashKey},
		{"blocks", "blocks/" + hashKey},
		{"blocks/", "blocks/" + hashKey},
		{"/blocks", "/blocks/" + hashKey},
		{"/blocks/", "/blocks/" + hashKey},
		{"/", "/" + hashKey},
		{"./blocks", "blocks/" + hashKey},
		{"blocks//sub", "blocks/sub/" + hashKey},
		{"path/to/dir", "path/to/dir/" + hashKey},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("%q", tc.subDir), func(t *testing.T) {
			s3Store, _ := setupTestS3(t)
			s3Store.options = options.NewStoreOptions(options.WithDefaultSubDirectory(tc.subDir))

			key, err := s3Store.getObjectKey(hash, fileformat.FileTypeBlock, s3Store.options)
			require.NoError(t, err)
			require.Equal(t, tc.want, aws.ToString(key))

			// A custom filename under the same prefix must stay under it too.
			merged := options.MergeOptions(s3Store.options, []options.FileOption{options.WithFilename("custom.dat")})
			key, err = s3Store.getObjectKey(hash, fileformat.FileTypeDat, merged)
			require.NoError(t, err)
			require.Equal(t, strings.TrimSuffix(tc.want, hashKey)+"custom.dat", aws.ToString(key))
		})
	}
}

// TestS3_SetAndGet_WithFilename round-trips a custom filename through the mock client end to
// end, proving a valid basename still works after validation was added.
func TestS3_SetAndGet_WithFilename(t *testing.T) {
	s3Store, mock := setupTestS3(t)
	s3Store.options = options.NewStoreOptions(options.WithDefaultSubDirectory("blocks/tenant-a"))

	ctx := context.Background()
	opt := options.WithFilename("lastProcessed.dat")

	require.NoError(t, s3Store.Set(ctx, nil, fileformat.FileTypeDat, []byte("42"), opt))

	_, ok := mock.store["blocks/tenant-a/lastProcessed.dat"]
	require.True(t, ok, "object must be stored under the configured prefix")

	exists, err := s3Store.Exists(ctx, nil, fileformat.FileTypeDat, opt)
	require.NoError(t, err)
	require.True(t, exists)

	got, err := s3Store.Get(ctx, nil, fileformat.FileTypeDat, opt)
	require.NoError(t, err)
	require.Equal(t, []byte("42"), got)

	require.NoError(t, s3Store.Del(ctx, nil, fileformat.FileTypeDat, opt))
}
