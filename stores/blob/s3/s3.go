// Package s3 implements an Amazon S3-compatible blob storage backend for the blob.Store interface.
//
// The S3 blob store provides a cloud-based, scalable, and durable storage solution for blobs
// by leveraging Amazon S3 or compatible object storage services. This implementation is designed
// for production use cases requiring high durability, availability, and scalability, such as:
//   - Long-term archival storage for blockchain data
//   - Storage of large transactions that exceed in-memory or local disk capacity
//   - Distributed deployments where multiple nodes need access to the same blob data
//   - Disaster recovery and backup scenarios
//
// The implementation supports configurable connection parameters, custom bucket and region
// settings, and optional subdirectory organization. It handles proper file formatting with
// headers and provides efficient streaming operations for large blobs.
//
// Note: While the S3 implementation supports most blob.Store interface methods, the
// Delete-At-Height (DAH) functionality is currently managed through S3's native TTL
// mechanisms rather than blockchain height.
package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/go-utils/expiringmap"
)

// S3 implements the blob.Store interface using Amazon S3 or compatible object storage services.
// It provides a cloud-based, scalable, and durable storage solution for blobs with configurable
// connection parameters, bucket settings, and optional subdirectory organization.
//
// The S3 implementation is particularly well-suited for:
// - Long-term archival storage for blockchain data
// - Storage of large transactions that exceed in-memory or local disk capacity
// - Distributed deployments where multiple nodes need access to the same blob data
// - Disaster recovery and backup scenarios
//
// The implementation handles proper file formatting with headers and provides efficient
// streaming operations for large blobs.
type S3 struct {
	// client is the S3 client interface for interacting with the S3 service
	client S3Client
	// bucket is the name of the S3 bucket where blobs are stored
	bucket string
	// logger provides structured logging for store operations
	logger ulogger.Logger
	// options contains configuration options for the store
	options *options.Options
}

var (
	// cache provides a short-lived in-memory cache for frequently accessed blobs
	// to reduce S3 API calls and improve performance. Items expire after 1 minute.
	cache = expiringmap.New[string, []byte](1 * time.Minute)
)

// New creates a new S3 blob store instance configured to use the specified S3 endpoint.
//
// The S3 blob store is designed for the following use cases:
// - Long-term storage to retrieve historical blockchain data
// - Storage of large transactions in production environments
// - Scalable and durable blob storage with high availability
//
// Note on expiration:
// - TTL is managed by S3's native expiration mechanisms
// - Delete-At-Height (DAH) functionality is planned but not fully implemented
// - Manual expiration via SetTTL is not currently supported
// New creates a new S3 blob store instance configured to use the specified S3 endpoint.
//
// The s3URL parameter should be formatted as:
// "s3://bucket-name?region=us-west-2&subDirectory=path/to/dir&MaxIdleConns=100&..."
//
// Supported URL query parameters:
// - region: AWS region (required)
// - subDirectory: Optional subdirectory within the bucket for blob storage
// - MaxIdleConns: Maximum number of idle connections (default: 100)
// - MaxIdleConnsPerHost: Maximum idle connections per host (default: 100)
// - IdleConnTimeoutSeconds: Idle connection timeout in seconds (default: 100)
// - TimeoutSeconds: Connection timeout in seconds (default: 30)
// - KeepAliveSeconds: Connection keep-alive in seconds (default: 300)
//
// Parameters:
//   - logger: Logger instance for store operations
//   - s3URL: URL containing the S3 bucket name and configuration parameters
//   - opts: Optional store configuration options
//
// Returns:
//   - *S3: The configured S3 store instance
//   - error: Configuration errors if any occurred
func New(logger ulogger.Logger, s3URL *url.URL, opts ...options.StoreOption) (*S3, error) {
	logger = logger.New("s3")

	if s3URL == nil {
		return nil, errors.NewConfigurationError("[S3] URL is nil")
	}

	// Extract bucket name from host instead of path
	bucket := s3URL.Host
	if bucket == "" {
		return nil, errors.NewConfigurationError("[S3] bucket name is required")
	}

	maxIdleConns, err := getQueryParamInt(s3URL, "MaxIdleConns", 100)
	if err != nil {
		return nil, errors.NewConfigurationError("[S3] failed to parse MaxIdleConns", err)
	}

	maxIdleConnsPerHost, err := getQueryParamInt(s3URL, "MaxIdleConnsPerHost", 100)
	if err != nil {
		return nil, errors.NewConfigurationError("[S3] failed to parse MaxIdleConnsPerHost", err)
	}

	idleConnTimeout, err := getQueryParamDuration(s3URL, "IdleConnTimeoutSeconds", 100, time.Second)
	if err != nil {
		return nil, errors.NewConfigurationError("[S3] failed to parse IdleConnTimeoutSeconds", err)
	}

	timeout, err := getQueryParamDuration(s3URL, "TimeoutSeconds", 30, time.Second)
	if err != nil {
		return nil, errors.NewConfigurationError("[S3] failed to parse TimeoutSeconds", err)
	}

	keepAlive, err := getQueryParamDuration(s3URL, "KeepAliveSeconds", 300, time.Second)
	if err != nil {
		return nil, errors.NewConfigurationError("[S3] failed to parse KeepAliveSeconds", err)
	}

	region := s3URL.Query().Get("region")
	subDirectory := s3URL.Query().Get("subDirectory")

	if len(subDirectory) > 0 {
		opts = append(opts, options.WithDefaultSubDirectory(subDirectory))
	}

	config, _ := config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
	config.HTTPClient = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        maxIdleConns,
			MaxIdleConnsPerHost: maxIdleConnsPerHost,
			IdleConnTimeout:     idleConnTimeout,
			DialContext: (&net.Dialer{
				Timeout:   timeout,
				KeepAlive: keepAlive,
			}).DialContext},
	}

	client := NewRealS3Client(config)

	s := &S3{
		client:  client,
		bucket:  bucket,
		logger:  logger,
		options: options.NewStoreOptions(opts...),
	}

	return s, nil
}

// Health checks the health status of the S3 blob store.
// It verifies connectivity to the S3 service by attempting to check if a test blob exists.
//
// Parameters:
//   - ctx: Context for the operation
//   - checkLiveness: Whether to perform a more thorough liveness check (unused in this implementation)
//
// Returns:
//   - int: HTTP status code (200 for healthy, 503 for unhealthy)
//   - string: Human-readable health status message
//   - error: Any error that occurred during the health check
func (g *S3) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	_, err := g.Exists(ctx, []byte("Health"), "check")
	if err != nil {
		return http.StatusServiceUnavailable, "S3 Store unavailable", err
	}

	return http.StatusOK, "S3 Store available", nil
}

// Close performs any necessary cleanup for the S3 store.
// This is primarily a no-op as the S3 client manages its own connection pool,
// but it's included to satisfy the blob.Store interface.
//
// Parameters:
//   - ctx: Context for the operation (unused in this implementation)
//
// Returns:
//   - error: Always returns nil
func (g *S3) Close(_ context.Context) error {
	_, _, endTrace := tracing.Tracer("s3").Start(context.Background(), "Close")
	defer endTrace()

	return nil
}

// SetFromReader stores a blob in S3 from a streaming reader.
// It efficiently handles large blobs by streaming the data directly to S3 without
// loading the entire blob into memory. The method adds appropriate file format headers
// before storing the blob.
//
// Parameters:
//   - ctx: Context for the operation
//   - key: The key identifying the blob
//   - fileType: The type of the file
//   - reader: Reader providing the blob data
//   - opts: Optional file options
//
// Returns:
//   - error: Any error that occurred during the storage operation
func (g *S3) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	_, _, endTrace := tracing.Tracer("s3").Start(ctx, "SetFromReader")

	defer func() {
		_ = reader.Close()

		endTrace()
	}()

	merged := options.MergeOptions(g.options, opts)

	objectKey := g.getObjectKey(key, fileType, merged)

	if !merged.AllowOverwrite {
		// Check if the object already exists
		if _, err := g.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(g.bucket),
			Key:    objectKey,
		}); err == nil {
			return errors.NewBlobAlreadyExistsError("[S3][SetFromReader] [%s] already exists in store", objectKey)
		}
	}

	// Create a new buffer to hold header + content + footer
	var buf bytes.Buffer

	header := fileformat.NewHeader(fileType)
	if err := header.Write(&buf); err != nil {
		return errors.NewStorageError("[S3][SetFromReader] failed to write header", err)
	}

	// Copy the reader content
	if _, err := io.Copy(&buf, reader); err != nil {
		return errors.NewStorageError("[S3][SetFromReader] failed to write content", err)
	}

	uploadInput := &s3.PutObjectInput{
		Bucket: aws.String(g.bucket),
		Key:    objectKey,
		Body:   bytes.NewReader(buf.Bytes()),
	}

	// if merged.BlockHeightRetention > 0 {
	// TODO DAH
	// expires := time.Now().Add(time.Duration(*merged.BlockHeightRetention))
	// uploadInput.Expires = &expires
	// }

	if _, err := g.client.Upload(ctx, uploadInput); err != nil {
		return errors.NewStorageError("[S3] [%s/%s] failed to set data from reader", g.bucket, objectKey, err)
	}

	return nil
}

func (g *S3) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	ctx, _, endSpan := tracing.Tracer("s3").Start(ctx, "s3:Set")
	defer endSpan()

	merged := options.MergeOptions(g.options, opts)

	objectKey := g.getObjectKey(key, fileType, merged)

	if !merged.AllowOverwrite {
		// Check if the object already exists
		if _, err := g.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(g.bucket),
			Key:    objectKey,
		}); err == nil {
			return errors.NewBlobAlreadyExistsError("[S3][Set] [%s] already exists in store", objectKey)
		}
	}

	if !merged.AllowOverwrite {
		// Check if the object already exists
		if _, err := g.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(g.bucket),
			Key:    objectKey,
		}); err == nil {
			return errors.NewBlobAlreadyExistsError("[S3][Set] [%s] already exists in store", objectKey)
		}
	}

	// Prepare the full content with header and footer
	var content []byte

	header := fileformat.NewHeader(fileType)

	content = append(content, header.Bytes()...)

	content = append(content, value...)

	uploadInput := &s3.PutObjectInput{
		Bucket: aws.String(g.bucket),
		Key:    objectKey,
		Body:   bytes.NewReader(content),
	}

	// Expires

	// if merged.BlockHeightRetention > 0 {
	// TODO DAH
	// expires := merged.BlockHeightRetention))
	// uploadInput.Expires = &expires
	// }

	if _, err := g.client.Upload(ctx, uploadInput); err != nil {
		return errors.NewStorageError("[S3] [%s/%s] failed to set data", g.bucket, objectKey, err)
	}

	cache.Set(*objectKey, value) // We store the value without header

	return nil
}

func (g *S3) SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, dah uint32, opts ...options.FileOption) error {
	_, _, endSpan := tracing.Tracer("s3").Start(ctx, "s3:SetDAH")
	defer endSpan()

	// TODO implement
	return nil
}

func (g *S3) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	ctx, _, endSpan := tracing.Tracer("s3").Start(ctx, "s3:GetIoReader")
	defer endSpan()

	merged := options.MergeOptions(g.options, opts)

	objectKey := g.getObjectKey(key, fileType, merged)

	result, err := g.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(g.bucket),
		Key:    objectKey,
	})
	if err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") {
			return nil, errors.ErrNotFound
		}

		return nil, errors.NewStorageError("[S3][GetIoReader] [%s/%s] failed to get s3 data", g.bucket, objectKey, err)
	}

	// Consume the fileformat.Header before returning the rest of the stream
	header := &fileformat.Header{}
	if err := header.Read(result.Body); err != nil {
		return nil, errors.NewStorageError("[S3][GetIoReader] [%s/%s] missing or invalid header: %v", g.bucket, objectKey, err)
	}
	// Optionally, verify the header matches the expected fileType
	if header.FileType() != fileType {
		return nil, errors.NewStorageError("[S3][GetIoReader] [%s/%s] header filetype mismatch: got %s, want %s", g.bucket, objectKey, header.FileType(), fileType)
	}

	return result.Body, nil
}

func (g *S3) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	ctx, span, endSpan := tracing.Tracer("s3").Start(ctx, "s3:Get")
	defer endSpan()

	merged := options.MergeOptions(g.options, opts)

	objectKey := g.getObjectKey(key, fileType, merged)

	// We log this, since this should not happen in a healthy system. Subtrees should be retrieved from the local ttl cache
	// g.logger.Warnf("[S3][%s] Getting object from S3: %s", utils.ReverseAndHexEncodeSlice(key), *objectKey)

	// check cache
	cached, ok := cache.Get(*objectKey)
	if ok {
		g.logger.Debugf("[S3] Cache hit for: %s", *objectKey)
		return cached, nil
	}

	buf := manager.NewWriteAtBuffer([]byte{})
	_, err := g.client.Download(ctx, buf,
		&s3.GetObjectInput{
			Bucket: aws.String(g.bucket),
			Key:    objectKey,
		})

	if err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") {
			span.RecordError(errors.ErrNotFound)
			return nil, errors.ErrNotFound
		}

		err = errors.NewStorageError("[S3] [%s/%s] failed to get data", g.bucket, objectKey, err)
		span.RecordError(err)

		return nil, err
	}

	// Remove header and footer from the downloaded content
	content := buf.Bytes()

	// Skip the header bytes
	header, err := fileformat.ReadHeaderFromBytes(content)
	if err != nil {
		err = errors.NewStorageError("[S3] [%s/%s] failed to read header", g.bucket, objectKey, err)
		span.RecordError(err)

		return nil, err
	}

	content = content[len(header.Bytes()):]

	cache.Set(*objectKey, content)

	return content, err
}

func (g *S3) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
	ctx, span, endSpan := tracing.Tracer("s3").Start(ctx, "s3:Exists")
	defer endSpan()

	merged := options.MergeOptions(g.options, opts)

	objectKey := g.getObjectKey(key, fileType, merged)

	// check cache
	_, ok := cache.Get(*objectKey)
	if ok {
		return true, nil
	}

	_, err := g.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(g.bucket),
		Key:    objectKey,
	})
	if err != nil {
		// there was a bug in the s3 library
		// https://github.com/aws/aws-sdk-go-v2-v2/issues/2084
		if strings.Contains(err.Error(), "NotFound") {
			return false, nil
		}

		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return false, nil
		}

		err = errors.NewStorageError("[S3] [%s/%s] failed to check whether object exists", g.bucket, objectKey, err)
		span.RecordError(err)

		return false, err
	}

	return true, nil
}

func (g *S3) Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
	ctx, span, endSpan := tracing.Tracer("s3").Start(ctx, "s3:Del")
	defer endSpan()

	merged := options.MergeOptions(g.options, opts)

	objectKey := g.getObjectKey(key, fileType, merged)

	cache.Delete(*objectKey)

	_, err := g.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(g.bucket),
		Key:    objectKey,
	})
	if err != nil {
		err = errors.NewStorageError("[S3] [%s/%s] unable to del data", g.bucket, objectKey, err)
		span.RecordError(err)

		return err
	}

	// do we need to wait until we can be sure that the object is deleted?
	// err = g.client.WaitUntilObjectNotExists(traceSpan.Ctx, &s3.HeadObjectInput{
	// 	Bucket: aws.String(g.bucket),
	// 	Key:    objectKey,
	// })
	// if err != nil {
	// 	traceSpan.RecordError(err)
	// 	return errors.NewStorageError("failed to del data", err)
	// }

	return nil
}

func (g *S3) SetCurrentBlockHeight(_ uint32) {
	// This method is intentionally left empty because the S3 backend does not
	// support or require block height functionality. Block height is not relevant
	// for this storage implementation.
}

// getObjectKey constructs the S3 object key for a blob based on its hash, file type, and options.
// The object key includes any configured subdirectory and uses the hash and file type to create
// a unique and consistent path within the S3 bucket.
//
// Parameters:
//   - hash: The blob hash/key
//   - fileType: The type of the file
//   - o: Options containing configuration such as subdirectory
//
// Returns:
//   - *string: The fully constructed S3 object key
func (g *S3) getObjectKey(hash []byte, fileType fileformat.FileType, o *options.Options) *string {
	var (
		key    string
		prefix string
		ext    string
	)

	ext = "." + fileType.String()

	if o.Filename != "" {
		key = o.Filename
	} else {
		key = fmt.Sprintf("%s%s", utils.ReverseAndHexEncodeSlice(hash), ext)

		prefix = o.CalculatePrefix(key)
	}

	return aws.String(filepath.Join(o.SubDirectory, prefix, key))
}

// getQueryParamInt extracts an integer parameter from a URL's query string.
// If the parameter is not present, it returns the specified default value.
//
// Parameters:
//   - url: The URL containing query parameters
//   - key: The name of the query parameter to extract
//   - defaultValue: The default value to return if the parameter is not present
//
// Returns:
//   - int: The extracted integer value or the default
//   - error: Any error that occurred during parsing
func getQueryParamInt(url *url.URL, key string, defaultValue int) (int, error) {
	value := url.Query().Get(key)
	if value == "" {
		return defaultValue, nil
	}

	result, err := strconv.Atoi(value)

	return result, err
}

// getQueryParamDuration extracts a duration parameter from a URL's query string.
// If the parameter is not present, it returns the specified default value.
// The duration is calculated by multiplying the extracted integer by the specified duration unit.
//
// Parameters:
//   - url: The URL containing query parameters
//   - key: The name of the query parameter to extract
//   - defaultValue: The default integer value to use if the parameter is not present
//   - duration: The duration unit to multiply the extracted value by
//
// Returns:
//   - time.Duration: The calculated duration
//   - error: Any error that occurred during parsing
func getQueryParamDuration(url *url.URL, key string, defaultValue int, duration time.Duration) (time.Duration, error) {
	value := url.Query().Get(key)
	if value == "" {
		return time.Duration(defaultValue) * duration, nil
	}

	result, err := strconv.Atoi(value)

	return time.Duration(result) * duration, err
}
