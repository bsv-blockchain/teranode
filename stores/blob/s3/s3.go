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
// Note: S3 is used exclusively as permanent storage in Teranode. Delete-At-Height (DAH)
// is intentionally not implemented — only the block persister promotes blobs to S3, and
// those blobs are already permanent (DAH=0). Temporary blobs with finite DAH are stored
// on the local file-based blob store where the pruner service manages their lifecycle.
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
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/tracing"
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
//
// Two deployment preconditions, neither enforced in code, both silent when unmet:
//
//   - The IAM principal needs s3:ListBucket on the bucket, not only s3:GetObject.
//     Without it S3 answers a GET for a key that does not exist with 403
//     AccessDenied instead of 404 NoSuchKey, to avoid disclosing existence. That is
//     indistinguishable from a genuine permission fault, so isMissingObject
//     deliberately reports it as a failure rather than a miss — which is correct in
//     isolation but means callers that branch on not-found never see it. A node
//     whose externalStore points here and relies on that branch (stores/utxo/aerospike
//     falls back to the .outputs blob on a miss) loses the fallback for every object
//     under such a policy.
//   - The bucket itself is never verified. S3 answers a HEAD against a missing or
//     misconfigured bucket with a bodyless 404, indistinguishable from a missing key
//     once the SDK has derived the error code (see isMissingObject), so Exists
//     reports "not stored" for every key rather than failing loudly. A one-time
//     HeadBucket at construction would turn that into a startup error; it is not
//     done here.
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

	cache.Stop()

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
			return errors.NewBlobAlreadyExistsError("[S3][SetFromReader] [%s] already exists in store", aws.ToString(objectKey))
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

	// DAH is intentionally not implemented for S3. S3 is used as permanent storage only —
	// blobs are promoted here by the block persister with DAH=0. Temporary blobs with finite
	// DAH are stored on local file-based blob stores where the pruner manages their lifecycle.

	if err := g.client.Upload(ctx, uploadInput); err != nil {
		return errors.NewStorageError("[S3] [%s/%s] failed to set data from reader", g.bucket, aws.ToString(objectKey), err)
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
			return errors.NewBlobAlreadyExistsError("[S3][Set] [%s] already exists in store", aws.ToString(objectKey))
		}
	}

	if !merged.AllowOverwrite {
		// Check if the object already exists
		if _, err := g.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(g.bucket),
			Key:    objectKey,
		}); err == nil {
			return errors.NewBlobAlreadyExistsError("[S3][Set] [%s] already exists in store", aws.ToString(objectKey))
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

	// DAH is intentionally not implemented for S3. See package doc and SetFromReader for rationale.

	if err := g.client.Upload(ctx, uploadInput); err != nil {
		return errors.NewStorageError("[S3] [%s/%s] failed to set data", g.bucket, aws.ToString(objectKey), err)
	}

	cache.Set(*objectKey, value) // We store the value without header

	return nil
}

// SetDAH is intentionally a no-op for S3. S3 is used exclusively as permanent storage —
// only the block persister promotes blobs to S3 with DAH=0 (permanent). Blobs in S3
// are never scheduled for automatic deletion by block height.
// A non-zero DAH is logged as a warning to surface accidental attempts to apply finite
// retention to S3, which would otherwise be silently ignored.
func (g *S3) SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, dah uint32, opts ...options.FileOption) error {
	_, _, endSpan := tracing.Tracer("s3").Start(ctx, "s3:SetDAH")
	defer endSpan()

	if dah != 0 {
		g.logger.Warnf("[S3][SetDAH] non-zero DAH (%d) requested for key=%x — S3 is permanent storage, DAH is not applied", dah, key)
	}

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
		if isMissingObject(err) {
			return nil, errors.ErrNotFound
		}

		return nil, errors.NewStorageError("[S3][GetIoReader] [%s/%s] failed to get s3 data", g.bucket, aws.ToString(objectKey), err)
	}

	// Consume the fileformat.Header before returning the rest of the stream.
	// result.Body is the S3 SDK's HTTP response body and holds a network
	// connection until Close - if header validation fails we must Close
	// before returning, otherwise the connection is held for the lifetime of
	// the process. Mirrors the file store's pattern at
	// stores/blob/file/file.go:996-1002 (validateFileHeader closes the
	// underlying *os.File on mismatch).
	header := &fileformat.Header{}
	if err := header.Read(result.Body); err != nil {
		_ = result.Body.Close()
		return nil, errors.NewStorageError("[S3][GetIoReader] [%s/%s] missing or invalid header", g.bucket, aws.ToString(objectKey), err)
	}
	// Optionally, verify the header matches the expected fileType
	if header.FileType() != fileType {
		_ = result.Body.Close()
		return nil, errors.NewStorageError("[S3][GetIoReader] [%s/%s] header filetype mismatch: got %s, want %s", g.bucket, aws.ToString(objectKey), header.FileType(), fileType)
	}

	return result.Body, nil
}

func (g *S3) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	ctx, span, endSpan := tracing.Tracer("s3").Start(ctx, "s3:Get")
	defer endSpan()

	merged := options.MergeOptions(g.options, opts)

	objectKey := g.getObjectKey(key, fileType, merged)

	// We log this, since this should not happen in a healthy system. Subtrees should be retrieved from the local ttl cache
	// g.logger.Warnf("[S3][%s] Getting object from S3: %s", util.ReverseAndHexEncodeSlice(key), *objectKey)

	// check cache
	cached, ok := cache.Get(*objectKey)
	if ok {
		g.logger.Debugf("[S3] Cache hit for: %s", *objectKey)
		return cached, nil
	}

	content, err := g.client.Download(ctx, &s3.GetObjectInput{
		Bucket: aws.String(g.bucket),
		Key:    objectKey,
	})
	if err != nil {
		if isMissingObject(err) {
			span.RecordError(errors.ErrNotFound)
			return nil, errors.ErrNotFound
		}

		err = errors.NewStorageError("[S3] [%s/%s] failed to get data", g.bucket, aws.ToString(objectKey), err)
		span.RecordError(err)

		return nil, err
	}

	// Skip the header bytes
	header, err := fileformat.ReadHeaderFromBytes(content)
	if err != nil {
		err = errors.NewStorageError("[S3] [%s/%s] failed to read header", g.bucket, aws.ToString(objectKey), err)
		span.RecordError(err)

		return nil, err
	}

	content = content[len(header.Bytes()):]

	cache.Set(*objectKey, content)

	return content, err
}

// isMissingObject reports whether err means "this object is not in the bucket",
// as opposed to a store failure.
//
// S3 surfaces a missing object three ways: a typed *types.NoSuchKey, a typed
// *types.NotFound, and — in the configurations affected by
// aws/aws-sdk-go-v2#2084 — a generic API error whose code is NotFound and whose
// message never mentions NoSuchKey. All three must map to ErrNotFound, because
// callers use that distinction to decide control flow: stores/utxo/aerospike
// falls back to the UTXO-set .outputs blob only on a genuine miss, so an
// unrecognised miss reads as a store failure and aborts an operation that should
// have succeeded. Equally, a real failure must not be reported as a miss, or
// that same fallback silently masks an outage.
func isMissingObject(err error) bool {
	var (
		noSuchKey *types.NoSuchKey
		notFound  *types.NotFound
	)

	if errors.As(err, &noSuchKey) || errors.As(err, &notFound) {
		return true
	}

	// The SDK bug above returns neither typed error, so fall back to the error
	// code the SDK parsed — not a scan of the message, which would mistake an
	// unrelated error that merely mentions NotFound in its body for a miss.
	//
	// Matching on the code covers the bodyless-404 case too, so no status-code
	// check is needed: the S3 deserializers pass UseStatusCode, so s3shared
	// derives the code "NotFound" from the status text when the response carries
	// no error body. Widening to "any 404" would gain nothing and lose the one
	// distinction there is to keep on a GET: NoSuchBucket is also a 404, arrives
	// with a body naming it, and must stay a failure rather than read as a miss.
	//
	// That distinction does NOT survive on a HEAD. A HEAD response carries no body
	// at all, so a wrong or missing bucket reaches us as the same derived
	// "NotFound" as a missing key, and Exists reports (false, nil) for every key in
	// a misconfigured bucket. Pre-existing — the previous strings.Contains match had
	// it too — and not something this helper can resolve, since by then the
	// distinguishing information is gone. Catching it needs a bucket-level check at
	// construction; see the note on the S3 type.
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return true
		}
	}

	return false
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
		if isMissingObject(err) {
			return false, nil
		}

		err = errors.NewStorageError("[S3] [%s/%s] failed to check whether object exists", g.bucket, aws.ToString(objectKey), err)
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
		err = errors.NewStorageError("[S3] [%s/%s] unable to del data", g.bucket, aws.ToString(objectKey), err)
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
		key = fmt.Sprintf("%s%s", util.ReverseAndHexEncodeSlice(hash), ext)

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
