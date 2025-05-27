package s3

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
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
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/blob/helper"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/go-utils/expiringmap"
	"github.com/ordishs/gocore"
)

type S3 struct {
	client  S3Client
	bucket  string
	logger  ulogger.Logger
	options *options.Options
}

var (
	cache = expiringmap.New[string, []byte](1 * time.Minute)
)

/**
* Used in longterm storage to retrieve old files.
* Used in Aerospike store for large transactions in production.
* TTL managed by S3. // TODO DAH
* SetTTL is not implemented meaning you cannot manually expire a file.
 */
func New(logger ulogger.Logger, s3URL *url.URL, opts ...options.StoreOption) (*S3, error) {
	logger = logger.New("s3")

	if s3URL == nil {
		return nil, errors.NewConfigurationError("[S3] URL is nil")
	}

	// Add header/footer handling
	if header := s3URL.Query().Get("header"); header != "" {
		headerBytes, err := base64.StdEncoding.DecodeString(header)
		if err != nil {
			headerBytes = []byte(header)
		}

		opts = append(opts, options.WithHeader(headerBytes))
	}

	if eofMarker := s3URL.Query().Get("eofmarker"); eofMarker != "" {
		eofMarkerBytes, err := hex.DecodeString(eofMarker)
		if err != nil {
			eofMarkerBytes = []byte(eofMarker)
		}

		opts = append(opts, options.WithFooter(options.NewFooter(len(eofMarkerBytes), eofMarkerBytes, nil)))
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

func (g *S3) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	_, err := g.Exists(ctx, []byte("Health"))
	if err != nil {
		return http.StatusServiceUnavailable, "S3 Store unavailable", err
	}

	return http.StatusOK, "S3 Store available", nil
}

func (g *S3) Close(_ context.Context) error {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_s3", true).NewStat("Close").AddTime(start)
	}()

	traceSpan := tracing.Start(context.Background(), "s3:Close")
	defer traceSpan.Finish()

	return nil
}

func (g *S3) SetFromReader(ctx context.Context, key []byte, reader io.ReadCloser, opts ...options.FileOption) error {
	start := gocore.CurrentTime()
	defer func() {
		_ = reader.Close()

		gocore.NewStat("prop_store_s3", true).NewStat("SetFromReader").AddTime(start)
	}()

	traceSpan := tracing.Start(ctx, "s3:SetFromReader")
	defer traceSpan.Finish()

	merged := options.MergeOptions(g.options, opts)

	objectKey := g.getObjectKey(key, merged)

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

	// Write header if present
	if merged.Header != nil {
		if _, err := buf.Write(merged.Header); err != nil {
			return errors.NewStorageError("[S3][SetFromReader] failed to write header", err)
		}
	}

	// Copy the reader content
	if _, err := io.Copy(&buf, reader); err != nil {
		return errors.NewStorageError("[S3][SetFromReader] failed to write content", err)
	}

	// Write footer if present
	if merged.Footer != nil {
		footer, err := merged.Footer.GetFooter()
		if err != nil {
			return errors.NewStorageError("[S3][SetFromReader] failed to get footer", err)
		}

		if _, err := buf.Write(footer); err != nil {
			return errors.NewStorageError("[S3][SetFromReader] failed to write footer", err)
		}
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

	if _, err := g.client.Upload(traceSpan.Ctx, uploadInput); err != nil {
		traceSpan.RecordError(err)
		return errors.NewStorageError("[S3] [%s/%s] failed to set data from reader", g.bucket, objectKey, err)
	}

	return nil
}

func (g *S3) Set(ctx context.Context, key []byte, value []byte, opts ...options.FileOption) error {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_s3", true).NewStat("Set").AddTime(start)
	}()

	traceSpan := tracing.Start(ctx, "s3:Set")
	defer traceSpan.Finish()

	merged := options.MergeOptions(g.options, opts)

	objectKey := g.getObjectKey(key, merged)

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
	if merged.Header != nil {
		content = append(content, merged.Header...)
	}

	content = append(content, value...)

	if merged.Footer != nil {
		footer, err := merged.Footer.GetFooter()
		if err != nil {
			return errors.NewStorageError("[S3][Set] failed to get footer", err)
		}

		content = append(content, footer...)
	}

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

	if _, err := g.client.Upload(traceSpan.Ctx, uploadInput); err != nil {
		traceSpan.RecordError(err)
		return errors.NewStorageError("[S3] [%s/%s] failed to set data", g.bucket, objectKey, err)
	}

	cache.Set(*objectKey, value)

	return nil
}

func (g *S3) SetDAH(ctx context.Context, key []byte, dah uint32, opts ...options.FileOption) error {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_s3", true).NewStat("SetDAH").AddTime(start)
	}()

	traceSpan := tracing.Start(ctx, "s3:SetDAH")
	defer traceSpan.Finish()

	// TODO implement
	return nil
}

func (g *S3) GetDAH(ctx context.Context, key []byte, opts ...options.FileOption) (uint32, error) {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_s3", true).NewStat("GetDAH").AddTime(start)
	}()

	traceSpan := tracing.Start(ctx, "s3:GetDAH")
	defer traceSpan.Finish()

	// TODO implement
	return 0, nil
}

func (g *S3) GetIoReader(ctx context.Context, key []byte, opts ...options.FileOption) (io.ReadCloser, error) {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_s3", true).NewStat("GetIoReader").AddTime(start)
	}()

	traceSpan := tracing.Start(ctx, "s3:Get")
	defer traceSpan.Finish()

	o := options.MergeOptions(g.options, opts)

	objectKey := g.getObjectKey(key, o)

	result, err := g.client.GetObject(traceSpan.Ctx, &s3.GetObjectInput{
		Bucket: aws.String(g.bucket),
		Key:    objectKey,
	})
	if err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") {
			return nil, errors.ErrNotFound
		}

		return nil, errors.NewStorageError("[S3] [%s/%s] failed to get s3 data", g.bucket, objectKey, err)
	}

	merged := options.MergeOptions(g.options, opts)

	return helper.ReaderWithHeaderAndFooterRemoved(result.Body, merged.Header, merged.Footer)
}

func (g *S3) Get(ctx context.Context, key []byte, opts ...options.FileOption) ([]byte, error) {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_s3", true).NewStat("Get").AddTime(start)
	}()

	traceSpan := tracing.Start(ctx, "s3:Get")
	defer traceSpan.Finish()

	o := options.MergeOptions(g.options, opts)

	objectKey := g.getObjectKey(key, o)

	// We log this, since this should not happen in a healthy system. Subtrees should be retrieved from the local ttl cache
	// g.logger.Warnf("[S3][%s] Getting object from S3: %s", utils.ReverseAndHexEncodeSlice(key), *objectKey)

	// check cache
	cached, ok := cache.Get(*objectKey)
	if ok {
		g.logger.Debugf("[S3] Cache hit for: %s", *objectKey)
		return cached, nil
	}

	buf := manager.NewWriteAtBuffer([]byte{})
	_, err := g.client.Download(traceSpan.Ctx, buf,
		&s3.GetObjectInput{
			Bucket: aws.String(g.bucket),
			Key:    objectKey,
		})

	if err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") {
			return nil, errors.ErrNotFound
		}

		traceSpan.RecordError(err)

		return nil, errors.NewStorageError("[S3] [%s/%s] failed to get data", g.bucket, objectKey, err)
	}

	// Remove header and footer from the downloaded content
	merged := options.MergeOptions(g.options, opts)

	content, err := helper.BytesWithHeadAndFooterRemoved(buf.Bytes(), merged.Header, merged.Footer)
	if err != nil {
		return nil, errors.NewStorageError("[S3][Get] failed to remove header and footer", err)
	}

	cache.Set(*objectKey, content)

	return content, err
}

func (g *S3) GetHead(ctx context.Context, key []byte, nrOfBytes int, opts ...options.FileOption) ([]byte, error) {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_s3", true).NewStat("GetHead").AddTime(start)
	}()

	traceSpan := tracing.Start(ctx, "s3:GetHead")
	defer traceSpan.Finish()

	merged := options.MergeOptions(g.options, opts)

	objectKey := g.getObjectKey(key, merged)

	// We log this, since this should not happen in a healthy system. Subtrees should be retrieved from the local ttl cache
	// g.logger.Warnf("[S3][%s] Getting object head from S3: %s", utils.ReverseAndHexEncodeSlice(key), *objectKey)

	// check cache
	cached, ok := cache.Get(*objectKey)
	if ok {
		g.logger.Debugf("[S3] Cache hit for: %s", *objectKey)

		if len(cached) < nrOfBytes {
			return cached, nil
		}

		return cached[:nrOfBytes], nil
	}

	// Adjust the range to account for header length
	headerLen := 0
	if merged.Header != nil {
		headerLen = len(merged.Header)
	}

	// Request enough bytes to include header plus desired content
	rangeStr := fmt.Sprintf("bytes=0-%d", headerLen+nrOfBytes-1)

	buf := manager.NewWriteAtBuffer([]byte{})
	input := &s3.GetObjectInput{
		Bucket: aws.String(g.bucket),
		Key:    objectKey,
		Range:  aws.String(rangeStr),
	}
	_, err := g.client.Download(traceSpan.Ctx, buf, input)

	if err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") {
			return nil, errors.ErrNotFound
		}

		traceSpan.RecordError(err)

		return nil, errors.NewStorageError("[S3] [%s/%s] failed to get data head", g.bucket, objectKey, err)
	}

	// Remove header and get requested bytes
	content, err := helper.BytesWithHeadAndFooterRemoved(buf.Bytes(), merged.Header, nil) // No footer needed for head
	if err != nil {
		return nil, errors.NewStorageError("[S3][GetHead] failed to remove header", err)
	}

	if len(content) > nrOfBytes {
		content = content[:nrOfBytes]
	}

	return content, nil
}

func (g *S3) Exists(ctx context.Context, key []byte, opts ...options.FileOption) (bool, error) {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_s3", true).NewStat("Exists").AddTime(start)
	}()

	traceSpan := tracing.Start(ctx, "s3:Exists")
	defer traceSpan.Finish()

	o := options.MergeOptions(g.options, opts)

	objectKey := g.getObjectKey(key, o)

	// check cache
	_, ok := cache.Get(*objectKey)
	if ok {
		return true, nil
	}

	_, err := g.client.HeadObject(traceSpan.Ctx, &s3.HeadObjectInput{
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

		traceSpan.RecordError(err)

		return false, errors.NewStorageError("[S3] [%s/%s] failed to check whether object exists", g.bucket, objectKey, err)
	}

	return true, nil
}

func (g *S3) Del(ctx context.Context, key []byte, opts ...options.FileOption) error {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_s3", true).NewStat("Del").AddTime(start)
	}()

	traceSpan := tracing.Start(ctx, "s3:Del")
	defer traceSpan.Finish()

	o := options.MergeOptions(g.options, opts)

	objectKey := g.getObjectKey(key, o)

	cache.Delete(*objectKey)

	_, err := g.client.DeleteObject(traceSpan.Ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(g.bucket),
		Key:    objectKey,
	})
	if err != nil {
		traceSpan.RecordError(err)
		return errors.NewStorageError("[S3] [%s/%s] unable to del data", g.bucket, objectKey, err)
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

func (g *S3) getObjectKey(hash []byte, o *options.Options) *string {
	var (
		key    string
		prefix string
		ext    string
	)

	if o.Extension != "" {
		ext = "." + o.Extension.String()
	}

	if o.Filename != "" {
		key = o.Filename
	} else {
		key = fmt.Sprintf("%s%s", utils.ReverseAndHexEncodeSlice(hash), ext)

		prefix = o.CalculatePrefix(key)
	}

	return aws.String(filepath.Join(o.SubDirectory, prefix, key))
}

func getQueryParamInt(url *url.URL, key string, defaultValue int) (int, error) {
	value := url.Query().Get(key)
	if value == "" {
		return defaultValue, nil
	}

	result, err := strconv.Atoi(value)

	return result, err
}

func getQueryParamDuration(url *url.URL, key string, defaultValue int, duration time.Duration) (time.Duration, error) {
	value := url.Query().Get(key)
	if value == "" {
		return time.Duration(defaultValue) * duration, nil
	}

	result, err := strconv.Atoi(value)

	return time.Duration(result) * duration, err
}

func (g *S3) GetFooterMetaData(ctx context.Context, key []byte, opts ...options.FileOption) ([]byte, error) {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_s3", true).NewStat("GetMetaData").AddTime(start)
	}()

	traceSpan := tracing.Start(ctx, "s3:GetMetaData")
	defer traceSpan.Finish()

	merged := options.MergeOptions(g.options, opts)

	// If there's no footer configured, return nil
	if merged.Footer == nil {
		return nil, nil
	}

	objectKey := g.getObjectKey(key, merged)

	// First get the object size using HeadObject
	headResult, err := g.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(g.bucket),
		Key:    objectKey,
	})
	if err != nil {
		if strings.Contains(err.Error(), "NotFound") {
			return nil, errors.ErrNotFound
		}

		return nil, errors.NewStorageError("[S3][GetMetaData] failed to get object head", err)
	}

	contentLength := headResult.ContentLength
	if contentLength == nil || *contentLength == int64(0) {
		return nil, errors.NewStorageError("[S3][GetMetaData] object is empty")
	}

	// Calculate the range for the footer
	footerSize := merged.Footer.Size()
	if *contentLength < int64(footerSize) {
		return nil, errors.NewStorageError("[S3][GetMetaData] object size smaller than footer size")
	}

	// Create range string to get only the footer bytes
	rangeStr := fmt.Sprintf("bytes=%d-%d", *contentLength-int64(footerSize), *contentLength-1)

	// Get only the footer bytes
	result, err := g.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(g.bucket),
		Key:    objectKey,
		Range:  aws.String(rangeStr),
	})
	if err != nil {
		return nil, errors.NewStorageError("[S3][GetMetaData] failed to get footer bytes", err)
	}
	defer result.Body.Close()

	// Read exactly footerSize bytes into a fixed buffer
	footerBytes := make([]byte, footerSize)

	n, err := io.ReadFull(result.Body, footerBytes)
	if err != nil {
		return nil, errors.NewStorageError("[S3][GetMetaData] failed to read footer bytes (read %d of %d bytes): %v", n, footerSize, err)
	}

	// check if the footer is valid
	if !merged.Footer.IsFooter(footerBytes) {
		return nil, errors.NewStorageError("[S3][GetMetaData] footer not found", err)
	}

	// Get metadata from the footer bytes
	metadata := merged.Footer.GetFooterMetaData(footerBytes)

	return metadata, nil
}

func (g *S3) GetHeader(ctx context.Context, key []byte, opts ...options.FileOption) ([]byte, error) {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_s3", true).NewStat("GetHeader").AddTime(start)
	}()

	traceSpan := tracing.Start(ctx, "s3:GetHeader")
	defer traceSpan.Finish()

	merged := options.MergeOptions(g.options, opts)

	// If there's no header configured, return nil
	if merged.Header == nil {
		return nil, nil
	}

	objectKey := g.getObjectKey(key, merged)

	headerSize := len(merged.Header)
	// Create range string to get only the header bytes
	rangeStr := fmt.Sprintf("bytes=0-%d", headerSize-1)

	// Get only the header bytes
	result, err := g.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(g.bucket),
		Key:    objectKey,
		Range:  aws.String(rangeStr),
	})
	if err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") {
			return nil, errors.ErrNotFound
		}

		return nil, errors.NewStorageError("[S3][GetHeader] failed to get header bytes", err)
	}
	defer result.Body.Close()

	// Read exactly headerSize bytes into a fixed buffer
	headerBytes := make([]byte, headerSize)

	n, err := io.ReadFull(result.Body, headerBytes)
	if err != nil {
		return nil, errors.NewStorageError("[S3][GetHeader] failed to read header bytes (read %d of %d bytes): %v", n, headerSize, err)
	}

	// Verify the header matches what we expect
	if !bytes.Equal(headerBytes, merged.Header) {
		return nil, errors.NewStorageError("[S3][GetHeader] header mismatch")
	}

	return headerBytes, nil
}
