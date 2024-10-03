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
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/go-utils/expiringmap"
	"github.com/ordishs/gocore"
)

type S3 struct {
	client     *s3.Client
	uploader   *manager.Uploader
	downloader *manager.Downloader
	bucket     string
	options    *options.Options
	logger     ulogger.Logger
}

var (
	cache = expiringmap.New[string, []byte](1 * time.Minute)
)

/**
* Used in Lustre store to retrieve old files.
* Used in Aerospike store for large transactions in production.
* TTL managed by S3.
* SetTTL is not implemented meaning you cannot manually expire a file.
 */
func New(logger ulogger.Logger, s3URL *url.URL, opts ...options.StoreOption) (*S3, error) {
	logger = logger.New("s3")

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
		opts = append(opts, options.WithSubDirectory(subDirectory))
	}

	config, _ := config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
	client := s3.NewFromConfig(config)

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

	s := &S3{
		client:     client,
		uploader:   manager.NewUploader(client),
		downloader: manager.NewDownloader(client),
		bucket:     s3URL.Path[1:], // remove leading slash
		logger:     logger,
		options:    options.NewStoreOptions(opts...),
	}

	return s, nil
}

func (g *S3) Health(ctx context.Context) (int, string, error) {
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

	// g.logger.Warnf("[S3][%s] Setting object reader from S3: %s", utils.ReverseAndHexEncodeSlice(key), *objectKey)

	uploadInput := &s3.PutObjectInput{
		Bucket: aws.String(g.bucket),
		Key:    objectKey,
		Body:   reader,
	}

	if merged.TTL != nil && *merged.TTL > 0 {
		expires := time.Now().Add(*merged.TTL)
		uploadInput.Expires = &expires
	}

	if _, err := g.uploader.Upload(traceSpan.Ctx, uploadInput); err != nil {
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

	buf := bytes.NewBuffer(value)

	uploadInput := &s3.PutObjectInput{
		Bucket: aws.String(g.bucket),
		Key:    objectKey,
		Body:   buf,
	}

	// Expires

	if merged.TTL != nil && *merged.TTL > 0 {
		expires := time.Now().Add(*merged.TTL)
		uploadInput.Expires = &expires
	}

	if _, err := g.uploader.Upload(traceSpan.Ctx, uploadInput); err != nil {
		traceSpan.RecordError(err)
		return errors.NewStorageError("[S3] [%s/%s] failed to set data", g.bucket, objectKey, err)
	}

	cache.Set(*objectKey, value)

	return nil
}

func (g *S3) SetTTL(ctx context.Context, key []byte, ttl time.Duration, opts ...options.FileOption) error {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_s3", true).NewStat("SetTTL").AddTime(start)
	}()

	traceSpan := tracing.Start(ctx, "s3:SetTTL")
	defer traceSpan.Finish()

	// TODO implement
	return nil
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

	// We log this, since this should not happen in a healthy system. Subtrees should be retrieved from the local ttl cache
	// g.logger.Warnf("[S3][%s] Getting object reader from S3: %s", utils.ReverseAndHexEncodeSlice(key), *objectKey)

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

	return result.Body, nil
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
	_, err := g.downloader.Download(traceSpan.Ctx, buf,
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

	return buf.Bytes(), err
}

func (g *S3) GetHead(ctx context.Context, key []byte, nrOfBytes int, opts ...options.FileOption) ([]byte, error) {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_s3", true).NewStat("GetHead").AddTime(start)
	}()

	traceSpan := tracing.Start(ctx, "s3:GetHead")
	defer traceSpan.Finish()

	o := options.MergeOptions(g.options, opts)

	objectKey := g.getObjectKey(key, o)

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

	buf := manager.NewWriteAtBuffer([]byte{})
	_, err := g.downloader.Download(traceSpan.Ctx, buf,
		&s3.GetObjectInput{
			Bucket: aws.String(g.bucket),
			Key:    objectKey,
			Range:  aws.String(fmt.Sprintf("bytes=0-%d", nrOfBytes-1)),
		})

	if err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") {
			return nil, errors.ErrNotFound
		}

		traceSpan.RecordError(err)

		return nil, errors.NewStorageError("[S3] [%s/%s] failed to get data head", g.bucket, objectKey, err)
	}

	return buf.Bytes(), err
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
		ext = "." + o.Extension
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
