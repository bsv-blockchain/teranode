package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/bitcoin-sv/ubsv/ubsverrors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
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
	prefixDir  int
	logger     ulogger.Logger
}

var (
	cache = expiringmap.New[string, []byte](1 * time.Minute)
)

func New(logger ulogger.Logger, s3URL *url.URL, opts ...options.Options) (*S3, error) {
	logger = logger.New("s3")

	maxIdleConns := getQueryParamInt(s3URL, "MaxIdleConns", 100)
	maxIdleConnsPerHost := getQueryParamInt(s3URL, "MaxIdleConnsPerHost", 100)
	idleConnTimeout := time.Duration(getQueryParamInt(s3URL, "IdleConnTimeoutSeconds", 100)) * time.Second
	timeout := time.Duration(getQueryParamInt(s3URL, "TimeoutSeconds", 30)) * time.Second
	keepAlive := time.Duration(getQueryParamInt(s3URL, "KeepAliveSeconds", 300)) * time.Second

	config, _ := config.LoadDefaultConfig(context.Background())
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
	}

	o := options.NewSetOptions(opts...)
	if o.PrefixDirectory > 0 {
		s.prefixDir = o.PrefixDirectory
	}

	return s, nil
}

func (g *S3) Health(ctx context.Context) (int, string, error) {
	_, err := g.Exists(ctx, []byte("Health"))
	if err != nil {
		return -1, "S3 Store", err
	}

	return 0, "S3 Store", nil
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

func (g *S3) SetFromReader(ctx context.Context, key []byte, reader io.ReadCloser, opts ...options.Options) error {
	start := gocore.CurrentTime()
	defer func() {
		_ = reader.Close()
		gocore.NewStat("prop_store_s3", true).NewStat("SetFromReader").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "s3:SetFromReader")
	defer traceSpan.Finish()

	objectKey := g.getObjectKey(key)

	uploadInput := &s3.PutObjectInput{
		Bucket: aws.String(g.bucket),
		Key:    objectKey,
		Body:   reader,
	}

	o := options.NewSetOptions(opts...)
	if o.TTL > 0 {
		expires := time.Now().Add(o.TTL)
		uploadInput.Expires = &expires
	}

	_, err := g.uploader.Upload(traceSpan.Ctx, uploadInput)
	if err != nil {
		traceSpan.RecordError(err)
		return fmt.Errorf("failed to set data from reader: %w", err)
	}

	return nil
}

func (g *S3) Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_s3", true).NewStat("Set").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "s3:Set")
	defer traceSpan.Finish()

	objectKey := g.getObjectKey(key)

	buf := bytes.NewBuffer(value)
	uploadInput := &s3.PutObjectInput{
		Bucket: aws.String(g.bucket),
		Key:    objectKey,
		Body:   buf,
	}

	// Expires

	o := options.NewSetOptions(opts...)
	if o.TTL > 0 {
		expires := time.Now().Add(o.TTL)
		uploadInput.Expires = &expires
	}

	_, err := g.uploader.Upload(traceSpan.Ctx, uploadInput)
	if err != nil {
		traceSpan.RecordError(err)
		return fmt.Errorf("failed to set data: %w", err)
	}

	cache.Set(*objectKey, value)

	return nil
}

func (g *S3) SetTTL(ctx context.Context, key []byte, ttl time.Duration) error {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_s3", true).NewStat("SetTTL").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "s3:SetTTL")
	defer traceSpan.Finish()

	// TODO implement
	return nil
}

func (g *S3) GetIoReader(ctx context.Context, key []byte) (io.ReadCloser, error) {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_s3", true).NewStat("GetIoReader").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "s3:Get")
	defer traceSpan.Finish()

	objectKey := g.getObjectKey(key)

	// We log this, since this should not happen in a healthy system. Subtrees should be retrieved from the local ttl cache
	g.logger.Warnf("[S3][%s] Getting object reader from S3: %s", utils.ReverseAndHexEncodeSlice(key), *objectKey)

	result, err := g.client.GetObject(traceSpan.Ctx, &s3.GetObjectInput{
		Bucket: aws.String(g.bucket),
		Key:    objectKey,
	})
	if err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") {
			return nil, ubsverrors.ErrNotFound
		}
		return nil, fmt.Errorf("failed to get s3 data: %w", err)
	}

	return result.Body, nil
}

func (g *S3) Get(ctx context.Context, hash []byte) ([]byte, error) {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_s3", true).NewStat("Get").AddTime(start)
		g.logger.Warnf("[S3][%s] Getting object from S3 DONE", utils.ReverseAndHexEncodeSlice(hash))
	}()
	traceSpan := tracing.Start(ctx, "s3:Get")
	defer traceSpan.Finish()

	objectKey := g.getObjectKey(hash)

	// We log this, since this should not happen in a healthy system. Subtrees should be retrieved from the local ttl cache
	g.logger.Warnf("[S3][%s] Getting object from S3: %s", utils.ReverseAndHexEncodeSlice(hash), *objectKey)

	// check cache
	cached, ok := cache.Get(*objectKey)
	if ok {
		g.logger.Debugf("Cache hit for: %s", *objectKey)
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
			return nil, ubsverrors.ErrNotFound
		}
		traceSpan.RecordError(err)
		return nil, fmt.Errorf("failed to get data: %w", err)
	}

	return buf.Bytes(), err
}

func (g *S3) Exists(ctx context.Context, hash []byte) (bool, error) {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_s3", true).NewStat("Exists").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "s3:Exists")
	defer traceSpan.Finish()

	objectKey := g.getObjectKey(hash)

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
		return false, fmt.Errorf("failed to check whether object exists: %w", err)
	}

	return true, nil
}

func (g *S3) Del(ctx context.Context, hash []byte) error {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_s3", true).NewStat("Del").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "s3:Del")
	defer traceSpan.Finish()

	objectKey := g.getObjectKey(hash)

	cache.Delete(*objectKey)

	_, err := g.client.DeleteObject(traceSpan.Ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(g.bucket),
		Key:    objectKey,
	})
	if err != nil {
		traceSpan.RecordError(err)
		return fmt.Errorf("unable to del data: %w", err)
	}

	// do we need to wait until we can be sure that the object is deleted?
	// err = g.client.WaitUntilObjectNotExists(traceSpan.Ctx, &s3.HeadObjectInput{
	// 	Bucket: aws.String(g.bucket),
	// 	Key:    objectKey,
	// })
	// if err != nil {
	// 	traceSpan.RecordError(err)
	// 	return fmt.Errorf("failed to del data: %w", err)
	// }

	return nil
}

func (g *S3) getObjectKey(key []byte) *string {
	objectKey := g.generateKey(key)

	if g.prefixDir > 0 {
		// take the first n bytes of the key and use them as a prefix directory
		objectKeyStr := *objectKey
		objectKey = aws.String(fmt.Sprintf("%s/%s", objectKeyStr[:g.prefixDir], objectKeyStr))
	}

	return objectKey
}

func (g *S3) generateKey(key []byte) *string {
	var reverseHexEncodedKey = utils.ReverseAndHexEncodeSlice(key)
	return aws.String(fmt.Sprintf("%s/%s", reverseHexEncodedKey[:10], reverseHexEncodedKey))
}

func getQueryParamInt(url *url.URL, key string, defaultValue int) int {
	value := url.Query().Get(key)
	if value == "" {
		return defaultValue
	}
	result, err := strconv.Atoi(value)
	if err != nil {
		panic(err)
	}
	return result
}
