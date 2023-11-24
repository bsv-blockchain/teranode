package seaweedfss3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type SeaweedFS struct {
	s3Client   *s3.S3
	bucketName string
	logger     ulogger.Logger
}

func New(logger ulogger.Logger, s3GatewayURL *url.URL) (*SeaweedFS, error) {
	logger = logger.New("seaweed")

	scheme := getQueryParamString(s3GatewayURL, "scheme", "http")
	s3ForcePathStyle := getQueryParamBool(s3GatewayURL, "S3ForcePathStyle", "false")
	maxIdleConns := getQueryParamInt(s3GatewayURL, "MaxIdleConns", 100)
	maxIdleConnsPerHost := getQueryParamInt(s3GatewayURL, "MaxIdleConnsPerHost", 100)
	idleConnTimeout := time.Duration(getQueryParamInt(s3GatewayURL, "IdleConnTimeoutSeconds", 100)) * time.Second
	timeout := time.Duration(getQueryParamInt(s3GatewayURL, "TimeoutSeconds", 30)) * time.Second
	keepAlive := time.Duration(getQueryParamInt(s3GatewayURL, "KeepAliveSeconds", 300)) * time.Second

	serverURL := url.URL{
		Scheme: scheme,
		Host:   s3GatewayURL.Host,
	}

	sess := session.Must(session.NewSession(&aws.Config{
		Region:           aws.String(s3GatewayURL.Query().Get("region")),
		Endpoint:         aws.String(serverURL.String()),
		S3ForcePathStyle: aws.Bool(s3ForcePathStyle), // Required when using a non-AWS S3 service
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        maxIdleConns,
				MaxIdleConnsPerHost: maxIdleConnsPerHost,
				IdleConnTimeout:     idleConnTimeout * time.Second,
				DialContext: (&net.Dialer{
					Timeout:   timeout,
					KeepAlive: keepAlive,
				}).DialContext},
		}}))

	svc := s3.New(sess)

	s := &SeaweedFS{
		s3Client:   svc,
		bucketName: s3GatewayURL.Path[1:],
		logger:     logger,
	}

	return s, nil
}

func (s *SeaweedFS) Health(ctx context.Context) (int, string, error) {
	_, err := s.Exists(ctx, []byte("Health"))
	if err != nil {
		return -1, "SeaweedFS Store", err
	}

	return 0, "SeaweedFS Store", nil
}

func (s *SeaweedFS) Close(_ context.Context) error {
	// return s.s3Client.Config.Credentials.(*credentials.StaticProvider).Session.Close()
	return nil
}

func (s *SeaweedFS) generateKey(key []byte) string {
	return utils.ReverseAndHexEncodeSlice(key)
}

func (s *SeaweedFS) SetFromReader(ctx context.Context, key []byte, reader io.ReadCloser, opts ...options.Options) error {
	defer reader.Close()

	b, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read data from reader: %w", err)
	}

	return s.Set(ctx, key, b, opts...)
}

func (s *SeaweedFS) Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error {
	// start := gocore.CurrentTime()
	// defer func() {
	// 	gocore.NewStat("prop_store_sws3", true).NewStat("Set").AddTime(start)
	// }()
	// traceSpan := tracing.Start(ctx, "seaweedfs:Set")
	// defer traceSpan.Finish()

	objectKey := s.generateKey(key)
	uploadInput := &s3.PutObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(value),
	}

	// Expires
	o := options.NewSetOptions(opts...)
	if o.TTL > 0 {
		expires := time.Now().Add(o.TTL)
		uploadInput.Expires = &expires
	}

	// Upload the value as an S3 object
	_, err := s.s3Client.PutObject(uploadInput)
	if err != nil {
		// traceSpan.RecordError(err)
		return fmt.Errorf("failed to set seaweedfs data: %w", err)
	}

	return nil
}

func (s *SeaweedFS) SetTTL(ctx context.Context, key []byte, ttl time.Duration) error {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_sws3", true).NewStat("SetTTL").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "s3:SetTTL")
	defer traceSpan.Finish()

	// TODO implement
	return nil
}

func (s *SeaweedFS) GetIoReader(ctx context.Context, key []byte) (io.ReadCloser, error) {
	b, err := s.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	return options.ReaderWrapper{Reader: bytes.NewBuffer(b), Closer: options.ReaderCloser{}}, nil
}

func (s *SeaweedFS) Get(ctx context.Context, hash []byte) ([]byte, error) {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_sws3", true).NewStat("Get").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "seaweedfs:Get")
	defer traceSpan.Finish()

	objectKey := s.generateKey(hash)

	// Download the S3 object
	resp, err := s.s3Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		traceSpan.RecordError(err)
		return nil, fmt.Errorf("failed to get seaweedfs data: %w", err)
	}
	defer resp.Body.Close()

	// Read the response body
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		traceSpan.RecordError(err)
		return nil, fmt.Errorf("failed to read seaweedfs data: %w", err)
	}

	return data, nil
}

func (s *SeaweedFS) Exists(ctx context.Context, hash []byte) (bool, error) {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_sws3", true).NewStat("Exists").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "seaweedfs:Exists")
	defer traceSpan.Finish()

	objectKey := s.generateKey(hash)

	// HeadObject checks if the S3 object exists without downloading it
	_, err := s.s3Client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		traceSpan.RecordError(err)
		if strings.Contains(err.Error(), "NotFound") {
			return false, nil // Object does not exist
		}
		return false, fmt.Errorf("failed to check seaweedfs data: %w", err)
	}

	return true, nil
}

func (s *SeaweedFS) Del(ctx context.Context, hash []byte) error {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_sws3", true).NewStat("Del").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "seaweedfs:Del")
	defer traceSpan.Finish()

	objectKey := s.generateKey(hash)

	// Delete the S3 object
	_, err := s.s3Client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		traceSpan.RecordError(err)
		return fmt.Errorf("failed to delete seaweedfs data: %w", err)
	}

	return nil
}

func getQueryParamString(url *url.URL, key string, defaultValue string) string {
	value := url.Query().Get(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func getQueryParamBool(url *url.URL, key string, defaultValue string) bool {
	value := url.Query().Get(key)
	if value == "" {
		return defaultValue == "true"
	}
	return value == "true"
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
