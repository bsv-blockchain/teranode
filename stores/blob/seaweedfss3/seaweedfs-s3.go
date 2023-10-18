package seaweedfss3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type SeaweedFS struct {
	s3Client   *s3.S3
	bucketName string
	logger     utils.Logger
}

func New(s3GatewayURL *url.URL) (*SeaweedFS, error) {
	logger := gocore.Log("seaweed")

	scheme := "http"
	if s3GatewayURL.Query().Get("scheme") != "" {
		scheme = s3GatewayURL.Query().Get("scheme")
	}

	serverURL := url.URL{
		Scheme: scheme,
		Host:   s3GatewayURL.Host,
	}

	// Initialize an AWS session with custom credentials pointing to SeaweedFS S3 Gateway
	sess := session.Must(session.NewSession(&aws.Config{
		Region:           aws.String(s3GatewayURL.Query().Get("region")),
		Endpoint:         aws.String(serverURL.String()),
		S3ForcePathStyle: aws.Bool(true), // Required when using a non-AWS S3 service
	}))

	// Create an S3 client
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

func (s *SeaweedFS) Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_seaweedfs").NewStat("Set").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "seaweedfs:Set")
	defer traceSpan.Finish()

	objectKey := s.generateKey(key)
	uploadInput := &s3.PutObjectInput{
		Bucket: aws.String(s.bucketName),
		// Key:    aws.String(fmt.Sprintf("%s/%s", s.bucketName, objectKey)),
		Key:  aws.String(objectKey),
		Body: bytes.NewReader(value),
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
		traceSpan.RecordError(err)
		return fmt.Errorf("failed to set seaweedfs data: %w", err)
	}

	return nil
}

func (s *SeaweedFS) SetTTL(ctx context.Context, key []byte, ttl time.Duration) error {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_s3").NewStat("SetTTL").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "s3:SetTTL")
	defer traceSpan.Finish()

	// TODO implement
	return nil
}

func (s *SeaweedFS) Get(ctx context.Context, hash []byte) ([]byte, error) {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_seaweedfs").NewStat("Get").AddTime(start)
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
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_seaweedfs").NewStat("Exists").AddTime(start)
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
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_seaweedfs").NewStat("Del").AddTime(start)
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
