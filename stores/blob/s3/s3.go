package s3

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type S3 struct {
	client     *s3.S3
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
	bucket     string
	logger     utils.Logger
}

func New(s3URL *url.URL) (*S3, error) {
	logger := gocore.Log("s3")

	// connect to aws s3 server
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(s3URL.Query().Get("region"))},
	)

	// Create S3 service client
	client := s3.New(sess)

	// Setup the S3 Upload Manager. Also see the SDK doc for the Upload Manager
	// for more information on configuring part size, and concurrency.
	//
	// http://docs.aws.amazon.com/sdk-for-go/api/service/s3/s3manager/#NewUploader
	uploader := s3manager.NewUploader(sess)

	downloader := s3manager.NewDownloader(sess)

	//client, err := s
	if err != nil {
		return nil, err
	}

	s := &S3{
		client:     client,
		uploader:   uploader,
		downloader: downloader,
		bucket:     s3URL.Path[1:], // remove leading slash
		logger:     logger,
	}

	return s, nil
}

func (g *S3) generateKey(key []byte) *string {
	var reverseHexEncodedKey = utils.ReverseAndHexEncodeSlice(key)
	return aws.String(fmt.Sprintf("%s/%s", reverseHexEncodedKey[:10], reverseHexEncodedKey))
}

func (g *S3) Close(_ context.Context) error {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_s3").NewStat("Close").AddTime(start)
	}()
	traceSpan := tracing.Start(context.Background(), "s3:Close")
	defer traceSpan.Finish()

	// return g.client.Close()
	return nil
}

func (g *S3) Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_s3").NewStat("Set").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "s3:Set")
	defer traceSpan.Finish()

	buf := bytes.NewBuffer(value)
	uploadInput := &s3manager.UploadInput{
		Bucket: aws.String(g.bucket),
		Key:    g.generateKey(key),
		Body:   buf,
	}

	// Expires

	o := options.NewSetOptions(opts...)
	if o.TTL > 0 {
		expires := time.Now().Add(o.TTL)
		uploadInput.Expires = &expires
	}

	_, err := g.uploader.Upload(uploadInput)
	if err != nil {
		traceSpan.RecordError(err)
		return fmt.Errorf("failed to set data: %w", err)
	}

	return nil
}

func (g *S3) SetTTL(ctx context.Context, key []byte, ttl time.Duration) error {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_s3").NewStat("SetTTL").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "s3:SetTTL")
	defer traceSpan.Finish()

	// TODO implement
	return nil
}

func (g *S3) Get(ctx context.Context, hash []byte) ([]byte, error) {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_s3").NewStat("Get").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "s3:Get")
	defer traceSpan.Finish()

	buf := aws.NewWriteAtBuffer([]byte{})
	_, err := g.downloader.Download(buf,
		&s3.GetObjectInput{
			Bucket: aws.String(g.bucket),
			Key:    g.generateKey(hash),
		})
	if err != nil {
		traceSpan.RecordError(err)
		return nil, fmt.Errorf("failed to get data: %w", err)
	}

	return buf.Bytes(), err
}

func (g *S3) Exists(ctx context.Context, hash []byte) (bool, error) {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_s3").NewStat("Exists").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "s3:Exists")
	defer traceSpan.Finish()

	_, err := g.client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(g.bucket),
		Key:    g.generateKey(hash),
	})
	if err != nil {
		// there was a bug in the s3 library
		// https://github.com/aws/aws-sdk-go-v2/issues/2084
		if strings.Contains(err.Error(), "NotFound") {
			return false, nil
		}

		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchKey {
			return false, nil
		}

		traceSpan.RecordError(err)
		return false, fmt.Errorf("failed to check whether object exists: %w", err)
	}

	return true, nil
}

func (g *S3) Del(ctx context.Context, hash []byte) error {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_s3").NewStat("Del").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "s3:Del")
	defer traceSpan.Finish()
	var key = g.generateKey(hash)

	_, err := g.client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(g.bucket),
		Key:    key,
	})
	if err != nil {
		traceSpan.RecordError(err)
		return fmt.Errorf("unable to del data: %w", err)
	}

	err = g.client.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(g.bucket),
		Key:    key,
	})
	if err != nil {
		traceSpan.RecordError(err)
		return fmt.Errorf("failed to del data: %w", err)
	}

	return nil
}
