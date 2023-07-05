package kinesiss3

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/TAAL-GmbH/ubsv/stores/blob"
	"github.com/TAAL-GmbH/ubsv/tracing"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type KinesisS3 struct {
	client         *s3.S3
	firehoseClient *firehose.Firehose
	uploader       *s3manager.Uploader
	downloader     *s3manager.Downloader
	bucket         string
	logger         utils.Logger
}

func New(s3URL *url.URL) (*KinesisS3, error) {
	logLevel, _ := gocore.Config().Get("logLevel")
	logger := gocore.Log("s3", gocore.NewLogLevelFromString(logLevel))

	region := s3URL.Query().Get("region")

	// connect to aws s3 server
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)

	// Create KinesisS3 service client
	client := s3.New(sess)

	// Setup the KinesisS3 Upload Manager. Also see the SDK doc for the Upload Manager
	// for more information on configuring part size, and concurrency.
	//
	// http://docs.aws.amazon.com/sdk-for-go/api/service/s3/s3manager/#NewUploader
	uploader := s3manager.NewUploader(sess)

	downloader := s3manager.NewDownloader(sess)

	firehoseClient := firehose.New(sess, aws.NewConfig().WithRegion(region))

	//client, err := s
	if err != nil {
		return nil, err
	}

	s3 := &KinesisS3{
		client:         client,
		firehoseClient: firehoseClient,
		uploader:       uploader,
		downloader:     downloader,
		bucket:         s3URL.Path[1:], // remove leading slash
		logger:         logger,
	}

	return s3, nil
}

func (g *KinesisS3) generateKey(key []byte) *string {
	var reverseHexEncodedKey = utils.ReverseAndHexEncodeSlice(key)
	return aws.String(fmt.Sprintf("%s/%s", reverseHexEncodedKey[:10], reverseHexEncodedKey))
}

func (g *KinesisS3) Close(_ context.Context) error {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_s3").NewStat("Close").AddTime(start)
	}()
	traceSpan := tracing.Start(context.Background(), "s3:Close")
	defer traceSpan.Finish()

	// return g.client.Close()
	return nil
}

func (g *KinesisS3) Set(ctx context.Context, key []byte, value []byte, opts ...blob.Options) error {
	var rec firehose.Record
	var recInput firehose.PutRecordInput

	rec.SetData(value)
	recInput.SetDeliveryStreamName("PUT-KinesisS3-uNJpd")
	recInput.SetRecord(&rec)

	_, err := g.firehoseClient.PutRecordWithContext(ctx, &recInput)

	if err != nil {
		return err
	}

	return nil
}

func (g *KinesisS3) Set_old(ctx context.Context, key []byte, value []byte, opts ...blob.Options) error {
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
	options := blob.NewSetOptions(opts...)
	if options.TTL > 0 {
		expires := time.Now().Add(options.TTL)
		uploadInput.Expires = &expires
	}

	_, err := g.uploader.Upload(uploadInput)
	if err != nil {
		traceSpan.RecordError(err)
		return fmt.Errorf("failed to set data: %w", err)
	}

	return nil
}

func (g *KinesisS3) SetTTL(ctx context.Context, key []byte, ttl time.Duration) error {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_s3").NewStat("SetTTL").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "s3:SetTTL")
	defer traceSpan.Finish()

	// TODO implement
	return nil
}

func (g *KinesisS3) Get(ctx context.Context, hash []byte) ([]byte, error) {
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

func (g *KinesisS3) Del(ctx context.Context, hash []byte) error {
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
