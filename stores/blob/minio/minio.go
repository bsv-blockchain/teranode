package minio

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/TAAL-GmbH/ubsv/stores/blob/options"
	"github.com/TAAL-GmbH/ubsv/tracing"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type Minio struct {
	client     *minio.Client
	bucketName string
	logger     utils.Logger
}

func New(minioURL *url.URL) (*Minio, error) {
	logger := gocore.Log("minio")

	useSSL := minioURL.Scheme == "minios"
	secretAccessKey, _ := minioURL.User.Password()
	client, err := minio.New(minioURL.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(minioURL.User.Username(), secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, err
	}

	bucketName := minioURL.Path[1:]
	location := "us-east-1"
	if minioURL.Query().Get("location") != "" {
		location = minioURL.Query().Get("location")
	}

	err = client.MakeBucket(context.Background(), bucketName, minio.MakeBucketOptions{
		Region: location,
	})
	if err != nil {
		exists, errBucketExists := client.BucketExists(context.Background(), bucketName)
		if errBucketExists == nil && exists {
			// log.Printf("We already own %s\n", bucketName)
		} else {
			return nil, fmt.Errorf("error creating bucket %s: %v", bucketName, err)
		}
	}

	return &Minio{
		client:     client,
		bucketName: bucketName,
		logger:     logger,
	}, nil
}

func (m *Minio) Close(ctx context.Context) error {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_minio").NewStat("Close").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "minio:Close")
	defer traceSpan.Finish()

	return nil //m.client.Close()
}

func (m *Minio) Set(ctx context.Context, hash []byte, value []byte, opts ...options.Options) error {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_minio").NewStat("Set").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "minio:Set")
	defer traceSpan.Finish()

	objectName := utils.ReverseAndHexEncodeSlice(hash)
	bufReader := bytes.NewReader(value)
	objectOptions := minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	}

	setOptions := options.NewSetOptions(opts...)
	if setOptions.TTL > 0 {
		objectOptions.RetainUntilDate = time.Now().Add(setOptions.TTL)
	}

	_, err := m.client.PutObject(ctx, m.bucketName, objectName, bufReader, int64(len(value)), objectOptions)
	if err != nil {
		traceSpan.RecordError(err)
		return fmt.Errorf("failed to set minio data: %w", err)
	}

	return nil
}

func (m *Minio) SetTTL(ctx context.Context, hash []byte, ttl time.Duration) error {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_minio").NewStat("SetTTL").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "minio:SetTTL")
	defer traceSpan.Finish()

	objectName := utils.ReverseAndHexEncodeSlice(hash)
	objectOptions := minio.PutObjectRetentionOptions{
		RetainUntilDate: nil,
	}

	if ttl > 0 {
		retention := time.Now().Add(ttl)
		objectOptions.RetainUntilDate = &retention
	}

	err := m.client.PutObjectRetention(ctx, m.bucketName, objectName, objectOptions)
	if err != nil {
		traceSpan.RecordError(err)
		return fmt.Errorf("failed to set minio retention options: %w", err)
	}

	return nil
}

func (m *Minio) Get(ctx context.Context, hash []byte) ([]byte, error) {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_minio").NewStat("Get").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "minio:Get")
	defer traceSpan.Finish()

	objectName := utils.ReverseAndHexEncodeSlice(hash)
	object, err := m.client.GetObject(ctx, m.bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		traceSpan.RecordError(err)
		return nil, fmt.Errorf("failed to get minio data: %w", err)
	}
	defer object.Close()

	var b []byte
	b, err = io.ReadAll(object)
	if err != nil && err != io.EOF {
		traceSpan.RecordError(err)
		return nil, fmt.Errorf("failed to read minio data: %w", err)
	}

	return b, err
}

func (m *Minio) Exists(ctx context.Context, hash []byte) (bool, error) {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_minio").NewStat("Exists").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "minio:Exists")
	defer traceSpan.Finish()

	objectName := utils.ReverseAndHexEncodeSlice(hash)
	_, err := m.client.StatObject(ctx, m.bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return false, nil
		}
		traceSpan.RecordError(err)
		return false, fmt.Errorf("failed to get minio data: %w", err)
	}

	return true, nil
}

func (m *Minio) Del(ctx context.Context, hash []byte) error {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_minio").NewStat("Del").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "minio:Del")
	defer traceSpan.Finish()

	objectName := utils.ReverseAndHexEncodeSlice(hash)
	err := m.client.RemoveObject(ctx, m.bucketName, objectName, minio.RemoveObjectOptions{
		GovernanceBypass: true,
	})
	if err != nil {
		traceSpan.RecordError(err)
		return fmt.Errorf("failed to del minio data: %w", err)
	}

	return nil
}
