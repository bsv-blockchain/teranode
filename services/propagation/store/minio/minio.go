package minio

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type Minio struct {
	client     *minio.Client
	bucketName string
	logger     utils.Logger
}

func New(minioURL *url.URL) (*Minio, error) {
	logLevel, _ := gocore.Config().Get("logLevel")
	logger := gocore.Log("minio", gocore.NewLogLevelFromString(logLevel))

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
	span, _ := opentracing.StartSpanFromContext(ctx, "minio:Close")
	defer span.Finish()

	return nil //m.client.Close()
}

func (m *Minio) Set(ctx context.Context, hash []byte, value []byte) error {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_minio").NewStat("Set").AddTime(start)
	}()
	span, _ := opentracing.StartSpanFromContext(ctx, "minio:Set")
	defer span.Finish()

	objectName := utils.ReverseAndHexEncodeSlice(hash)
	bufReader := bytes.NewReader(value)
	contentType := "application/octet-stream"
	_, err := m.client.PutObject(ctx, m.bucketName, objectName, bufReader, int64(len(value)), minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		span.SetTag(string(ext.Error), true)
		span.LogFields(log.Error(err))
		return fmt.Errorf("failed to set minio data: %w", err)
	}

	return nil
}

func (m *Minio) Get(ctx context.Context, hash []byte) ([]byte, error) {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_minio").NewStat("Get").AddTime(start)
	}()
	span, _ := opentracing.StartSpanFromContext(ctx, "minio:Get")
	defer span.Finish()

	objectName := utils.ReverseAndHexEncodeSlice(hash)
	object, err := m.client.GetObject(context.Background(), m.bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		span.SetTag(string(ext.Error), true)
		span.LogFields(log.Error(err))
		return nil, fmt.Errorf("failed to get minio data: %w", err)
	}
	defer object.Close()

	var b []byte
	b, err = io.ReadAll(object)
	if err != nil && err != io.EOF {
		span.SetTag(string(ext.Error), true)
		span.LogFields(log.Error(err))
		return nil, fmt.Errorf("failed to read minio data: %w", err)
	}

	return b, err
}

func (m *Minio) Del(ctx context.Context, hash []byte) error {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_minio").NewStat("Del").AddTime(start)
	}()
	span, _ := opentracing.StartSpanFromContext(ctx, "minio:Del")
	defer span.Finish()

	objectName := utils.ReverseAndHexEncodeSlice(hash)
	err := m.client.RemoveObject(ctx, m.bucketName, objectName, minio.RemoveObjectOptions{
		GovernanceBypass: true,
	})
	if err != nil {
		span.SetTag(string(ext.Error), true)
		span.LogFields(log.Error(err))
		return fmt.Errorf("failed to del minio data: %w", err)
	}

	return nil
}
