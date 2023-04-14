package gcs

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/api/option"
)

type GCS struct {
	client *storage.Client
	bucket *storage.BucketHandle
	logger utils.Logger
}

func New(bucketName string) (*GCS, error) {
	logLevel, _ := gocore.Config().Get("logLevel")
	logger := gocore.Log("bdgr", gocore.NewLogLevelFromString(logLevel))

	client, err := storage.NewClient(context.Background(), option.WithCredentialsFile("./keyfile.json"))
	if err != nil {
		return nil, err
	}
	bucket := client.Bucket(bucketName) // "ubsv-utxo-store")

	gcs := &GCS{
		client: client,
		bucket: bucket,
		logger: logger,
	}

	return gcs, nil
}

func (g *GCS) Close(ctx context.Context) error {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_gcs").NewStat("Close").AddTime(start)
	}()
	span, _ := opentracing.StartSpanFromContext(ctx, "gcs:Close")
	defer span.Finish()

	return g.client.Close()
}

func (g *GCS) Set(ctx context.Context, key []byte, value []byte) error {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_gcs").NewStat("Set").AddTime(start)
	}()
	span, _ := opentracing.StartSpanFromContext(ctx, "gcs:Set")
	defer span.Finish()

	// upload the tx data to gcs bucket
	wc := g.bucket.Object(utils.ReverseAndHexEncodeSlice(key)).NewWriter(ctx)
	if _, err := wc.Write(value); err != nil {
		span.SetTag(string(ext.Error), true)
		span.LogFields(log.Error(err))
		return fmt.Errorf("failed to set data: %w", err)
	}
	if err := wc.Close(); err != nil {
		span.SetTag(string(ext.Error), true)
		span.LogFields(log.Error(err))
		return fmt.Errorf("failed to set data: %w", err)
	}

	return nil
}

func (g *GCS) Get(ctx context.Context, hash []byte) ([]byte, error) {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_gcs").NewStat("Get").AddTime(start)
	}()
	span, _ := opentracing.StartSpanFromContext(ctx, "gcs:Get")
	defer span.Finish()

	// download the tx data from gcs bucket
	rc, err := g.bucket.Object(utils.ReverseAndHexEncodeSlice(hash)).NewReader(ctx)
	if err != nil {
		span.SetTag(string(ext.Error), true)
		span.LogFields(log.Error(err))
		return nil, fmt.Errorf("failed to get data: %w", err)
	}
	defer rc.Close()

	result, err := io.ReadAll(rc)
	if err != nil {
		span.SetTag(string(ext.Error), true)
		span.LogFields(log.Error(err))
		return nil, fmt.Errorf("failed to get data: %w", err)
	}

	return result, err
}

func (g *GCS) Del(ctx context.Context, hash []byte) error {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_gcs").NewStat("Del").AddTime(start)
	}()
	span, _ := opentracing.StartSpanFromContext(ctx, "gcs:Del")
	defer span.Finish()

	// delete the tx data from gcs bucket
	if err := g.bucket.Object(utils.ReverseAndHexEncodeSlice(hash)).Delete(ctx); err != nil {
		span.SetTag(string(ext.Error), true)
		span.LogFields(log.Error(err))
		return fmt.Errorf("failed to del data: %w", err)
	}

	return nil
}
