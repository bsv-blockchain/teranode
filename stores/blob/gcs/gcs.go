package gcs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"cloud.google.com/go/storage"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/tracing"
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
	logger := gocore.Log("gcs")

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

func (g *GCS) Health(ctx context.Context) (int, string, error) {
	_, err := g.Exists(ctx, []byte("Health"))
	if err != nil {
		return -1, "GCS Store", err
	}

	return 0, "GCS Store", nil
}

func (g *GCS) Close(_ context.Context) error {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_gcs", true).NewStat("Close").AddTime(start)
	}()
	traceSpan := tracing.Start(context.Background(), "gcs:Close")
	defer traceSpan.Finish()

	return g.client.Close()
}

func (g *GCS) SetFromReader(ctx context.Context, key []byte, reader io.ReadCloser, opts ...options.Options) error {
	defer reader.Close()

	b, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read data from reader: %w", err)
	}

	return g.Set(ctx, key, b, opts...)
}

func (g *GCS) Set(ctx context.Context, key []byte, value []byte, _ ...options.Options) error {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_gcs", true).NewStat("Set").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "gcs:Set")
	defer traceSpan.Finish()

	// upload the tx data to gcs bucket
	wc := g.bucket.Object(utils.ReverseAndHexEncodeSlice(key)).NewWriter(ctx)
	if _, err := wc.Write(value); err != nil {
		traceSpan.RecordError(err)
		return fmt.Errorf("failed to set data: %w", err)
	}

	// TODO handle options
	// TTL on object is not supported in GCS

	if err := wc.Close(); err != nil {
		traceSpan.RecordError(err)
		return fmt.Errorf("failed to set data: %w", err)
	}

	return nil
}

func (g *GCS) SetTTL(ctx context.Context, key []byte, ttl time.Duration) error {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_gcs", true).NewStat("SetTTL").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "gcs:SetTTL")
	defer traceSpan.Finish()

	// TODO implement
	return nil
}

func (g *GCS) GetIoReader(ctx context.Context, key []byte) (io.ReadCloser, error) {
	b, err := g.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	return options.ReaderWrapper{Reader: bytes.NewBuffer(b), Closer: options.ReaderCloser{}}, nil
}

func (g *GCS) Get(ctx context.Context, hash []byte) ([]byte, error) {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_gcs", true).NewStat("Get").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "gcs:Get")
	defer traceSpan.Finish()

	// download the tx data from gcs bucket
	rc, err := g.bucket.Object(utils.ReverseAndHexEncodeSlice(hash)).NewReader(ctx)
	if err != nil {
		traceSpan.RecordError(err)
		return nil, fmt.Errorf("failed to get data: %w", err)
	}
	defer rc.Close()

	result, err := io.ReadAll(rc)
	if err != nil {
		traceSpan.RecordError(err)
		return nil, fmt.Errorf("failed to get data: %w", err)
	}

	return result, err
}

func (g *GCS) Exists(ctx context.Context, hash []byte) (bool, error) {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_gcs", true).NewStat("Exists").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "gcs:Exists")
	defer traceSpan.Finish()

	rc, err := g.bucket.Object(utils.ReverseAndHexEncodeSlice(hash)).NewReader(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return false, nil
		}

		traceSpan.RecordError(err)
		return false, fmt.Errorf("failed to get data: %w", err)
	}
	defer rc.Close()

	return true, nil
}

func (g *GCS) Del(ctx context.Context, hash []byte) error {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_gcs", true).NewStat("Del").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "gcs:Del")
	defer traceSpan.Finish()

	// delete the tx data from gcs bucket
	if err := g.bucket.Object(utils.ReverseAndHexEncodeSlice(hash)).Delete(ctx); err != nil {
		traceSpan.RecordError(err)
		return fmt.Errorf("failed to del data: %w", err)
	}

	return nil
}
