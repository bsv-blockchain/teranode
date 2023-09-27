package seaweedfs

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/linxGnu/goseaweedfs"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type SeaweedFS struct {
	client     *goseaweedfs.Seaweed
	collection string
	logger     utils.Logger
}

func New(seaweedFsURL *url.URL) (*SeaweedFS, error) {
	logger := gocore.Log("seaweed")

	scheme := "http"
	if seaweedFsURL.Query().Get("scheme") != "" {
		scheme = seaweedFsURL.Query().Get("scheme")
	}

	serverURL := url.URL{
		Scheme: scheme,
		Host:   seaweedFsURL.Host,
	}

	filers := strings.Split(seaweedFsURL.Query().Get("filers"), ",")
	if len(filers) == 0 {
		return nil, fmt.Errorf("no filers specified")
	}

	chunkSize := 512
	if seaweedFsURL.Query().Get("chunkSize") != "" {
		configChunkSize, err := strconv.Atoi(seaweedFsURL.Query().Get("chunkSize"))
		if err != nil {
			return nil, fmt.Errorf("invalid chunkSize: %w", err)
		}
		chunkSize = configChunkSize
	}

	httpTimeout := 5 * time.Second
	if seaweedFsURL.Query().Get("httpTimeout") != "" {
		configHttpTimeout, err := strconv.Atoi(seaweedFsURL.Query().Get("httpTimeout"))
		if err != nil {
			return nil, fmt.Errorf("invalid httpTimeout: %w", err)
		}
		httpTimeout = time.Duration(configHttpTimeout) * time.Second
	}

	client, err := goseaweedfs.NewSeaweed(serverURL.String(), filers, int64(chunkSize), &http.Client{Timeout: httpTimeout})
	if err != nil {
		return nil, fmt.Errorf("failed to create seaweedfs client: %w", err)
	}

	s := &SeaweedFS{
		client:     client,
		collection: strings.Replace(seaweedFsURL.Path, "/", "", 1),
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
	return s.client.Close()
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

	// Expires
	o := options.NewSetOptions(opts...)
	ttl := ""
	if o.TTL > 0 {
		ttl = fmt.Sprintf("%dm", int(math.Round(o.TTL.Minutes())))
	}

	objectKey := s.generateKey(key)

	var err error
	filers := s.client.Filers()
	for _, filer := range filers {
		_, err = filer.Upload(bytes.NewReader(value), int64(len(value)), objectKey, s.collection, ttl)
		if err == nil {
			break
		}
	}
	if err != nil {
		traceSpan.RecordError(err)
		return fmt.Errorf("failed to set seaweedfs data: %w", err)
	}

	return nil
}

func (s *SeaweedFS) SetTTL(ctx context.Context, key []byte, ttl time.Duration) error {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_seaweedfs").NewStat("SetTTL").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "seaweedfs:SetTTL")
	defer traceSpan.Finish()

	// TODO

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

	var data []byte
	var err error
	filers := s.client.Filers()
	for _, filer := range filers {
		data, _, err = filer.Get(objectKey, nil, nil)
		if err == nil {
			break
		}
	}

	if err != nil {
		traceSpan.RecordError(err)
		return nil, fmt.Errorf("failed to get seaweedfs data: %w", err)
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

	var err error
	filers := s.client.Filers()
	for _, filer := range filers {
		_, _, err = filer.Get(objectKey, nil, nil)
		if err == nil {
			break
		}
	}

	if err != nil {
		traceSpan.RecordError(err)
		return false, fmt.Errorf("failed to get seaweedfs data: %w", err)
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

	var err error
	filers := s.client.Filers()
	for _, filer := range filers {
		err = filer.Delete(objectKey, nil)
		if err == nil {
			break
		}
	}

	if err != nil {
		traceSpan.RecordError(err)
		return fmt.Errorf("failed to delete seaweedfs data: %w", err)
	}

	return nil
}
