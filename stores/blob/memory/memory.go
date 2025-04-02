package memory

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
)

type Memory struct {
	mu         sync.RWMutex
	headers    map[string][]byte
	footers    map[string][]byte
	keys       map[string][]byte
	blobs      map[string][]byte
	blobTimes  map[string]time.Time
	ttls       map[string]time.Duration
	options    *options.Options
	Counters   map[string]int
	countersMu sync.Mutex
}

func New(opts ...options.StoreOption) *Memory {
	m := &Memory{
		keys:      make(map[string][]byte),
		blobs:     make(map[string][]byte),
		blobTimes: make(map[string]time.Time),
		ttls:      make(map[string]time.Duration),
		headers:   make(map[string][]byte),
		footers:   make(map[string][]byte),
		options:   options.NewStoreOptions(opts...),
		Counters:  make(map[string]int),
	}

	go m.ttlCleaner(context.Background(), 1*time.Minute)

	return m
}

func (m *Memory) ttlCleaner(ctx context.Context, interval time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			cleanExpiredFiles(m)
		}
	}
}

func cleanExpiredFiles(m *Memory) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key, expiryTime := range m.blobTimes {
		ttl, ok := m.ttls[key]
		if !ok {
			continue
		}

		if time.Now().After(expiryTime.Add(ttl)) {
			delete(m.blobs, key)
			delete(m.blobTimes, key)
			delete(m.ttls, key)
			delete(m.headers, key)
			delete(m.footers, key)
		}
	}
}

func (m *Memory) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	m.countersMu.Lock()
	m.Counters["health"]++
	m.countersMu.Unlock()

	return http.StatusOK, "Memory Store", nil
}

func (m *Memory) Close(_ context.Context) error {
	// The lock is to ensure that no other operations are happening while we close the store
	m.mu.Lock()
	defer m.mu.Unlock()

	m.countersMu.Lock()
	m.Counters["close"]++
	m.countersMu.Unlock()

	// noop
	return nil
}

func (m *Memory) SetFromReader(ctx context.Context, key []byte, reader io.ReadCloser, opts ...options.FileOption) error {
	defer reader.Close()

	merged := options.MergeOptions(m.options, opts)

	if !merged.AllowOverwrite {
		// for consistency with other stores, check if the blob already exists and throw BlobAlreadyExistsError if it does
		if exists, err := m.Exists(ctx, key); err != nil {
			return err
		} else if exists {
			return errors.NewBlobAlreadyExistsError("blob already exists")
		}
	}

	b, err := io.ReadAll(reader)
	if err != nil {
		return errors.NewStorageError("failed to read data from reader", err)
	}

	return m.Set(ctx, key, b, opts...)
}

func (m *Memory) Set(ctx context.Context, hash []byte, value []byte, opts ...options.FileOption) error {
	merged := options.MergeOptions(m.options, opts)

	storeKey := hashKey(hash, merged)

	if !merged.AllowOverwrite {
		// for consistency with other stores, check if the blob already exists and throw BlobAlreadyExistsError if it does
		if exists, err := m.Exists(ctx, hash); err != nil {
			return err
		} else if exists {
			return errors.NewBlobAlreadyExistsError("blob already exists")
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.countersMu.Lock()
	m.Counters["set"]++
	m.countersMu.Unlock()

	m.keys[string(hash)] = []byte{}
	m.blobs[storeKey] = value
	m.blobTimes[storeKey] = time.Now()

	if merged.Header != nil {
		m.headers[storeKey] = merged.Header
	}

	if merged.Footer != nil {
		footer, err := merged.Footer.GetFooter()
		if err != nil {
			return err
		}

		m.footers[storeKey] = footer
	}

	if merged.TTL != nil && *merged.TTL > 0 {
		m.ttls[storeKey] = *merged.TTL
	}

	return nil
}

func (m *Memory) SetTTL(_ context.Context, hash []byte, newTTL time.Duration, opts ...options.FileOption) error {
	merged := options.MergeOptions(m.options, opts)

	storeKey := hashKey(hash, merged)

	m.mu.Lock()
	defer m.mu.Unlock()

	if newTTL > 0 {
		m.blobTimes[storeKey] = time.Now()
		m.ttls[storeKey] = newTTL
	} else {
		delete(m.ttls, storeKey)
	}

	return nil
}

func (m *Memory) GetTTL(_ context.Context, hash []byte, opts ...options.FileOption) (time.Duration, error) {
	merged := options.MergeOptions(m.options, opts)

	storeKey := hashKey(hash, merged)

	m.mu.RLock()
	defer m.mu.RUnlock()

	if _, ok := m.blobs[storeKey]; !ok {
		return 0, errors.ErrNotFound
	}

	ttl, ok := m.ttls[storeKey]
	if !ok {
		return 0, nil
	}

	return ttl, nil
}

func (m *Memory) GetIoReader(ctx context.Context, key []byte, opts ...options.FileOption) (io.ReadCloser, error) {
	setOptions := options.NewFileOptions(opts...)

	storeKey := key
	if setOptions.Extension != "" {
		storeKey = append(storeKey, []byte(setOptions.Extension)...)
	}

	b, err := m.Get(ctx, storeKey)
	if err != nil {
		return nil, err
	}

	return io.NopCloser(bytes.NewBuffer(b)), nil
}

func (m *Memory) Get(_ context.Context, hash []byte, opts ...options.FileOption) ([]byte, error) {
	merged := options.MergeOptions(m.options, opts)

	storeKey := hashKey(hash, merged)

	m.mu.RLock()
	defer m.mu.RUnlock()

	m.countersMu.Lock()
	m.Counters["get"]++
	m.countersMu.Unlock()

	bytes, ok := m.blobs[storeKey]
	if !ok {
		return nil, errors.ErrNotFound
	}

	return bytes, nil
}

func (m *Memory) GetHead(_ context.Context, hash []byte, nrOfBytes int, opts ...options.FileOption) ([]byte, error) {
	b, err := m.Get(context.Background(), hash, opts...)
	if err != nil {
		return nil, err
	}

	if nrOfBytes > len(b) {
		return b, nil
	}

	m.countersMu.Lock()
	m.Counters["getHead"]++
	m.countersMu.Unlock()

	return b[:nrOfBytes], nil
}

func (m *Memory) Exists(_ context.Context, hash []byte, opts ...options.FileOption) (bool, error) {
	merged := options.MergeOptions(m.options, opts)

	storeKey := hashKey(hash, merged)

	m.mu.RLock()
	defer m.mu.RUnlock()

	m.countersMu.Lock()
	m.Counters["exists"]++
	m.countersMu.Unlock()

	_, ok := m.blobs[storeKey]

	return ok, nil
}

func (m *Memory) Del(_ context.Context, hash []byte, opts ...options.FileOption) error {
	merged := options.MergeOptions(m.options, opts)

	storeKey := hashKey(hash, merged)

	m.mu.Lock()
	defer m.mu.Unlock()

	m.countersMu.Lock()
	m.Counters["del"]++
	m.countersMu.Unlock()

	delete(m.blobs, storeKey)
	delete(m.keys, string(hash))

	return nil
}

func hashKey(key []byte, options *options.Options) string {
	var storeKey []byte

	if len(options.Filename) > 0 {
		storeKey = []byte(options.Filename)
	} else {
		storeKey = key
	}

	if len(options.Extension) > 0 {
		storeKey = append(storeKey, []byte(options.Extension)...)
	}

	return string(storeKey)
}

func (m *Memory) GetHeader(ctx context.Context, hash []byte, opts ...options.FileOption) ([]byte, error) {
	merged := options.MergeOptions(m.options, opts)

	storeKey := hashKey(hash, merged)

	return m.headers[storeKey], nil
}

func (m *Memory) GetFooterMetaData(ctx context.Context, hash []byte, opts ...options.FileOption) ([]byte, error) {
	merged := options.MergeOptions(m.options, opts)

	if merged.Footer == nil {
		return nil, nil
	}

	storeKey := hashKey(hash, merged)

	return merged.Footer.GetFooterMetaData(m.footers[storeKey]), nil
}

func (m *Memory) ListKeys() [][]byte {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys := make([][]byte, 0, len(m.keys))
	for k := range m.keys {
		keys = append(keys, []byte(k))
	}

	return keys
}
