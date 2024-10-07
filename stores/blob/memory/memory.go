package memory

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/libsv/go-bt/v2/chainhash"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
)

type Memory struct {
	mu         sync.RWMutex
	blobs      map[[32]byte][]byte
	ttls       map[[32]byte]time.Time
	options    *options.Options
	Counters   map[string]int
	countersMu sync.Mutex
}

func New(opts ...options.StoreOption) *Memory {
	return &Memory{
		blobs:    make(map[[32]byte][]byte),
		ttls:     make(map[[32]byte]time.Time),
		options:  options.NewStoreOptions(opts...),
		Counters: make(map[string]int),
	}
}

func (m *Memory) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	m.countersMu.Lock()
	m.Counters["health"]++
	m.countersMu.Unlock()

	return 0, "Memory Store", nil
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

	m.blobs[storeKey] = value

	if merged.TTL != nil && *merged.TTL > 0 {
		m.ttls[storeKey] = time.Now().Add(*merged.TTL)
	}

	return nil
}

func (m *Memory) SetTTL(_ context.Context, hash []byte, ttl time.Duration, opts ...options.FileOption) error {
	merged := options.MergeOptions(m.options, opts)

	storeKey := hashKey(hash, merged)

	if merged.TTL != nil {
		if *merged.TTL > 0 {
			m.ttls[storeKey] = time.Now().Add(*merged.TTL)
		} else {
			delete(m.ttls, storeKey)
		}
	}

	return nil
}

func (m *Memory) GetTTL(_ context.Context, hash []byte, opts ...options.FileOption) (time.Duration, error) {
	merged := options.MergeOptions(m.options, opts)

	storeKey := hashKey(hash, merged)

	if _, ok := m.blobs[storeKey]; !ok {
		return 0, errors.ErrNotFound
	}

	ttl, ok := m.ttls[storeKey]
	if !ok {
		return 0, nil
	}

	return time.Until(ttl), nil
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
	b, err := m.Get(context.Background(), hash)
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

	return nil
}

func hashKey(key []byte, options *options.Options) [32]byte {
	var storeKey []byte

	if len(options.Filename) > 0 {
		storeKey = []byte(options.Filename)
	} else {
		storeKey = key
	}

	if len(options.Extension) > 0 {
		storeKey = append(storeKey, []byte(options.Extension)...)
	}

	return chainhash.HashH(storeKey)
}
