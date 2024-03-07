package blockassembly

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/libsv/go-bt/v2/chainhash"

	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/file"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ubsverrors"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type WrapperInterface interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Exists(ctx context.Context, key []byte) (bool, error)
	Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error
	SetTTL(ctx context.Context, key []byte, ttl time.Duration) error
	SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error
}

type Wrapper struct {
	logger                ulogger.Logger
	store                 blob.Store
	localTTLCache         blob.Store
	localTTL              time.Duration
	AssetClient           WrapperInterface
	blockValidationClient WrapperInterface
}

func NewRemoteTTLWrapper(logger ulogger.Logger, store blob.Store, AssetClient, blockValidationClient WrapperInterface) (*Wrapper, error) {
	w := &Wrapper{
		logger:                logger,
		store:                 store,
		AssetClient:           AssetClient,
		blockValidationClient: blockValidationClient,
	}

	localTTLCacheDir, ok := gocore.Config().Get("blockassembly_localTTLCache", "")
	if ok {
		localTTLCacheDirs := strings.Split(localTTLCacheDir, "|")
		for i, item := range localTTLCacheDirs {
			localTTLCacheDirs[i] = strings.TrimSpace(item)
		}

		var err error
		if w.localTTLCache, err = file.New(logger, localTTLCacheDir, localTTLCacheDirs); err != nil {
			return nil, fmt.Errorf("failed to create local ttl store: %w", err)
		}
		// TODO make configurable
		w.localTTL = 120 * time.Minute
	}

	return w, nil
}

func (r Wrapper) Health(ctx context.Context) (int, string, error) {
	return r.store.Health(ctx)
}

func (r Wrapper) Exists(ctx context.Context, key []byte, opts ...options.Options) (bool, error) {
	ok, err := r.store.Exists(ctx, key)
	if err != nil {
		r.logger.Errorf("failed to check if key %s exists in store: %v", utils.ReverseAndHexEncodeSlice(key), err)
	}

	okAsset, err := r.AssetClient.Exists(ctx, key)
	if err != nil {
		r.logger.Errorf("failed to check if key %s exists in asset client: %v", utils.ReverseAndHexEncodeSlice(key), err)
	}

	return ok && okAsset, nil
}

func (r Wrapper) GetIoReader(ctx context.Context, key []byte, opts ...options.Options) (io.ReadCloser, error) {
	// first get from local ttl cache
	if r.localTTLCache != nil {
		subtreeReader, err := r.localTTLCache.GetIoReader(ctx, key)
		if err != nil {
			if !errors.Is(err, ubsverrors.ErrNotFound) {
				r.logger.Warnf("using local ttl cache in block assembly for subtree %x error: %v", utils.ReverseAndHexEncodeSlice(key), err)
			}
		}

		if err == nil && subtreeReader != nil {
			prometheusBlockAssemblyLocalTTLCacheHit.Inc()
			return subtreeReader, nil
		}
	}

	// try block validation service
	subtreeBytes, err := r.blockValidationClient.Get(ctx, key)
	if err != nil {
		r.logger.Warnf("using block validation service in block assembly for subtree %x error: %v", key, err)
	}
	if subtreeBytes != nil {
		prometheusBlockAssemblyLocalTTLCacheMiss.Inc()
		return io.NopCloser(bytes.NewReader(subtreeBytes)), nil
	}

	// try asset client
	r.logger.Warnf("using block validation service in block assembly for subtree %x failed", key)
	subtreeBytes, err = r.AssetClient.Get(ctx, key)
	if err != nil {
		r.logger.Errorf("using asset client in block assembly for subtree %x error: %v", key, err)
	}
	if subtreeBytes != nil {
		prometheusBlockAssemblyLocalTTLCacheMiss.Inc()
		return io.NopCloser(bytes.NewReader(subtreeBytes)), nil
	}

	// try store
	r.logger.Warnf("using asset client in block assembly for subtree %x failed", key)
	subtreeBytes, err = r.store.Get(ctx, key)

	// set in local ttl cache
	if subtreeBytes != nil && r.localTTLCache != nil {
		prometheusBlockAssemblyLocalTTLCacheMiss.Inc()
		if err = r.localTTLCache.Set(ctx, key, subtreeBytes, options.WithTTL(r.localTTL)); err != nil {
			r.logger.Warnf("using local ttl cache in block assembly for subtree %x error: %v", utils.ReverseAndHexEncodeSlice(key), err)
		}
	}

	return io.NopCloser(bytes.NewReader(subtreeBytes)), err
}

func (r Wrapper) GetHead(ctx context.Context, key []byte, nrOfBytes int, opts ...options.Options) ([]byte, error) {
	b, err := r.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	if nrOfBytes > len(b) {
		return b, nil
	}

	return b[:nrOfBytes], nil
}

func (r Wrapper) Get(ctx context.Context, key []byte, opts ...options.Options) ([]byte, error) {
	// first get from local ttl cache
	if r.localTTLCache != nil {
		subtreeBytes, err := r.localTTLCache.Get(ctx, key)
		if err != nil {
			if !errors.Is(err, ubsverrors.ErrNotFound) {
				r.logger.Warnf("using local ttl cache in block assembly for subtree %x error: %v", utils.ReverseAndHexEncodeSlice(key), err)
			}
		}

		if subtreeBytes != nil {
			prometheusBlockAssemblyLocalTTLCacheHit.Inc()
			return subtreeBytes, nil
		}
	}

	subtreeBytes, err := r.blockValidationClient.Get(ctx, key)
	if err != nil {
		r.logger.Warnf("using block validation service in block assembly for subtree %x error: %v", key, err)
	}

	if subtreeBytes == nil {
		r.logger.Warnf("using block validation service in block assembly for subtree %x failed", key)
		subtreeBytes, err = r.AssetClient.Get(ctx, key)
		if err != nil {
			r.logger.Errorf("using asset client in block assembly for subtree %x error: %v", key, err)
		}

		if subtreeBytes == nil {
			r.logger.Warnf("using asset client in block assembly for subtree %x failed", key)
			subtreeBytes, err = r.store.Get(ctx, key)
		}
	}

	// set in local ttl cache
	if subtreeBytes != nil && r.localTTLCache != nil {
		prometheusBlockAssemblyLocalTTLCacheMiss.Inc()
		if err = r.localTTLCache.Set(ctx, key, subtreeBytes, options.WithTTL(r.localTTL)); err != nil {
			r.logger.Warnf("using local ttl cache in block assembly for subtree %x error: %v", utils.ReverseAndHexEncodeSlice(key), err)
		}
	}

	return subtreeBytes, err
}

func (r Wrapper) SetFromReader(ctx context.Context, key []byte, value io.ReadCloser, opts ...options.Options) error {
	b, err := io.ReadAll(value)
	if err != nil {
		return fmt.Errorf("failed to read data from reader: %w", err)
	}

	return r.Set(ctx, key, b, opts...)
}

func (r Wrapper) Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error {
	// first set in local ttl cache
	if r.localTTLCache != nil {
		if err := r.localTTLCache.Set(ctx, key, value, options.WithTTL(r.localTTL)); err != nil {
			r.logger.Warnf("using local ttl cache in block assembly for subtree %x error: %v", utils.ReverseAndHexEncodeSlice(key), err)
		}
	}

	return r.AssetClient.Set(ctx, key, value, opts...)
}

func (r Wrapper) SetTTL(ctx context.Context, key []byte, ttl time.Duration, opts ...options.Options) error {
	return r.AssetClient.SetTTL(ctx, key, ttl)
}

func (r Wrapper) Del(_ context.Context, key []byte, opts ...options.Options) error {
	return fmt.Errorf("not implemented")
}

func (r Wrapper) Close(ctx context.Context) error {
	return r.store.Close(ctx)
}
