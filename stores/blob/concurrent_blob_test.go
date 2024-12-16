package blob

import (
	"context"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/blob/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestConcurrentBlob_GetBlobExists(t *testing.T) {
	ctx := context.Background()
	key := [32]byte{1, 2, 3}
	blobStore := memory.New()

	err := blobStore.Set(ctx, key[:], []byte("existing data"))
	require.NoError(t, err)

	cb := NewConcurrentBlob(blobStore)
	reader, err := cb.Get(ctx, key, func() (io.ReadCloser, error) {
		return nil, errors.NewStorageError("should not be called")
	})

	require.NoError(t, err)
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "existing data", string(data))
}

func TestConcurrentBlob_GetBlobNotExists(t *testing.T) {
	ctx := context.Background()
	key := [32]byte{1, 2, 3}
	blobStore := memory.New()

	cb := NewConcurrentBlob(blobStore)
	reader, err := cb.Get(ctx, key, func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("new data")), nil
	})

	require.NoError(t, err)
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "new data", string(data))

	reader, err = cb.Get(ctx, key, func() (io.ReadCloser, error) {
		return nil, errors.NewStorageError("should not be called")
	})

	require.NoError(t, err)
	data, err = io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "new data", string(data))
}

func TestConcurrentBlob_GetBlobReader_Multi(t *testing.T) {
	ctx := context.Background()
	key := [32]byte{1, 2, 3}
	blobStore := memory.New()

	accessCount := atomic.Uint32{}

	cb := NewConcurrentBlob(blobStore)

	wg := errgroup.Group{}

	for i := 0; i < 100; i++ {
		wg.Go(func() error {
			reader, err := cb.Get(ctx, key, func() (io.ReadCloser, error) {
				accessCount.Add(1)
				return io.NopCloser(strings.NewReader("blob data")), nil
			})
			if err != nil {
				return err
			}

			data, err := io.ReadAll(reader)
			if err != nil {
				return err
			}

			assert.Equal(t, "blob data", string(data))

			return nil
		})
	}

	err := wg.Wait()
	require.NoError(t, err)

	// make sure the blob was fetched only once
	assert.Equal(t, uint32(1), accessCount.Load())
}

func TestConcurrentBlob_GetBlobFetchError(t *testing.T) {
	ctx := context.Background()
	key := [32]byte{1, 2, 3}
	blobStore := memory.New()

	cb := NewConcurrentBlob(blobStore)
	_, err := cb.Get(ctx, key, func() (io.ReadCloser, error) {
		return nil, errors.NewStorageError("fetch error")
	})

	assert.Error(t, err)
}

func TestConcurrentBlob_GetBlobSetError(t *testing.T) {
	ctx := context.Background()
	key := [32]byte{1, 2, 3}
	blobStore := memory.New()

	cb := NewConcurrentBlob(blobStore)
	_, err := cb.Get(ctx, key, func() (io.ReadCloser, error) {
		return io.NopCloser(&errorReader{}), nil
	})

	assert.Error(t, err)
}

type errorReader struct{}

func (e *errorReader) Read(p []byte) (n int, err error) {
	return 0, errors.NewStorageError("read error")
}

func (e *errorReader) Close() error {
	return nil
}
