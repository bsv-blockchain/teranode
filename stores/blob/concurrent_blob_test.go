// Package blob provides blob storage functionality with various storage backend implementations.
package blob

import (
	"context"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestConcurrentBlob_GetBlobExists(t *testing.T) {
	ctx := context.Background()
	key := [32]byte{1, 2, 3}
	blobStore := memory.New()

	err := blobStore.Set(ctx, key[:], fileformat.FileTypeTesting, []byte("existing data"))
	require.NoError(t, err)

	cb := NewConcurrentBlob(blobStore)
	reader, err := cb.Get(ctx, key, fileformat.FileTypeTesting, func() (io.ReadCloser, error) {
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
	reader, err := cb.Get(ctx, key, fileformat.FileTypeTesting, func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("new data")), nil
	})

	require.NoError(t, err)
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "new data", string(data))

	reader, err = cb.Get(ctx, key, fileformat.FileTypeTesting, func() (io.ReadCloser, error) {
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
			reader, err := cb.Get(ctx, key, fileformat.FileTypeTesting, func() (io.ReadCloser, error) {
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
	_, err := cb.Get(ctx, key, fileformat.FileTypeTesting, func() (io.ReadCloser, error) {
		return nil, errors.NewStorageError("fetch error")
	})

	assert.Error(t, err)
}

func TestConcurrentBlob_GetBlobSetError(t *testing.T) {
	ctx := context.Background()
	key := [32]byte{1, 2, 3}
	blobStore := memory.New()

	cb := NewConcurrentBlob(blobStore)
	_, err := cb.Get(ctx, key, fileformat.FileTypeTesting, func() (io.ReadCloser, error) {
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

// closingReader counts how many times Close was called and returns a
// configurable error from Read. Used to assert the close contract.
type closingReader struct {
	readErr    error
	closeCount int
}

func (r *closingReader) Read([]byte) (int, error) {
	if r.readErr != nil {
		return 0, r.readErr
	}
	return 0, io.EOF
}

func (r *closingReader) Close() error { r.closeCount++; return nil }

// nonClosingSetStore wraps an underlying blob.Store but its SetFromReader
// does NOT close the input reader - matching the file-store contract
// (memory closes via defer, file does not). This exposes whether the
// CALLER (ConcurrentBlob.Get in our case) closes the reader itself, which
// is what determines whether ConcurrentBlob leaks under a file store.
type nonClosingSetStore struct {
	inner   Store
	failSet bool
}

func (s *nonClosingSetStore) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	if s.failSet {
		return errors.NewStorageError("simulated SetFromReader failure")
	}
	// Mimic file-store behaviour: drain the reader but do NOT close it.
	_, _ = io.Copy(io.Discard, reader)
	return s.inner.SetFromReader(ctx, key, fileType, io.NopCloser(strings.NewReader("dummy")), opts...)
}

func (s *nonClosingSetStore) Health(ctx context.Context, b bool) (int, string, error) {
	return s.inner.Health(ctx, b)
}

func (s *nonClosingSetStore) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
	return s.inner.Exists(ctx, key, fileType, opts...)
}

func (s *nonClosingSetStore) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	return s.inner.Get(ctx, key, fileType, opts...)
}

func (s *nonClosingSetStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	return s.inner.GetIoReader(ctx, key, fileType, opts...)
}

func (s *nonClosingSetStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	return s.inner.Set(ctx, key, fileType, value, opts...)
}

func (s *nonClosingSetStore) SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, newDAH uint32, opts ...options.FileOption) error {
	return s.inner.SetDAH(ctx, key, fileType, newDAH, opts...)
}

func (s *nonClosingSetStore) Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
	return s.inner.Del(ctx, key, fileType, opts...)
}

func (s *nonClosingSetStore) Close(ctx context.Context) error { return s.inner.Close(ctx) }
func (s *nonClosingSetStore) SetCurrentBlockHeight(h uint32)  { s.inner.SetCurrentBlockHeight(h) }

// TestConcurrentBlob_GetBlobClosesReaderOnSetError pins that the source
// io.ReadCloser returned by the user's getBlobReader callback is Closed
// even when SetFromReader fails - and that ConcurrentBlob does the close
// itself rather than relying on the store. The Store.SetFromReader
// contract on whether the input reader is closed differs between
// implementations (memory closes via defer, file does not), so a test
// that uses memory cannot catch the leak. nonClosingSetStore models the
// file-store contract.
func TestConcurrentBlob_GetBlobClosesReaderOnSetError(t *testing.T) {
	ctx := context.Background()
	key := [32]byte{1, 2, 3}
	store := &nonClosingSetStore{inner: memory.New(), failSet: true}

	reader := &closingReader{} // any reader; we just track Close calls
	cb := NewConcurrentBlob(store)
	_, err := cb.Get(ctx, key, fileformat.FileTypeTesting, func() (io.ReadCloser, error) {
		return reader, nil
	})

	assert.Error(t, err)
	assert.GreaterOrEqual(t, reader.closeCount, 1,
		"ConcurrentBlob.Get must Close the source reader itself when the underlying store's SetFromReader fails - otherwise the reader (HTTP body, file-store permit, etc.) leaks against any store that doesn't close its input")
}

// TestConcurrentBlob_GetBlobClosesReaderOnSuccess pins the same close
// contract on the success path: ConcurrentBlob.Get must Close the source
// reader itself, not rely on the store's SetFromReader to do it.
func TestConcurrentBlob_GetBlobClosesReaderOnSuccess(t *testing.T) {
	ctx := context.Background()
	key := [32]byte{4, 5, 6}
	store := &nonClosingSetStore{inner: memory.New()}

	reader := &closingReader{}
	cb := NewConcurrentBlob(store)
	r, err := cb.Get(ctx, key, fileformat.FileTypeTesting, func() (io.ReadCloser, error) {
		return reader, nil
	})
	require.NoError(t, err)
	_ = r.Close()

	assert.GreaterOrEqual(t, reader.closeCount, 1, "source reader must be Closed on the success path too")
}
