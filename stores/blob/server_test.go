// Package blob provides blob storage functionality with various storage backend implementations.
package blob

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/http"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerOperations(t *testing.T) {
	// Create a temporary directory for the file store
	tempDir, err := os.MkdirTemp("", "test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a logger
	logger := ulogger.New("blob-server-test")

	// Add a unique parameter to ensure a new DAH cleaner is started for this test
	serverStoreURL, err := url.Parse(fmt.Sprintf("file://%s?testId=%d", tempDir, time.Now().UnixNano()))
	require.NoError(t, err)

	blobServer, err := NewHTTPBlobServer(
		logger,
		serverStoreURL,
		options.WithDefaultSubDirectory("sub"),
	)
	require.NoError(t, err)

	serverAddr := "localhost:7979"
	go func() {
		err := blobServer.Start(context.Background(), serverAddr)
		if err != nil {
			t.Logf("Server stopped: %v", err)
		}
	}()

	// Wait for the server to start
	time.Sleep(100 * time.Millisecond)

	clientStoreURL, err := url.Parse("http://localhost:7979")
	require.NoError(t, err)

	client, err := http.New(logger, clientStoreURL)
	require.NoError(t, err)

	t.Run("SetAndGet", func(t *testing.T) {
		key := []byte("testKey1")
		value := []byte("testValue1")

		err := client.Set(context.Background(), key, fileformat.FileTypeTesting, value)
		require.NoError(t, err)

		retrievedValue, err := client.Get(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)

		assert.Equal(t, value, retrievedValue)

		err = client.Del(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
	})

	// SetDAH test removed - automatic DAH cleanup now handled by pruner service, not file store

	t.Run("Exists", func(t *testing.T) {
		key := []byte("testKey3")
		value := []byte("testValue3")

		err := client.Set(context.Background(), key, fileformat.FileTypeTesting, value)
		require.NoError(t, err)

		exists, err := client.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		assert.True(t, exists)

		err = client.Del(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)

		exists, err = client.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("SetFromReader", func(t *testing.T) {
		key := []byte("testKey4")

		largeData := make([]byte, 10*1024*1024) // 10 MB of data
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		reader := bytes.NewReader(largeData)

		err := client.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, io.NopCloser(reader))
		require.NoError(t, err)

		// Retrieve the data
		retrievedReader, err := client.GetIoReader(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		defer retrievedReader.Close()

		retrievedData, err := io.ReadAll(retrievedReader)
		require.NoError(t, err)

		assert.Equal(t, largeData, retrievedData)

		// Clean up
		err = client.Del(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
	})

	t.Run("WithFilename", func(t *testing.T) {
		key := []byte("testKey5")
		value := []byte("testValue5")

		err := client.Set(context.Background(), key, fileformat.FileTypeTesting, value, options.WithFilename("testFilename"))
		require.NoError(t, err)

		exists, err := client.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		assert.False(t, exists)

		exists, err = client.Exists(context.Background(), key, fileformat.FileTypeTesting, options.WithFilename("testFilename"))
		require.NoError(t, err)
		assert.True(t, exists)

		err = client.Del(context.Background(), key, fileformat.FileTypeTesting, options.WithFilename("testFilename"))
		require.NoError(t, err)
	})

	t.Run("WithExtension", func(t *testing.T) {
		key := []byte("testKey5")
		value := []byte("testValue5")

		err := client.Set(context.Background(), key, fileformat.FileTypeTesting, value)
		require.NoError(t, err)

		exists, err := client.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		assert.True(t, exists)

		err = client.Del(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
	})
}

// countingReadSeekCloser records Close calls and exposes a configurable
// Read/Seek so handleRangeRequest's different error paths can be exercised
// in isolation. It backs onto a bytes.Reader for the happy path.
type countingReadSeekCloser struct {
	src        *bytes.Reader
	readErr    error
	seekErr    error
	closeCount int
}

func newCountingReadSeekCloser(data []byte) *countingReadSeekCloser {
	return &countingReadSeekCloser{src: bytes.NewReader(data)}
}

func (c *countingReadSeekCloser) Read(p []byte) (int, error) {
	if c.readErr != nil {
		return 0, c.readErr
	}
	return c.src.Read(p)
}

func (c *countingReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	if c.seekErr != nil {
		return 0, c.seekErr
	}
	return c.src.Seek(offset, whence)
}

func (c *countingReadSeekCloser) Close() error { c.closeCount++; return nil }

// nonSeekingCloser is an io.ReadCloser that deliberately does NOT implement
// io.Seeker, so it triggers handleRangeRequest's "Store does not support
// seeking" branch (handlers should still close it on the way out).
type nonSeekingCloser struct{ closeCount int }

func (n *nonSeekingCloser) Read([]byte) (int, error) { return 0, io.EOF }
func (n *nonSeekingCloser) Close() error             { n.closeCount++; return nil }

// fakeRangeStore is a minimal blob.Store whose only meaningful method is
// GetIoReader, which returns a caller-supplied io.ReadCloser. Every other
// method panics so a test that accidentally exercises an unrelated code
// path fails loudly. handleRangeRequest only calls GetIoReader.
type fakeRangeStore struct {
	reader io.ReadCloser
	err    error
}

func (s *fakeRangeStore) Health(context.Context, bool) (int, string, error) {
	panic("fakeRangeStore.Health should not be called")
}
func (s *fakeRangeStore) Exists(context.Context, []byte, fileformat.FileType, ...options.FileOption) (bool, error) {
	panic("fakeRangeStore.Exists should not be called")
}
func (s *fakeRangeStore) Get(context.Context, []byte, fileformat.FileType, ...options.FileOption) ([]byte, error) {
	panic("fakeRangeStore.Get should not be called")
}
func (s *fakeRangeStore) GetIoReader(context.Context, []byte, fileformat.FileType, ...options.FileOption) (io.ReadCloser, error) {
	return s.reader, s.err
}
func (s *fakeRangeStore) Set(context.Context, []byte, fileformat.FileType, []byte, ...options.FileOption) error {
	panic("fakeRangeStore.Set should not be called")
}
func (s *fakeRangeStore) SetFromReader(context.Context, []byte, fileformat.FileType, io.ReadCloser, ...options.FileOption) error {
	panic("fakeRangeStore.SetFromReader should not be called")
}
func (s *fakeRangeStore) SetDAH(context.Context, []byte, fileformat.FileType, uint32, ...options.FileOption) error {
	panic("fakeRangeStore.SetDAH should not be called")
}
func (s *fakeRangeStore) GetDAH(context.Context, []byte, fileformat.FileType, ...options.FileOption) (uint32, error) {
	panic("fakeRangeStore.GetDAH should not be called")
}
func (s *fakeRangeStore) Del(context.Context, []byte, fileformat.FileType, ...options.FileOption) error {
	panic("fakeRangeStore.Del should not be called")
}
func (s *fakeRangeStore) Close(context.Context) error {
	panic("fakeRangeStore.Close should not be called")
}
func (s *fakeRangeStore) SetCurrentBlockHeight(uint32) {
	panic("fakeRangeStore.SetCurrentBlockHeight should not be called")
}

// TestHandleRangeRequest_ClosesReaderOnAllPaths pins the close contract on
// HTTPBlobServer.handleRangeRequest. Before the fix, the function never
// closed the io.ReadCloser returned by GetIoReader - not on the four
// mid-function error returns (seek failure, store-not-seekable, Read
// failure, the fall-through after Write), and not on the success path.
// Each unclosed semaphoreReadCloser holds a file-store read permit; under
// any non-trivial range-request volume the read semaphore (default 768)
// exhausts and subsequent reads fail with SERVICE_UNAVAILABLE.
func TestHandleRangeRequest_ClosesReaderOnAllPaths(t *testing.T) {
	logger := ulogger.New("test")

	t.Run("success", func(t *testing.T) {
		reader := newCountingReadSeekCloser([]byte("hello, range request world!"))
		srv := &HTTPBlobServer{store: &fakeRangeStore{reader: reader}, logger: logger}

		req := httptest.NewRequest("GET", "/blob/Zm9vLnRlc3Rpbmc=", nil)
		req.Header.Set("Range", "bytes=0-4")
		rr := httptest.NewRecorder()

		srv.handleRangeRequest(rr, req, []byte("foo"), fileformat.FileTypeTesting)

		require.Equal(t, 1, reader.closeCount, "reader must be Closed exactly once on the success path")
	})

	t.Run("seek error", func(t *testing.T) {
		reader := newCountingReadSeekCloser([]byte("data"))
		reader.seekErr = errors.New(errors.ERR_PROCESSING, "deliberate seek failure")
		srv := &HTTPBlobServer{store: &fakeRangeStore{reader: reader}, logger: logger}

		req := httptest.NewRequest("GET", "/blob/Zm9vLnRlc3Rpbmc=", nil)
		req.Header.Set("Range", "bytes=10-20")
		rr := httptest.NewRecorder()

		srv.handleRangeRequest(rr, req, []byte("foo"), fileformat.FileTypeTesting)

		require.Equal(t, 1, reader.closeCount, "reader must be Closed when Seek fails")
	})

	t.Run("store not seekable", func(t *testing.T) {
		reader := &nonSeekingCloser{}
		srv := &HTTPBlobServer{store: &fakeRangeStore{reader: reader}, logger: logger}

		req := httptest.NewRequest("GET", "/blob/Zm9vLnRlc3Rpbmc=", nil)
		req.Header.Set("Range", "bytes=10-20")
		rr := httptest.NewRecorder()

		srv.handleRangeRequest(rr, req, []byte("foo"), fileformat.FileTypeTesting)

		require.Equal(t, 1, reader.closeCount, "reader must be Closed when the store does not support seeking")
		require.Contains(t, rr.Body.String(), "does not support seeking")
	})

	t.Run("read error", func(t *testing.T) {
		reader := newCountingReadSeekCloser([]byte("xx"))
		reader.readErr = errors.New(errors.ERR_PROCESSING, "deliberate read failure")
		srv := &HTTPBlobServer{store: &fakeRangeStore{reader: reader}, logger: logger}

		req := httptest.NewRequest("GET", "/blob/Zm9vLnRlc3Rpbmc=", nil)
		req.Header.Set("Range", "bytes=0-10")
		rr := httptest.NewRecorder()

		srv.handleRangeRequest(rr, req, []byte("foo"), fileformat.FileTypeTesting)

		require.Equal(t, 1, reader.closeCount, "reader must be Closed when ReadFull fails")
	})
}
