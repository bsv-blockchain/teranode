// Package blob provides blob storage functionality with various storage backend implementations.
package blob

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blobhttp "github.com/bsv-blockchain/teranode/stores/blob/http"
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

	const serverAuthToken = "server-operations-token"

	blobServer, err := NewHTTPBlobServer(
		logger,
		serverStoreURL,
		serverAuthToken,
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

	client, err := blobhttp.New(logger, clientStoreURL, options.WithHTTPAuthToken(serverAuthToken))
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
	maxReadLen int // largest buffer handed to Read
}

func newCountingReadSeekCloser(data []byte) *countingReadSeekCloser {
	return &countingReadSeekCloser{src: bytes.NewReader(data)}
}

func (c *countingReadSeekCloser) Read(p []byte) (int, error) {
	c.maxReadLen = max(c.maxReadLen, len(p))

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
// io.Seeker, so it triggers handleRangeRequest's whole-blob fallback
// (handlers should still close it on the way out).
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
		require.Equal(t, http.StatusOK, rr.Code)
	})

	t.Run("read error", func(t *testing.T) {
		reader := newCountingReadSeekCloser([]byte("xx"))
		reader.readErr = errors.New(errors.ERR_PROCESSING, "deliberate read failure")
		logs := &logRecorder{}
		srv := &HTTPBlobServer{store: &fakeRangeStore{reader: reader}, logger: ulogger.NewErrorTestLogger(logs)}

		req := httptest.NewRequest("GET", "/blob/Zm9vLnRlc3Rpbmc=", nil)
		req.Header.Set("Range", "bytes=0-10")
		rr := httptest.NewRecorder()

		srv.handleRangeRequest(rr, req, []byte("foo"), fileformat.FileTypeTesting)

		require.Equal(t, 1, reader.closeCount, "reader must be Closed when the copy fails")

		// The 206 headers go out before the copy, so the failure cannot change the status;
		// the client sees a body shorter than Content-Length and the server logs it.
		require.Equal(t, http.StatusPartialContent, rr.Code)
		require.Equal(t, "2", rr.Header().Get("Content-Length"))
		require.Empty(t, rr.Body.Bytes())
		require.Len(t, logs.lines, 1)
		require.Contains(t, logs.lines[0], "range read failed after 0/2 bytes")
	})
}

// fileBackedFakeStore is a Store implementation backed by an *os.File so
// GetIoReader returns the real file-store wrapper. This is the minimum
// scaffolding needed to exercise handleRangeRequest's end-to-end flow with
// the same reader type the file store returns in production.
type fileBackedFakeStore struct {
	dir string
}

func newFileBackedFakeStore(t *testing.T, payload []byte) *fileBackedFakeStore {
	dir, err := os.MkdirTemp("", "rangereq")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	storeURL, err := url.Parse("file://" + dir)
	require.NoError(t, err)
	// No token: this fixture only serves GETs, so a read-only server is also a check
	// that reads are unaffected by the mutation gate.
	srv, err := NewHTTPBlobServer(ulogger.New("rangereq"), storeURL, "")
	require.NoError(t, err)
	_ = srv // just used for the store construction

	store, err := NewStore(ulogger.New("rangereq"), storeURL)
	require.NoError(t, err)
	require.NoError(t, store.Set(context.Background(), []byte("k"), fileformat.FileTypeTesting, payload))

	return &fileBackedFakeStore{dir: dir}
}

func (s *fileBackedFakeStore) underlying(t *testing.T) Store {
	storeURL, err := url.Parse("file://" + s.dir)
	require.NoError(t, err)
	store, err := NewStore(ulogger.New("rangereq"), storeURL)
	require.NoError(t, err)
	return store
}

// TestHandleRangeRequest_FileStoreReaderIsSeekable is a guard against the
// regression where semaphoreReadCloser embeds io.ReadCloser (no Seek method),
// so dataReader.(io.Seeker) fails the type assertion in handleRangeRequest
// and every range request with start>0 against the file store returns HTTP
// 500 "Store does not support seeking" - even though the underlying *os.File
// is Seekable. The fix adds a Seek method on semaphoreReadCloser that
// delegates to the wrapped reader.
func TestHandleRangeRequest_FileStoreReaderIsSeekable(t *testing.T) {
	payload := []byte("0123456789abcdefghij") // 20 bytes
	store := newFileBackedFakeStore(t, payload).underlying(t)

	srv := &HTTPBlobServer{store: store, logger: ulogger.New("rangereq")}

	req := httptest.NewRequest("GET", "/blob/", nil)
	req.Header.Set("Range", "bytes=5-9")
	rr := httptest.NewRecorder()

	srv.handleRangeRequest(rr, req, []byte("k"), fileformat.FileTypeTesting)

	require.Equal(t, http.StatusPartialContent, rr.Code,
		"a range request with start>0 against a file-backed store must succeed; "+
			"non-200 status means the file-store reader's Seeker check failed (probably "+
			"semaphoreReadCloser does not promote Seek)")
	require.Equal(t, "56789", rr.Body.String(), "must return bytes 5..9 of the payload")
}

// TestHandleRangeRequest_ContentRangeReportsActualTotal pins the fix for the
// Content-Range total reported in the response. Before the fix, the handler
// emitted "bytes start-end/len(data)" where len(data) is the size of the
// returned slice (end-start), not the total blob length. RFC 7233 §4.2
// requires the total length of the underlying representation in the
// "/total" position.
func TestHandleRangeRequest_ContentRangeReportsActualTotal(t *testing.T) {
	payload := []byte("0123456789") // 10 bytes total
	store := newFileBackedFakeStore(t, payload).underlying(t)

	srv := &HTTPBlobServer{store: store, logger: ulogger.New("rangereq")}

	req := httptest.NewRequest("GET", "/blob/", nil)
	req.Header.Set("Range", "bytes=2-4")
	rr := httptest.NewRecorder()

	srv.handleRangeRequest(rr, req, []byte("k"), fileformat.FileTypeTesting)

	require.Equal(t, http.StatusPartialContent, rr.Code)
	gotCR := rr.Header().Get("Content-Range")
	// Total should be 10 (full blob), not 3 (end-start = the returned slice).
	require.Equal(t, "bytes 2-4/10", gotCR,
		"Content-Range must report the full blob length in the /total position per RFC 7233 §4.2, not the returned-slice length")
}

// logRecorder is a ulogger.TestingT that keeps every line ErrorTestLogger writes.
type logRecorder struct{ lines []string }

func (l *logRecorder) Errorf(format string, args ...interface{}) {
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}
func (l *logRecorder) FailNow() {}
func (l *logRecorder) Logf(format string, args ...any) {
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func TestParseByteRange(t *testing.T) {
	valid := []struct {
		header string
		want   byteRange
	}{
		{header: "bytes=0-4", want: byteRange{first: 0, last: 4}},
		{header: "bytes=5-5", want: byteRange{first: 5, last: 5}},
		{header: "bytes=5-", want: byteRange{first: 5, openEnded: true}},
		{header: "bytes=-3", want: byteRange{suffix: true, suffixLen: 3}},
		{header: "bytes=0-9223372036854775807", want: byteRange{first: 0, last: math.MaxInt64}},
	}

	for _, tt := range valid {
		t.Run("valid "+tt.header, func(t *testing.T) {
			got, status := parseByteRange(tt.header)
			require.Equal(t, http.StatusOK, status)
			require.Equal(t, tt.want, got)
		})
	}

	malformed := []string{
		"",
		"0-4",
		"items=0-4",
		"bytes=",
		"bytes=-",
		"bytes=5",
		"bytes=+5-6",
		"bytes=5-+6",
		"bytes=--5",
		"bytes= 5-6",
		"bytes=5-6 ",
		"bytes=a-b",
		"bytes=5-6-7",
		"bytes=99999999999999999999-",
		"bytes=0-9223372036854775808",
		"bytes=-99999999999999999999",
	}

	for _, header := range malformed {
		t.Run("malformed "+header, func(t *testing.T) {
			_, status := parseByteRange(header)
			require.Equal(t, http.StatusBadRequest, status)
		})
	}

	notSatisfiable := []string{"bytes=0-1,3-4", "bytes=5-2"}

	for _, header := range notSatisfiable {
		t.Run("not satisfiable "+header, func(t *testing.T) {
			_, status := parseByteRange(header)
			require.Equal(t, http.StatusRequestedRangeNotSatisfiable, status)
		})
	}
}

// TestHandleRangeRequest_AllocationIndependentOfRangeHeader is the regression test for
// bitcoin-sv/teranode issue 4853: the handler used to make([]byte, end-start) from the
// header before looking at the blob, so a one-byte object with a multi-gigabyte range
// allocated gigabytes. The auditor observed the reader being handed an 8 MiB buffer for
// a 1-byte blob. The span must now be clamped to the real size and the body streamed.
func TestHandleRangeRequest_AllocationIndependentOfRangeHeader(t *testing.T) {
	for _, header := range []string{"bytes=0-8388607", "bytes=0-2147483647", "bytes=0-9223372036854775807"} {
		t.Run(header, func(t *testing.T) {
			reader := newCountingReadSeekCloser([]byte{0x42})
			srv := &HTTPBlobServer{store: &fakeRangeStore{reader: reader}, logger: ulogger.New("rangereq")}

			req := httptest.NewRequest("GET", "/blob/", nil)
			req.Header.Set("Range", header)
			rr := httptest.NewRecorder()

			srv.handleRangeRequest(rr, req, []byte("k"), fileformat.FileTypeTesting)

			require.Equal(t, http.StatusPartialContent, rr.Code)
			require.Equal(t, []byte{0x42}, rr.Body.Bytes())
			require.Equal(t, "bytes 0-0/1", rr.Header().Get("Content-Range"))
			require.Equal(t, "1", rr.Header().Get("Content-Length"))
			require.LessOrEqual(t, reader.maxReadLen, 64*1024, "read buffer must not be sized from the Range header")
			require.Equal(t, 1, reader.closeCount)
		})
	}
}

func TestHandleRangeRequest_ResolvesAgainstBlobSize(t *testing.T) {
	payload := []byte("0123456789") // 10 bytes
	store := newFileBackedFakeStore(t, payload).underlying(t)
	srv := &HTTPBlobServer{store: store, logger: ulogger.New("rangereq")}

	serve := func(header string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", "/blob/", nil)
		req.Header.Set("Range", header)
		rr := httptest.NewRecorder()

		srv.handleRangeRequest(rr, req, []byte("k"), fileformat.FileTypeTesting)

		return rr
	}

	served := []struct {
		header       string
		body         string
		contentRange string
	}{
		{header: "bytes=5-", body: "56789", contentRange: "bytes 5-9/10"},
		{header: "bytes=0-", body: "0123456789", contentRange: "bytes 0-9/10"},
		{header: "bytes=-3", body: "789", contentRange: "bytes 7-9/10"},
		{header: "bytes=-50", body: "0123456789", contentRange: "bytes 0-9/10"},
		{header: "bytes=8-100", body: "89", contentRange: "bytes 8-9/10"},
		{header: "bytes=9-9", body: "9", contentRange: "bytes 9-9/10"},
		{header: "bytes=3-9223372036854775807", body: "3456789", contentRange: "bytes 3-9/10"},
	}

	for _, tt := range served {
		t.Run(tt.header, func(t *testing.T) {
			rr := serve(tt.header)

			require.Equal(t, http.StatusPartialContent, rr.Code)
			require.Equal(t, tt.body, rr.Body.String())
			require.Equal(t, tt.contentRange, rr.Header().Get("Content-Range"))
			require.Equal(t, strconv.Itoa(len(tt.body)), rr.Header().Get("Content-Length"))
		})
	}

	unsatisfiable := []string{"bytes=10-", "bytes=10-20", "bytes=9223372036854775807-", "bytes=-0"}

	for _, header := range unsatisfiable {
		t.Run(header+" is 416 with size", func(t *testing.T) {
			rr := serve(header)

			require.Equal(t, http.StatusRequestedRangeNotSatisfiable, rr.Code)
			require.Equal(t, "bytes */10", rr.Header().Get("Content-Range"))
		})
	}

	for _, header := range []string{"bytes=5-2", "bytes=0-1,3-4"} {
		t.Run(header+" is 416", func(t *testing.T) {
			require.Equal(t, http.StatusRequestedRangeNotSatisfiable, serve(header).Code)
		})
	}

	t.Run("malformed is 400", func(t *testing.T) {
		require.Equal(t, http.StatusBadRequest, serve("bytes=+1-2").Code)
	})
}

// TestHandleRangeRequest_NonSeekableServesWholeBlob pins the fallback for readers
// without a size (memory, S3, HTTP stores). A 206 would have to advertise a
// Content-Range before knowing the blob holds those bytes, so for "bytes=0-1048575"
// against an 11-byte blob it claimed 1 MiB and sent 11 bytes. The handler now ignores
// the range (RFC 9110 section 14.2) and streams the whole blob with 200, which also
// replaces the old 500 for the suffix, open-ended and first>0 forms.
func TestHandleRangeRequest_NonSeekableServesWholeBlob(t *testing.T) {
	headers := []string{
		"bytes=0-4", "bytes=0-0", "bytes=0-1048575", "bytes=0-9223372036854775807",
		"bytes=-3", "bytes=0-", "bytes=2-", "bytes=2-4",
	}

	for _, header := range headers {
		t.Run(header, func(t *testing.T) {
			reader := &closeCountingReader{r: strings.NewReader("hello world")}
			srv := &HTTPBlobServer{store: &fakeRangeStore{reader: reader}, logger: ulogger.New("rangereq")}

			req := httptest.NewRequest("GET", "/blob/", nil)
			req.Header.Set("Range", header)
			rr := httptest.NewRecorder()

			srv.handleRangeRequest(rr, req, []byte("k"), fileformat.FileTypeTesting)

			require.Equal(t, http.StatusOK, rr.Code)
			require.Equal(t, "hello world", rr.Body.String())
			require.Empty(t, rr.Header().Get("Content-Range"), "a 200 must not claim a range")
			require.Equal(t, 1, reader.closeCount)
		})
	}

	t.Run("invalid ranges are still rejected before opening the blob", func(t *testing.T) {
		srv := &HTTPBlobServer{store: &fakeRangeStore{reader: &nonSeekingCloser{}}, logger: ulogger.New("rangereq")}

		for header, want := range map[string]int{"bytes=+1-2": http.StatusBadRequest, "bytes=5-2": http.StatusRequestedRangeNotSatisfiable} {
			req := httptest.NewRequest("GET", "/blob/", nil)
			req.Header.Set("Range", header)
			rr := httptest.NewRecorder()

			srv.handleRangeRequest(rr, req, []byte("k"), fileformat.FileTypeTesting)

			require.Equal(t, want, rr.Code, header)
		}
	})
}

// closeCountingReader is a non-seekable io.ReadCloser with content that counts Close
// calls. It wraps io.Reader rather than embedding *strings.Reader, which would promote
// Seek and take the seekable path.
type closeCountingReader struct {
	r          io.Reader
	closeCount int
}

func (c *closeCountingReader) Read(p []byte) (int, error) { return c.r.Read(p) }
func (c *closeCountingReader) Close() error               { c.closeCount++; return nil }

// TestGetKeyFromPath_ShortPathsDoNotPanic pins the bound check: a dot before the end of the
// "/blob/" prefix used to slice out of range.
func TestGetKeyFromPath_ShortPathsDoNotPanic(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantErr bool
		wantKey []byte
	}{
		{name: "dot at root", path: "/.tx", wantErr: true},
		{name: "dot inside prefix", path: "/blob.tx", wantErr: true},
		{name: "empty key", path: "/blob/.tx", wantKey: []byte{}},
		{name: "prefix not at start", path: "/x/blob/abc.tx", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				key []byte
				err error
			)

			require.NotPanics(t, func() { key, _, err = getKeyFromPath(tt.path) })

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.wantKey, key)
		})
	}
}

// TestHTTPBlobServer_ShortGetPathIsBadRequest drives the same path through the handler: GET is
// unauthenticated, so this is reachable by anyone who can reach the listener.
func TestHTTPBlobServer_ShortGetPathIsBadRequest(t *testing.T) {
	server, _ := newAuthTestServer(t, "")

	require.Equal(t, http.StatusBadRequest, doBlobRequest(t, server, http.MethodGet, "/.tx", "", nil))
}
