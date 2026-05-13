package file

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/rand"
)

func TestFileGetWithAbsolutePath(t *testing.T) {
	t.Run("test", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		// check if the directory is absolute
		_, err = os.Stat(tempDir)
		require.NoError(t, err)

		// check if the directory is not relative
		_, err = os.Stat(tempDir[1:])
		require.Error(t, err)

		// Create a URL from the tempDir
		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		err = f.Set(context.Background(), []byte("key"), fileformat.FileTypeTesting, []byte("value"))
		require.NoError(t, err)

		value, err := f.Get(context.Background(), []byte("key"), fileformat.FileTypeTesting)
		require.NoError(t, err)

		require.Equal(t, []byte("value"), value)

		err = f.Del(context.Background(), []byte("key"), fileformat.FileTypeTesting)
		require.NoError(t, err)
	})
}

func TestFileGetWithRelativePath(t *testing.T) {
	ctx := context.Background()
	// random directory name
	relativePath := "test-path-" + rand.String(12)

	// Create a URL from the relative path
	u, err := url.Parse("file://./" + relativePath)
	require.NoError(t, err)

	f, err := New(ulogger.TestLogger{}, u)
	require.NoError(t, err)

	// check if the directory is created
	_, err = os.Stat("./" + relativePath)
	require.NoError(t, err)

	// check if the directory is relative
	_, err = os.Stat("/" + relativePath)
	require.Error(t, err)

	err = f.Set(ctx, []byte("key"), fileformat.FileTypeTesting, []byte("value"))
	require.NoError(t, err)

	value, err := f.Get(ctx, []byte("key"), fileformat.FileTypeTesting)
	require.NoError(t, err)

	require.Equal(t, []byte("value"), value)

	err = f.Del(ctx, []byte("key"), fileformat.FileTypeTesting)
	require.NoError(t, err)

	// cleanup
	_ = os.RemoveAll(relativePath)

	f.Close(ctx)
}

func TestFileAbsoluteAndRelativePath(t *testing.T) {
	absoluteURL, err := url.ParseRequestURI("file:///absolute/path/to/file")
	require.NoError(t, err)
	require.Equal(t, "/absolute/path/to/file", GetPathFromURL(absoluteURL))

	relativeURL, err := url.ParseRequestURI("file://./relative/path/to/file")
	require.NoError(t, err)
	require.Equal(t, "relative/path/to/file", GetPathFromURL(relativeURL))
}

func GetPathFromURL(u *url.URL) string {
	if u.Host == "." {
		return u.Path[1:]
	}

	return u.Path
}

func TestFileNewWithEmptyPath(t *testing.T) {
	t.Run("empty path", func(t *testing.T) {
		f, err := New(ulogger.TestLogger{}, nil)
		require.Error(t, err)
		require.Nil(t, f)
	})
}

func TestFileNewWithInvalidDirectory(t *testing.T) {
	t.Run("invalid directory", func(t *testing.T) {
		invalidPath := "/invalid-directory" // Assuming this path cannot be created

		u, err := url.Parse("file://" + invalidPath)
		require.NoError(t, err)

		_, err = New(ulogger.TestLogger{}, u)
		require.Error(t, err) // "mkdir /invalid-directory: read-only file system"
		require.Contains(t, err.Error(), "failed to create directory")
	})
}

// TestFileLoadDAHsCleanupTmpFiles removed - loadDAHs() functionality moved to pruner service

func TestFileConcurrentAccess(t *testing.T) {
	t.Run("concurrent set and get", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		var wg sync.WaitGroup

		concurrency := 100

		for i := 0; i < concurrency; i++ {
			wg.Add(1)

			go func(i int) {
				defer wg.Done()

				key := []byte(fmt.Sprintf("key-%d", i))
				value := []byte(fmt.Sprintf("value-%d", i))

				err := f.Set(context.Background(), key, fileformat.FileTypeTesting, value)
				require.NoError(t, err)

				retrievedValue, err := f.Get(context.Background(), key, fileformat.FileTypeTesting)
				require.NoError(t, err)
				require.Equal(t, value, retrievedValue)
			}(i)
		}

		wg.Wait()
	})
}

func TestFileSetWithSubdirectoryOptionIgnored(t *testing.T) {
	t.Run("subdirectory usage", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		subDir := "subDir"

		f, err := New(ulogger.TestLogger{}, u, options.WithDefaultSubDirectory(subDir))
		require.NoError(t, err)

		key := []byte("key")
		value := []byte("value")

		err = f.Set(context.Background(), key, fileformat.FileTypeTesting, value)
		require.NoError(t, err)

		// Construct the expected file path in the subdirectory
		expectedDir := filepath.Join(tempDir, subDir)
		expectedFilePath := filepath.Join(expectedDir, util.ReverseAndHexEncodeSlice(key)+"."+fileformat.FileTypeTesting.String())

		_, err = os.Stat(expectedFilePath)
		require.NoError(t, err)
	})
}

func TestFileSetWithSubdirectoryOption(t *testing.T) {
	t.Run("subdirectory usage", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		subDir := "subDir"

		f, err := New(ulogger.TestLogger{}, u, options.WithDefaultSubDirectory(subDir))
		require.NoError(t, err)

		key := []byte("key")
		value := []byte("value")

		err = f.Set(context.Background(), key, fileformat.FileTypeTesting, value, options.WithFilename("filename"))
		require.NoError(t, err)

		// Construct the expected file path in the subdirectory
		expectedDir := filepath.Join(tempDir, subDir)
		expectedFilePath := filepath.Join(expectedDir, "filename"+"."+fileformat.FileTypeTesting.String())

		// Check if the file was created in the subdirectory
		_, err = os.Stat(expectedFilePath)
		require.NoError(t, err, "expected file found in subdirectory")
	})
}

func TestFileWithHeader(t *testing.T) {
	t.Run("set and get with header", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "TestFileWithHeader")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("key-with-header")
		content := "This is the main content"

		// Test setting content with header using Set
		err = f.Set(context.Background(), key, fileformat.FileTypeTesting, []byte(content))
		require.NoError(t, err)

		// Verify content using Get
		value, err := f.Get(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		assert.Equal(t, content, string(value))

		// Verify content using GetIoReader
		reader, err := f.GetIoReader(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)

		// Read all the content from the reader
		readContent, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, content, string(readContent))

		// delete the file
		err = f.Del(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)

		// Test setting content with header using SetFromReader
		newContent := "New content from reader"

		contentReader := strings.NewReader(newContent)
		readCloser := io.NopCloser(contentReader)

		err = f.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, readCloser)
		require.NoError(t, err)

		// Verify new content using Get
		value, err = f.Get(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		assert.Equal(t, newContent, string(value))

		// Verify new content using GetIoReader
		reader, err = f.GetIoReader(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)

		readContent, err = io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, newContent, string(readContent))

		// delete the file
		err = f.Del(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
	})
}

func TestFileWithFooter(t *testing.T) {
	t.Run("set and get with footer", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "TestFileWithFooter")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("key-with-footer")
		content := "This is the main content"

		// Test setting content with footer using Set
		err = f.Set(context.Background(), key, fileformat.FileTypeTesting, []byte(content))
		require.NoError(t, err)

		// Verify content using Get
		value, err := f.Get(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		assert.Equal(t, content, string(value))

		// Verify content using GetIoReader
		reader, err := f.GetIoReader(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)

		// Read all the content from the reader
		readContent, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, content, string(readContent))

		// delete the file
		err = f.Del(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)

		// Test setting content with footer using SetFromReader
		newContent := "New content from reader"
		contentReader := strings.NewReader(newContent)
		readCloser := io.NopCloser(contentReader)

		err = f.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, readCloser)
		require.NoError(t, err)

		// Verify new content using Get
		value, err = f.Get(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		assert.Equal(t, newContent, string(value))

		// Verify new content using GetIoReader
		reader, err = f.GetIoReader(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)

		readContent, err = io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, newContent, string(readContent))

		// delete the file
		err = f.Del(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
	})
}

func TestFileSetFromReaderAndGetIoReader(t *testing.T) {
	t.Run("set content from reader", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("key")
		content := "This is test reader content"
		reader := strings.NewReader(content)

		// Wrap the reader to satisfy the io.ReadCloser interface
		readCloser := io.NopCloser(reader)

		err = f.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, readCloser)
		require.NoError(t, err)

		// Verify the content was correctly stored
		storedReader, err := f.GetIoReader(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)

		// Read all the content from the storedReader
		storedContent, err := io.ReadAll(storedReader)
		require.NoError(t, err)
		assert.Equal(t, content, string(storedContent))
	})
}

// errorAfterNBytesReader returns an error after reading n bytes
type errorAfterNBytesReader struct {
	data          []byte
	pos           int
	errorAt       int
	errorToReturn error
}

func (r *errorAfterNBytesReader) Read(p []byte) (int, error) {
	if r.pos >= r.errorAt {
		return 0, r.errorToReturn
	}
	remaining := r.errorAt - r.pos
	if len(p) > remaining {
		p = p[:remaining]
	}
	n := copy(p, r.data[r.pos:r.pos+len(p)])
	r.pos += n
	if r.pos >= r.errorAt {
		return n, r.errorToReturn
	}
	return n, nil
}

func (r *errorAfterNBytesReader) Close() error {
	return nil
}

func TestSetFromReader_CleansUpTempFileOnError(t *testing.T) {
	t.Run("temp file is removed when reader returns error", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test-cleanup")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("cleanup-test-key")

		// Create a reader that returns an error after some data
		testData := make([]byte, 1000)
		for i := range testData {
			testData[i] = byte(i % 256)
		}
		readErr := errors.NewProcessingError("simulated reader error")
		errorReader := &errorAfterNBytesReader{
			data:          testData,
			errorAt:       500, // Error after 500 bytes
			errorToReturn: readErr,
		}

		// SetFromReader should fail with the reader error
		err = f.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, errorReader)
		require.Error(t, err, "SetFromReader should return an error when reader fails")

		// Verify no temp files remain in the directory
		files, err := os.ReadDir(tempDir)
		require.NoError(t, err)

		for _, file := range files {
			assert.False(t, strings.HasSuffix(file.Name(), ".tmp"),
				"Temp file should be cleaned up on error: %s", file.Name())
		}

		// Verify the final file was NOT created
		exists, err := f.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		assert.False(t, exists, "File should not exist after failed SetFromReader")
	})
}

func TestSetFromReader_CleansUpTempFileOnPipeClose(t *testing.T) {
	t.Run("temp file is removed when pipe is closed with error", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test-pipe-cleanup")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("pipe-cleanup-test-key")

		// Create a pipe and close it with an error to simulate abort
		pr, pw := io.Pipe()

		// Start writing some data in a goroutine
		go func() {
			_, _ = pw.Write([]byte("some initial data"))
			// Close the pipe with an error to simulate abort
			_ = pw.CloseWithError(errors.NewProcessingError("simulated abort"))
		}()

		// SetFromReader should fail with the pipe error
		err = f.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, pr)
		require.Error(t, err, "SetFromReader should return an error when pipe is closed with error")

		// Verify no temp files remain
		files, err := os.ReadDir(tempDir)
		require.NoError(t, err)

		for _, file := range files {
			assert.False(t, strings.HasSuffix(file.Name(), ".tmp"),
				"Temp file should be cleaned up on pipe error: %s", file.Name())
		}

		// Verify the final file was NOT created
		exists, err := f.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		assert.False(t, exists, "File should not exist after aborted SetFromReader")
	})
}

func TestSetFromReader_DoesNotExposeFinalFileUntilReaderCompletes(t *testing.T) {
	tempDir := t.TempDir()

	u, err := url.Parse("file://" + tempDir)
	require.NoError(t, err)

	f, err := New(ulogger.TestLogger{}, u)
	require.NoError(t, err)

	key := []byte("streaming-atomic-publication")
	filename, err := f.options.ConstructFilename(tempDir, key, fileformat.FileTypeTesting)
	require.NoError(t, err)

	reader, writer := io.Pipe()
	done := make(chan error, 1)

	go func() {
		done <- f.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, reader)
	}()

	_, err = writer.Write([]byte("partial payload"))
	require.NoError(t, err)

	_, err = os.Stat(filename)
	require.True(t, os.IsNotExist(err), "final filename must not be visible while the reader is still open")

	require.NoError(t, writer.Close())
	require.NoError(t, <-done)

	_, err = os.Stat(filename)
	require.NoError(t, err, "final filename should be visible after the reader completes")
}

func TestSetFromReader_RemovesBlobWhenChecksumPublicationFails(t *testing.T) {
	tempDir := t.TempDir()

	u, err := url.Parse("file://" + tempDir)
	require.NoError(t, err)

	f, err := New(ulogger.TestLogger{}, u)
	require.NoError(t, err)

	key := []byte("checksum-publication-fails")
	filename, err := f.options.ConstructFilename(tempDir, key, fileformat.FileTypeTesting)
	require.NoError(t, err)

	require.NoError(t, os.Mkdir(filename+checksumExtension, 0755))

	err = f.Set(context.Background(), key, fileformat.FileTypeTesting, []byte("value"))
	require.Error(t, err)

	_, statErr := os.Stat(filename)
	require.True(t, os.IsNotExist(statErr), "blob final filename should not remain after checksum publication fails")

	entries, err := os.ReadDir(tempDir)
	require.NoError(t, err)
	for _, entry := range entries {
		require.False(t, strings.HasSuffix(entry.Name(), ".tmp"), "temporary file should be cleaned up: %s", entry.Name())
	}
}

func TestSetFromReader_ConcurrentWritersNeverExposePartialFinal(t *testing.T) {
	tempDir := t.TempDir()

	u, err := url.Parse("file://" + tempDir)
	require.NoError(t, err)

	f, err := New(ulogger.TestLogger{}, u)
	require.NoError(t, err)

	key := []byte("concurrent-atomic-publication")

	// Build the expected complete file payload (header + body) once so the reader can
	// match against it. Every successful read of the final file must be byte-identical
	// to this payload — never a truncated prefix.
	body := bytes.Repeat([]byte("XYZ"), 4096)
	header := fileformat.NewHeader(fileformat.FileTypeTesting)

	var expected bytes.Buffer
	require.NoError(t, header.Write(&expected))
	expected.Write(body)
	expectedBytes := expected.Bytes()

	filename, err := f.options.ConstructFilename(tempDir, key, fileformat.FileTypeTesting)
	require.NoError(t, err)

	const writers = 8
	stop := make(chan struct{})
	writerErrs := make(chan error, writers)
	writerWG := sync.WaitGroup{}

	for i := 0; i < writers; i++ {
		writerWG.Add(1)
		go func() {
			defer writerWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}

				if err := f.Set(context.Background(), key, fileformat.FileTypeTesting, body, options.WithAllowOverwrite(true)); err != nil {
					writerErrs <- err
					return
				}
			}
		}()
	}

	// Reader loop: every successful read must be byte-identical to expectedBytes. A
	// partial read here would mean the atomic-publication invariant was violated.
	readerDone := make(chan struct{})
	var readerErr error

	go func() {
		defer close(readerDone)
		deadline := time.Now().Add(500 * time.Millisecond)
		for time.Now().Before(deadline) {
			data, err := os.ReadFile(filename)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				readerErr = err
				return
			}

			if !bytes.Equal(data, expectedBytes) {
				readerErr = errors.NewStorageError("observed partial final file: got %d bytes, expected %d", len(data), len(expectedBytes))
				return
			}
		}
	}()

	<-readerDone
	close(stop)
	writerWG.Wait()
	close(writerErrs)

	for err := range writerErrs {
		require.NoError(t, err)
	}
	require.NoError(t, readerErr)
}

func TestRenameTempFile_OverwriteAndRejectSemantics(t *testing.T) {
	// renameTempFile's cross-platform contract: on POSIX, rename atomically replaces an
	// existing destination regardless of allowOverwrite; on non-POSIX, allowOverwrite
	// controls whether an existing destination is replaced or whether ErrBlobAlreadyExists
	// is returned. This test documents the observable POSIX behaviour and exercises both
	// allowOverwrite values so a regression on either branch shows up.
	tempDir := t.TempDir()

	u, err := url.Parse("file://" + tempDir)
	require.NoError(t, err)

	f, err := New(ulogger.TestLogger{}, u)
	require.NoError(t, err)

	key := []byte("rename-temp-file-semantics")

	require.NoError(t, f.Set(context.Background(), key, fileformat.FileTypeTesting, []byte("first")))

	// allowOverwrite=true: the second publication should replace the first cleanly.
	require.NoError(t, f.Set(context.Background(), key, fileformat.FileTypeTesting, []byte("second"), options.WithAllowOverwrite(true)))

	got, err := f.Get(context.Background(), key, fileformat.FileTypeTesting)
	require.NoError(t, err)
	require.Equal(t, []byte("second"), got)

	// allowOverwrite=false: errorOnOverwrite stops the call before renameTempFile is
	// reached, so the existing value must remain intact.
	err = f.Set(context.Background(), key, fileformat.FileTypeTesting, []byte("third"))
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlobAlreadyExists), "expected ErrBlobAlreadyExists, got %v", err)

	got, err = f.Get(context.Background(), key, fileformat.FileTypeTesting)
	require.NoError(t, err)
	require.Equal(t, []byte("second"), got)
}

func TestParseFsyncMode(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want fsyncMode
		ok   bool
	}{
		{"", fsyncModeFull, true},
		{"full", fsyncModeFull, true},
		{"FULL", fsyncModeFull, true},
		{"data", fsyncModeData, true},
		{"none", fsyncModeNone, true},
		{"bogus", fsyncModeFull, false},
	} {
		got, err := parseFsyncMode(tc.in)
		if tc.ok {
			require.NoError(t, err, "input=%q", tc.in)
			require.Equal(t, tc.want, got, "input=%q", tc.in)
		} else {
			require.Error(t, err, "input=%q", tc.in)
		}
	}
}

func TestNewWithFsyncMode(t *testing.T) {
	tempDir := t.TempDir()

	for _, mode := range []string{"full", "data", "none"} {
		t.Run(mode, func(t *testing.T) {
			u, err := url.Parse("file://" + tempDir + "?fsyncMode=" + mode)
			require.NoError(t, err)

			f, err := New(ulogger.TestLogger{}, u)
			require.NoError(t, err)

			key := []byte("fsync-mode-" + mode)
			require.NoError(t, f.Set(context.Background(), key, fileformat.FileTypeTesting, []byte("value")))

			got, err := f.Get(context.Background(), key, fileformat.FileTypeTesting)
			require.NoError(t, err)
			require.Equal(t, []byte("value"), got)
		})
	}

	t.Run("invalid", func(t *testing.T) {
		u, err := url.Parse("file://" + tempDir + "?fsyncMode=bogus")
		require.NoError(t, err)

		_, err = New(ulogger.TestLogger{}, u)
		require.Error(t, err)
	})
}

// BenchmarkSetFromReader_FsyncModes measures the cost of the atomic-publication path
// across the three fsyncMode levels. fsync overhead dominates the small-payload case
// on local filesystems and is intended to make any regression in the fsync schedule
// visible in CI. Operators sizing for NFS-backed deployments can compare the gap
// between fsyncModeFull and fsyncModeNone here against measurements on their target
// filesystem to decide whether to opt out of the directory fsync.
func BenchmarkSetFromReader_FsyncModes(b *testing.B) {
	for _, payloadSize := range []int{256, 4 * 1024, 64 * 1024} {
		for _, mode := range []string{"full", "data", "none"} {
			b.Run(fmt.Sprintf("payload=%dB/mode=%s", payloadSize, mode), func(b *testing.B) {
				tempDir := b.TempDir()

				u, err := url.Parse("file://" + tempDir + "?fsyncMode=" + mode)
				require.NoError(b, err)

				f, err := New(ulogger.TestLogger{}, u)
				require.NoError(b, err)

				payload := bytes.Repeat([]byte("x"), payloadSize)
				ctx := context.Background()

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					key := []byte(fmt.Sprintf("bench-%d", i))
					if err := f.Set(ctx, key, fileformat.FileTypeTesting, payload); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func TestFileStoreRelPath(t *testing.T) {
	tempDir := t.TempDir()
	f := &File{path: tempDir}

	rel, err := f.storeRelPath(filepath.Join(tempDir, "subdir", "blob.testing"))
	require.NoError(t, err)
	require.Equal(t, filepath.Join("subdir", "blob.testing"), rel)

	_, err = f.storeRelPath(filepath.Join(tempDir, "..", "outside.testing"))
	require.Error(t, err)

	_, err = f.storeRelPath(tempDir + "-sibling/blob.testing")
	require.Error(t, err)

	f = &File{path: filepath.Join("relative", "store")}
	rel, err = f.storeRelPath(filepath.Join("relative", "store", "subdir", "blob.testing"))
	require.NoError(t, err)
	require.Equal(t, filepath.Join("subdir", "blob.testing"), rel)
}

func TestFileGetHead(t *testing.T) {
	t.Run("get head of content", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("key")
		content := "This is test head content"
		reader := strings.NewReader(content)

		// Wrap the reader to satisfy the io.ReadCloser interface
		readCloser := io.NopCloser(reader)

		// First, set the content
		err = f.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, readCloser)
		require.NoError(t, err)

		// Get metadata using GetHead
		headReader, err := f.GetIoReader(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)

		head := make([]byte, 1) // Read only the first byte
		_, err = headReader.Read(head)
		require.NoError(t, err)

		assert.NotNil(t, head)
		assert.Equal(t, content[:1], string(head), "head content doesn't match")
	})
}

func TestFileGetHeadWithHeader(t *testing.T) {
	t.Run("get head of content", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("key")
		content := "This is test head content"
		reader := strings.NewReader(content)

		// Wrap the reader to satisfy the io.ReadCloser interface
		readCloser := io.NopCloser(reader)

		// First, set the content
		err = f.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, readCloser)
		require.NoError(t, err)

		// Get metadata using GetHead
		headReader, err := f.GetIoReader(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)

		head := make([]byte, 1) // Read only the first byte
		_, err = headReader.Read(head)
		require.NoError(t, err)

		assert.NotNil(t, head)
		assert.Equal(t, content[:1], string(head), "head content doesn't match")
	})
}

func TestFileExists(t *testing.T) {
	t.Run("check if content exists", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("key-exists")
		content := "This is test exists content"
		reader := strings.NewReader(content)

		// Content should not exist before setting
		exists, err := f.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.False(t, exists)

		// Wrap the reader to satisfy the io.ReadCloser interface
		readCloser := io.NopCloser(reader)

		// Set the content
		err = f.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, readCloser)
		require.NoError(t, err)

		// Now content should exist
		exists, err = f.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.True(t, exists)
	})
}

func TestFileDAHUntouchedOnExistingFileWhenOverwriteDisabled(t *testing.T) {
	t.Skip("DAH functionality now requires pruner service - covered by e2e tests")
}

func TestFileSetWithHashPrefix(t *testing.T) {
	u, err := url.Parse("file:///data/subtreestore?hashPrefix=2")
	require.NoError(t, err)
	require.Equal(t, "/data/subtreestore", u.Path)
	require.Equal(t, "2", u.Query().Get("hashPrefix"))

	u, err = url.Parse("null:///?localTTLStore=file&localTTLStorePath=./data/subtreestore-dah?hashPrefix=2")
	require.NoError(t, err)

	localTTLStoreURL := u.Query().Get("localTTLStorePath")
	u2, err := url.Parse(localTTLStoreURL)
	require.NoError(t, err)

	hashPrefix := u2.Query().Get("hashPrefix")
	require.Equal(t, "2", hashPrefix)
}

func TestFileSetHashPrefixOverride(t *testing.T) {
	u, err := url.Parse("file://./data/subtreestore?hashPrefix=2")
	require.NoError(t, err)

	f, err := New(ulogger.TestLogger{}, u, options.WithHashPrefix(1))
	require.NoError(t, err)

	// Even though the option is set to 1, the URL hashPrefix should override it
	require.Equal(t, 2, f.options.HashPrefix)
}

func TestFileHealth(t *testing.T) {
	t.Run("healthy state", func(t *testing.T) {
		// Setup
		tempDir, err := os.MkdirTemp("", "test-health")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		// Test
		status, message, err := f.Health(context.Background(), false)

		// Assert
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, "File Store: Healthy", message)
	})

	t.Run("non-existent path", func(t *testing.T) {
		// Setup
		nonExistentPath := "./not_exist"
		u, err := url.Parse("file://" + nonExistentPath)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		// creating a New store will create the folder
		// so we need to remove it before we test
		var path string
		if u.Host == "." {
			path = u.Path[1:] // relative path
		} else {
			path = u.Path // absolute path
		}

		err = os.RemoveAll(path)
		require.NoError(t, err)

		// Test
		status, message, err := f.Health(context.Background(), false)

		// Assert
		require.Error(t, err)
		require.Equal(t, http.StatusInternalServerError, status)
		require.Equal(t, "File Store: Path does not exist", message)
	})

	t.Run("read-only directory", func(t *testing.T) {
		// Setup
		tempDir, err := os.MkdirTemp("", "test-health-readonly")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		// Make the directory read-only
		err = os.Chmod(tempDir, 0o555)
		require.NoError(t, err)

		// nolint:errcheck
		defer os.Chmod(tempDir, 0o755) // Restore permissions for cleanup

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		// Test
		status, message, err := f.Health(context.Background(), false)

		// Assert
		require.Error(t, err)
		require.Equal(t, http.StatusInternalServerError, status)
		require.Equal(t, "File Store: Unable to create temporary file", message)
	})

	t.Run("write permission denied", func(t *testing.T) {
		// Setup
		tempDir, err := os.MkdirTemp("", "test-health-write-denied")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		// Make the directory read-only
		err = os.Chmod(tempDir, 0o555)
		require.NoError(t, err)

		// nolint:errcheck
		defer os.Chmod(tempDir, 0o755) // Restore permissions for cleanup

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		// Test
		status, message, err := f.Health(context.Background(), false)

		// Assert
		require.Error(t, err)
		require.Equal(t, http.StatusInternalServerError, status)
		require.Equal(t, "File Store: Unable to create temporary file", message)
	})
}

func TestFileWithURLHeaderFooter(t *testing.T) {
	t.Run("with header and footer in URL", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test-header-footer")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		// Create URL with header and footer parameters
		u, err := url.Parse(fmt.Sprintf("file://%s?header=START&eofmarker=END", tempDir))
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("test-key")
		content := "test content"

		// Test Set
		err = f.Set(context.Background(), key, fileformat.FileTypeTesting, []byte(content))
		require.NoError(t, err)

		// Read raw file to verify header and footer
		filename, err := f.options.ConstructFilename(tempDir, key, fileformat.FileTypeTesting)
		require.NoError(t, err)

		rawData, err := os.ReadFile(filename)
		require.NoError(t, err)

		// Verify header and footer are present in raw data
		expectedData := append([]byte("TESTING "), []byte(content)...)
		assert.Equal(t, expectedData, rawData)

		// Test Get - should return content without header/footer
		value, err := f.Get(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		assert.Equal(t, content, string(value))

		// Test GetIoReader - should return content without header/footer
		reader, err := f.GetIoReader(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		defer reader.Close()

		readContent, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, content, string(readContent))
	})
}

func TestWithSHA256Checksum(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "file_store_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	logger := ulogger.TestLogger{}

	storeURL := "file://" + tempDir
	value := []byte("test data")
	key := []byte("testkey123")
	fileType := fileformat.FileTypeTesting

	// Parse URL
	u, err := url.Parse(storeURL)
	require.NoError(t, err)

	// Create file store
	store, err := New(logger, u)
	require.NoError(t, err)

	// Set data with extension
	err = store.Set(context.Background(), key, fileType, value)
	require.NoError(t, err)

	// Construct expected filename
	filename, err := store.options.ConstructFilename(tempDir, key, fileType)
	require.NoError(t, err)

	// Verify main file exists and contains correct data
	data, err := os.ReadFile(filename)
	require.NoError(t, err)

	expectedData := make([]byte, len(fileType.ToMagicBytes())+len(value))
	ft := fileType.ToMagicBytes()
	copy(expectedData, ft[:])
	copy(expectedData[len(ft):], value)

	require.Equal(t, expectedData, data)

	// Check SHA256 file
	sha256Filename := filename + checksumExtension
	_, err = os.Stat(sha256Filename)

	require.NoError(t, err, "SHA256 file should exist")

	// Read and verify SHA256 file content
	hashFileContent, err := os.ReadFile(sha256Filename)
	require.NoError(t, err)

	// Calculate expected hash
	hasher := sha256.New()
	hasher.Write(ft[:])
	hasher.Write(value)
	expectedHash := fmt.Sprintf("%x", hasher.Sum(nil))

	// Verify hash file format
	hashFileStr := string(hashFileContent)
	parts := strings.Fields(hashFileStr)
	require.Len(t, parts, 2, "Hash file should have hash and filename separated by two spaces")

	// Verify hash matches
	require.Equal(t, expectedHash, parts[0], "Hash in file should match calculated hash")

	// Verify filename part
	expectedFilename := fmt.Sprintf("%x.%s", bt.ReverseBytes(key), fileType)
	require.Equal(t, expectedFilename, parts[1], "Filename in hash file should match expected format")
}

func TestSetFromReaderWithSHA256(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "file_store_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	logger := ulogger.TestLogger{}
	u, err := url.Parse("file://" + tempDir)
	require.NoError(t, err)

	store, err := New(logger, u)
	require.NoError(t, err)

	// Test data
	testData := []byte("test data for SetFromReader")
	key := []byte("testreaderkey")

	// Create reader
	reader := io.NopCloser(bytes.NewReader(testData))

	// Set data
	err = store.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, reader)
	require.NoError(t, err)

	// Construct filename
	merged := options.MergeOptions(store.options, []options.FileOption{})
	filename, err := merged.ConstructFilename(tempDir, key, fileformat.FileTypeTesting)
	require.NoError(t, err)

	// Verify main file
	data, err := os.ReadFile(filename)
	require.NoError(t, err)

	expectedData := make([]byte, len(fileformat.FileTypeTesting.ToMagicBytes())+len(testData))
	ft := fileformat.FileTypeTesting.ToMagicBytes()
	copy(expectedData, ft[:])
	copy(expectedData[len(ft):], testData)

	require.Equal(t, expectedData, data)

	// Verify SHA256 file
	sha256Filename := filename + checksumExtension
	hashFileContent, err := os.ReadFile(sha256Filename)
	require.NoError(t, err)

	// Calculate expected hash
	hasher := sha256.New()
	hasher.Write(ft[:])
	hasher.Write(testData)
	expectedHash := fmt.Sprintf("%x", hasher.Sum(nil))

	// Verify hash file format
	hashFileStr := string(hashFileContent)
	parts := strings.Fields(hashFileStr)
	require.Len(t, parts, 2)
	require.Equal(t, expectedHash, parts[0])

	// Verify filename part
	expectedFilename := fmt.Sprintf("%x.%s", bt.ReverseBytes(key), fileformat.FileTypeTesting)
	require.Equal(t, expectedFilename, parts[1])
}

func TestSHA256WithHeaderFooter(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "file_store_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	logger := ulogger.TestLogger{}
	u, err := url.Parse("file://" + tempDir)
	require.NoError(t, err)

	store, err := New(logger, u)
	require.NoError(t, err)

	// Test data
	data := []byte("test data")
	key := []byte("testheaderfooter")

	// Set data
	err = store.Set(context.Background(), key, fileformat.FileTypeTesting, data)
	require.NoError(t, err)

	// Construct filename
	merged := options.MergeOptions(store.options, []options.FileOption{})
	filename, err := merged.ConstructFilename(tempDir, key, fileformat.FileTypeTesting)
	require.NoError(t, err)

	// Verify main file includes header and footer
	fileContent, err := os.ReadFile(filename)
	require.NoError(t, err)

	expectedData := make([]byte, len(fileformat.FileTypeTesting.ToMagicBytes())+len(data))
	ft := fileformat.FileTypeTesting.ToMagicBytes()
	copy(expectedData, ft[:])
	copy(expectedData[len(ft):], data)

	assert.Equal(t, expectedData, fileContent)

	// Verify SHA256 file
	sha256Filename := filename + checksumExtension
	hashFileContent, err := os.ReadFile(sha256Filename)
	require.NoError(t, err)

	// Calculate expected hash (including header and footer)
	hasher := sha256.New()
	hasher.Write(expectedData)
	expectedHash := fmt.Sprintf("%x", hasher.Sum(nil))

	// Verify hash file format
	hashFileStr := string(hashFileContent)
	parts := strings.Fields(hashFileStr)
	require.Len(t, parts, 2)
	require.Equal(t, expectedHash, parts[0])
}

func TestFile_SetFromReader_WithHeaderAndFooter(t *testing.T) {
	dir := t.TempDir()

	// Use TestLogger instead of NewSimpleLogger
	logger := ulogger.TestLogger{}

	storeURL, _ := url.Parse("file://" + dir)
	q := storeURL.Query()
	q.Set("header", "header")

	storeURL.RawQuery = q.Encode()

	store, err := New(logger, storeURL)
	require.NoError(t, err)

	// Test data
	key := []byte("test")
	data := []byte("test data")
	reader := io.NopCloser(bytes.NewReader(data))

	// Set data
	err = store.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, reader)
	require.NoError(t, err)

	// Read file directly
	filename, err := store.options.ConstructFilename(dir, key, fileformat.FileTypeTesting)
	require.NoError(t, err)

	content, err := os.ReadFile(filename)
	require.NoError(t, err)

	// Verify content includes header
	expectedData := make([]byte, len(fileformat.FileTypeTesting.ToMagicBytes())+len(data))
	ft := fileformat.FileTypeTesting.ToMagicBytes()
	copy(expectedData, ft[:])
	copy(expectedData[len(ft):], data)

	assert.Equal(t, expectedData, content)
}

func TestFile_Set_WithHeaderAndFooter(t *testing.T) {
	dir := t.TempDir()

	store, err := New(
		ulogger.TestLogger{},
		&url.URL{Path: dir})
	require.NoError(t, err)

	// Test data
	key := []byte("test")
	data := []byte("test data")

	// Set data
	err = store.Set(context.Background(), key, fileformat.FileTypeTesting, data)
	require.NoError(t, err)

	// Read file directly
	filename, err := store.options.ConstructFilename(dir, key, fileformat.FileTypeTesting)
	require.NoError(t, err)

	content, err := os.ReadFile(filename)
	require.NoError(t, err)

	// Verify content includes header and footer
	expectedData := make([]byte, len(fileformat.FileTypeTesting.ToMagicBytes())+len(data))
	ft := fileformat.FileTypeTesting.ToMagicBytes()
	copy(expectedData, ft[:])
	copy(expectedData[len(ft):], data)

	assert.Equal(t, expectedData, content)
}

func TestFile_Get_WithHeaderAndFooter(t *testing.T) {
	dir := t.TempDir()

	store, err := New(
		ulogger.TestLogger{},
		&url.URL{Path: dir})
	require.NoError(t, err)

	// Test data
	key := []byte("test")
	data := []byte("test data")

	// Set data
	err = store.Set(context.Background(), key, fileformat.FileTypeTesting, data)
	require.NoError(t, err)

	// Get data
	retrieved, err := store.Get(context.Background(), key, fileformat.FileTypeTesting)
	require.NoError(t, err)

	// Verify retrieved data matches original (without header)
	assert.Equal(t, data, retrieved)
}

func TestFile_GetIoReader_WithHeaderAndFooter(t *testing.T) {
	dir := t.TempDir()

	store, err := New(
		ulogger.TestLogger{},
		&url.URL{Path: dir})
	require.NoError(t, err)

	// Test data
	key := []byte("test")
	data := []byte("test data")

	// Set data
	err = store.Set(context.Background(), key, fileformat.FileTypeTesting, data)
	require.NoError(t, err)

	// Get reader
	reader, err := store.GetIoReader(context.Background(), key, fileformat.FileTypeTesting)
	require.NoError(t, err)
	defer reader.Close()

	// Read data from reader
	retrieved, err := io.ReadAll(reader)
	require.NoError(t, err)

	// Verify retrieved data matches original (without header/footer)
	assert.Equal(t, data, retrieved)
}

func TestFileGetAndSetDAH(t *testing.T) {
	t.Skip("DAH functionality now requires pruner service - covered by e2e tests")
}

func TestFileURLParameters(t *testing.T) {
	t.Run("hashSuffix from URL", func(t *testing.T) {
		u, err := url.Parse("file://./data/subtreestore?hashSuffix=3")
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u, options.WithHashPrefix(1))
		require.NoError(t, err)

		// hashSuffix in URL should set HashPrefix to negative value
		require.Equal(t, -3, f.options.HashPrefix)
	})

	t.Run("invalid hashSuffix in URL", func(t *testing.T) {
		u, err := url.Parse("file://./data/subtreestore?hashSuffix=invalid")
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.Error(t, err)
		require.Nil(t, f)
		require.Contains(t, err.Error(), "failed to parse hashSuffix")
	})
}

func TestFileGetNonExistent(t *testing.T) {
	t.Run("get non-existent file", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test-get-nonexistent")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		// Try to get non-existent file
		key := []byte("nonexistent-key-1")
		_, err = f.Get(context.Background(), key, fileformat.FileTypeTesting)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNotFound))

		// Try to get non-existent file with options
		_, err = f.Get(context.Background(), key, fileformat.FileTypeTesting)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNotFound))
	})

	t.Run("get io reader for non-existent file", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test-get-reader-nonexistent")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		// Try to get reader for non-existent file
		key := []byte("nonexistent-key-2")
		reader, err := f.GetIoReader(context.Background(), key, fileformat.FileTypeTesting)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNotFound))
		require.Nil(t, reader)

		// Try to get reader for non-existent file with options
		reader, err = f.GetIoReader(context.Background(), key, fileformat.FileTypeTesting)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNotFound))
		require.Nil(t, reader)
	})
}

func TestFileChecksumNotDeletedOnDelete(t *testing.T) {
	// Get a temporary directory
	tempDir, err := os.MkdirTemp("", "test-checksum-delete-on-delete")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	u, err := url.Parse("file://" + tempDir)
	require.NoError(t, err)

	f, err := New(ulogger.TestLogger{}, u)
	require.NoError(t, err)

	key := "test-key-delete-checksum"
	content := []byte("test content")

	// Put a file with checksum
	err = f.Set(context.Background(), []byte(key), fileformat.FileTypeTesting, content)
	require.NoError(t, err)

	// Construct filename
	merged := options.MergeOptions(f.options, []options.FileOption{})
	filename, err := merged.ConstructFilename(tempDir, []byte(key), fileformat.FileTypeTesting)
	require.NoError(t, err)

	// Verify the checksum file exists
	_, err = os.Stat(filename)
	require.NoError(t, err, "checksum file should exist")

	// Delete the file
	err = f.Del(context.Background(), []byte(key), fileformat.FileTypeTesting)
	require.NoError(t, err, "file deletion should succeed")

	// Check if checksum file still exists - this is the bug
	_, err = os.Stat(filename)
	require.True(t, os.IsNotExist(err), "Checksum file should be removed when content file is deleted")
}
