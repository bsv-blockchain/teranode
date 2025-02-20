package file

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/blob/helper"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/ordishs/go-utils"
	"golang.org/x/exp/rand"
)

type File struct {
	path        string
	logger      ulogger.Logger
	options     *options.Options
	fileTTLs    map[string]time.Time
	fileTTLsMu  sync.Mutex
	fileTTLsCtx context.Context
}

/*
* Used for dev environments as replacement for lustre.
* Has background jobs to clean up TTL.
* Able to specify multiple folders - files will be spread across folders based on key/hash/filename supplied.
 */
func New(logger ulogger.Logger, storeURL *url.URL, opts ...options.StoreOption) (*File, error) {
	if storeURL == nil {
		return nil, errors.NewConfigurationError("storeURL is nil")
	}

	// Add header/footer handling
	if header := storeURL.Query().Get("header"); header != "" {
		// Try hex decode first
		headerBytes, err := hex.DecodeString(header)
		if err != nil {
			// If hex decode fails, use as plain text
			headerBytes = []byte(header)
		}

		opts = append(opts, options.WithHeader(headerBytes))
	}

	if eofMarker := storeURL.Query().Get("eofmarker"); eofMarker != "" {
		// Try hex decode first
		eofMarkerBytes, err := hex.DecodeString(eofMarker)
		if err != nil {
			// If hex decode fails, use as plain text
			eofMarkerBytes = []byte(eofMarker)
		}

		opts = append(opts, options.WithFooter(options.NewFooter(len(eofMarkerBytes), eofMarkerBytes, nil)))
	}

	// Add SHA256 handling from URL query parameter
	if storeURL.Query().Get("checksum") == "true" {
		opts = append(opts, options.WithSHA256Checksum())
	}

	if storeURL.Query().Get("ttlCleanerInterval") != "" {
		ttlCleanerInterval, err := time.ParseDuration(storeURL.Query().Get("ttlCleanerInterval"))
		if err != nil {
			return nil, errors.NewStorageError("[File] failed to parse ttlCleanerInterval", err)
		}

		return newStore(logger, storeURL, ttlCleanerInterval, opts...)
	}

	return newStore(logger, storeURL, 1*time.Minute, opts...)
}

func newStore(logger ulogger.Logger, storeURL *url.URL, ttlCleanerInterval time.Duration, opts ...options.StoreOption) (*File, error) {
	logger = logger.New("file")

	if storeURL == nil {
		return nil, errors.NewConfigurationError("storeURL is nil")
	}

	var path string
	if storeURL.Host == "." {
		path = storeURL.Path[1:] // relative path
	} else {
		path = storeURL.Path // absolute path
	}

	// create the path if necessary
	if len(path) > 0 {
		if err := os.MkdirAll(path, 0755); err != nil {
			return nil, errors.NewStorageError("[File] failed to create directory", err)
		}
	}

	fileOptions := options.NewStoreOptions(opts...)

	if hashPrefix := storeURL.Query().Get("hashPrefix"); len(hashPrefix) > 0 {
		val, err := strconv.ParseInt(hashPrefix, 10, 64)
		if err != nil {
			return nil, errors.NewStorageError("[File] failed to parse hashPrefix", err)
		}

		fileOptions.HashPrefix = int(val)
	}

	if hashSuffix := storeURL.Query().Get("hashSuffix"); len(hashSuffix) > 0 {
		val, err := strconv.ParseInt(hashSuffix, 10, 64)
		if err != nil {
			return nil, errors.NewStorageError("[File] failed to parse hashSuffix", err)
		}

		fileOptions.HashPrefix = -int(val)
	}

	if len(fileOptions.SubDirectory) > 0 {
		if err := os.MkdirAll(filepath.Join(path, fileOptions.SubDirectory), 0755); err != nil {
			return nil, errors.NewStorageError("[File] failed to create sub directory", err)
		}
	}

	fileStore := &File{
		path:        path,
		logger:      logger,
		options:     fileOptions,
		fileTTLs:    make(map[string]time.Time),
		fileTTLsCtx: context.Background(),
	}

	// load ttl's in background
	go func() {
		if err := fileStore.loadTTLs(); err != nil {
			fileStore.logger.Warnf("[File] failed to load ttls: %v", err)
		}
	}()

	go fileStore.ttlCleaner(fileStore.fileTTLsCtx, ttlCleanerInterval)

	return fileStore, nil
}

func (s *File) loadTTLs() error {
	s.logger.Infof("[File] Loading file TTLs: %s", s.path)

	// get all files in the directory that end with .ttl
	files, err := findFilesByExtension(s.path, ".ttl")
	if err != nil {
		return errors.NewStorageError("[File] failed to find ttl files", err)
	}

	var ttl *time.Time

	for _, fileName := range files {
		if fileName[len(fileName)-4:] != ".ttl" {
			continue
		}

		ttl, err = s.readTTLFromFile(fileName)
		if err != nil {
			return err
		}

		// check whether we actually have a ttl
		if ttl == nil {
			continue
		}

		s.fileTTLsMu.Lock()
		s.fileTTLs[fileName[:len(fileName)-4]] = *ttl
		s.fileTTLsMu.Unlock()
	}

	return nil
}

func (s *File) readTTLFromFile(fileName string) (*time.Time, error) {
	// read the ttl
	ttlBytes, err := os.ReadFile(fileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}

		return nil, errors.NewStorageError("[File] failed to read ttl file", err)
	}

	ttl, err := time.Parse(time.RFC3339, string(ttlBytes))
	if err != nil {
		return nil, errors.NewProcessingError("[File] failed to parse ttl from %s", fileName, err)
	}

	return &ttl, nil
}

func (s *File) ttlCleaner(ctx context.Context, interval time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			cleanExpiredFiles(s)
		}
	}
}

func cleanExpiredFiles(s *File) {
	s.logger.Debugf("[File] Cleaning file TTLs")

	filesToRemove := getExpiredFiles(s)
	for _, fileName := range filesToRemove {
		cleanupExpiredFile(s, fileName)
	}
}

func getExpiredFiles(s *File) []string {
	s.fileTTLsMu.Lock()
	filesToRemove := make([]string, 0, len(s.fileTTLs))

	for fileName, ttl := range s.fileTTLs {
		if ttl.Before(time.Now()) {
			filesToRemove = append(filesToRemove, fileName)
		}
	}
	s.fileTTLsMu.Unlock()

	return filesToRemove
}

func cleanupExpiredFile(s *File, fileName string) {
	// check if the ttl file still exists, even if the map says it has expired, another process might have updated it
	fileTTL, err := s.readTTLFromFile(fileName + ".ttl")
	if err != nil {
		s.logger.Warnf("[File] failed to read ttl from file: %s", fileName+".ttl")
		return
	}

	if fileTTL == nil {
		removeTTLFromMap(s, fileName)
		return
	}

	if !shouldRemoveFile(s, fileName, fileTTL) {
		return
	}

	removeFiles(s, fileName)
	removeTTLFromMap(s, fileName)
}

func shouldRemoveFile(s *File, fileName string, fileTTL *time.Time) bool {
	now := time.Now()
	if !fileTTL.Before(now) {
		// Update the TTL in our map
		s.fileTTLsMu.Lock()
		mapTTL := s.fileTTLs[fileName]
		s.fileTTLs[fileName] = *fileTTL
		s.fileTTLsMu.Unlock()

		s.logger.Warnf("[File] ttl file %s has expiry of %s, but map has %s",
			fileName+".ttl",
			fileTTL.UTC().Format(time.RFC3339),
			mapTTL.UTC().Format(time.RFC3339))

		return false
	}

	return true
}

func removeFiles(s *File, fileName string) {
	if err := os.Remove(fileName); err != nil && !os.IsNotExist(err) {
		s.logger.Warnf("[File] failed to remove file: %s", fileName)
	}

	if err := os.Remove(fileName + ".ttl"); err != nil && !os.IsNotExist(err) {
		s.logger.Warnf("[File] failed to remove ttl file: %s", fileName+".ttl")
	}
}

func removeTTLFromMap(s *File, fileName string) {
	s.fileTTLsMu.Lock()
	delete(s.fileTTLs, fileName)
	s.fileTTLsMu.Unlock()
}

func (s *File) Health(_ context.Context, _ bool) (int, string, error) {
	// Check if the path exists
	if _, err := os.Stat(s.path); os.IsNotExist(err) {
		return http.StatusInternalServerError, "File Store: Path does not exist", err
	}

	// Create a temporary file to test read/write/delete permissions
	tempFile, err := os.CreateTemp(s.path, "health-check-*.tmp")
	if err != nil {
		return http.StatusInternalServerError, "File Store: Unable to create temporary file", err
	}

	tempFileName := tempFile.Name()
	defer os.Remove(tempFileName) // Ensure the temp file is removed

	// Test write permission
	testData := []byte("health check")
	if _, err := tempFile.Write(testData); err != nil {
		return http.StatusInternalServerError, "File Store: Unable to write to file", err
	}

	tempFile.Close()

	// Test read permission
	readData, err := os.ReadFile(tempFileName)
	if err != nil {
		return http.StatusInternalServerError, "File Store: Unable to read file", err
	}

	if !bytes.Equal(readData, testData) {
		return http.StatusInternalServerError, "File Store: Data integrity check failed", nil
	}

	// Test delete permission
	if err := os.Remove(tempFileName); err != nil {
		return http.StatusInternalServerError, "File Store: Unable to delete file", err
	}

	return http.StatusOK, "File Store: Healthy", nil
}

func (s *File) Close(_ context.Context) error {
	// stop ttl cleaner
	s.fileTTLsCtx.Done()

	return nil
}

func (s *File) errorOnOverwrite(filename string, opts *options.Options) error {
	if !opts.AllowOverwrite {
		if _, err := os.Stat(filename); err == nil {
			return errors.NewBlobAlreadyExistsError("[File][allowOverwrite] [%s] already exists in store", filename)
		}
	}

	return nil
}

func (s *File) SetFromReader(_ context.Context, key []byte, reader io.ReadCloser, opts ...options.FileOption) error {
	filename, err := s.constructFilenameWithTTL(key, opts)
	if err != nil {
		return errors.NewStorageError("[File][SetFromReader] [%s] failed to get file name", utils.ReverseAndHexEncodeSlice(key), err)
	}

	merged := options.MergeOptions(s.options, opts)

	if err := s.errorOnOverwrite(filename, merged); err != nil {
		return err
	}

	tmpFilename := fmt.Sprintf("%s.%d.tmp", filename, rand.Int())

	// Create the file first
	file, err := os.Create(tmpFilename)
	if err != nil {
		return errors.NewStorageError("[File][SetFromReader] [%s] failed to create file", filename, err)
	}
	defer file.Close()

	// Set up the writer and hasher
	writer, hasher := s.createWriter(file, merged)

	if merged.Header != nil {
		if _, err = writer.Write(merged.Header); err != nil {
			return errors.NewStorageError("[File][SetFromReader] [%s] failed to write header to file", filename, err)
		}
	}

	if _, err = io.Copy(writer, reader); err != nil {
		return errors.NewStorageError("[File][SetFromReader] [%s] failed to write data to file", filename, err)
	}

	if merged.Footer != nil {
		b, err := merged.Footer.GetFooter()
		if err != nil {
			return errors.NewStorageError("[File][SetFromReader] [%s] failed to write footer to file", filename, err)
		}

		if _, err = writer.Write(b); err != nil {
			return errors.NewStorageError("[File][SetFromReader] [%s] failed to write footer to file", filename, err)
		}
	}

	// rename the file to remove the .tmp extension
	if err = os.Rename(tmpFilename, filename); err != nil {
		return errors.NewStorageError("[File][SetFromReader] [%s] failed to rename file from tmp", filename, err)
	}

	// Write SHA256 hash file
	if err = s.writeHashFile(hasher, filename); err != nil {
		return errors.NewStorageError("[File][SetFromReader] failed to write hash file", err)
	}

	return nil
}

func (s *File) createWriter(file *os.File, opts *options.Options) (io.Writer, hash.Hash) {
	// Then set up the writer and hasher
	var (
		writer io.Writer
		hasher hash.Hash
	)

	// Only create hasher if SHA256 is enabled
	if opts.GenerateSHA256 {
		hasher = sha256.New()
		writer = io.MultiWriter(file, hasher)
	} else {
		writer = file
	}

	return writer, hasher
}

func (s *File) createWriterToBuffer(buf *bytes.Buffer, opts *options.Options) (io.Writer, hash.Hash) {
	// Set up the writer and hasher
	var (
		writer io.Writer = buf
		hasher hash.Hash
	)

	// Only create hasher if SHA256 is enabled
	if opts.GenerateSHA256 {
		hasher = sha256.New()
		writer = io.MultiWriter(writer, hasher)
	}

	return writer, hasher
}

func (s *File) writeHashFile(hasher hash.Hash, filename string) error {
	if hasher == nil {
		return nil
	}

	// Get the base name and extension separately
	base := filepath.Base(filename)

	// Format: "<hash>  <reversed_key>.<extension>\n"
	hashStr := fmt.Sprintf("%x  %s\n", // N.B. The 2 spaces is important for the hash to be valid
		hasher.Sum(nil),
		base)

	hashFilename := filename + ".sha256"
	tmpHashFilename := hashFilename + ".tmp"

	//nolint:gosec // G306: Expect WriteFile permissions to be 0600 or less (gosec)
	if err := os.WriteFile(tmpHashFilename, []byte(hashStr), 0644); err != nil {
		return errors.NewStorageError("[File] failed to write hash file", err)
	}

	if err := os.Rename(tmpHashFilename, hashFilename); err != nil {
		return errors.NewStorageError("[File] failed to rename hash file from tmp", err)
	}

	return nil
}

func (s *File) Set(_ context.Context, key []byte, value []byte, opts ...options.FileOption) error {
	filename, err := s.constructFilenameWithTTL(key, opts)
	if err != nil {
		return errors.NewStorageError("[File][Set] [%s] failed to get file name", utils.ReverseAndHexEncodeSlice(key), err)
	}

	merged := options.MergeOptions(s.options, opts)

	if err := s.errorOnOverwrite(filename, merged); err != nil {
		return err
	}

	tmpFilename := fmt.Sprintf("%s.%d.tmp", filename, rand.Int())

	// Set up writers
	var buf bytes.Buffer
	writer, hasher := s.createWriterToBuffer(&buf, merged)

	// Write header if present
	if merged.Header != nil {
		if _, err := writer.Write(merged.Header); err != nil {
			return errors.NewStorageError("[File][Set] [%s] failed to write header", filename, err)
		}
	}

	// Write main content
	if _, err := writer.Write(value); err != nil {
		return errors.NewStorageError("[File][Set] [%s] failed to write content", filename, err)
	}

	// Write footer if present
	if merged.Footer != nil {
		b, err := merged.Footer.GetFooter()
		if err != nil {
			return errors.NewStorageError("[File][Set] [%s] failed to get footer", filename, err)
		}

		if _, err := writer.Write(b); err != nil {
			return errors.NewStorageError("[File][Set] [%s] failed to write footer", filename, err)
		}
	}

	// Write bytes to file
	//nolint:gosec // G306: Expect WriteFile permissions to be 0600 or less (gosec)
	if err = os.WriteFile(tmpFilename, buf.Bytes(), 0644); err != nil {
		return errors.NewStorageError("[File][Set] [%s] failed to write data to file", filename, err)
	}

	if err = os.Rename(tmpFilename, filename); err != nil {
		return errors.NewStorageError("[File][Set] [%s] failed to rename file from tmp", filename, err)
	}

	// Write SHA256 hash file
	if err = s.writeHashFile(hasher, filename); err != nil {
		return errors.NewStorageError("[File][Set] failed to write hash file", err)
	}

	return nil
}

func (s *File) constructFilenameWithTTL(hash []byte, opts []options.FileOption) (string, error) {
	merged := options.MergeOptions(s.options, opts)

	if merged.SubDirectory != "" {
		if err := os.MkdirAll(filepath.Join(s.path, merged.SubDirectory), 0755); err != nil {
			return "", errors.NewStorageError("[File] failed to create sub directory", err)
		}
	}

	fileName, err := merged.ConstructFilename(s.path, hash)
	if err != nil {
		return "", err
	}

	if merged.TTL != nil && *merged.TTL > 0 {
		// write bytes to file
		ttl := time.Now().Add(*merged.TTL)

		ttlFilename := fileName + ".ttl"
		ttlTempFilename := ttlFilename + ".tmp"

		//nolint:gosec // G306: Expect WriteFile permissions to be 0600 or less (gosec)
		if err = os.WriteFile(ttlTempFilename, []byte(ttl.Format(time.RFC3339)), 0644); err != nil {
			return "", errors.NewStorageError("[File][%s] failed to write ttl to file", err)
		}

		if err = os.Rename(ttlTempFilename, ttlFilename); err != nil {
			return "", errors.NewStorageError("[File][%s] failed to rename file from tmp", ttlFilename, err)
		}

		s.fileTTLsMu.Lock()
		s.fileTTLs[fileName] = ttl
		s.fileTTLsMu.Unlock()
	} else {
		// delete ttl file, if it existed
		_ = os.Remove(fileName + ".ttl")

		s.fileTTLsMu.Lock()
		delete(s.fileTTLs, fileName)
		s.fileTTLsMu.Unlock()
	}

	return fileName, nil
}

// create a global limiting semaphore for setTTL file operations
var setTTLFileSemaphore = make(chan struct{}, 256)

func (s *File) SetTTL(_ context.Context, key []byte, newTTL time.Duration, opts ...options.FileOption) error {
	// limit the number of concurrent file operations
	setTTLFileSemaphore <- struct{}{}
	defer func() {
		<-setTTLFileSemaphore
	}()

	merged := options.MergeOptions(s.options, opts)

	fileName, err := merged.ConstructFilename(s.path, key)
	if err != nil {
		return errors.NewStorageError("[File] failed to get file name", err)
	}

	if newTTL == 0 {
		s.fileTTLsMu.Lock()
		delete(s.fileTTLs, fileName)
		s.fileTTLsMu.Unlock()

		// delete the ttl file
		if err = os.Remove(fileName + ".ttl"); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}

			return errors.NewStorageError("[File] failed to remove ttl file", err)
		}

		return nil
	}

	// make sure the file exists
	if _, err = os.Stat(fileName); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return errors.ErrNotFound
		}

		return errors.NewStorageError("[File][%s] failed to get file info", fileName, err)
	}

	// write bytes to file
	ttl := time.Now().Add(newTTL).UTC()

	ttlFilename := fileName + ".ttl"
	ttlTempFilename := ttlFilename + ".tmp"

	//nolint:gosec // G306: Expect WriteFile permissions to be 0600 or less (gosec)
	if err = os.WriteFile(ttlTempFilename, []byte(ttl.Format(time.RFC3339)), 0644); err != nil {
		return errors.NewStorageError("failed to write ttl to file", err)
	}

	if err = os.Rename(ttlTempFilename, ttlFilename); err != nil {
		return errors.NewStorageError("[File][%s] failed to rename file from tmp", ttlFilename, err)
	}

	s.fileTTLsMu.Lock()
	s.fileTTLs[fileName] = ttl
	s.fileTTLsMu.Unlock()

	return nil
}

func (s *File) GetTTL(ctx context.Context, key []byte, opts ...options.FileOption) (time.Duration, error) {
	merged := options.MergeOptions(s.options, opts)

	fileName, err := merged.ConstructFilename(s.path, key)
	if err != nil {
		return 0, err
	}

	exists, err := s.Exists(ctx, key, opts...)
	if err != nil {
		return 0, err
	}

	if !exists {
		return 0, errors.ErrNotFound
	}

	s.fileTTLsMu.Lock()
	ttl, ok := s.fileTTLs[fileName]
	s.fileTTLsMu.Unlock()

	if !ok {
		return 0, nil
	}

	return time.Until(ttl), nil
}

func (s *File) GetIoReader(_ context.Context, hash []byte, opts ...options.FileOption) (io.ReadCloser, error) {
	merged := options.MergeOptions(s.options, opts)

	fileName, err := merged.ConstructFilename(s.path, hash)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(fileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, errors.ErrNotFound
		}

		return nil, errors.NewStorageError("[File][GetIoReader] [%s] unable to open file", fileName, err)
	}

	return helper.ReaderWithHeaderAndFooterRemoved(f, merged.Header, merged.Footer)
}

func (s *File) Get(_ context.Context, hash []byte, opts ...options.FileOption) ([]byte, error) {
	s.logger.Debugf("[File] Get: %s", utils.ReverseAndHexEncodeSlice(hash))

	merged := options.MergeOptions(s.options, opts)

	filename, err := merged.ConstructFilename(s.path, hash)
	if err != nil {
		return nil, err
	}

	b, err := os.ReadFile(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, errors.ErrNotFound
		}

		return nil, errors.NewStorageError("[File][Get] [%s] failed to read data from file", filename, err)
	}

	b, err = helper.BytesWithHeadAndFooterRemoved(b, merged.Header, merged.Footer)
	if err != nil {
		return nil, errors.NewStorageError("[File][Get] [%s] failed to remove header and footer", filename, err)
	}

	return b, nil
}

func (s *File) GetHead(_ context.Context, hash []byte, nrOfBytes int, opts ...options.FileOption) ([]byte, error) {
	s.logger.Debugf("[File] Get: %s", utils.ReverseAndHexEncodeSlice(hash))

	merged := options.MergeOptions(s.options, opts)

	fileName, err := merged.ConstructFilename(s.path, hash)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(fileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, errors.ErrNotFound
		}

		return nil, errors.NewStorageError("[File] [%s] failed to open file", fileName, err)
	}

	bytes := make([]byte, nrOfBytes)
	nRead, err := file.Read(bytes)

	if err != nil {
		return nil, errors.NewStorageError("[File][GetHead] [%s] failed to read data from file", fileName, err)
	}

	return bytes[:nRead], err
}

func (s *File) Exists(_ context.Context, hash []byte, opts ...options.FileOption) (bool, error) {
	s.logger.Debugf("[File] Exists: %s", utils.ReverseAndHexEncodeSlice(hash))

	merged := options.MergeOptions(s.options, opts)

	fileName, err := merged.ConstructFilename(s.path, hash)
	if err != nil {
		return false, err
	}

	_, err = os.Stat(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}

		return false, errors.NewStorageError("[File][Exists] [%s] failed to read data from file", fileName, err)
	}

	return true, nil
}

func (s *File) Del(_ context.Context, hash []byte, opts ...options.FileOption) error {
	s.logger.Debugf("[File] Del: %s", utils.ReverseAndHexEncodeSlice(hash))

	merged := options.MergeOptions(s.options, opts)

	fileName, err := merged.ConstructFilename(s.path, hash)
	if err != nil {
		return err
	}

	// remove ttl file, if exists
	_ = os.Remove(fileName + ".ttl")

	return os.Remove(fileName)
}

func findFilesByExtension(root, ext string) ([]string, error) {
	var a []string

	err := filepath.WalkDir(root, func(s string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}

		if filepath.Ext(d.Name()) == ext {
			a = append(a, s)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return a, nil
}

func (s *File) GetHeader(ctx context.Context, hash []byte, opts ...options.FileOption) ([]byte, error) {
	merged := options.MergeOptions(s.options, opts)

	if merged.Header == nil {
		return nil, nil
	}

	fileName, err := merged.ConstructFilename(s.path, hash)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(fileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, errors.ErrNotFound
		}

		return nil, errors.NewStorageError("[File][GetHeader] [%s] unable to open file", fileName, err)
	}

	defer f.Close()

	return helper.HeaderMetaData(f, len(merged.Header))
}

func (s *File) GetFooterMetaData(ctx context.Context, hash []byte, opts ...options.FileOption) ([]byte, error) {
	merged := options.MergeOptions(s.options, opts)

	if merged.Footer == nil {
		return nil, nil
	}

	fileName, err := merged.ConstructFilename(s.path, hash)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(fileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, errors.ErrNotFound
		}

		return nil, errors.NewStorageError("[File][GetMetaData] [%s] unable to open file", fileName, err)
	}

	defer f.Close()

	return helper.FooterMetaData(f, merged.Footer)
}
