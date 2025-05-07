package file

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/blob/helper"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/ordishs/go-utils"
)

const checksumExtension = ".sha256"

type File struct {
	path               string
	logger             ulogger.Logger
	options            *options.Options
	fileDAHs           map[string]uint32
	fileDAHsMu         sync.Mutex
	fileDAHsCtxCancel  context.CancelFunc
	currentBlockHeight atomic.Uint32
	persistSubDir      string
	longtermClient     longtermStore
}

type longtermStore interface {
	Get(ctx context.Context, key []byte, opts ...options.FileOption) ([]byte, error)
	GetIoReader(ctx context.Context, key []byte, opts ...options.FileOption) (io.ReadCloser, error)
	Exists(ctx context.Context, key []byte, opts ...options.FileOption) (bool, error)
	GetFooterMetaData(ctx context.Context, key []byte, opts ...options.FileOption) ([]byte, error)
	GetHeader(ctx context.Context, key []byte, opts ...options.FileOption) ([]byte, error)
}

// create a global limiting semaphore for file operations
var fileSemaphore = make(chan struct{}, 1024)

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

	if storeURL.Query().Get("dahCleanerInterval") != "" {
		dahCleanerInterval, err := time.ParseDuration(storeURL.Query().Get("dahCleanerInterval"))
		if err != nil {
			return nil, errors.NewStorageError("[File] failed to parse dahCleanerInterval", err)
		}

		return newStore(logger, storeURL, dahCleanerInterval, opts...)
	}

	return newStore(logger, storeURL, 1*time.Minute, opts...)
}

// globally limit the number of cleaners that are started to one
var fileCleanerOnce sync.Map

func newStore(logger ulogger.Logger, storeURL *url.URL, dahCleanerInterval time.Duration, opts ...options.StoreOption) (*File, error) {
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

	options := options.NewStoreOptions(opts...)

	if hashPrefix := storeURL.Query().Get("hashPrefix"); len(hashPrefix) > 0 {
		val, err := strconv.ParseInt(hashPrefix, 10, 64)
		if err != nil {
			return nil, errors.NewStorageError("[File] failed to parse hashPrefix", err)
		}

		options.HashPrefix = int(val)
	}

	if hashSuffix := storeURL.Query().Get("hashSuffix"); len(hashSuffix) > 0 {
		val, err := strconv.ParseInt(hashSuffix, 10, 64)
		if err != nil {
			return nil, errors.NewStorageError("[File] failed to parse hashSuffix", err)
		}

		options.HashPrefix = -int(val)
	}

	if len(options.SubDirectory) > 0 {
		if err := os.MkdirAll(filepath.Join(path, options.SubDirectory), 0755); err != nil {
			return nil, errors.NewStorageError("[File] failed to create sub directory", err)
		}
	}

	fileDAHsCtx, fileDAHsCtxCancel := context.WithCancel(context.Background())

	fileStore := &File{
		path:              path,
		logger:            logger,
		options:           options,
		fileDAHs:          make(map[string]uint32),
		fileDAHsCtxCancel: fileDAHsCtxCancel,
		persistSubDir:     options.PersistSubDir,
	}

	// Check if longterm storage options are provided
	if options.PersistSubDir != "" {
		// Create persistent subdirectory
		if err := os.MkdirAll(filepath.Join(path, options.PersistSubDir), 0755); err != nil {
			return nil, errors.NewStorageError("[File] failed to create persist sub directory", err)
		}

		// Initialize longterm storage client if URL is provided
		if options.LongtermStoreURL != nil {
			var err error

			fileStore.longtermClient, err = newStore(logger, options.LongtermStoreURL, 0)
			if err != nil {
				return nil, errors.NewStorageError("[File] failed to create longterm client", err)
			}
		}
	}

	if dahCleanerInterval != 0 {
		_, loaded := fileCleanerOnce.LoadOrStore(storeURL.String(), true)
		if !loaded { // i.e. it was stored and not loaded
			// load dah's in the background and start the dah cleaner
			go func() {
				if err := fileStore.loadDAHs(); err != nil {
					fileStore.logger.Warnf("[File] failed to load dahs: %v", err)
				}

				// start the dah cleaner
				go fileStore.dahCleaner(fileDAHsCtx, dahCleanerInterval)
			}()
		}
	}

	return fileStore, nil
}

func (s *File) SetCurrentBlockHeight(height uint32) {
	s.currentBlockHeight.Store(height)
}

func (s *File) loadDAHs() error {
	s.logger.Infof("[File] Loading file DAHs: %s", s.path)

	// get all files in the directory that end with .dah
	files, err := findFilesByExtension(s.path, ".dah")
	if err != nil {
		return errors.NewStorageError("[File] failed to find DAH files", err)
	}

	var dah uint32

	for _, fileName := range files {
		if fileName[len(fileName)-4:] != ".dah" {
			continue
		}

		dah, err = s.readDAHFromFile(fileName)
		if err != nil {
			return err
		}

		// check whether we actually have a dah
		if dah == 0 {
			continue
		}

		s.fileDAHsMu.Lock()
		s.fileDAHs[fileName[:len(fileName)-4]] = dah
		s.fileDAHsMu.Unlock()
	}

	return nil
}

func (s *File) readDAHFromFile(fileName string) (uint32, error) {
	fileSemaphore <- struct{}{}
	defer func() {
		<-fileSemaphore
	}()

	// read the dah
	dahBytes, err := os.ReadFile(fileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}

		return 0, errors.NewStorageError("[File] failed to read DAH file", err)
	}

	dah, err := strconv.ParseUint(string(dahBytes), 10, 32)
	if err != nil {
		return 0, errors.NewProcessingError("[File] failed to parse DAH from %s", fileName, err)
	}

	return uint32(dah), nil
}

func (s *File) dahCleaner(ctx context.Context, interval time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			s.cleanupExpiredFiles()
		}
	}
}

func (s *File) cleanupExpiredFiles() {
	s.logger.Debugf("[File] Cleaning file DAHs")

	filesToRemove := s.getExpiredFiles()
	for _, fileName := range filesToRemove {
		s.cleanupExpiredFile(fileName)
	}
}

func (s *File) getExpiredFiles() []string {
	s.fileDAHsMu.Lock()
	filesToRemove := make([]string, 0, len(s.fileDAHs))

	currentBlockHeight := s.currentBlockHeight.Load()

	for fileName, dah := range s.fileDAHs {
		if dah <= currentBlockHeight {
			filesToRemove = append(filesToRemove, fileName)
		}
	}
	s.fileDAHsMu.Unlock()

	return filesToRemove
}

func (s *File) cleanupExpiredFile(fileName string) {
	// check if the DAH file still exists, even if the map says it has expired, another process might have updated it
	dah, err := s.readDAHFromFile(fileName + ".dah")
	if err != nil {
		s.logger.Warnf("[File] failed to read DAH from file: %s", fileName+".dah")
		return
	}

	if dah == 0 {
		s.removeDAHFromMap(fileName)
		return
	}

	if !s.shouldRemoveFile(fileName, dah) {
		return
	}

	s.removeFiles(fileName)
	s.removeDAHFromMap(fileName)
}

func (s *File) shouldRemoveFile(fileName string, fileDAH uint32) bool {
	currentBlockHeight := s.currentBlockHeight.Load()

	if fileDAH > currentBlockHeight {
		// Update the DAH in our map
		s.fileDAHsMu.Lock()
		mapDAH := s.fileDAHs[fileName]
		s.fileDAHs[fileName] = fileDAH
		s.fileDAHsMu.Unlock()

		s.logger.Warnf("[File] DAH file %s has DAH of %d, but map has %d",
			fileName+".dah",
			fileDAH,
			mapDAH)

		return false
	}

	return true
}

func (s *File) removeFiles(fileName string) {
	fileSemaphore <- struct{}{}
	defer func() {
		<-fileSemaphore
	}()

	if err := os.Remove(fileName); err != nil && !os.IsNotExist(err) {
		s.logger.Warnf("[File] failed to remove file: %s", fileName)
	}

	if err := os.Remove(fileName + ".dah"); err != nil && !os.IsNotExist(err) {
		s.logger.Warnf("[File] failed to remove DAH file: %s", fileName+".dah")
	}

	if err := os.Remove(fileName + checksumExtension); err != nil && !os.IsNotExist(err) {
		s.logger.Warnf("[File] failed to remove checksum file: %s", fileName+checksumExtension)
	}
}

func (s *File) removeDAHFromMap(fileName string) {
	s.fileDAHsMu.Lock()
	delete(s.fileDAHs, fileName)
	s.fileDAHsMu.Unlock()
}

func (s *File) Health(_ context.Context, _ bool) (int, string, error) {
	fileSemaphore <- struct{}{}
	defer func() {
		<-fileSemaphore
	}()

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
	// stop DAH cleaner
	s.fileDAHsCtxCancel()

	return nil
}

func (s *File) errorOnOverwrite(filename string, opts *options.Options) error {
	if !opts.AllowOverwrite {
		fileSemaphore <- struct{}{}
		defer func() {
			<-fileSemaphore
		}()

		if _, err := os.Stat(filename); err == nil {
			return errors.NewBlobAlreadyExistsError("[File][allowOverwrite] [%s] already exists in store", filename)
		}
	}

	return nil
}

func (s *File) SetFromReader(_ context.Context, key []byte, reader io.ReadCloser, opts ...options.FileOption) error {
	fileSemaphore <- struct{}{}
	defer func() {
		<-fileSemaphore
	}()

	filename, err := s.constructFilename(key, opts)
	if err != nil {
		return errors.NewStorageError("[File][SetFromReader] [%s] failed to get file name", utils.ReverseAndHexEncodeSlice(key), err)
	}

	merged := options.MergeOptions(s.options, opts)

	if err := s.errorOnOverwrite(filename, merged); err != nil {
		return err
	}

	// Generate a cryptographically secure random number
	randNum, err := rand.Int(rand.Reader, big.NewInt(1<<63-1))
	if err != nil {
		return errors.NewStorageError("[File][SetFromReader] failed to generate random number", err)
	}

	tmpFilename := fmt.Sprintf("%s.%d.tmp", filename, randNum)

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
	fileSemaphore <- struct{}{}
	defer func() {
		<-fileSemaphore
	}()

	if hasher == nil {
		return nil
	}

	// Get the base name and extension separately
	base := filepath.Base(filename)

	// Format: "<hash>  <reversed_key>.<extension>\n"
	hashStr := fmt.Sprintf("%x  %s\n", // N.B. The 2 spaces is important for the hash to be valid
		hasher.Sum(nil),
		base)

	hashFilename := filename + checksumExtension
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
	fileSemaphore <- struct{}{}
	defer func() {
		<-fileSemaphore
	}()

	filename, err := s.constructFilename(key, opts)
	if err != nil {
		return errors.NewStorageError("[File][Set] [%s] failed to get file name", utils.ReverseAndHexEncodeSlice(key), err)
	}

	merged := options.MergeOptions(s.options, opts)

	if err := s.errorOnOverwrite(filename, merged); err != nil {
		return err
	}

	// Generate a cryptographically secure random number
	randNum, err := rand.Int(rand.Reader, big.NewInt(1<<63-1))
	if err != nil {
		return errors.NewStorageError("[File][Set] failed to generate random number", err)
	}

	tmpFilename := fmt.Sprintf("%s.%d.tmp", filename, randNum)

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

func (s *File) constructFilename(hash []byte, opts []options.FileOption) (string, error) {
	fileSemaphore <- struct{}{}
	defer func() {
		<-fileSemaphore
	}()

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

	currentBlockHeight := s.currentBlockHeight.Load()

	dah := merged.DAH
	if dah == 0 && merged.BlockHeightRetention > 0 {
		// write DAH to file
		dah = currentBlockHeight + merged.BlockHeightRetention
	}

	if dah > 0 {
		dahFilename := fileName + ".dah"
		dahTempFilename := dahFilename + ".tmp"

		//nolint:gosec // G306: Expect WriteFile permissions to be 0600 or less (gosec)
		if err = os.WriteFile(dahTempFilename, []byte(strconv.FormatUint(uint64(dah), 10)), 0644); err != nil {
			return "", errors.NewStorageError("[File][%s] failed to write DAH to file", dahTempFilename, err)
		}

		if err := os.Rename(dahTempFilename, dahFilename); err != nil {
			return "", errors.NewStorageError("[File][%s] failed to rename file from tmp", dahFilename, err)
		}

		s.fileDAHsMu.Lock()
		s.fileDAHs[fileName] = dah
		s.fileDAHsMu.Unlock()
	} else {
		// delete DAH file, if it existed
		_ = os.Remove(fileName + ".dah")

		s.fileDAHsMu.Lock()
		delete(s.fileDAHs, fileName)
		s.fileDAHsMu.Unlock()
	}

	return fileName, nil
}

func (s *File) SetDAH(_ context.Context, key []byte, newDAH uint32, opts ...options.FileOption) error {
	// limit the number of concurrent file operations
	fileSemaphore <- struct{}{}
	defer func() {
		<-fileSemaphore
	}()

	merged := options.MergeOptions(s.options, opts)

	fileName, err := merged.ConstructFilename(s.path, key)
	if err != nil {
		return errors.NewStorageError("[File] failed to get file name", err)
	}

	if newDAH == 0 {
		s.fileDAHsMu.Lock()
		delete(s.fileDAHs, fileName)
		s.fileDAHsMu.Unlock()

		// delete the DAH file
		if err = os.Remove(fileName + ".dah"); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}

			return errors.NewStorageError("[File][%s] failed to remove DAH file", fileName, err)
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
	dahFilename := fileName + ".dah"
	dahTempFilename := dahFilename + ".tmp"

	//nolint:gosec // G306: Expect WriteFile permissions to be 0600 or less (gosec)
	if err = os.WriteFile(dahTempFilename, []byte(strconv.FormatUint(uint64(newDAH), 10)), 0644); err != nil {
		return errors.NewStorageError("[File][%s] failed to write DAH to file", dahTempFilename, err)
	}

	if err = os.Rename(dahTempFilename, dahFilename); err != nil {
		return errors.NewStorageError("[File][%s] failed to rename file from tmp", dahFilename, err)
	}

	s.fileDAHsMu.Lock()
	s.fileDAHs[fileName] = newDAH
	s.fileDAHsMu.Unlock()

	return nil
}

func (s *File) GetDAH(ctx context.Context, key []byte, opts ...options.FileOption) (uint32, error) {
	merged := options.MergeOptions(s.options, opts)

	fileName, err := merged.ConstructFilename(s.path, key)
	if err != nil {
		return 0, err
	}

	// Check if the file (not the DAH file) exists
	exists, err := s.Exists(ctx, key, opts...)
	if err != nil {
		return 0, err
	}

	// If the file doesn't exist, why are we trying to get the DAH?  Return an error
	if !exists {
		return 0, errors.ErrNotFound
	}

	// Get the DAH from the map
	s.fileDAHsMu.Lock()
	dah, ok := s.fileDAHs[fileName]
	s.fileDAHsMu.Unlock()

	if !ok {
		return 0, nil
	}

	return dah, nil
}

func (s *File) GetIoReader(_ context.Context, hash []byte, opts ...options.FileOption) (io.ReadCloser, error) {
	fileSemaphore <- struct{}{}
	defer func() {
		<-fileSemaphore
	}()

	merged := options.MergeOptions(s.options, opts)

	fileName, err := merged.ConstructFilename(s.path, hash)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(fileName)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, errors.NewStorageError("[File][GetIoReader] [%s] failed to open file", fileName, err)
		}

		if s.persistSubDir != "" {
			persistedFilename, err := merged.ConstructFilename(filepath.Join(s.path, s.persistSubDir), hash)
			if err != nil {
				return nil, err
			}

			f, err = os.Open(persistedFilename)
			if err != nil {
				if !errors.Is(err, os.ErrNotExist) {
					return nil, errors.NewStorageError("[File][GetIoReader] [%s] failed to open file in persist directory", persistedFilename, err)
				}

				if s.longtermClient != nil {
					fileReader, err := s.longtermClient.GetIoReader(context.Background(), hash, opts...)
					if err != nil {
						if errors.Is(err, os.ErrNotExist) {
							return nil, errors.ErrNotFound
						}

						return nil, errors.NewStorageError("[File][GetIoReader] [%s] unable to open longterm storage file", fileName, err)
					}

					return helper.ReaderWithHeaderAndFooterRemoved(fileReader, merged.Header, merged.Footer)
				}

				return nil, errors.ErrNotFound
			}
		} else {
			if s.longtermClient != nil {
				fileReader, err := s.longtermClient.GetIoReader(context.Background(), hash, opts...)
				if err != nil {
					if errors.Is(err, os.ErrNotExist) {
						return nil, errors.ErrNotFound
					}

					return nil, errors.NewStorageError("[File][GetIoReader] [%s] unable to open longterm storage file", fileName, err)
				}

				return helper.ReaderWithHeaderAndFooterRemoved(fileReader, merged.Header, merged.Footer)
			}

			return nil, errors.ErrNotFound
		}
	}

	return helper.ReaderWithHeaderAndFooterRemoved(f, merged.Header, merged.Footer)
}

func (s *File) Get(_ context.Context, hash []byte, opts ...options.FileOption) ([]byte, error) {
	fileSemaphore <- struct{}{}
	defer func() {
		<-fileSemaphore
	}()

	s.logger.Debugf("[File] Get: %s", utils.ReverseAndHexEncodeSlice(hash))

	merged := options.MergeOptions(s.options, opts)

	filename, err := merged.ConstructFilename(s.path, hash)
	if err != nil {
		return nil, err
	}

	b, err := os.ReadFile(filename)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, errors.NewStorageError("[File][Get] [%s] failed to read data from file", filename, err)
		}

		// If file not found in primary location, check persistent directory
		if s.persistSubDir != "" {
			persistedFilename, err := merged.ConstructFilename(filepath.Join(s.path, s.persistSubDir), hash)
			if err != nil {
				return nil, err
			}

			b, err = os.ReadFile(persistedFilename)
			if err != nil {
				if !errors.Is(err, os.ErrNotExist) {
					return nil, errors.NewStorageError("[File][Get] [%s] failed to read data from persist file", persistedFilename, err)
				}

				// If longterm client is configured, try to get from longterm storage
				if s.longtermClient != nil {
					b, err = s.longtermClient.Get(context.Background(), hash, opts...)
					if err != nil {
						if errors.Is(err, os.ErrNotExist) {
							return nil, errors.ErrNotFound
						}

						return nil, errors.NewStorageError("[File][Get] [%s] unable to open longterm storage file", filename, err)
					}
				} else {
					return nil, errors.ErrNotFound
				}
			}
		} else {
			// If longterm client is configured but no persist directory, try longterm storage directly
			if s.longtermClient != nil {
				b, err = s.longtermClient.Get(context.Background(), hash, opts...)
				if err != nil {
					if errors.Is(err, os.ErrNotExist) {
						return nil, errors.ErrNotFound
					}

					return nil, errors.NewStorageError("[File][Get] [%s] unable to open longterm storage file", filename, err)
				}
			} else {
				return nil, errors.ErrNotFound
			}
		}
	}

	b, err = helper.BytesWithHeadAndFooterRemoved(b, merged.Header, merged.Footer)
	if err != nil {
		return nil, errors.NewStorageError("[File][Get] [%s] failed to remove header and footer", filename, err)
	}

	return b, nil
}

func (s *File) GetHead(_ context.Context, hash []byte, nrOfBytes int, opts ...options.FileOption) ([]byte, error) {
	fileSemaphore <- struct{}{}
	defer func() {
		<-fileSemaphore
	}()

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

	if len(merged.Header) > 0 {
		if _, err = file.Seek(int64(len(merged.Header)), 0); err != nil {
			return nil, err // nolint:gosec
		}
	}

	bytes := make([]byte, nrOfBytes)

	nRead, err := file.Read(bytes)
	if err != nil {
		return nil, errors.NewStorageError("[File][GetHead] [%s] failed to read data from file", fileName, err)
	}

	return bytes[:nRead], err
}

func (s *File) Exists(_ context.Context, hash []byte, opts ...options.FileOption) (bool, error) {
	fileSemaphore <- struct{}{}
	defer func() {
		<-fileSemaphore
	}()

	s.logger.Debugf("[File] Exists: %s", utils.ReverseAndHexEncodeSlice(hash))

	merged := options.MergeOptions(s.options, opts)

	fileName, err := merged.ConstructFilename(s.path, hash)
	if err != nil {
		return false, err
	}

	_, err = os.Stat(fileName)
	if err != nil {
		if !os.IsNotExist(err) {
			return false, errors.NewStorageError("[File][Exists] [%s] failed to read data from file", fileName, err)
		}

		// If file not found in primary location, check persistent directory
		if s.persistSubDir != "" {
			persistedFilename, err := merged.ConstructFilename(filepath.Join(s.path, s.persistSubDir), hash)
			if err != nil {
				return false, err
			}

			_, err = os.Stat(persistedFilename)
			if err != nil {
				if !os.IsNotExist(err) {
					return false, errors.NewStorageError("[File][Exists] [%s] failed to read data from persist file", persistedFilename, err)
				}

				// If longterm client is configured, check longterm storage
				if s.longtermClient != nil {
					exists, err := s.longtermClient.Exists(context.Background(), hash, opts...)
					if err != nil {
						if errors.Is(err, os.ErrNotExist) {
							return false, nil
						}

						return false, errors.NewStorageError("[File][Exists] failed to read data from longterm storage file", err)
					}

					return exists, nil
				}

				return false, nil
			}

			return true, nil
		} else {
			// If longterm client is configured but no persist directory, check longterm storage directly
			if s.longtermClient != nil {
				exists, err := s.longtermClient.Exists(context.Background(), hash, opts...)
				if err != nil {
					if errors.Is(err, os.ErrNotExist) {
						return false, nil
					}

					return false, errors.NewStorageError("[File][Exists] failed to read data from longterm storage file", err)
				}

				return exists, nil
			}

			return false, nil
		}
	}

	return true, nil
}

func (s *File) Del(_ context.Context, hash []byte, opts ...options.FileOption) error {
	fileSemaphore <- struct{}{}
	defer func() {
		<-fileSemaphore
	}()

	s.logger.Debugf("[File] Del: %s", utils.ReverseAndHexEncodeSlice(hash))

	merged := options.MergeOptions(s.options, opts)

	fileName, err := merged.ConstructFilename(s.path, hash)
	if err != nil {
		return err
	}

	// remove DAH file, if exists
	_ = os.Remove(fileName + ".dah")

	// remove checksum file, if exists
	_ = os.Remove(fileName + checksumExtension)

	return os.Remove(fileName)
}

func findFilesByExtension(root, ext string) ([]string, error) {
	fileSemaphore <- struct{}{}
	defer func() {
		<-fileSemaphore
	}()

	var a []string

	useFind := runtime.GOOS == "linux" || runtime.GOOS == "darwin"

	// Check if 'find' is available
	if useFind {
		if _, err := exec.LookPath("find"); err == nil {
			pattern := "*." + ext
			cmd := exec.Command("find", root, "-type", "f", "-name", pattern)

			var out bytes.Buffer

			cmd.Stdout = &out
			if err := cmd.Run(); err != nil {
				return nil, err
			}

			for _, line := range strings.Split(out.String(), "\n") {
				if line != "" {
					a = append(a, line)
				}
			}

			return a, nil
		}
	}

	err := filepath.Walk(root, func(s string, d os.FileInfo, e error) error {
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

func (s *File) GetFooterMetaData(ctx context.Context, hash []byte, opts ...options.FileOption) ([]byte, error) {
	fileSemaphore <- struct{}{}
	defer func() {
		<-fileSemaphore
	}()

	merged := options.MergeOptions(s.options, opts)

	if merged.Footer == nil {
		return nil, nil
	}

	fileName, err := merged.ConstructFilename(s.path, hash)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(fileName)
	if err == nil {
		defer f.Close()
		return helper.FooterMetaData(f, merged.Footer)
	}

	if !errors.Is(err, os.ErrNotExist) {
		return nil, errors.NewStorageError("[File][GetMetaData] [%s] unable to open file", fileName, err)
	}

	if s.persistSubDir != "" {
		persistedFilename, err := merged.ConstructFilename(filepath.Join(s.path, s.persistSubDir), hash)
		if err != nil {
			return nil, err
		}

		f, err = os.Open(persistedFilename)
		if err == nil {
			defer f.Close()
			return helper.FooterMetaData(f, merged.Footer)
		}

		if !errors.Is(err, os.ErrNotExist) {
			return nil, errors.NewStorageError("[File][GetMetaData] [%s] unable to open persist file", persistedFilename, err)
		}
	}

	if s.longtermClient != nil {
		b, err := s.longtermClient.GetFooterMetaData(ctx, hash, opts...)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, errors.ErrNotFound
			}

			return nil, errors.NewStorageError("[File][GetMetaData] [%s] unable to open longterm storage file", fileName, err)
		}

		return b, nil
	}

	return nil, errors.ErrNotFound
}

func (s *File) GetHeader(ctx context.Context, hash []byte, opts ...options.FileOption) ([]byte, error) {
	fileSemaphore <- struct{}{}
	defer func() {
		<-fileSemaphore
	}()

	merged := options.MergeOptions(s.options, opts)

	if merged.Header == nil {
		return nil, nil
	}

	fileName, err := merged.ConstructFilename(s.path, hash)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(fileName)
	if err == nil {
		defer f.Close()
		return helper.HeaderMetaData(f, len(merged.Header))
	}

	if !errors.Is(err, os.ErrNotExist) {
		return nil, errors.NewStorageError("[File][GetHeader] [%s] unable to open file", fileName, err)
	}

	if s.persistSubDir != "" {
		persistedFilename, err := merged.ConstructFilename(filepath.Join(s.path, s.persistSubDir), hash)
		if err != nil {
			return nil, err
		}

		f, err = os.Open(persistedFilename)
		if err == nil {
			defer f.Close()
			return helper.HeaderMetaData(f, len(merged.Header))
		}

		if !errors.Is(err, os.ErrNotExist) {
			return nil, errors.NewStorageError("[File][GetHeader] [%s] unable to open persist file", persistedFilename, err)
		}
	}

	if s.longtermClient != nil {
		b, err := s.longtermClient.GetHeader(ctx, hash, opts...)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, errors.ErrNotFound
			}

			return nil, errors.NewStorageError("[File][GetHeader] [%s] unable to open longterm storage file", fileName, err)
		}

		return b, nil
	}

	return nil, errors.ErrNotFound
}
