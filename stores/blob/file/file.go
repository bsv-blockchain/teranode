package file

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
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
	// mu     sync.RWMutex
}

/*
* Used for dev environments as replacement for lustre.
* Has background jobs to clean up TTL.
* Able to specify multiple folders - files will be spread across folders based on key/hash/filename supplied.
 */
func New(logger ulogger.Logger, storeURL *url.URL, opts ...options.StoreOption) (*File, error) {
	if storeURL != nil && storeURL.Query().Get("ttlCleanerInterval") != "" {
		// This is a special case for testing where we want to set the ttlCleanerInterval to a very short interval
		// so we don't have to wait long to see the effect of the ttl cleaner.
		ttlCleanerInterval, err := time.ParseDuration(storeURL.Query().Get("ttlCleanerInterval"))
		if err != nil {
			return nil, errors.NewStorageError("[File] failed to parse ttlCleanerInterval", err)
		}

		return new(logger, storeURL, ttlCleanerInterval, opts...)
	}

	return new(logger, storeURL, 1*time.Minute, opts...)
}

func new(logger ulogger.Logger, storeURL *url.URL, ttlCleanerInterval time.Duration, opts ...options.StoreOption) (*File, error) {
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

	fileStore := &File{
		path:        path,
		logger:      logger,
		options:     options,
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

	var ttlBytes []byte

	var ttl time.Time

	for _, fileName := range files {
		if fileName[len(fileName)-4:] != ".ttl" {
			continue
		}

		// read the ttl
		ttlBytes, err = os.ReadFile(fileName)
		if err != nil {
			return errors.NewStorageError("[File] failed to read ttl file", err)
		}

		ttl, err = time.Parse(time.RFC3339, string(ttlBytes))
		if err != nil {
			s.logger.Warnf("[File] failed to parse ttl from %s: %v", fileName, err)
			continue
		}

		s.fileTTLsMu.Lock()
		s.fileTTLs[fileName[:len(fileName)-4]] = ttl
		s.fileTTLsMu.Unlock()
	}

	return nil
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

	s.fileTTLsMu.Lock()
	filesToRemove := make([]string, 0, len(s.fileTTLs))

	for fileName, ttl := range s.fileTTLs {
		if ttl.Before(time.Now()) {
			filesToRemove = append(filesToRemove, fileName)
		}
	}
	s.fileTTLsMu.Unlock()

	for _, fileName := range filesToRemove {
		if err := os.Remove(fileName); err != nil {
			if !os.IsNotExist(err) {
				s.logger.Warnf("[File] failed to remove file: %s", fileName)
			}
		}

		if err := os.Remove(fileName + ".ttl"); err != nil {
			if !os.IsNotExist(err) {
				s.logger.Warnf("[File] failed to remove ttl file: %s", fileName+".ttl")
			}
		}

		s.fileTTLsMu.Lock()
		delete(s.fileTTLs, fileName)
		s.fileTTLsMu.Unlock()
	}
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

func (s *File) SetFromReader(_ context.Context, key []byte, reader io.ReadCloser, opts ...options.FileOption) error {
	defer reader.Close()

	filename, err := s.constructFilenameWithTTL(key, opts)
	if err != nil {
		return errors.NewStorageError("[File][SetFromReader] [%s] failed to get file name", utils.ReverseAndHexEncodeSlice(key), err)
	}

	merged := options.MergeOptions(s.options, opts)

	if !merged.AllowOverwrite {
		if _, err := os.Stat(filename); err == nil {
			return errors.NewBlobAlreadyExistsError("[File][SetFromReader] [%s] already exists in store", filename)
		}
	}

	tmpFilename := fmt.Sprintf("%s.%d.tmp", filename, rand.Int())

	// write the bytes from the reader to a file with the filename
	file, err := os.Create(tmpFilename)
	if err != nil {
		return errors.NewStorageError("[File][SetFromReader] [%s] failed to create file", filename, err)
	}
	defer file.Close()

	if _, err = io.Copy(file, reader); err != nil {
		return errors.NewStorageError("[File][SetFromReader] [%s] failed to write data to file", filename, err)
	}

	// rename the file to remove the .tmp extension
	if err = os.Rename(tmpFilename, filename); err != nil {
		return errors.NewStorageError("[File][SetFromReader] [%s] failed to rename file from tmp", filename, err)
	}

	return nil
}

func (s *File) Set(_ context.Context, hash []byte, value []byte, opts ...options.FileOption) error {
	filename, err := s.constructFilenameWithTTL(hash, opts)
	if err != nil {
		return errors.NewStorageError("[File][Set] [%s] failed to get file name", utils.ReverseAndHexEncodeSlice(hash), err)
	}

	merged := options.MergeOptions(s.options, opts)

	if !merged.AllowOverwrite {
		if _, err = os.Stat(filename); err == nil {
			return errors.NewBlobAlreadyExistsError("[File][Set] [%s] already exists in store", filename)
		}
	}

	tmpFilename := fmt.Sprintf("%s.%d.tmp", filename, rand.Int())

	// write bytes to file
	//nolint:gosec // G306: Expect WriteFile permissions to be 0600 or less (gosec)
	if err = os.WriteFile(tmpFilename, value, 0644); err != nil {
		return errors.NewStorageError("[File][Set] [%s] failed to write data to file", filename, err)
	}

	if err = os.Rename(tmpFilename, filename); err != nil {
		return errors.NewStorageError("[File][Set] [%s] failed to rename file from tmp", filename, err)
	}

	return nil
}

func (s *File) constructFilenameWithTTL(hash []byte, opts []options.FileOption) (string, error) {
	merged := options.MergeOptions(s.options, opts)

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

func (s *File) SetTTL(_ context.Context, key []byte, newTTL time.Duration, opts ...options.FileOption) error {
	merged := options.MergeOptions(s.options, opts)

	fileName, err := merged.ConstructFilename(s.path, key)
	if err != nil {
		return errors.NewStorageError("[File] failed to get file name", err)
	}

	if newTTL == 0 {
		// delete the ttl file
		if err = os.Remove(fileName + ".ttl"); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}

			return errors.NewStorageError("[File] failed to remove ttl file", err)
		}

		s.fileTTLsMu.Lock()
		delete(s.fileTTLs, fileName)
		s.fileTTLsMu.Unlock()

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

	file, err := os.Open(fileName)
	// file, err := directio.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, errors.ErrNotFound
		}

		return nil, errors.NewStorageError("[File] [%s] unable to open file", fileName, err)
	}

	return file, nil
}

func (s *File) Get(_ context.Context, hash []byte, opts ...options.FileOption) ([]byte, error) {
	s.logger.Debugf("[File] Get: %s", utils.ReverseAndHexEncodeSlice(hash))

	merged := options.MergeOptions(s.options, opts)

	fileName, err := merged.ConstructFilename(s.path, hash)
	if err != nil {
		return nil, err
	}

	bytes, err := os.ReadFile(fileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, errors.ErrNotFound
		}

		return nil, errors.NewStorageError("[File][Get] [%s] failed to read data from file", fileName, err)
	}

	return bytes, err
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
