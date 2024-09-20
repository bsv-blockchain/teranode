package file

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/go-utils"
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
func New(logger ulogger.Logger, path string, opts ...options.StoreOption) (*File, error) {
	return new(logger, path, 1*time.Minute, opts...)
}

func new(logger ulogger.Logger, path string, ttlCleanerInterval time.Duration, opts ...options.StoreOption) (*File, error) {
	logger = logger.New("file")

	// create the path if necessary
	if len(path) > 0 {
		if err := os.MkdirAll(path, 0755); err != nil {
			return nil, errors.NewStorageError("[File] failed to create directory", err)
		}
	}

	options := options.NewStoreOptions(opts...)

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

func (s *File) Health(_ context.Context) (int, string, error) {
	return 0, "File Store", nil
}

func (s *File) Close(_ context.Context) error {
	// stop ttl cleaner
	s.fileTTLsCtx.Done()

	return nil
}

func (s *File) SetFromReader(_ context.Context, key []byte, reader io.ReadCloser, opts ...options.FileOption) error {
	defer reader.Close()

	fileName, err := s.constructFileNameWithTTL(key, opts)
	if err != nil {
		return errors.NewStorageError("[File][SetFromReader] [%s] failed to get file name", utils.ReverseAndHexEncodeSlice(key), err)
	}

	if _, err := os.Stat(fileName); err == nil {
		return errors.NewBlobAlreadyExistsError("[File][SetFromReader] [%s] already exists in store", fileName)
	}

	// write the bytes from the reader to a file with the filename
	file, err := os.Create(fileName + ".tmp")
	if err != nil {
		return errors.NewStorageError("[File][SetFromReader] [%s] failed to create file", fileName, err)
	}
	defer file.Close()

	if _, err = io.Copy(file, reader); err != nil {
		return errors.NewStorageError("[File][SetFromReader] [%s] failed to write data to file", fileName, err)
	}

	// rename the file to remove the .tmp extension
	if err = os.Rename(fileName+".tmp", fileName); err != nil {
		return errors.NewStorageError("[File][SetFromReader] [%s] failed to rename file", fileName, err)
	}

	return nil
}

func (s *File) Set(_ context.Context, hash []byte, value []byte, opts ...options.FileOption) error {
	fileName, err := s.constructFileNameWithTTL(hash, opts)

	if err != nil {
		return errors.NewStorageError("[File][Set] [%s] failed to get file name", utils.ReverseAndHexEncodeSlice(hash), err)
	}

	if _, err := os.Stat(fileName); err == nil {
		return errors.NewBlobAlreadyExistsError("[File][Set] [%s] already exists in store", fileName)
	}

	// write bytes to file
	//nolint:gosec // G306: Expect WriteFile permissions to be 0600 or less (gosec)
	if err = os.WriteFile(fileName, value, 0644); err != nil {
		return errors.NewStorageError("[File][Set] [%s] failed to write data to file", fileName, err)
	}

	return nil
}

func (s *File) constructFileNameWithTTL(hash []byte, opts []options.FileOption) (string, error) {
	merged := options.MergeOptions(s.options, opts)

	fileName, err := merged.ConstructFilename(s.path, hash)
	if err != nil {
		return "", err
	}

	if merged.TTL != nil && *merged.TTL > 0 {
		// write bytes to file
		ttl := time.Now().Add(*merged.TTL)
		//nolint:gosec // G306: Expect WriteFile permissions to be 0600 or less (gosec)
		if err := os.WriteFile(fileName+".ttl", []byte(ttl.Format(time.RFC3339)), 0644); err != nil {
			return "", errors.NewStorageError("failed to write ttl to file", err)
		}

		s.fileTTLsMu.Lock()
		s.fileTTLs[fileName] = ttl
		s.fileTTLsMu.Unlock()
	}

	return fileName, nil
}

func (s *File) SetTTL(_ context.Context, key []byte, newTTL time.Duration, opts ...options.FileOption) error {
	fileName, err := s.constructFileNameWithTTL(key, opts)
	if err != nil {
		return errors.NewStorageError("[File] failed to get file name", err)
	}

	if newTTL == 0 {
		// delete the ttl file
		if err := os.Remove(fileName + ".ttl"); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}

			return errors.NewStorageError("[File] failed to remove ttl file", err)
		}

		return nil
	}

	// write bytes to file
	ttl := time.Now().Add(newTTL)
	//nolint:gosec // G306: Expect WriteFile permissions to be 0600 or less (gosec)
	if err := os.WriteFile(fileName+".ttl", []byte(ttl.Format(time.RFC3339)), 0644); err != nil {
		return errors.NewStorageError("[File] [%s] failed to write ttl to file", fileName, err)
	}

	s.fileTTLsMu.Lock()
	s.fileTTLs[fileName] = ttl
	s.fileTTLsMu.Unlock()

	return nil
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
