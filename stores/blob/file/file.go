package file

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/go-utils"
)

type File struct {
	paths       []string
	logger      ulogger.Logger
	options     *options.SetOptions
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
func New(logger ulogger.Logger, paths []string, opts ...options.Options) (*File, error) {
	return new(logger, paths, 1*time.Minute, opts...)
}

func new(logger ulogger.Logger, paths []string, ttlCleanerInterval time.Duration, opts ...options.Options) (*File, error) {
	logger = logger.New("file")

	// create the directories if they don't exist
	for _, d := range paths {
		if err := os.MkdirAll(d, 0755); err != nil {
			return nil, errors.NewStorageError("[File] failed to create directory", err)
		}
	}

	options := options.NewSetOptions(nil, opts...)

	if options.PrefixDirectory > 0 {
		logger.Warnf("[File] prefix directory option will be ignored (only supported in S3 store)")
	}

	if options.SubDirectory != "" {
		os.MkdirAll(filepath.Join(paths[0], options.SubDirectory), 0755)
	}

	fileStore := &File{
		paths:       paths,
		logger:      logger,
		options:     options,
		fileTTLs:    make(map[string]time.Time),
		fileTTLsCtx: context.Background(),
	}

	fileTTLs, err := fileStore.loadTTLs()
	if err != nil {
		fileStore.logger.Warnf("[File] failed to load ttls: %v", err)
	}

	fileStore.fileTTLs = fileTTLs

	go fileStore.ttlCleaner(fileStore.fileTTLsCtx, ttlCleanerInterval)

	return fileStore, nil
}

func (s *File) loadTTLs() (map[string]time.Time, error) {
	s.fileTTLsMu.Lock()
	defer s.fileTTLsMu.Unlock()

	fileTTLs := make(map[string]time.Time)

	for _, path := range s.paths {
		s.logger.Infof("[File] Loading file TTLs: %s", path)

		// get all files in the directory that end with .ttl
		files, err := findFilesByExtension(path, ".ttl")
		if err != nil {
			return nil, errors.NewStorageError("[File] failed to find ttl files", err)
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
				return nil, errors.NewStorageError("[File] failed to read ttl file", err)
			}

			ttl, err = time.Parse(time.RFC3339, string(ttlBytes))
			if err != nil {
				s.logger.Warnf("[File] failed to parse ttl from %s: %v", fileName, err)
				continue
			}

			fileTTLs[fileName[:len(fileName)-4]] = ttl
		}
	}

	return fileTTLs, nil
}

func (s *File) ttlCleaner(ctx context.Context, interval time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
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
					s.logger.Warnf("[File] failed to remove file: %s", fileName)
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

func (s *File) SetFromReader(_ context.Context, key []byte, reader io.ReadCloser, opts ...options.Options) error {
	s.logger.Debugf("[File] SetFromReader: %s", utils.ReverseAndHexEncodeSlice(key))
	defer reader.Close()

	fileName, err := s.getFileNameForSet(key, opts)
	if err != nil {
		return errors.NewStorageError("[File] [%s] failed to get file name", utils.ReverseAndHexEncodeSlice(key), err)
	}

	// write the bytes from the reader to a file with the filename
	file, err := os.Create(fileName + ".tmp")
	if err != nil {
		return errors.NewStorageError("[File] [%s] failed to create file", fileName, err)
	}
	defer file.Close()

	if _, err = io.Copy(file, reader); err != nil {
		return errors.NewStorageError("[File] [%s] failed to write data to file", fileName, err)
	}

	// rename the file to remove the .tmp extension
	if err = os.Rename(fileName+".tmp", fileName); err != nil {
		return errors.NewStorageError("[File] [%s] failed to rename file", fileName, err)
	}

	return nil
}

func (s *File) Set(_ context.Context, hash []byte, value []byte, opts ...options.Options) error {
	// s.logger.Debugf("[File] Set: %s", utils.ReverseAndHexEncodeSlice(hash))

	fileName, err := s.getFileNameForSet(hash, opts)
	if err != nil {
		return errors.NewStorageError("[File] [%s] failed to get file name", utils.ReverseAndHexEncodeSlice(hash), err)
	}

	// write bytes to file
	//nolint:gosec // G306: Expect WriteFile permissions to be 0600 or less (gosec)
	if err = os.WriteFile(fileName, value, 0644); err != nil {
		return errors.NewStorageError("[File] [%s] failed to write data to file", fileName, err)
	}

	return nil
}

func (s *File) getFileNameForGet(hash []byte, opts []options.Options) (string, error) {
	fileOptions := options.NewSetOptions(s.options, opts...)

	var fileName string

	if fileOptions.Filename != "" {
		if len(fileOptions.SubDirectory) > 0 && fileOptions.SubDirectory[:1] == "/" {
			// if the subdirectory starts with a /, then it is a full path
			fileName = filepath.Join(fileOptions.SubDirectory, fileOptions.Filename)
		} else {
			fileName = filepath.Join(s.paths[0], fileOptions.SubDirectory, fileOptions.Filename)
		}
	} else {
		if fileOptions.SubDirectory != "" {
			s.logger.Warnf("[File] SubDirectory %q ignored when no opt.Filename specified", fileOptions.SubDirectory)
		}

		fileName = s.filename(hash)
	}

	if fileOptions.Extension != "" {
		fileName = fmt.Sprintf("%s.%s", fileName, fileOptions.Extension)
	}

	return fileName, nil
}
func (s *File) getFileNameForSet(hash []byte, opts []options.Options) (string, error) {
	fileName, err := s.getFileNameForGet(hash, opts)
	if err != nil {
		return "", err
	}

	fileOptions := options.NewSetOptions(s.options, opts...)

	if fileOptions.TTL > 0 {
		// write bytes to file
		ttl := time.Now().Add(fileOptions.TTL)
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

func (s *File) SetTTL(_ context.Context, key []byte, newTtl time.Duration, opts ...options.Options) error {
	fileName, err := s.getFileNameForSet(key, opts)
	if err != nil {
		return errors.NewStorageError("[File] failed to get file name", err)
	}

	if newTtl == 0 {
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
	ttl := time.Now().Add(newTtl)
	//nolint:gosec // G306: Expect WriteFile permissions to be 0600 or less (gosec)
	if err := os.WriteFile(fileName+".ttl", []byte(ttl.Format(time.RFC3339)), 0644); err != nil {
		return errors.NewStorageError("[File] [%s] failed to write ttl to file", fileName, err)
	}
	s.fileTTLsMu.Lock()
	s.fileTTLs[fileName] = ttl
	s.fileTTLsMu.Unlock()

	return nil
}

func (s *File) GetIoReader(_ context.Context, hash []byte, opts ...options.Options) (io.ReadCloser, error) {
	fileName, err := s.getFileNameForGet(hash, opts)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(fileName)
	//file, err := directio.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, errors.ErrNotFound
		}
		return nil, errors.NewStorageError("[File] [%s] unable to open file", fileName, err)
	}

	return file, nil
}

func (s *File) Get(_ context.Context, hash []byte, opts ...options.Options) ([]byte, error) {
	s.logger.Debugf("[File] Get: %s", utils.ReverseAndHexEncodeSlice(hash))
	fileName, err := s.getFileNameForGet(hash, opts)
	if err != nil {
		return nil, err
	}

	bytes, err := os.ReadFile(fileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, errors.ErrNotFound
		}
		return nil, errors.NewStorageError("[File] [%s] failed to read data from file", fileName, err)
	}

	return bytes, err
}

func (s *File) GetHead(_ context.Context, hash []byte, nrOfBytes int, opts ...options.Options) ([]byte, error) {
	s.logger.Debugf("[File] Get: %s", utils.ReverseAndHexEncodeSlice(hash))
	fileName, err := s.getFileNameForGet(hash, opts)
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
		return nil, errors.NewStorageError("[File] [%s] failed to read data from file", fileName, err)
	}

	return bytes[:nRead], err
}

func (s *File) Exists(_ context.Context, hash []byte, opts ...options.Options) (bool, error) {
	s.logger.Debugf("[File] Exists: %s", utils.ReverseAndHexEncodeSlice(hash))
	fileName, err := s.getFileNameForGet(hash, opts)
	if err != nil {
		return false, err
	}

	_, err = os.Stat(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, errors.NewStorageError("[File] [%s] failed to read data from file", fileName, err)
	}

	return true, nil
}

func (s *File) Del(_ context.Context, hash []byte, opts ...options.Options) error {
	s.logger.Debugf("[File] Del: %s", utils.ReverseAndHexEncodeSlice(hash))
	fileName, err := s.getFileNameForGet(hash, opts)
	if err != nil {
		return err
	}

	// remove ttl file, if exists
	_ = os.Remove(fileName + ".ttl")
	return os.Remove(fileName)
}

func (s *File) filename(hash []byte) string {
	// determine path to use, based on the first byte of the hash and the number of paths
	path := s.paths[hash[0]%byte(len(s.paths))]
	return fmt.Sprintf("%s/%x", path, bt.ReverseBytes(hash))
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
