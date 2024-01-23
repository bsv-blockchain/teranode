package shared

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/go-utils"
)

type Shared struct {
	paths         []string
	logger        ulogger.Logger
	persistSubDir string
}

func New(logger ulogger.Logger, dir string, persistDir string, multiDirs ...[]string) (*Shared, error) {
	logger = logger.New("file")

	sharedStore := &Shared{
		paths:         []string{dir},
		logger:        logger,
		persistSubDir: filepath.Clean(persistDir) + "/",
	}

	if len(multiDirs) > 0 {
		sharedStore.paths = multiDirs[0]
		// create the directories if they don't exist
		for _, d := range multiDirs[0] {
			if err := os.MkdirAll(d, 0755); err != nil {
				return nil, fmt.Errorf("failed to create directory: %w", err)
			}
			if err := os.MkdirAll(filepath.Clean(d+"/"+persistDir), 0755); err != nil {
				return nil, fmt.Errorf("failed to create directory: %w", err)
			}
		}
	} else {
		// create directory if not exists
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory: %w", err)
		}
		if err := os.MkdirAll(filepath.Clean(dir+"/"+persistDir), 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory: %w", err)
		}
	}

	return sharedStore, nil
}

func (s *Shared) Health(_ context.Context) (int, string, error) {
	return 0, "Shared blob Store", nil
}

func (s *Shared) Close(_ context.Context) error {
	return nil
}

func (s *Shared) SetFromReader(_ context.Context, key []byte, reader io.ReadCloser, opts ...options.Options) error {
	s.logger.Debugf("[File] SetFromReader: %s", utils.ReverseAndHexEncodeSlice(key))
	defer reader.Close()

	fileName, err := s.getFileNameForSet(key, opts)
	if err != nil {
		return fmt.Errorf("failed to get file name: %w", err)
	}

	// write the bytes from the reader to a file with the filename
	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	if _, err = io.Copy(file, reader); err != nil {
		return fmt.Errorf("failed to write data to file: %w", err)
	}

	return nil
}

func (s *Shared) Set(_ context.Context, hash []byte, value []byte, opts ...options.Options) error {
	s.logger.Debugf("[File] Set: %s", utils.ReverseAndHexEncodeSlice(hash))

	fileName, err := s.getFileNameForSet(hash, opts)
	if err != nil {
		return fmt.Errorf("failed to get file name: %w", err)
	}

	// write bytes to file
	if err = os.WriteFile(fileName, value, 0644); err != nil {
		return fmt.Errorf("failed to write data to file: %w", err)
	}

	return nil
}

func (s *Shared) SetTTL(_ context.Context, hash []byte, ttl time.Duration) error {
	filename := s.filename(hash)
	persistedFilename := s.getFileNameForPersist(filename)
	if ttl <= 0 {
		// check whether the persisted file exists
		_, err := os.Stat(persistedFilename)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// nothing to do
				return nil
			}
			return fmt.Errorf("unable to stat file %q, %v", persistedFilename, err)
		}

		// the file should be persisted
		return os.Rename(filename, persistedFilename)
	}

	// the filename should be moved from the persist sub dir to the main dir
	return os.Rename(persistedFilename, filename)
}

func (s *Shared) GetIoReader(_ context.Context, hash []byte) (io.ReadCloser, error) {
	fileName := s.filename(hash)

	file, err := os.Open(fileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// check the persist sub dir
			file, err = os.Open(s.getFileNameForPersist(fileName))
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return nil, options.ErrNotFound
				}
				return nil, fmt.Errorf("unable to open file %q, %v", fileName, err)
			}
			return file, nil
		}
		return nil, fmt.Errorf("unable to open file %q, %v", fileName, err)
	}

	return file, nil
}

func (s *Shared) Get(_ context.Context, hash []byte) ([]byte, error) {
	s.logger.Debugf("[File] Get: %s", utils.ReverseAndHexEncodeSlice(hash))
	fileName := s.filename(hash)

	bytes, err := os.ReadFile(fileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// check the persist sub dir
			bytes, err = os.ReadFile(s.getFileNameForPersist(fileName))
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return nil, options.ErrNotFound
				}
				return nil, fmt.Errorf("failed to read data from file: %w", err)
			}
			return bytes, err
		}
		return nil, fmt.Errorf("failed to read data from file: %w", err)
	}

	return bytes, err
}

func (s *Shared) Exists(_ context.Context, hash []byte) (bool, error) {
	s.logger.Debugf("[File] Exists: %s", utils.ReverseAndHexEncodeSlice(hash))
	fileName := s.filename(hash)

	_, err := os.Stat(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			// check the persist sub dir
			_, err = os.Stat(s.getFileNameForPersist(fileName))
			if err != nil {
				if os.IsNotExist(err) {
					return false, nil
				}
				return false, fmt.Errorf("failed to read data from file: %w", err)
			}
			return true, nil
		}
		return false, fmt.Errorf("failed to read data from file: %w", err)
	}

	return true, nil
}

func (s *Shared) Del(_ context.Context, hash []byte) error {
	s.logger.Debugf("[File] Del: %s", utils.ReverseAndHexEncodeSlice(hash))
	fileName := s.filename(hash)

	// remove ttl file, if exists
	errPersist := os.Remove(s.getFileNameForPersist(fileName))
	err := os.Remove(fileName)

	if err != nil && errPersist != nil {
		return err
	}

	return nil
}

func (s *Shared) filename(hash []byte) string {
	// determine path to use, based on the first byte of the hash and the number of paths
	path := s.paths[hash[0]%byte(len(s.paths))]
	return fmt.Sprintf("%s/%x", path, bt.ReverseBytes(hash))
}

func (s *Shared) getFileNameForPersist(filename string) string {
	// persisted files are stored in a subdirectory
	// add the persist dir before the file in the filepath
	fileParts := strings.Split(filename, string(os.PathSeparator))
	fileParts[len(fileParts)-1] = s.persistSubDir + fileParts[len(fileParts)-1]

	// clean the paths
	return filepath.Clean("/" + filepath.Join(fileParts...))
}

func (s *Shared) getFileNameForSet(hash []byte, opts []options.Options) (string, error) {
	fileName := s.filename(hash)

	fileOptions := options.NewSetOptions(opts...)

	if fileOptions.TTL <= 0 {
		// the file should be persisted
		fileName = s.getFileNameForPersist(fileName)
	}

	if fileOptions.Extension != "" {
		fileName = fmt.Sprintf("%s.%s", fileName, fileOptions.Extension)
	}

	return fileName, nil
}
