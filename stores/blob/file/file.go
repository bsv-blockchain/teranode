package file

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ubsverrors"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/go-utils"
)

type File struct {
	paths       []string
	logger      ulogger.Logger
	fileTTLs    map[string]time.Time
	fileTTLsMu  sync.Mutex
	fileTTLsCtx context.Context
	// mu     sync.RWMutex
}

func New(logger ulogger.Logger, dir string, multiDirs ...[]string) (*File, error) {
	logger = logger.New("file")

	paths := []string{dir}
	if len(multiDirs) > 0 {
		paths = multiDirs[0]
		// create the directories if they don't exist
		for _, d := range multiDirs[0] {
			if err := os.MkdirAll(d, 0755); err != nil {
				return nil, fmt.Errorf("failed to create directory: %w", err)
			}
		}
	} else {
		// create directory if not exists
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory: %w", err)
		}
	}

	fileStore := &File{
		paths:       paths,
		logger:      logger,
		fileTTLs:    make(map[string]time.Time),
		fileTTLsCtx: context.Background(),
	}

	err := fileStore.loadTTLs()
	if err != nil {
		return nil, fmt.Errorf("failed to load ttls: %w", err)
	}

	go fileStore.ttlCleaner(fileStore.fileTTLsCtx)

	return fileStore, nil
}

func (s *File) loadTTLs() error {
	s.fileTTLsMu.Lock()
	defer s.fileTTLsMu.Unlock()

	for _, path := range s.paths {
		s.logger.Infof("Loading file TTLs: %s", path)

		// get all files in the directory that end with .ttl
		files, err := findFilesByExtension(path, ".ttl")
		if err != nil {
			return fmt.Errorf("failed to find ttl files: %w", err)
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
				return fmt.Errorf("failed to read ttl file: %w", err)
			}

			ttl, err = time.Parse(time.RFC3339, string(ttlBytes))
			if err != nil {
				s.logger.Warnf("failed to parse ttl from %s: %v", fileName, err)
				continue
			}

			s.fileTTLs[fileName[:len(fileName)-4]] = ttl
		}
	}

	return nil
}

func (s *File) ttlCleaner(ctx context.Context) {
	for {
		time.Sleep(1 * time.Minute)

		select {
		case <-ctx.Done():
			return
		default:
			s.logger.Debugf("Cleaning file TTLs")

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
					s.logger.Warnf("failed to remove file: %s", fileName)
				}
				if err := os.Remove(fileName + ".ttl"); err != nil {
					s.logger.Warnf("failed to remove ttl file: %s", fileName+".ttl")
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

func (s *File) Set(_ context.Context, hash []byte, value []byte, opts ...options.Options) error {
	// s.logger.Debugf("[File] Set: %s", utils.ReverseAndHexEncodeSlice(hash))

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

func (s *File) getFileNameForSet(hash []byte, opts []options.Options) (string, error) {
	fileName := s.filename(hash)

	fileOptions := options.NewSetOptions(opts...)

	if fileOptions.TTL > 0 {
		// write bytes to file
		ttl := time.Now().Add(fileOptions.TTL)
		if err := os.WriteFile(fileName+".ttl", []byte(ttl.Format(time.RFC3339)), 0644); err != nil {
			return "", fmt.Errorf("failed to write ttl to file: %w", err)
		}
		s.fileTTLsMu.Lock()
		s.fileTTLs[fileName] = ttl
		s.fileTTLsMu.Unlock()
	}

	if fileOptions.Extension != "" {
		fileName = fmt.Sprintf("%s.%s", fileName, fileOptions.Extension)
	}

	return fileName, nil
}

func (s *File) SetTTL(_ context.Context, hash []byte, ttl time.Duration) error {
	// s.logger.Debugf("[File] SetTTL: %s", utils.ReverseAndHexEncodeSlice(hash))
	// not supported on files yet
	return nil
}

func (s *File) GetIoReader(_ context.Context, hash []byte) (io.ReadCloser, error) {
	fileName := s.filename(hash)
	file, err := os.Open(fileName)
	//file, err := directio.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ubsverrors.ErrNotFound
		}
		return nil, fmt.Errorf("unable to open file %q, %v", fileName, err)
	}

	return file, nil
}

func (s *File) Get(_ context.Context, hash []byte) ([]byte, error) {
	s.logger.Debugf("[File] Get: %s", utils.ReverseAndHexEncodeSlice(hash))
	fileName := s.filename(hash)

	bytes, err := os.ReadFile(fileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ubsverrors.ErrNotFound
		}
		return nil, fmt.Errorf("failed to read data from file: %w", err)
	}

	return bytes, err
}

func (s *File) GetHead(_ context.Context, hash []byte, nrOfBytes int) ([]byte, error) {
	s.logger.Debugf("[File] Get: %s", utils.ReverseAndHexEncodeSlice(hash))
	fileName := s.filename(hash)

	file, err := os.Open(fileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ubsverrors.ErrNotFound
		}
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	bytes := make([]byte, nrOfBytes)
	nRead, err := file.Read(bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to read data from file: %w", err)
	}

	return bytes[:nRead], err
}

func (s *File) Exists(_ context.Context, hash []byte) (bool, error) {
	s.logger.Debugf("[File] Exists: %s", utils.ReverseAndHexEncodeSlice(hash))
	fileName := s.filename(hash)

	_, err := os.Stat(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to read data from file: %w", err)
	}

	return true, nil
}

func (s *File) Del(_ context.Context, hash []byte) error {
	s.logger.Debugf("[File] Del: %s", utils.ReverseAndHexEncodeSlice(hash))
	fileName := s.filename(hash)

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
