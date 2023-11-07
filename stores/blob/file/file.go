package file

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type File struct {
	path        string
	logger      utils.Logger
	fileTTLs    map[string]time.Time
	fileTTLsCtx context.Context
	// mu     sync.RWMutex
}

func New(dir string) (*File, error) {
	logger := gocore.Log("file")

	// create directory if not exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	fileStore := &File{
		path:        dir,
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
	s.logger.Infof("Loading file TTLs: %s", s.path)

	// get all files in the directory that end with .ttl
	files, err := findFilesByExtension(s.path, ".ttl")
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
			s.logger.Warnf("failed to parse ttl from %s: %w", fileName, err)
			continue
		}

		s.fileTTLs[fileName[:len(fileName)-4]] = ttl
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
			s.logger.Debugf("Cleaning file TTLs: %s", s.path)
			for fileName, ttl := range s.fileTTLs {
				if ttl.Before(time.Now()) {
					if err := os.Remove(fileName); err != nil {
						s.logger.Errorf("failed to remove file: %s", fileName)
					}
					if err := os.Remove(fileName + ".ttl"); err != nil {
						s.logger.Errorf("failed to remove ttl file: %s", fileName)
					}
					delete(s.fileTTLs, fileName)
				}
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

func (s *File) Set(_ context.Context, hash []byte, value []byte, opts ...options.Options) error {
	s.logger.Debugf("[File] Set: %s", utils.ReverseAndHexEncodeSlice(hash))
	fileName := s.filename(hash)

	// TODO: handle options
	// TTL is not supported by file store
	fileOptions := options.NewSetOptions(opts...)

	if fileOptions.TTL > 0 {
		// write bytes to file
		ttl := time.Now().Add(fileOptions.TTL)
		if err := os.WriteFile(fileName+".ttl", []byte(ttl.Format(time.RFC3339)), 0644); err != nil {
			return fmt.Errorf("failed to write ttl to file: %w", err)
		}
		s.fileTTLs[fileName] = ttl
	}

	if fileOptions.Extension != "" {
		fileName = fmt.Sprintf("%s.%s", fileName, fileOptions.Extension)
	}

	// write bytes to file
	if err := os.WriteFile(fileName, value, 0644); err != nil {
		return fmt.Errorf("failed to write data to file: %w", err)
	}

	return nil
}

func (s *File) SetTTL(_ context.Context, hash []byte, ttl time.Duration) error {
	s.logger.Debugf("[File] SetTTL: %s", utils.ReverseAndHexEncodeSlice(hash))
	// not supported on files yet
	return nil
}

func (s *File) Get(_ context.Context, hash []byte) ([]byte, error) {
	s.logger.Debugf("[File] Get: %s", utils.ReverseAndHexEncodeSlice(hash))
	fileName := s.filename(hash)

	bytes, err := os.ReadFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to read data from file: %w", err)
	}

	return bytes, err
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

	// remove ttl file
	if err := os.Remove(fileName + ".ttl"); err != nil {
		return fmt.Errorf("failed to remove ttl file: %w", err)
	}

	return os.Remove(fileName)
}

func (s *File) filename(hash []byte) string {
	return fmt.Sprintf("%s/%x", s.path, bt.ReverseBytes(hash))
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
