package file

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type File struct {
	path   string
	logger utils.Logger
	// mu     sync.RWMutex
}

func New(dir string) (*File, error) {
	logger := gocore.Log("file")

	// create directory if not exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	fileStore := &File{
		path:   dir,
		logger: logger,
	}

	return fileStore, nil
}

func (s *File) Health(ctx context.Context) (int, string, error) {
	return 0, "File Store", nil
}

func (s *File) Close(_ context.Context) error {
	// noop
	return nil
}

func (s *File) Set(_ context.Context, hash []byte, value []byte, opts ...options.Options) error {
	s.logger.Debugf("[File] Set: %s", utils.ReverseAndHexEncodeSlice(hash))
	fileName := s.filename(hash)

	// TODO: handle options
	// TTL is not supported by file store

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

	return os.Remove(fileName)
}

func (s *File) filename(hash []byte) string {
	return fmt.Sprintf("%s/%x", s.path, bt.ReverseBytes(hash))
}
