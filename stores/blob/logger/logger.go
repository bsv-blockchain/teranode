package logger

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bsv-blockchain/go-bt/v2"
)

type blobStore interface {
	Health(ctx context.Context, checkLiveness bool) (int, string, error)
	Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error)
	Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error)
	GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error)
	Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error
	SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, value io.ReadCloser, opts ...options.FileOption) error
	SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, newDAH uint32, opts ...options.FileOption) error
	GetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (uint32, error)
	Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error
	Close(ctx context.Context) error
	SetCurrentBlockHeight(height uint32)
}

type Logger struct {
	logger ulogger.Logger
	store  blobStore
}

func New(logger ulogger.Logger, store blobStore) blobStore {
	s := &Logger{
		logger: logger,
		store:  store,
	}

	return s
}

func caller() string {
	var callers []string

	depth := 5

	for i := 0; i < depth; i++ {
		pc, file, line, ok := runtime.Caller(2 + i)
		if !ok {
			break
		}

		folders := strings.Split(file, string(filepath.Separator))
		if len(folders) > 0 {
			if folders[0] == "github.com" {
				folders = folders[1:]
			}

			if folders[0] == "bitcoin-sv" {
				folders = folders[1:]
			}

			if folders[0] == "teranode" {
				folders = folders[1:]
			}
		}

		file = filepath.Join(folders...)

		funcName := runtime.FuncForPC(pc).Name()
		funcPaths := strings.Split(funcName, "/")
		funcName = funcPaths[len(funcPaths)-1]

		callers = append(callers, fmt.Sprintf("called from %s: %s:%d", funcName, file, line))
	}

	return strings.Join(callers, ",")
}

func (s *Logger) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	s.logger.Debugf("[BlobStore][logger][Health] : %s", caller())
	return s.store.Health(ctx, checkLiveness)
}

func (s *Logger) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
	exists, err := s.store.Exists(ctx, key, fileType, opts...)
	k := bt.ReverseBytes(key)
	s.logger.Debugf("[BlobStore][logger][Exists] key %x, fileType %s, exists %t, err %v : %s", k, fileType, exists, err, caller())

	return exists, err
}

func (s *Logger) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	value, err := s.store.Get(ctx, key, fileType, opts...)
	k := bt.ReverseBytes(key)
	s.logger.Debugf("[BlobStore][logger][Get] key %x, fileType %s, err %v : %s", k, fileType, err, caller())

	return value, err
}

func (s *Logger) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	reader, err := s.store.GetIoReader(ctx, key, fileType, opts...)
	k := bt.ReverseBytes(key)
	s.logger.Debugf("[BlobStore][logger][GetIoReader] key %x, fileType %s, err %v : %s", k, fileType, err, caller())

	return reader, err
}

func (s *Logger) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	err := s.store.Set(ctx, key, fileType, value, opts...)
	k := bt.ReverseBytes(key)
	s.logger.Debugf("[BlobStore][logger][Set] key %x, fileType %s, err %v : %s", k, fileType, err, caller())

	return err
}

func (s *Logger) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	err := s.store.SetFromReader(ctx, key, fileType, reader, opts...)
	k := bt.ReverseBytes(key)
	s.logger.Debugf("[BlobStore][logger][SetFromReader] key %x, fileType %s, err %v : %s", k, fileType, err, caller())

	return err
}

func (s *Logger) SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, dah uint32, opts ...options.FileOption) error {
	err := s.store.SetDAH(ctx, key, fileType, dah, opts...)
	k := bt.ReverseBytes(key)
	s.logger.Debugf("[BlobStore][logger][SetDAH] key %x, fileType %s, dah %d, err %v : %s", k, fileType, dah, err, caller())

	return err
}

func (s *Logger) GetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (uint32, error) {
	dah, err := s.store.GetDAH(ctx, key, fileType, opts...)
	k := bt.ReverseBytes(key)
	s.logger.Debugf("[BlobStore][logger][GetDAH] key %x, fileType %s, dah %d, err %v : %s", k, fileType, dah, err, caller())

	return dah, err
}

func (s *Logger) Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
	err := s.store.Del(ctx, key, fileType, opts...)
	k := bt.ReverseBytes(key)
	s.logger.Debugf("[BlobStore][logger][Del] key %x, fileType %s, err %v : %s", k, fileType, err, caller())

	return err
}

func (s *Logger) Close(ctx context.Context) error {
	err := s.store.Close(ctx)
	s.logger.Debugf("[BlobStore][logger][Close] err %v : %s", err, caller())

	return err
}

func (s *Logger) SetCurrentBlockHeight(height uint32) {
	s.store.SetCurrentBlockHeight(height)
	s.logger.Debugf("[BlobStore][logger][SetCurrentBlockHeight] height %d : %s", height, caller())
}
