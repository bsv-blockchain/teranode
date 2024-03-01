package lustre

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/blob/s3"
	"github.com/bitcoin-sv/ubsv/ubsverrors"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type s3Store interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	GetIoReader(ctx context.Context, key []byte) (io.ReadCloser, error)
}

type Lustre struct {
	paths         []string
	logger        ulogger.Logger
	persistSubDir string
	s3Client      s3Store
}

func New(logger ulogger.Logger, s3Url *url.URL, dir string, persistDir string) (*Lustre, error) {
	logger = logger.New("lustre")

	s3Client, err := s3.New(logger, s3Url)
	if err != nil {
		return nil, fmt.Errorf("failed to create s3 client: %w", err)
	}

	lustreStore := &Lustre{
		paths:         []string{dir},
		logger:        logger,
		persistSubDir: filepath.Clean(persistDir) + "/",
		s3Client:      s3Client,
	}

	// create directory if not exists
	if err = os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}
	if err = os.MkdirAll(filepath.Clean(dir+"/"+persistDir), 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	return lustreStore, nil
}

func (s *Lustre) Health(_ context.Context) (int, string, error) {
	return 0, "Lustre blob Store", nil
}

func (s *Lustre) Close(_ context.Context) error {
	return nil
}

func (s *Lustre) SetFromReader(_ context.Context, key []byte, reader io.ReadCloser, opts ...options.Options) error {
	s.logger.Debugf("[File] SetFromReader: %s", utils.ReverseAndHexEncodeSlice(key))
	defer reader.Close()

	fileName, err := s.getFileNameForSet(key, opts)
	if err != nil {
		return fmt.Errorf("[%s] failed to get file name: %w", utils.ReverseAndHexEncodeSlice(key), err)
	}

	// write the bytes from the reader to a file with the filename
	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("[%s] failed to create file: %w", fileName, err)
	}
	defer file.Close()

	if _, err = io.Copy(file, reader); err != nil {
		return fmt.Errorf("[%s] failed to write data to file: %w", fileName, err)
	}

	return nil
}

func (s *Lustre) Set(_ context.Context, hash []byte, value []byte, opts ...options.Options) error {
	s.logger.Debugf("[File] Set: %s", utils.ReverseAndHexEncodeSlice(hash))

	fileName, err := s.getFileNameForSet(hash, opts)
	if err != nil {
		return fmt.Errorf("[%s] failed to get file name: %w", utils.ReverseAndHexEncodeSlice(hash), err)
	}

	// write bytes to file
	if err = os.WriteFile(fileName, value, 0644); err != nil {
		return fmt.Errorf("[%s] failed to write data to file: %w", fileName, err)
	}

	return nil
}

func (s *Lustre) SetTTL(_ context.Context, hash []byte, ttl time.Duration) error {
	filename := s.filename(hash)
	persistedFilename := s.getFileNameForPersist(filename)
	if ttl <= 0 {
		// check whether the persisted file exists
		_, err := os.Stat(persistedFilename)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("[%s] unable to stat file, %v", persistedFilename, err)
		}

		// the file should be persisted
		return os.Rename(filename, persistedFilename)
	}

	// the filename should be moved from the persist sub dir to the main dir
	return os.Rename(persistedFilename, filename)
}

func (s *Lustre) GetIoReader(ctx context.Context, hash []byte) (io.ReadCloser, error) {
	maxRetries, _ := gocore.Config().GetInt("lustre_store_max_retries", 3)
	retrySleepDuration, err, _ := gocore.Config().GetDuration("lustre_store_retry_sleep_duration", 500*time.Millisecond)
	if err != nil {
		panic(fmt.Errorf("failed to get duration from config: %w", err))
	}

	var file io.ReadCloser

	for i := 0; i < maxRetries; i++ {
		fileName := s.filename(hash)

		file, err = os.Open(fileName)
		if err == nil {
			break
		}

		if errors.Is(err, os.ErrNotExist) {
			// check the persist sub dir
			file, err = os.Open(s.getFileNameForPersist(fileName))
			if err == nil {
				break
			}
		}

		s.logger.Warnf("[%s] file not found in subtree temp dir: %v", fileName, err)

		if errors.Is(err, os.ErrNotExist) {
			// check s3
			file, err = s.s3Client.GetIoReader(ctx, hash)
			if err == nil {
				break
			}
		}

		if errors.Is(err, os.ErrNotExist) {
			err = ubsverrors.ErrNotFound
			if i < maxRetries-1 {
				time.Sleep(retrySleepDuration)
			}
		} else {
			s.logger.Warnf("[%s] unable to open s3 entry: %v", fileName, err)
		}
	}

	return file, err
}

func (s *Lustre) Get(ctx context.Context, hash []byte) ([]byte, error) {
	s.logger.Debugf("[File] Get: %s", utils.ReverseAndHexEncodeSlice(hash))
	fileName := s.filename(hash)

	maxRetries, _ := gocore.Config().GetInt("lustre_store_max_retries", 3)
	retrySleepDuration, err, _ := gocore.Config().GetDuration("lustre_store_retry_sleep_duration", 500*time.Millisecond)
	if err != nil {
		panic(fmt.Errorf("failed to get duration from config: %w", err))
	}

	var bytes []byte

	for i := 0; i < maxRetries; i++ {
		bytes, err = os.ReadFile(fileName)
		if err == nil {
			break
		}

		if errors.Is(err, os.ErrNotExist) {
			// check the persist sub dir
			bytes, err = os.ReadFile(s.getFileNameForPersist(fileName))
			if err == nil {
				break
			}
		}

		s.logger.Warnf("[%s][attempt #%d] failed to load subtree file from temp and persist dirs: %v", i, fileName, err)

		if errors.Is(err, os.ErrNotExist) {
			// check s3
			bytes, err = s.s3Client.Get(ctx, hash)
			if err == nil {
				break
			}
		}

		if errors.Is(err, os.ErrNotExist) {
			err = ubsverrors.ErrNotFound
			if i < maxRetries-1 {
				time.Sleep(retrySleepDuration)
			}
		} else {
			s.logger.Warnf("[%s][attempt #%d]  unable to open s3 entry: %v", fileName, i, err)
		}
	}

	return bytes, err
}

func (s *Lustre) GetHead(ctx context.Context, hash []byte, nrOfBytes int) ([]byte, error) {
	s.logger.Debugf("[File] Get: %s", utils.ReverseAndHexEncodeSlice(hash))
	fileName := s.filename(hash)

	maxRetries, _ := gocore.Config().GetInt("lustre_store_max_retries", 3)
	retrySleepDuration, err, _ := gocore.Config().GetDuration("lustre_store_retry_sleep_duration", 500*time.Millisecond)
	if err != nil {
		panic(fmt.Errorf("failed to get duration from config: %w", err))
	}

	var bytes []byte

	for i := 0; i < maxRetries; i++ {
		bytes, err = os.ReadFile(fileName)
		if err == nil {
			break
		}

		if errors.Is(err, os.ErrNotExist) {
			// check the persist sub dir
			bytes, err = os.ReadFile(s.getFileNameForPersist(fileName))
			if err == nil {
				break
			}
		}

		s.logger.Warnf("[%s] file not found in subtree temp dir: %v", fileName, err)

		if errors.Is(err, os.ErrNotExist) {
			// check s3
			bytes, err = s.s3Client.Get(ctx, hash)
			if err == nil {
				break
			}
		}

		if errors.Is(err, os.ErrNotExist) {
			err = ubsverrors.ErrNotFound
			if i < maxRetries-1 {
				time.Sleep(retrySleepDuration)
			}
		} else {
			s.logger.Warnf("[%s] unable to open file: %v", fileName, err)
		}
	}

	return bytes, err
}

func (s *Lustre) Exists(_ context.Context, hash []byte) (bool, error) {
	s.logger.Debugf("[File] Exists: %s", utils.ReverseAndHexEncodeSlice(hash))
	fileName := s.filename(hash)

	maxRetries, _ := gocore.Config().GetInt("lustre_store_max_retries", 3)
	retrySleepDuration, err, _ := gocore.Config().GetDuration("lustre_store_retry_sleep_duration", 500*time.Millisecond)
	if err != nil {
		panic(fmt.Errorf("failed to get duration from config: %w", err))
	}

	var exists bool

	for i := 0; i < maxRetries; i++ {
		_, err = os.Stat(fileName)
		if err == nil {
			exists = true
			break
		}

		if os.IsNotExist(err) {
			// check the persist sub dir
			_, err = os.Stat(s.getFileNameForPersist(fileName))
			if err == nil {
				exists = true
				break
			}
		}

		if os.IsNotExist(err) {
			exists = false
			if i < maxRetries-1 {
				time.Sleep(retrySleepDuration)
			}
		}
	}

	return exists, err
}

func (s *Lustre) Del(_ context.Context, hash []byte) error {
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

func (s *Lustre) filename(hash []byte) string {
	// determine path to use, based on the first byte of the hash and the number of paths
	path := s.paths[hash[0]%byte(len(s.paths))]
	return fmt.Sprintf("%s/%x", path, bt.ReverseBytes(hash))
}

func (s *Lustre) getFileNameForPersist(filename string) string {
	// persisted files are stored in a subdirectory
	// add the persist dir before the file in the filepath
	fileParts := strings.Split(filename, string(os.PathSeparator))
	fileParts[len(fileParts)-1] = s.persistSubDir + fileParts[len(fileParts)-1]

	// clean the paths
	return filepath.Clean("/" + filepath.Join(fileParts...))
}

func (s *Lustre) getFileNameForSet(hash []byte, opts []options.Options) (string, error) {
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
