package lustre

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/blob/s3"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/go-utils"
)

type s3Store interface {
	Get(ctx context.Context, key []byte, opts ...options.Options) ([]byte, error)
	GetIoReader(ctx context.Context, key []byte, opts ...options.Options) (io.ReadCloser, error)
	Exists(ctx context.Context, key []byte, opts ...options.Options) (bool, error)
}

type Lustre struct {
	paths         []string
	logger        ulogger.Logger
	options       *options.SetOptions
	persistSubDir string
	s3Client      s3Store
}

/**
* Primary usage to share files between services running in a node.
* Secondary usage to store artefacts in production.
* Used as subtree store (subtree validation), block store (block persister).
* No background TTL cleanup as per File store
* The only way to expire a file is by calling SetTTL explicitly with TTL = 0
* Has 3 layers, files in primary path, files in 'S3 persist' path and S3
 */
func New(logger ulogger.Logger, s3Url *url.URL, dir string, persistDir string, opts ...options.Options) (*Lustre, error) {
	logger = logger.New("lustre")

	var (
		err      error
		s3Client *s3.S3
	)

	logger.Infof("Creating lustre store s3 Url: %s, dir: %s, persistDir: %s", s3Url, dir, persistDir)

	if s3Url != nil && s3Url.Host != "" && s3Url.Path != "" {
		s3Client, err = s3.New(logger, s3Url)
		if err != nil {
			return nil, errors.NewStorageError("[Lustre] failed to create s3 client", err)
		}
	} else {
		s3Client = nil
		logger.Infof("[Lustre] S3 URL (host and path) is not provided, S3 client will not be created")
	}

	return NewLustreStore(logger, s3Client, dir, persistDir, opts...)
}

func NewLustreStore(logger ulogger.Logger, s3Client s3Store, dir string, persistDir string, opts ...options.Options) (*Lustre, error) {
	logger = logger.New("lustre")

	options := options.NewSetOptions(nil, opts...)

	if options.PrefixDirectory > 0 {
		logger.Warnf("[Lustre] prefix directory option will be ignored (only supported in S3 store)")
	}

	if options.SubDirectory != "" {
		logger.Warnf("[Lustre] subdirectory option will be ignored (only supported in S3 store)")
	}

	lustreStore := &Lustre{
		paths:         []string{dir + "/"},
		logger:        logger,
		options:       options,
		persistSubDir: persistDir + "/",
		s3Client:      s3Client,
	}

	// create directory if not exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, errors.NewStorageError("[Lustre] failed to create main lustre directory: %s", dir, err)
	}

	persistPath := path.Join(dir, persistDir)
	if err := os.MkdirAll(persistPath, 0755); err != nil {
		return nil, errors.NewStorageError("[Lustre] failed to create persist lustre directory: %s", persistPath, err)
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
	s.logger.Debugf("[Lustre] SetFromReader: %s", utils.ReverseAndHexEncodeSlice(key))

	defer reader.Close()

	fileName, err := s.getFileNameForSet(key, opts...)
	if err != nil {
		return errors.NewStorageError("[Lustre][SetFromReader] [%s] failed to get file name", utils.ReverseAndHexEncodeSlice(key), err)
	}

	if _, err := os.Stat(fileName); err == nil {
		return errors.NewBlobAlreadyExistsError("[Lustre][SetFromReader] [%s] already exists in store", fileName)
	}

	// write the bytes from the reader to a file with the filename
	file, err := os.Create(fileName + ".tmp")
	if err != nil {
		return errors.NewStorageError("[Lustre][SetFromReader] [%s] failed to create file", fileName, err)
	}
	defer file.Close()

	if _, err := io.Copy(file, reader); err != nil {
		return errors.NewStorageError("[Lustre][SetFromReader] [%s] failed to write data to file", fileName, err)
	}

	// rename the file to the final name
	if err := os.Rename(fileName+".tmp", fileName); err != nil {
		if _, err := os.Stat(fileName); err == nil {
			// there is another thread/process creating the same Tx at the same time!!!!
			return errors.NewBlobAlreadyExistsError("[Lustre][Set] [%s] already exists in store", fileName)
		}

		return errors.NewStorageError("[Lustre][SetFromReader] [%s] failed to rename file from tmp", fileName, err)
	}

	return nil
}

func (s *Lustre) Set(_ context.Context, hash []byte, value []byte, opts ...options.Options) error {
	s.logger.Debugf("[Lustre]  Set: %s", utils.ReverseAndHexEncodeSlice(hash))

	fileName, err := s.getFileNameForSet(hash, opts...)
	if err != nil {
		return errors.NewStorageError("[Lustre][Set] [%s] failed to get file name", utils.ReverseAndHexEncodeSlice(hash), err)
	}

	if _, err := os.Stat(fileName); err == nil {
		return errors.NewBlobAlreadyExistsError("[Lustre][Set] [%s] already exists in store", fileName)
	}

	// write bytes to file
	//nolint:gosec // G306: Expect WriteFile permissions to be 0600 or less (gosec)
	if err := os.WriteFile(fileName+".tmp", value, 0644); err != nil {
		return errors.NewStorageError("[Lustre][Set] [%s] failed to write data to file", fileName, err)
	}

	// rename the file to the final name
	if err := os.Rename(fileName+".tmp", fileName); err != nil {
		if _, err := os.Stat(fileName); err == nil {
			// there is another thread/process creating the same Tx at the same time!!!!
			return errors.NewBlobAlreadyExistsError("[Lustre][Set] [%s] already exists in store", fileName)
		}

		return errors.NewStorageError("[Lustre][Set] [%s] failed to rename file from tmp", fileName, err)
	}

	return nil
}

func (s *Lustre) SetTTL(_ context.Context, hash []byte, ttl time.Duration, opts ...options.Options) error {
	fileName, err := s.getFileNameForGet(hash, opts...)
	if err != nil {
		return err
	}

	persistedFilename := s.getFileNameForPersist(fileName)
	if ttl <= 0 {
		// check whether the persisted file exists
		_, err := os.Stat(persistedFilename)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return errors.NewStorageError("[Lustre] [%s] unable to stat file", persistedFilename, err)
		}

		// the file is already persisted
		if err == nil {
			return nil
		}

		// err is ErrNotExist, so the file should be persisted, copy it from the main dir to the persist dir
		f, err := os.Open(fileName)
		if err != nil {
			return errors.NewStorageError("[Lustre][SetTTL] [%s] unable to open file", fileName, err)
		}
		defer f.Close()

		persistedFile, err := os.Create(persistedFilename)
		if err != nil {
			return errors.NewStorageError("[Lustre] [%s] unable to create file", persistedFilename, err)
		}
		defer persistedFile.Close()

		if _, err = io.Copy(persistedFile, f); err != nil {
			return errors.NewStorageError("[Lustre] [%s] unable to copy file", fileName, err)
		}

		return nil
	}

	_, err = os.Stat(fileName)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.NewStorageError("[Lustre] [%s] unable to stat file", persistedFilename, err)
	}

	// the file is already exists in the main dir, remove it from the persist dir
	if err == nil {
		return os.Remove(fileName)
	}

	// the filename should be moved from the persist sub dir to the main dir
	return os.Rename(persistedFilename, fileName)
}

func (s *Lustre) GetIoReader(ctx context.Context, hash []byte, opts ...options.Options) (io.ReadCloser, error) {
	fileName, err := s.getFileNameForGet(hash, opts...)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(fileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// check the persist sub dir
			file, err = os.Open(s.getFileNameForPersist(fileName))
			if err != nil {
				// s.logger.Warnf("[Lustre][GetIoReader] [%s] file not found in subtree temp dir: %v", fileName, err)
				if errors.Is(err, os.ErrNotExist) {
					if s.s3Client == nil {
						return nil, errors.ErrNotFound
					}

					// check s3
					fileReader, err := s.s3Client.GetIoReader(ctx, hash, opts...)
					if err != nil {
						if errors.Is(err, os.ErrNotExist) {
							return nil, errors.ErrNotFound
						}

						return nil, errors.NewStorageError("[Lustre][GetIoReader] [%s] unable to open S3 file", fileName, err)
					}

					return fileReader, nil
				}

				return nil, errors.NewStorageError("[Lustre][GetIoReader] [%s] unable to open persist file", fileName, err)
			}

			return file, nil
		}

		return nil, errors.NewStorageError("[Lustre][GetIoReader] [%s] unable to open file", fileName, err)
	}

	return file, nil
}

func (s *Lustre) Get(ctx context.Context, hash []byte, opts ...options.Options) ([]byte, error) {
	s.logger.Debugf("[Lustre]  Get: %s", utils.ReverseAndHexEncodeSlice(hash))

	fileName, err := s.getFileNameForGet(hash, opts...)
	if err != nil {
		return nil, err
	}

	bytes, err := os.ReadFile(fileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			s.logger.Warnf("[Lustre][Get] [%s] file not found in local dir: %v", fileName, err)
			// check the persist sub dir
			bytes, err = os.ReadFile(s.getFileNameForPersist(fileName))
			if err != nil {
				s.logger.Warnf("[Lustre][Get] [%s] file not found in persist dir: %v", fileName, err)

				if errors.Is(err, os.ErrNotExist) {
					if s.s3Client == nil {
						return nil, errors.ErrNotFound
					}

					// check s3
					bytes, err = s.s3Client.Get(ctx, hash, opts...)
					if err != nil {
						if errors.Is(err, os.ErrNotExist) {
							return nil, errors.ErrNotFound
						}

						return nil, errors.NewStorageError("[Lustre][Get] [%s] unable to open S3 file", fileName, err)
					}

					return bytes, nil
				}

				return nil, errors.NewStorageError("[Lustre][Get] [%s] failed to read data from persist file", fileName, err)
			}

			return bytes, nil
		}

		return nil, errors.NewStorageError("[Lustre][Get] [%s] failed to read data from file", fileName, err)
	}

	return bytes, err
}

func (s *Lustre) GetHead(ctx context.Context, hash []byte, nrOfBytes int, opts ...options.Options) ([]byte, error) {
	s.logger.Debugf("[File] Get: %s", utils.ReverseAndHexEncodeSlice(hash))

	fileName, err := s.getFileNameForGet(hash, opts...)
	if err != nil {
		return nil, err
	}

	bytes, err := os.ReadFile(fileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// check the persist sub dir
			bytes, err = os.ReadFile(s.getFileNameForPersist(fileName))
			if err != nil {
				// s.logger.Warnf("[Lustre][GetHead] [%s] file not found in subtree temp dir: %v", fileName, err)
				if errors.Is(err, os.ErrNotExist) {
					if s.s3Client == nil {
						return nil, errors.ErrNotFound
					}

					// check s3
					bytes, err = s.s3Client.Get(ctx, hash, opts...)
					if err != nil {
						if errors.Is(err, os.ErrNotExist) {
							return nil, errors.ErrNotFound
						}

						return nil, errors.NewStorageError("[Lustre][GetHead] [%s] unable to open S3 file", fileName, err)
					}

					return bytes[:nrOfBytes], nil
				}

				return nil, errors.NewStorageError("[Lustre][GetHead] [%s] failed to read data from persist file", fileName, err)
			}

			return bytes[:nrOfBytes], nil
		}

		return nil, errors.NewStorageError("[Lustre][GetHead] [%s] failed to read data from file", fileName, err)
	}

	return bytes[:nrOfBytes], err
}

func (s *Lustre) Exists(_ context.Context, hash []byte, opts ...options.Options) (bool, error) {
	fileName, err := s.getFileNameForGet(hash, opts...)
	if err != nil {
		return false, err
	}

	_, err = os.Stat(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			// check the persist sub dir
			_, err = os.Stat(s.getFileNameForPersist(fileName))
			if err != nil {
				if os.IsNotExist(err) {
					if s.s3Client == nil {
						return false, nil
					}

					// check s3
					exists, err := s.s3Client.Exists(context.Background(), hash, opts...)
					if err != nil {
						if errors.Is(err, os.ErrNotExist) {
							return false, nil
						}

						return false, errors.NewStorageError("[Lustre] failed to read data from S3 file", err)
					}

					return exists, nil
				}

				return false, errors.NewStorageError("[Lustre] failed to read data from persist file", err)
			}

			return true, nil
		}

		return false, errors.NewStorageError("[Lustre] failed to read data from file", err)
	}

	return true, nil
}

func (s *Lustre) Del(_ context.Context, hash []byte, opts ...options.Options) error {
	s.logger.Debugf("[Lustre] Del: %s", utils.ReverseAndHexEncodeSlice(hash))

	fileName, err := s.getFileNameForGet(hash, opts...)
	if err != nil {
		return err
	}

	// remove ttl file, if exists
	errPersist := os.Remove(s.getFileNameForPersist(fileName))
	err = os.Remove(fileName)

	if err != nil && errPersist != nil {
		return err
	}

	return nil
}

func (s *Lustre) filename(hash []byte) string {
	// determine path to use, based on the first byte of the hash and the number of paths
	path := s.paths[hash[0]%byte(len(s.paths))]
	return filepath.Join(path, hex.EncodeToString(bt.ReverseBytes(hash)))
}

func (s *Lustre) getFileNameForPersist(filename string) string {
	// persisted files are stored in a subdirectory
	// add the persist dir before the file in the filepath
	fileParts := strings.Split(filename, string(os.PathSeparator))
	fileParts[len(fileParts)-1] = s.persistSubDir + fileParts[len(fileParts)-1]
	if fileParts[0] == "" {
		fileParts[0] = "/"
	}

	// clean the paths
	// return filepath.Clean(filepath.Join(fileParts...))
	return filepath.Join(fileParts...)
}

func (s *Lustre) getFileNameForGet(hash []byte, opts ...options.Options) (string, error) {
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
			s.logger.Warnf("[Lustre] SubDirectory %q ignored when no opt.Filename specified", fileOptions.SubDirectory)
		}

		fileName = s.filename(hash)
	}

	if fileOptions.Extension != "" {
		fileName = fmt.Sprintf("%s.%s", fileName, fileOptions.Extension)
	}

	return fileName, nil
}
func (s *Lustre) getFileNameForSet(hash []byte, opts ...options.Options) (string, error) {
	fileName, err := s.getFileNameForGet(hash, opts...)
	if err != nil {
		return "", err
	}

	fileOptions := options.NewSetOptions(s.options, opts...)

	if fileOptions.TTL <= 0 {
		// the file should be persisted
		fileName = s.getFileNameForPersist(fileName)
	}

	return fileName, nil
}
