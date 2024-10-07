package lustre

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/blob/s3"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/go-utils"
)

type s3Store interface {
	Get(ctx context.Context, key []byte, opts ...options.FileOption) ([]byte, error)
	GetIoReader(ctx context.Context, key []byte, opts ...options.FileOption) (io.ReadCloser, error)
	Exists(ctx context.Context, key []byte, opts ...options.FileOption) (bool, error)
}

type Lustre struct {
	path          string
	logger        ulogger.Logger
	options       *options.Options
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
func New(logger ulogger.Logger, s3Url *url.URL, dir string, persistDir string, opts ...options.StoreOption) (*Lustre, error) {
	logger = logger.New("lustre")

	var (
		err      error
		s3Client s3Store
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

func NewLustreStore(logger ulogger.Logger, s3Client s3Store, dir string, persistDir string, opts ...options.StoreOption) (*Lustre, error) {
	logger = logger.New("lustre")

	storeOptions := options.NewStoreOptions(opts...)

	lustreStore := &Lustre{
		path:          dir + "/",
		logger:        logger,
		options:       storeOptions,
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

func (s *Lustre) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	var issues []string

	// Check main path
	if err := checkDirectoryPermissions(s.path); err != nil {
		issues = append(issues, fmt.Sprintf("Main path issue: %v", err))
	}

	// Check persist subdirectory
	persistPath := filepath.Join(s.path, s.persistSubDir)
	if err := checkDirectoryPermissions(persistPath); err != nil {
		issues = append(issues, fmt.Sprintf("Persist subdirectory issue: %v", err))
	}

	// Check S3 client if configured
	if s.s3Client != nil {
		if err := checkS3Connection(ctx, s.s3Client); err != nil {
			issues = append(issues, fmt.Sprintf("S3 client issue: %v", err))
		}
	}

	if len(issues) > 0 {
		return http.StatusServiceUnavailable, fmt.Sprintf("Lustre blob Store issues: %v", issues), nil
	}

	return http.StatusOK, "Lustre blob Store healthy", nil
}

func checkDirectoryPermissions(path string) error {
	// Check if directory exists
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to stat directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("path is not a directory")
	}

	// Check read, write, and delete permissions with a single file operation
	testFile := filepath.Join(path, ".lustre_test_rwx")

	// Check write permission
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		return fmt.Errorf("failed to write test file: %w", err)
	}

	// Check read permission
	if _, err := os.ReadFile(testFile); err != nil {
		return fmt.Errorf("failed to read test file: %w", err)
	}

	// Check delete permission
	if err := os.Remove(testFile); err != nil {
		return fmt.Errorf("failed to delete test file: %w", err)
	}

	return nil
}

func checkS3Connection(ctx context.Context, s3Client s3Store) error {
	// Create a context with a timeout
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Attempt to check if a known non-existent key exists
	// This should return false without an error if the connection is working
	exists, err := s3Client.Exists(ctx, []byte("lustre_health_check_nonexistent_key"))
	if err != nil {
		return fmt.Errorf("failed to check S3 connection: %w", err)
	}
	if exists {
		return fmt.Errorf("unexpected result from S3 connection check")
	}

	return nil
}

func (s *Lustre) Close(_ context.Context) error {
	return nil
}

func (s *Lustre) SetFromReader(_ context.Context, key []byte, reader io.ReadCloser, opts ...options.FileOption) error {
	defer reader.Close()

	s.logger.Debugf("[Lustre] SetFromReader: %s", utils.ReverseAndHexEncodeSlice(key))

	filename, err := s.getFilenameForSet(key, opts)
	if err != nil {
		return errors.NewStorageError("[Lustre][SetFromReader] [%s] failed to get file name", utils.ReverseAndHexEncodeSlice(key), err)
	}

	merged := options.MergeOptions(s.options, opts)

	if !merged.AllowOverwrite {
		if _, err := os.Stat(filename); err == nil {
			return errors.NewBlobAlreadyExistsError("[Lustre][SetFromReader] [%s] already exists in store", filename)
		}
	}

	tmpFilename := filename + ".tmp"

	// write the bytes from the reader to a f with the filename
	f, err := os.Create(tmpFilename)
	if err != nil {
		return errors.NewStorageError("[Lustre][SetFromReader] [%s] failed to create file", filename, err)
	}
	defer f.Close()

	if _, err := io.Copy(f, reader); err != nil {
		return errors.NewStorageError("[Lustre][SetFromReader] [%s] failed to write data to file", filename, err)
	}

	// rename the file to the final name
	if err := os.Rename(tmpFilename, filename); err != nil {
		if _, err := os.Stat(filename); err == nil {
			// there is another thread/process creating the same Tx at the same time!!!!
			return errors.NewBlobAlreadyExistsError("[Lustre][Set] [%s] already exists in store", filename)
		}

		return errors.NewStorageError("[Lustre][SetFromReader] [%s] failed to rename file from tmp", filename, err)
	}

	return nil
}

func (s *Lustre) Set(_ context.Context, hash []byte, value []byte, opts ...options.FileOption) error {
	s.logger.Debugf("[Lustre]  Set: %s", utils.ReverseAndHexEncodeSlice(hash))

	filename, err := s.getFilenameForSet(hash, opts)
	if err != nil {
		return errors.NewStorageError("[Lustre][Set] [%s] failed to get file name", utils.ReverseAndHexEncodeSlice(hash), err)
	}

	merged := options.MergeOptions(s.options, opts)

	if !merged.AllowOverwrite {
		if _, err := os.Stat(filename); err == nil {
			return errors.NewBlobAlreadyExistsError("[Lustre][Set] [%s] already exists in store", filename)
		}
	}

	tmpFilename := filename + ".tmp"

	// write bytes to file
	//nolint:gosec // G306: Expect WriteFile permissions to be 0600 or less (gosec)
	if err := os.WriteFile(tmpFilename, value, 0644); err != nil {
		return errors.NewStorageError("[Lustre][Set] [%s] failed to write data to file", filename, err)
	}

	// rename the file to the final name
	if err := os.Rename(tmpFilename, filename); err != nil {
		if _, err := os.Stat(filename); err == nil {
			// there is another thread/process creating the same Tx at the same time!!!!
			return errors.NewBlobAlreadyExistsError("[Lustre][Set] [%s] already exists in store", filename)
		}

		return errors.NewStorageError("[Lustre][Set] [%s] failed to rename file from tmp", filename, err)
	}

	return nil
}

func (s *Lustre) SetTTL(_ context.Context, hash []byte, ttl time.Duration, opts ...options.FileOption) error {
	merged := options.MergeOptions(s.options, opts)

	filename, err := merged.ConstructFilename(s.path, hash)
	if err != nil {
		return err
	}

	persistedFilename, err := merged.ConstructFilename(filepath.Join(s.path, s.persistSubDir), hash)
	if err != nil {
		return err
	}

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
		f, err := os.Open(filename)
		if err != nil {
			return errors.NewStorageError("[Lustre][SetTTL] [%s] unable to open file", filename, err)
		}
		defer f.Close()

		persistedFile, err := os.Create(persistedFilename)
		if err != nil {
			return errors.NewStorageError("[Lustre] [%s] unable to create file", persistedFilename, err)
		}
		defer persistedFile.Close()

		if _, err = io.Copy(persistedFile, f); err != nil {
			return errors.NewStorageError("[Lustre] [%s] unable to copy file", filename, err)
		}

		return nil
	}

	_, err = os.Stat(filename)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.NewStorageError("[Lustre] [%s] unable to stat file", persistedFilename, err)
	}

	// the file is already exists in the main dir, remove it from the persist dir
	if err == nil {
		return os.Remove(filename)
	}

	// the filename should be moved from the persist sub dir to the main dir
	return os.Rename(persistedFilename, filename)
}

func (s *Lustre) GetTTL(_ context.Context, hash []byte, opts ...options.FileOption) (time.Duration, error) {
	merged := options.MergeOptions(s.options, opts)

	filename, err := merged.ConstructFilename(s.path, hash)
	if err != nil {
		return 0, err
	}

	_, err = os.Stat(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// check the persist sub dir
			persistedFilename, err := merged.ConstructFilename(filepath.Join(s.path, s.persistSubDir), hash)
			if err != nil {
				return 0, err
			}

			_, err = os.Stat(persistedFilename)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return 0, errors.ErrNotFound
				}

				return 0, errors.NewStorageError("[Lustre] failed to read data from persist file", err)
			}

			return 0, nil
		}

		return 0, errors.NewStorageError("[Lustre] failed to read data from file", err)
	}

	// file exists in the ttl dir, so we can return the default TTL
	return *s.options.TTL, nil
}

func (s *Lustre) GetIoReader(ctx context.Context, hash []byte, opts ...options.FileOption) (io.ReadCloser, error) {
	merged := options.MergeOptions(s.options, opts)

	filename, err := merged.ConstructFilename(s.path, hash)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// check the persist sub dir
			persistedFilename, err := merged.ConstructFilename(filepath.Join(s.path, s.persistSubDir), hash)
			if err != nil {
				return nil, err
			}

			f, err = os.Open(persistedFilename)
			if err != nil {
				// s.logger.Warnf("[Lustre][GetIoReader] [%s] file not found in subtree temp dir: %v", filename, err)
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

						return nil, errors.NewStorageError("[Lustre][GetIoReader] [%s] unable to open S3 file", filename, err)
					}

					return fileReader, nil
				}

				return nil, errors.NewStorageError("[Lustre][GetIoReader] [%s] unable to open persist file", filename, err)
			}

			return f, nil
		}

		return nil, errors.NewStorageError("[Lustre][GetIoReader] [%s] unable to open file", filename, err)
	}

	return f, nil
}

func (s *Lustre) Get(ctx context.Context, hash []byte, opts ...options.FileOption) ([]byte, error) {
	s.logger.Debugf("[Lustre]  Get: %s", utils.ReverseAndHexEncodeSlice(hash))

	merged := options.MergeOptions(s.options, opts)

	filename, err := merged.ConstructFilename(s.path, hash)
	if err != nil {
		return nil, err
	}

	bytes, err := os.ReadFile(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			s.logger.Warnf("[Lustre][Get] [%s] file not found in local dir: %v", filename, err)
			// check the persist sub dir
			persistedFilename, err := merged.ConstructFilename(filepath.Join(s.path, s.persistSubDir), hash)
			if err != nil {
				return nil, err
			}

			bytes, err = os.ReadFile(persistedFilename)
			if err != nil {
				s.logger.Warnf("[Lustre][Get] [%s] file not found in persist dir: %v", filename, err)

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

						return nil, errors.NewStorageError("[Lustre][Get] [%s] unable to open S3 file", filename, err)
					}

					return bytes, nil
				}

				return nil, errors.NewStorageError("[Lustre][Get] [%s] failed to read data from persist file", filename, err)
			}

			return bytes, nil
		}

		return nil, errors.NewStorageError("[Lustre][Get] [%s] failed to read data from file", filename, err)
	}

	return bytes, err
}

func (s *Lustre) GetHead(ctx context.Context, hash []byte, nrOfBytes int, opts ...options.FileOption) ([]byte, error) {
	s.logger.Debugf("[File] Get: %s", utils.ReverseAndHexEncodeSlice(hash))

	merged := options.MergeOptions(s.options, opts)

	filename, err := merged.ConstructFilename(s.path, hash)
	if err != nil {
		return nil, err
	}

	bytes, err := os.ReadFile(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// check the persist sub dir
			persistedFilename, err := merged.ConstructFilename(filepath.Join(s.path, s.persistSubDir), hash)
			if err != nil {
				return nil, err
			}

			bytes, err = os.ReadFile(persistedFilename)
			if err != nil {
				// s.logger.Warnf("[Lustre][GetHead] [%s] file not found in subtree temp dir: %v", filename, err)
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

						return nil, errors.NewStorageError("[Lustre][GetHead] [%s] unable to open S3 file", filename, err)
					}

					return bytes[:nrOfBytes], nil
				}

				return nil, errors.NewStorageError("[Lustre][GetHead] [%s] failed to read data from persist file", filename, err)
			}

			return bytes[:nrOfBytes], nil
		}

		return nil, errors.NewStorageError("[Lustre][GetHead] [%s] failed to read data from file", filename, err)
	}

	return bytes[:nrOfBytes], err
}

func (s *Lustre) Exists(_ context.Context, hash []byte, opts ...options.FileOption) (bool, error) {
	merged := options.MergeOptions(s.options, opts)

	filename, err := merged.ConstructFilename(s.path, hash)
	if err != nil {
		return false, err
	}

	_, err = os.Stat(filename)
	if err != nil {
		if os.IsNotExist(err) {
			// check the persist sub dir
			persistedFilename, err := merged.ConstructFilename(filepath.Join(s.path, s.persistSubDir), hash)
			if err != nil {
				return false, err
			}

			_, err = os.Stat(persistedFilename)
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

func (s *Lustre) Del(_ context.Context, hash []byte, opts ...options.FileOption) error {
	s.logger.Debugf("[Lustre] Del: %s", utils.ReverseAndHexEncodeSlice(hash))

	merged := options.MergeOptions(s.options, opts)

	filename, err := merged.ConstructFilename(s.path, hash)
	if err != nil {
		return err
	}

	persistedFilename, err := merged.ConstructFilename(filepath.Join(s.path, s.persistSubDir), hash)
	if err != nil {
		return err
	}

	// remove ttl file, if exists
	errPersist := os.Remove(persistedFilename)
	err = os.Remove(filename)

	if err != nil && errPersist != nil {
		return err
	}

	return nil
}

func (s *Lustre) getFilenameForSet(hash []byte, opts []options.FileOption) (string, error) {
	basePath := s.path

	merged := options.MergeOptions(s.options, opts)

	if merged.TTL == nil || *merged.TTL <= 0 {
		// the file should be persisted
		basePath = filepath.Join(basePath, s.persistSubDir)
	}

	filename, err := merged.ConstructFilename(basePath, hash)
	if err != nil {
		return "", err
	}

	return filename, nil
}
