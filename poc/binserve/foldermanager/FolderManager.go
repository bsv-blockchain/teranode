package foldermanager

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

var (
	binsPath, _           = gocore.Config().Get("bins_path", "./bins")
	distributionFactor, _ = gocore.Config().GetInt("distribution_factor", 1)
	defaultExtensions     = []string{"", ".meta", ".ttl"}
	extensions, _         = gocore.Config().GetMulti("extensions", ",", defaultExtensions)
	errNoExpiredExtension = errors.New("cannot have an extension of .expired")
	errFileTooShort       = fmt.Errorf("filename must be at least %d characters long", distributionFactor)
)

type folder struct {
	path   string
	stopCh chan struct{}
	logger utils.Logger
}

type FolderManager struct {
	logger  utils.Logger
	folders []*folder
}

func New(logger utils.Logger) *FolderManager {
	if distributionFactor > 3 {
		logger.Fatalf("Max distribution factor is 3 (4096 folders)")
	}

	folderCount := int(math.Pow(16, float64(distributionFactor)))

	logger.Debugf("Distribution factor is %d: %d folder(s) will be used", distributionFactor, folderCount)

	folderManager := &FolderManager{
		logger:  logger,
		folders: make([]*folder, folderCount),
	}

	for i := 0; i < folderCount; i++ {
		path := filepath.Join(binsPath, fmt.Sprintf("%x", i))

		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			logger.Fatalf("Failed to create folder %q: %v", path, err)
		}

		f := &folder{
			path:   path,
			logger: logger,
			stopCh: make(chan struct{}),
		}

		folderManager.folders[i] = f

		go f.ttl()
	}

	return folderManager
}

func GetFilePath(filename string) (string, error) {
	if strings.HasSuffix(filename, ".expired") {
		return "", errNoExpiredExtension
	}

	if len(filename) < distributionFactor {
		return "", errFileTooShort
	}

	subfolder := filename[:distributionFactor]

	return filepath.Join(binsPath, subfolder, filename), nil
}

func (fm *FolderManager) Stop() {
	for _, f := range fm.folders {
		close(f.stopCh)
	}
}

func (f *folder) ttl() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-f.stopCh:
			return

		case <-ticker.C:
			// Get all .ttl file in path
			files, err := findFilesByExtension(f.path, ".ttl")
			if err != nil {
				f.logger.Warnf("Could not get TTL files: %v", err)
				continue
			}

			if len(files) > 0 {
				f.logger.Debugf("Found %d TTL files in %s", len(files), f.path)

				nowMillis := time.Now().UnixMilli()

				for _, file := range files {
					b, err := os.ReadFile(file)
					if err != nil {
						f.logger.Warnf("Could not read TTL file %q: %v", file, err)
						continue
					}

					expiry, _ := binary.Varint(b)

					if expiry <= nowMillis {
						filename := file[:len(file)-4]
						if err := f.expireFiles(filename); err != nil {
							f.logger.Warnf("Could not expire files for %q: %v", filename, err)
							continue
						}
					}
				}
			}
		}
	}
}

func findFilesByExtension(path, ext string) ([]string, error) {
	var a []string
	err := filepath.WalkDir(path, func(s string, d fs.DirEntry, e error) error {
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

func (f *folder) expireFiles(filename string) error {
	for _, ext := range extensions {
		oldName := filename + ext
		newName := filename + ext + ".expired"

		if err := os.Rename(oldName, newName); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				f.logger.Debugf("Renamed %q to %q - not found", oldName, newName)
				continue
			} else {
				return fmt.Errorf("could not rename %q to %q: %w", oldName, newName, err)
			}
		}

		f.logger.Debugf("Renamed %q to %q", oldName, newName)

		time.AfterFunc(5*time.Second, func() {
			if err := os.Remove(newName); err != nil {
				if errors.Is(err, os.ErrNotExist) {
					f.logger.Debugf("Removed %s - not found", newName)
				} else {
					f.logger.Warnf("could not remove %q: %v", newName, err)
					return
				}
			}
			f.logger.Debugf("Removed %s", newName)
		})
	}

	return nil
}
