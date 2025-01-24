package tstore

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/bitcoin-sv/teranode/errors"
)

// TStoreService is used to implement TStoreServer.
type TStoreService struct {
	UnimplementedTStoreServer
	rootDir string
}

// ReadFile implements the ReadFile RPC method
func (s *TStoreService) ReadFile(ctx context.Context, req *ReadFileRequest) (*ReadFileResponse, error) {
	slog.Info("ReadFile", "FilePath", req.GetFilePath())

	var readContent []byte

	if absPath, err := s.getAbsPath(req.GetFilePath()); err == nil {
		file, err := os.Open(absPath)
		if err != nil {
			return nil, err
		}

		defer file.Close()

		// Read all the content
		if readContent, err = io.ReadAll(file); err != nil {
			return nil, err
		}
	} else {
		slog.Error("unable to get absolute path", "FilePath", req.GetFilePath())
		return nil, err
	}

	return &ReadFileResponse{Content: readContent}, nil
}

// Copy implements the Copy RPC method
// For now, the destination file to be created assume its parent directory exist
// Otherwise it'll throw an error
func (s *TStoreService) Copy(ctx context.Context, req *CopyRequest) (*CopyResponse, error) {
	slog.Info("Copy", "SrcPath", req.GetSrcPath(), "DestPath", req.GetDestPath())

	absPathSrc, errSrc := s.getAbsPath(req.GetSrcPath())
	if errSrc != nil {
		slog.Error("unable to get absolute path", "SrcPath", req.GetSrcPath())
		return nil, errSrc
	}

	absPathDest, errDest := s.getAbsPath(req.GetDestPath())
	if errDest != nil {
		slog.Error("unable to get absolute path", "DestPath", req.GetDestPath())
		return nil, errDest
	}

	srcFileStat, err := os.Stat(absPathSrc)
	if err != nil {
		return nil, err
	}

	if !srcFileStat.Mode().IsRegular() {
		return nil, errors.New(errors.ERR_UNKNOWN, "source file is not a regular file")
	}

	source, err := os.Open(absPathSrc)
	if err != nil {
		return nil, err
	}
	defer source.Close()

	destination, err := os.Create(absPathDest)
	if err != nil {
		return nil, err
	}
	defer destination.Close()

	_, errCopy := io.Copy(destination, source)
	if errCopy != nil {
		return nil, errCopy
	}

	return &CopyResponse{Ok: true}, nil
}

// Glob implements the Glob RPC method.
func (s *TStoreService) Glob(ctx context.Context, req *GlobRequest) (*GlobResponse, error) {
	slog.Info("Glob", "RootPath", req.GetRootPath())

	absPathRoot, err := s.getAbsPath(req.GetRootPath())
	if err != nil {
		slog.Error("unable to get absolute path", "RootPath", req.GetRootPath())
		return nil, err
	}

	files := []string{}

	// Walk the directory tree
	if err := filepath.Walk(absPathRoot, func(path string, info fs.FileInfo, errWalk error) error {
		if errWalk != nil {
			return errWalk
		}

		// Make sure it is a file
		if info.IsDir() {
			return nil
		}

		// If not reqired recursive, then skip the grand child files
		if !req.Recursive {
			grandParentDir := filepath.Dir(filepath.Dir(path))
			if strings.Contains(grandParentDir, absPathRoot) {
				return nil
			}
		}

		files = append(files, path)

		return nil
	}); err != nil {
		return nil, err
	}

	return &GlobResponse{Paths: files}, nil
}

// getAbsPath get the absolut path from relative path
func (s *TStoreService) getAbsPath(relPath string) (string, error) {
	return filepath.Abs(fmt.Sprintf("%v/%v", s.rootDir, relPath))
}

// NewTStoreService initialize TStore Service, providing an existing directory
// as a root directory to manage.
func NewTStoreService(rootPath string) TStoreServer {
	absPath, err := filepath.Abs(rootPath)
	if err != nil {
		slog.Error("unable to get absolute path", "rootPath", rootPath)
		panic(err)
	}

	slog.Info("Create new TStoreService with", "RootDir", absPath)

	info, err := os.Stat(rootPath)
	if os.IsNotExist(err) {
		panic(err)
	}

	if !info.IsDir() {
		panic(fmt.Sprintf("%v is not a directory", rootPath))
	}

	return &TStoreService{
		rootDir: rootPath,
	}
}
