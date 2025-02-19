package tstore

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/ulogger"
)

// TStoreService is used to implement TStoreServer.
type TStoreService struct {
	UnimplementedTStoreServer
	rootDir string
	logger  ulogger.Logger
}

// ReadFile implements the ReadFile RPC method
func (s *TStoreService) ReadFile(ctx context.Context, req *ReadFileRequest) (*ReadFileResponse, error) {
	s.logger.Infof("ReadFile FilePath %v", req.GetFilePath())

	var readContent []byte

	if absPath, err := s.getAbsPath(req.GetFilePath()); err == nil {
		file, err := os.Open(absPath)
		if err != nil {
			return nil, err
		}

		defer file.Close()

		s.logger.Infof("ReadFile Abs FilePath %v", absPath)

		// Read all the content
		if readContent, err = io.ReadAll(file); err != nil {
			return nil, err
		}
	} else {
		s.logger.Errorf("unable to get absolute path FilePath %v", req.GetFilePath())
		return nil, err
	}

	return &ReadFileResponse{Content: readContent}, nil
}

// Copy implements the Copy RPC method
// For now, the destination file to be created assume its parent directory exist
// Otherwise it'll throw an error
func (s *TStoreService) Copy(ctx context.Context, req *CopyRequest) (*CopyResponse, error) {
	s.logger.Infof("Copy SrcPath %v  DestPath %v", req.GetSrcPath(), req.GetDestPath())

	absPathSrc, errSrc := s.getAbsPath(req.GetSrcPath())
	if errSrc != nil {
		s.logger.Errorf("unable to get absolute path SrcPath %v", req.GetSrcPath())
		return nil, errSrc
	}

	absPathDest, errDest := s.getAbsPath(req.GetDestPath())
	if errDest != nil {
		s.logger.Errorf("unable to get absolute path DestPath %v", req.GetDestPath())
		return nil, errDest
	}

	s.logger.Infof("Copy Abs SrcPath %v Abs DestPath %v", absPathSrc, absPathDest)

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
	s.logger.Infof("Glob RootPath : %v", req.GetRootPath())
	absPathRoot, err := s.getAbsPath(req.GetRootPath())

	if err != nil {
		s.logger.Errorf("unable to get absolute path RootPath : %v", req.GetRootPath())
		return nil, err
	}

	workDir := absPathRoot
	matchPattern := ""

	if info, err := os.Stat(workDir); os.IsNotExist(err) || !info.IsDir() {
		// If the path does not exist as a directory, then the path /my/data/filepattern
		// is not a directory, we'll walk through the /my/data and search for files with pattern filepattern*
		matchPattern = workDir
		workDir = filepath.Dir(workDir)

		s.logger.Infof("Request dir %v does not exist", absPathRoot)
		s.logger.Infof("working on its parent dir %v", workDir)
	}

	s.logger.Infof("Glob Working Dir : %v", workDir)

	files := []string{}

	// Walk the directory tree
	if err := filepath.Walk(workDir, func(path string, info fs.FileInfo, errWalk error) error {
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

		if strings.Contains(path, matchPattern) {
			files = append(files, path)
		}

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
func NewTStoreService(rootPath string, l ulogger.Logger) TStoreServer {
	absPath, err := filepath.Abs(rootPath)
	if err != nil {
		l.Errorf("unable to get absolute path rootPath %v", rootPath)
		panic(err)
	}

	l.Infof("Create new TStoreService with RootDir %v", absPath)

	info, err := os.Stat(rootPath)
	if os.IsNotExist(err) {
		l.Errorf("Directory not exist %v", absPath)
		panic(err)
	}

	if !info.IsDir() {
		l.Errorf("Not a directory %v", absPath)
		panic(fmt.Sprintf("%v is not a directory", rootPath))
	}

	return &TStoreService{
		rootDir: rootPath,
		logger:  l,
	}
}
