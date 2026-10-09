package file

import (
	"bufio"
	"context"
	"crypto/sha256"
	"hash"
	"io"
	"os"
	"path/filepath"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
)

// pendingBufferSize is how much a pending file buffers before writing to disk.
const pendingBufferSize = 1 << 20

// PendingFile is a blob written before its key is known. It is streamed into a temporary file in
// the store's own directory tree and renamed into place by Commit, so nothing is copied and the
// whole blob is never held in memory. Abort discards it.
//
// The legacy block converter streams each subtree's data file this way: the file is named by the
// subtree's root hash, which is known only once the subtree's last transaction has arrived.
// Holding the transactions until then kept 3.65 GB of parsed scripts live on mainnet on
// 2026-09-24, and garbage collection took over two thirds of the CPU scanning them.
//
// A PendingFile is not safe for concurrent use.
type PendingFile struct {
	s        *File
	fileType fileformat.FileType
	file     *os.File
	tmp      string
	w        *bufio.Writer
	hasher   hash.Hash
	written  int64
	done     bool
}

// NewPendingFile starts a blob of fileType whose key will be given to Commit. opts are those a
// Set would take; they are applied again at Commit, where the key is known.
func (s *File) NewPendingFile(_ context.Context, fileType fileformat.FileType, opts ...options.FileOption) (*PendingFile, error) {
	merged := options.MergeOptions(s.options, opts)

	if merged.SubDirectory != "" {
		if err := os.MkdirAll(filepath.Join(s.path, merged.SubDirectory), 0755); err != nil {
			return nil, errors.NewStorageError("[File][NewPendingFile] failed to create sub directory", err)
		}
	}

	// A name in the directory tree the final file will live in, so the rename at Commit
	// stays on one filesystem. The key is unknown, so a zero key stands in for it.
	placeholder, err := merged.ConstructFilename(s.path, make([]byte, 32), fileType)
	if err != nil {
		return nil, err
	}

	if err = os.MkdirAll(filepath.Dir(placeholder), 0755); err != nil {
		return nil, errors.NewStorageError("[File][NewPendingFile] failed to create directory for %s", placeholder, err)
	}

	file, tmp, err := s.createTempSibling(placeholder, 0644)
	if err != nil {
		return nil, err
	}

	p := &PendingFile{s: s, fileType: fileType, file: file, tmp: tmp, w: bufio.NewWriterSize(file, pendingBufferSize)}

	if s.checksum {
		p.hasher = sha256.New()
	}

	if !merged.SkipHeader {
		dst := io.Writer(p.w)
		if p.hasher != nil {
			dst = io.MultiWriter(p.w, p.hasher)
		}

		if err = fileformat.NewHeader(fileType).Write(dst); err != nil {
			p.Abort()

			return nil, errors.NewStorageError("[File][NewPendingFile] failed to write header", err)
		}
	}

	return p, nil
}

// Write appends to the blob's body.
func (p *PendingFile) Write(b []byte) (int, error) {
	if p.done {
		return 0, errors.NewStorageError("[File][PendingFile] write after commit or abort")
	}

	n, err := p.w.Write(b)
	if n > 0 {
		if p.hasher != nil {
			_, _ = p.hasher.Write(b[:n]) // hash.Hash never reports a write error
		}

		p.written += int64(n)
	}

	return n, err
}

// Commit publishes the blob under key, exactly as a SetFromReader of the same bytes would. A blob
// already present under key is reported as ErrBlobAlreadyExists unless opts allow overwriting,
// and the pending file is discarded either way.
func (p *PendingFile) Commit(ctx context.Context, key []byte, opts ...options.FileOption) error {
	if p.done {
		return errors.NewStorageError("[File][PendingFile] commit after commit or abort")
	}

	if err := acquireWritePermit(ctx); err != nil {
		p.Abort()

		return errors.NewStorageError("[File][PendingFile] failed to acquire write permit", err)
	}
	defer releaseWritePermit()

	if p.written == 0 {
		p.Abort()

		return errors.NewStorageError("[File][PendingFile] no bytes were written")
	}

	if err := p.w.Flush(); err != nil {
		p.Abort()

		return errors.NewStorageError("[File][PendingFile] failed to flush %s", p.tmp, err)
	}

	filename, err := p.s.constructFilename(key, p.fileType, opts)
	if err != nil {
		p.Abort()

		return errors.NewStorageError("[File][PendingFile] failed to get file name", err)
	}

	merged := options.MergeOptions(p.s.options, opts)

	if err = p.s.errorOnOverwrite(filename, merged); err != nil {
		p.Abort()

		return err
	}

	file := p.file
	p.file = nil

	if err = p.s.syncAndCloseTempFile(file, p.tmp); err != nil {
		p.Abort()

		return err
	}

	if err = p.s.publishTempFile(p.tmp, filename, merged, p.hasher); err != nil {
		p.Abort()

		return err
	}

	p.done = true

	return nil
}

// Abort discards the pending file. It is safe to call more than once, and after Commit.
func (p *PendingFile) Abort() {
	if p.done {
		return
	}

	p.done = true

	if p.file != nil {
		if err := p.file.Close(); err != nil {
			p.s.logger.Warnf("[File][PendingFile] failed to close %s: %v", p.tmp, err)
		}

		p.file = nil
	}

	if err := p.s.removeStorePath(p.tmp); err != nil && !os.IsNotExist(err) {
		p.s.logger.Warnf("[File][PendingFile] failed to remove %s: %v", p.tmp, err)
	}
}
