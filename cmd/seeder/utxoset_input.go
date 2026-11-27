package seeder

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/klauspost/compress/zstd"
)

// compressedUTXOSetExtension marks a UTXO set compressed with zstd. The path
// is either one .zst file or a directory of .zst pieces, read in name order.
// A concatenation of zstd frames is one valid zstd stream, so a snapshot cut
// into byte ranges and compressed range by range decodes back to the original.
const compressedUTXOSetExtension = ".zst"

// zstdDecoderConcurrency is how many blocks a zstd stream decoder keeps in
// flight; 1 decodes synchronously on the reading goroutine. On a 4 GiB slice
// of the mainnet set, one synchronous decoder ran at 1.8 GB/s and four async
// ones at 1.2 GB/s, both far above the import's own rate (about 53 MB/s into
// PostgreSQL), so the default is the faster and simpler one.
var zstdDecoderConcurrency = 1

// findUTXOSet returns the path of the UTXO set for hash in inputDir: the plain
// <hash>.utxo-set, or <hash>.utxo-set.zst as a file or a directory of pieces.
// When neither exists it returns the plain path, so the caller reports that
// name as missing. Both at once is refused: the seed would read one and the
// operator could not tell which. A path that exists but cannot be checked is an
// error, not an absence.
func findUTXOSet(inputDir, hash string) (string, error) {
	plain := filepath.Join(inputDir, hash+".utxo-set")
	compressed := plain + compressedUTXOSetExtension

	plainExists, err := pathExists(plain)
	if err != nil {
		return "", err
	}

	compressedExists, err := pathExists(compressed)
	if err != nil {
		return "", err
	}

	switch {
	case plainExists && compressedExists:
		return "", errors.NewConfigurationError("both %s and %s exist; remove one so the seed reads the UTXO set you meant", plain, compressed)
	case compressedExists:
		return compressed, nil
	default:
		return plain, nil
	}
}

// pathExists reports whether path exists; any failure other than its absence is an error.
func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)

	switch {
	case err == nil:
		return true, nil
	case os.IsNotExist(err):
		return false, nil
	default:
		return false, errors.NewStorageError("cannot check %s", path, err)
	}
}

// isCompressedUTXOSet reports whether path names a zstd-compressed UTXO set.
func isCompressedUTXOSet(path string) bool {
	return strings.HasSuffix(path, compressedUTXOSetExtension)
}

// uncompressedUTXOSetPath returns the name the UTXO set at path has when
// uncompressed. Its checksum sidecar sits at that name plus ".sha256" and
// hashes the uncompressed bytes.
func uncompressedUTXOSetPath(path string) string {
	return strings.TrimSuffix(path, compressedUTXOSetExtension)
}

// utxoSetPieces returns the zstd files of the compressed UTXO set at path in
// stream order: path itself when it is a file, otherwise every *.zst file in
// the directory, sorted by name. Other files, such as a fetch's unfinished
// .part files, are ignored. Nothing here notices a missing piece: one that
// removes records is caught later by the footer counts or the checksum.
//
// Every piece must be a regular file. Each import pass opens the pieces itself,
// and two passes on one named pipe would split its bytes between them; a pipe
// can also block a read that cancellation cannot reach. A pipe goes under the
// plain <hash>.utxo-set name instead, which the import checks for itself.
func utxoSetPieces(path string) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, errors.NewStorageError("failed to open compressed UTXO set %s", path, err)
	}

	if !info.IsDir() {
		if !info.Mode().IsRegular() {
			return nil, errors.NewConfigurationError("compressed UTXO set %s is not a regular file; a named pipe must use the plain .utxo-set name", path)
		}

		return []string{path}, nil
	}

	entries, err := os.ReadDir(path) // sorted by name
	if err != nil {
		return nil, errors.NewStorageError("failed to list the pieces of %s", path, err)
	}

	var pieces []string

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), compressedUTXOSetExtension) {
			continue
		}

		piece := filepath.Join(path, e.Name())

		pieceInfo, err := os.Stat(piece)
		if err != nil {
			return nil, errors.NewStorageError("failed to open UTXO set piece %s", piece, err)
		}

		if !pieceInfo.Mode().IsRegular() {
			return nil, errors.NewConfigurationError("UTXO set piece %s is not a regular file; a named pipe must use the plain .utxo-set name", piece)
		}

		pieces = append(pieces, piece)
	}

	if len(pieces) == 0 {
		return nil, errors.NewProcessingError("compressed UTXO set directory %s holds no *%s pieces", path, compressedUTXOSetExtension)
	}

	return pieces, nil
}

// openRegularFile opens path without blocking and refuses anything but a
// regular file. utxoSetPieces checked the type earlier, but a piece replaced by
// a named pipe since then would otherwise block this open beyond any cancel.
// O_NONBLOCK changes nothing for a regular file's reads.
func openRegularFile(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, errors.NewStorageError("failed to open UTXO set piece %s", path, err)
	}

	info, err := f.Stat()
	if err == nil && !info.Mode().IsRegular() {
		err = errors.NewConfigurationError("UTXO set piece %s is not a regular file; a named pipe must use the plain .utxo-set name", path)
	}

	if err != nil {
		_ = f.Close()
		return nil, err
	}

	return f, nil
}

// pieceReader reads files one after another, with one open at a time.
type pieceReader struct {
	pieces []string
	cur    *os.File
}

func (p *pieceReader) Read(b []byte) (int, error) {
	for {
		if p.cur == nil {
			if len(p.pieces) == 0 {
				return 0, io.EOF
			}

			f, err := openRegularFile(p.pieces[0])
			if err != nil {
				return 0, err
			}

			p.cur, p.pieces = f, p.pieces[1:]
		}

		// An *os.File reports io.EOF only from a read that returned no bytes.
		n, err := p.cur.Read(b)
		if err == io.EOF {
			_ = p.cur.Close()
			p.cur = nil

			continue
		}

		return n, err
	}
}

func (p *pieceReader) Close() error {
	if p.cur == nil {
		return nil
	}

	err := p.cur.Close()
	p.cur = nil

	return err
}

// utxoSetDecoder keeps the decoder's terminal error even when the record
// reader treats io.ErrUnexpectedEOF as the snapshot footer boundary.
type utxoSetDecoder struct {
	decoder *zstd.Decoder
	err     error
}

func (d *utxoSetDecoder) Read(b []byte) (int, error) {
	n, err := d.decoder.Read(b)
	if err != nil {
		d.err = err
	}

	return n, err
}

// utxoSetStream reads the uncompressed bytes of a UTXO set, from a plain file
// or through a zstd decoder, optionally hashing every byte it reads.
type utxoSetStream struct {
	reader  *bufio.Reader
	file    *os.File // the plain file, so the footer can be read by seeking; nil when compressed
	close   func() error
	decoder *utxoSetDecoder // nil for a plain file
	sum     hash.Hash       // nil unless the stream is being hashed
}

// openUTXOSetStream opens the UTXO set at path (see findUTXOSet). With hashIt,
// every byte the reader consumes is also fed to a SHA-256 for verify.
func openUTXOSetStream(path string, hashIt bool) (*utxoSetStream, error) {
	s := &utxoSetStream{}

	var src io.Reader

	if isCompressedUTXOSet(path) {
		pieces, err := utxoSetPieces(path)
		if err != nil {
			return nil, err
		}

		pr := &pieceReader{pieces: pieces}

		dec, err := zstd.NewReader(pr, zstd.WithDecoderConcurrency(zstdDecoderConcurrency))
		if err != nil {
			return nil, errors.NewProcessingError("failed to start the zstd decoder for %s", path, err)
		}

		s.decoder = &utxoSetDecoder{decoder: dec}
		src = s.decoder
		s.close = func() error {
			dec.Close()
			return pr.Close()
		}
	} else {
		f, err := os.Open(path)
		if err != nil {
			return nil, errors.NewStorageError("failed to open file", err)
		}

		src, s.file, s.close = f, f, f.Close
	}

	if hashIt {
		s.sum = sha256.New()
		src = io.TeeReader(src, s.sum)
	}

	s.reader = bufio.NewReader(src)

	return s, nil
}

// footerSource is what readUTXOFrames may seek to read the footer: the plain
// file, or nil for a compressed stream, whose footer can only come from the
// stream itself. A nil *os.File must not become a non-nil io.Reader.
func (s *utxoSetStream) footerSource() io.Reader {
	if s.file == nil {
		return nil
	}

	return s.file
}

// Close releases the file or the decoder and its open piece.
func (s *utxoSetStream) Close() error {
	return s.close()
}

// verify compares the SHA-256 of every byte read with expected. The caller
// must have read the stream to its end: readUTXOFrames does when it finds the
// footer, since the footer is the last 16 bytes of the stream.
func (s *utxoSetStream) verify(expected, path string) error {
	actual := hex.EncodeToString(s.sum.Sum(nil))
	if actual != expected {
		return errors.NewProcessingError("checksum mismatch for %s: its sidecar says %s, the uncompressed stream hashes to %s", path, expected, actual)
	}

	return nil
}

// verifyUTXOSetFile reads the whole UTXO set at path through the hash and
// checks it against expected, for a run that uses the set without importing
// it, and returns the tip read from the verified bytes. An empty expected
// checks nothing and returns no tip. It returns as soon as ctx is cancelled,
// even when an open or a read of a named pipe is blocked: that read is left
// to end with the process, which is about to exit.
func verifyUTXOSetFile(ctx context.Context, path, expected string) (*utxoSetTip, error) {
	if expected == "" {
		return nil, nil
	}

	type result struct {
		tip *utxoSetTip
		err error
	}

	done := make(chan result, 1)

	go func() {
		tip, err := readAndVerifyUTXOSetFile(ctx, path, expected)
		done <- result{tip, err}
	}()

	select {
	case r := <-done:
		return r.tip, r.err
	case <-ctx.Done():
		return nil, errors.NewProcessingError("checksum verification of %s cancelled", path, ctx.Err())
	}
}

// readAndVerifyUTXOSetFile is verifyUTXOSetFile's read, checking ctx between reads.
func readAndVerifyUTXOSetFile(ctx context.Context, path, expected string) (*utxoSetTip, error) {
	s, hash, height, err := openUTXOSetFile(path, true)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = s.Close()
	}()

	buf := make([]byte, 1<<20)

	for {
		if err = ctx.Err(); err != nil {
			return nil, errors.NewProcessingError("checksum verification of %s cancelled", path, err)
		}

		if _, err = s.reader.Read(buf); err == io.EOF {
			break
		}

		if err != nil {
			return nil, errors.NewStorageError("failed to read %s for checksum verification", path, err)
		}
	}

	if err = s.verify(expected, path); err != nil {
		return nil, err
	}

	return &utxoSetTip{hash: hash, height: height}, nil
}
