package seeder

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"

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
// operator could not tell which.
func findUTXOSet(inputDir, hash string) (string, error) {
	plain := filepath.Join(inputDir, hash+".utxo-set")
	compressed := plain + compressedUTXOSetExtension

	_, plainErr := os.Stat(plain)
	_, compressedErr := os.Stat(compressed)

	switch {
	case plainErr == nil && compressedErr == nil:
		return "", errors.NewConfigurationError("both %s and %s exist; remove one so the seed reads the UTXO set you meant", plain, compressed)
	case compressedErr == nil:
		return compressed, nil
	default:
		return plain, nil
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
// .part files, are ignored; a piece missing from the middle is caught by the
// footer counts and the checksum, not here.
func utxoSetPieces(path string) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, errors.NewStorageError("failed to open compressed UTXO set %s", path, err)
	}

	if !info.IsDir() {
		return []string{path}, nil
	}

	entries, err := os.ReadDir(path) // sorted by name
	if err != nil {
		return nil, errors.NewStorageError("failed to list the pieces of %s", path, err)
	}

	var pieces []string

	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), compressedUTXOSetExtension) {
			pieces = append(pieces, filepath.Join(path, e.Name()))
		}
	}

	if len(pieces) == 0 {
		return nil, errors.NewProcessingError("compressed UTXO set directory %s holds no *%s pieces", path, compressedUTXOSetExtension)
	}

	return pieces, nil
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

			f, err := os.Open(p.pieces[0])
			if err != nil {
				return 0, errors.NewStorageError("failed to open UTXO set piece %s", p.pieces[0], err)
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

// utxoSetStream reads the uncompressed bytes of a UTXO set, from a plain file
// or through a zstd decoder, optionally hashing every byte it reads.
type utxoSetStream struct {
	reader *bufio.Reader
	file   *os.File // the plain file, so the footer can be read by seeking; nil when compressed
	close  func() error
	sum    hash.Hash // nil unless the stream is being hashed
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

		src = dec
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

// verify compares the SHA-256 of every byte read with expected. It is called
// once readUTXOFrames has found the footer at the very end of the stream, so
// every byte has been read; a mismatch means the records came from a snapshot
// that is not the one the sidecar describes.
func (s *utxoSetStream) verify(expected, path string) error {
	actual := hex.EncodeToString(s.sum.Sum(nil))
	if actual != expected {
		return errors.NewProcessingError("checksum mismatch for %s: its sidecar says %s, the uncompressed stream hashes to %s. The UTXO store now holds records from this snapshot: wipe the UTXO store, fetch the snapshot again and re-run the seed", path, expected, actual)
	}

	return nil
}
