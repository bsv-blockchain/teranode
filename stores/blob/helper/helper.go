package helper

import (
	"bytes"
	"io"
	"os"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
)

type headerFooterReader struct {
	reader          io.ReadCloser
	header          []byte
	footer          *options.Footer
	headerProcessed bool
	buffer          []byte // Buffer for content including potential footer
}

func ReaderWithHeaderAndFooterRemoved(r io.ReadCloser, header []byte, footer *options.Footer) (io.ReadCloser, error) {
	if header == nil && footer == nil {
		return r, nil
	}

	hfr := &headerFooterReader{
		reader:          r,
		header:          header,
		footer:          footer,
		headerProcessed: false,
		buffer:          []byte{},
	}

	return hfr, nil
}

func (r *headerFooterReader) Read(p []byte) (n int, err error) {
	// Handle header on first read
	if err = r.handleHeader(); err != nil {
		return 0, err
	}

	footerSize := 0
	if r.footer != nil {
		footerSize = r.footer.Size()
	}

	// Read enough to include potential footer
	readSize := len(p) + footerSize

	bufferSize := len(r.buffer)

	// Fill buffer
	if bufferSize < readSize {
		r.buffer = append(r.buffer, make([]byte, readSize-len(r.buffer))...)
		nRead, err := r.reader.Read(r.buffer[bufferSize:])
		r.buffer = r.buffer[:bufferSize+nRead]

		if err != nil && err != io.EOF {
			return 0, err
		}
	}

	// Copy data to output, excluding footer if present
	available := len(r.buffer) - footerSize

	if available > 0 {
		n = copy(p, r.buffer[:available])
		r.buffer = r.buffer[n:]

		return n, nil
	}

	// We have nothing available
	// it's time to verify the footer
	if r.footer != nil {
		if len(r.buffer) < footerSize {
			return 0, ErrInvalidFooter
		}

		if !r.verifyFooter(r.buffer) {
			return 0, ErrInvalidFooter
		}
	}

	r.buffer = nil

	return 0, io.EOF
}

func (r *headerFooterReader) handleHeader() error {
	if !r.headerProcessed {
		if len(r.header) > 0 {
			headerBuf := make([]byte, len(r.header))

			_, err := io.ReadFull(r.reader, headerBuf)
			if err != nil {
				return err
			}

			if !bytes.Equal(headerBuf, r.header) {
				return ErrInvalidHeader
			}
		}

		r.headerProcessed = true
	}

	return nil
}

func (r *headerFooterReader) verifyFooter(buf []byte) bool {
	if len(buf) < r.footer.Size() {
		return false
	}

	footerBytes := buf[len(buf)-r.footer.Size():]

	return r.footer.IsFooter(footerBytes)
}

func (r *headerFooterReader) Close() error {
	return r.reader.Close()
}

// BytesWithHeadAndFooterRemoved removes header and footer from data if they exist
func BytesWithHeadAndFooterRemoved(b []byte, header []byte, footer *options.Footer) ([]byte, error) {
	if len(b) == 0 {
		return b, nil
	}

	if header != nil {
		if len(b) < len(header) {
			return nil, errors.NewStorageError("bytes shorter than header")
		}

		if !bytes.Equal(b[:len(header)], header) {
			return nil, errors.NewStorageError("header mismatch")
		}

		b = b[len(header):]
	}

	if footer != nil {
		size := footer.Size()
		if len(b) < size {
			return nil, errors.NewStorageError("bytes shorter than footer")
		}

		footerBytes := b[len(b)-size:]
		if !footer.IsFooter(footerBytes) {
			return nil, errors.NewStorageError("footer mismatch")
		}

		b = b[:len(b)-size]
	}

	return b, nil
}

// ErrInvalidHeader is returned when the header is invalid
var ErrInvalidHeader = io.ErrUnexpectedEOF

// ErrInvalidFooter is returned when the footer is invalid
var ErrInvalidFooter = io.ErrUnexpectedEOF

// ReadLastNBytes reads the last n bytes from a file
func ReadLastNBytes(file *os.File, size int64) ([]byte, error) {
	// Get the current file size
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, errors.NewBlobError("failed to get file info", err)
	}

	fileSize := fileInfo.Size()
	if fileSize < size {
		return nil, errors.NewBlobError("file size %d is smaller than footer size %d", fileSize, size)
	}

	// Seek to the position where footer should start
	_, err = file.Seek(-size, io.SeekEnd)
	if err != nil {
		return nil, errors.NewBlobError("failed to seek to footer position", err)
	}

	// Read the footer bytes
	footer := make([]byte, size)

	_, err = io.ReadFull(file, footer)
	if err != nil {
		return nil, errors.NewBlobError("failed to read footer", err)
	}

	return footer, nil
}

func HeaderMetaData(file *os.File, headerSize int) ([]byte, error) {
	headerBytes := make([]byte, headerSize)

	_, err := file.Read(headerBytes)
	if err != nil {
		return nil, err
	}

	return headerBytes, nil
}

func FooterMetaData(file *os.File, footer *options.Footer) ([]byte, error) {
	footerBytes, err := ReadLastNBytes(file, int64(footer.Size()))
	if err != nil {
		return nil, err
	}

	if !footer.IsFooter(footerBytes) {
		return nil, errors.NewStorageError("[File][GetMetadata] footer not found", err)
	}

	return footer.GetFooterMetaData(footerBytes), nil
}
