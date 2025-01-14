package options

import (
	"bytes"

	"github.com/bitcoin-sv/teranode/errors"
)

type Footer struct {
	size          int
	eofMarker     []byte
	fetchMetaData func() []byte
}

func NewFooter(size int, eofMarker []byte, fetchMetaData func() []byte) *Footer {
	return &Footer{
		size:          size,
		eofMarker:     eofMarker,
		fetchMetaData: fetchMetaData,
	}
}

func (f *Footer) Size() int {
	return f.size
}

func (f *Footer) GetFooter() ([]byte, error) {
	metaData := []byte{}
	if f.fetchMetaData != nil {
		metaData = f.fetchMetaData()
	}

	size := len(f.eofMarker) + len(metaData)

	if size != f.size {
		return nil, errors.NewBlobFooterSizeMismatchError("footer size mismatch, expected %d got %d", f.size, size)
	}

	return append(f.eofMarker, metaData...), nil
}

func (f *Footer) IsFooter(b []byte) bool {
	if len(b) < f.size {
		return false
	}

	return bytes.HasPrefix(b, f.eofMarker)
}

func (f *Footer) GetFooterMetaData(footerBytes []byte) []byte {
	return footerBytes[len(f.eofMarker):]
}
