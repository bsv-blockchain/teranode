package options

import "io"

type ReaderWrapper struct {
	io.Reader
	io.Closer
}

type ReaderCloser struct {
	io.Reader
}

func (r ReaderCloser) Close() error {
	return nil
}
