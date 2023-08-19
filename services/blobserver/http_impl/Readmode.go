package http_impl

type ReadMode int

const (
	BINARY_STREAM ReadMode = iota
	HEX
	JSON
)
