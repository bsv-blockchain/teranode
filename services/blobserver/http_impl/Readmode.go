package http_impl

type ReadMode int

const (
	BINARY_STREAM ReadMode = iota
	HEX
	JSON
)

func (r ReadMode) String() string {
	switch r {
	case BINARY_STREAM:
		return "BINARY"
	case HEX:
		return "HEX"
	case JSON:
		return "JSON"
	default:
		return "UNKNOWN"
	}
}
