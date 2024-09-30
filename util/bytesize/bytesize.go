package bytesize

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/bitcoin-sv/ubsv/errors"
)

// ByteSize represents a memory size in bytes
type ByteSize int

const (
	B  ByteSize = 1
	KB          = B * 1024
	MB          = KB * 1024
	GB          = MB * 1024
	TB          = GB * 1024
)

func Parse(s string) (ByteSize, error) {
	s = strings.TrimSpace(s)
	s = strings.ToUpper(s)

	i := strings.IndexFunc(s, func(r rune) bool {
		return (r < '0' || r > '9') && r != '.'
	})

	var (
		num  float64
		err  error
		unit string
	)

	if i == -1 {
		// If no unit is specified, assume bytes
		num, err = strconv.ParseFloat(s, 64)
		unit = "B"
	} else {
		num, err = strconv.ParseFloat(s[:i], 64)
		unit = strings.TrimSpace(s[i:])
	}

	if err != nil {
		return 0, errors.NewProcessingError("invalid number", err)
	}

	var bytes ByteSize

	switch unit {
	case "B":
		bytes = ByteSize(num)
	case "KB", "K":
		bytes = ByteSize(num * float64(KB))
	case "MB", "M":
		bytes = ByteSize(num * float64(MB))
	case "GB", "G":
		bytes = ByteSize(num * float64(GB))
	case "TB", "T":
		bytes = ByteSize(num * float64(TB))
	default:
		return 0, errors.NewProcessingError("invalid unit %v", unit)
	}

	return bytes, nil
}

// String returns a human-readable string representation of the ByteSize
func (b ByteSize) String() string {
	abs := b
	if b < 0 {
		abs = -b
	}

	switch {
	case abs >= TB:
		return fmt.Sprintf("%.2f TB", float64(b)/float64(TB))
	case abs >= GB:
		return fmt.Sprintf("%.2f GB", float64(b)/float64(GB))
	case abs >= MB:
		return fmt.Sprintf("%.2f MB", float64(b)/float64(MB))
	case abs >= KB:
		return fmt.Sprintf("%.2f KB", float64(b)/float64(KB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
func (b ByteSize) Int() int {
	return int(b)
}
