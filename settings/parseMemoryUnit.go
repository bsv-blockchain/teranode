package settings

import (
	"errors" //nolint:depguard // refactor needed to use the internal errors package
	"regexp"
	"strconv"
	"strings"
)

// ParseMemoryUnit takes a string representing a memory size (e.g., "1GB", "512MB", "2.5KiB")
func ParseMemoryUnit(sizeStr string) (uint64, error) {
	// remove leading/trailing whitespace
	sizeStr = strings.TrimSpace(sizeStr)

	// extract the numeric value and unit using a regular expression
	re := regexp.MustCompile(`^(\d+(?:\.\d+)?)\s*([kKMGTP]?i?B)?$`)
	matches := re.FindStringSubmatch(sizeStr)

	if len(matches) != 3 {
		return 0, errors.New("invalid memory unit format: " + sizeStr)
	}

	// parse the numeric value
	size, err := strconv.ParseFloat(matches[1], 64)
	if err != nil {
		return 0, errors.New("invalid size value: " + matches[1])
	}

	// convert the size to bytes based on the unit
	unit := strings.ToUpper(matches[2])
	switch unit {
	case "", "B":
		return uint64(size), nil
	case "KB", "KIB":
		return uint64(size * 1024), nil
	case "MB", "MIB":
		return uint64(size * 1024 * 1024), nil
	case "GB", "GIB":
		return uint64(size * 1024 * 1024 * 1024), nil
	case "TB", "TIB":
		return uint64(size * 1024 * 1024 * 1024 * 1024), nil
	case "PB", "PIB":
		return uint64(size * 1024 * 1024 * 1024 * 1024 * 1024), nil
	default:
		return 0, errors.New("unsupported memory unit: " + unit)
	}
}
