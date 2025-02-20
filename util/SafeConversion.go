package util

import (
	"math"
	"math/big"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
)

// SafeUint32 converts an int to uint32 after ensuring it’s in range.
// Returns an error if the input is negative or exceeds the maximum value of a uint32.
func SafeIntToUint32(v int) (uint32, error) {
	if v < 0 || v > int(math.MaxUint32) {
		return 0, errors.NewProcessingError("value %d out of range", v)
	}

	return uint32(v), nil
}

// SafeUint64toUint32 converts a uint64 value to a uint32 value after ensuring it fits into 32 bits.
// Returns an error if the input value is too large.
func SafeUint64ToUint32(v uint64) (uint32, error) {
	// ^uint32(0) is the maximum value of a uint32 (all bits set).
	const maxUint32 = ^uint32(0)
	if v > uint64(maxUint32) {
		return 0, errors.NewProcessingError("value %d overflows uint32 (max %d)", v, maxUint32)
	}

	return uint32(v), nil
}

// SafeInt64ToUint64 safely converts an int64 to uint64.
// Returns an error if the input is negative.
func SafeInt64ToUint64(value int64) (uint64, error) {
	if value < 0 {
		return 0, errors.NewProcessingError("negative value cannot be converted to uint64: %d", value)
	}

	return uint64(value), nil
}

// SafeIntToUint64 safely converts an int to uint64.
// Returns an error if the input is negative.
func SafeIntToUint64(value int) (uint64, error) {
	if value < 0 {
		return 0, errors.NewProcessingError("negative value cannot be converted to uint64: %d", value)
	}

	return uint64(value), nil
}

// SafeUint64ToInt safely converts a uint64 to int.
// Returns an error if the value exceeds the limits of an int.
func SafeUint64ToInt(value uint64) (int, error) {
	if value > math.MaxInt {
		return 0, errors.NewProcessingError("value exceeds int limit: %d", value)
	}

	return int(value), nil
}

// SafeInt64ToInt32 safely converts an int64 to int32.
// Returns an error if the value is outside the range of int32.
func SafeInt64ToInt32(value int64) (int32, error) {
	if value < math.MinInt32 || value > math.MaxInt32 {
		return 0, errors.NewProcessingError("value out of int32 range: %d", value)
	}

	return int32(value), nil
}

// SafeIntToInt32 safely converts an int to int32.
// Checks if the value is within the valid int32 range.
func SafeIntToInt32(value int) (int32, error) {
	if value < math.MinInt32 || value > math.MaxInt32 {
		return 0, errors.NewProcessingError("value out of int32 range: %d", value)
	}

	return int32(value), nil
}

// SafeInt32ToUint32 safely converts an int32 to uint32.
// Checks only for negative values, as positive int32 values are always within uint32 range.
func SafeInt32ToUint32(value int32) (uint32, error) {
	if value < 0 {
		return 0, errors.NewProcessingError("negative value cannot be converted to uint32: %d", value)
	}

	return uint32(value), nil
}

// SafeInt64ToUint32 safely converts an int64 to uint32.
// Checks if the value is non-negative and within the uint32 range.
func SafeInt64ToUint32(value int64) (uint32, error) {
	if value < 0 {
		return 0, errors.NewProcessingError("negative value cannot be converted to uint32: %d", value)
	}

	if value > math.MaxUint32 {
		return 0, errors.NewProcessingError("value exceeds uint32 range: %d", value)
	}

	return uint32(value), nil
}

// SafeBigWordToUint32 safely converts a big.Word to uint32.
// It ensures that the value is within the valid uint32 range before conversion.
//
// The function explicitly converts `big.Word` to `uint64` first to avoid the G115
// lint warning (integer overflow conversion). This is necessary because `big.Word`
// can be either `uint32` (on 32-bit systems) or `uint64` (on 64-bit systems).
// Without this explicit conversion, the linter assumes a potential risk when
// directly converting from `big.Word` to `uint32`.
func SafeBigWordToUint32(value big.Word) (uint32, error) {
	valueUint64 := uint64(value)

	if valueUint64 > math.MaxUint32 {
		return 0, errors.NewProcessingError("big.Word exceeds uint32 range: %d", valueUint64)
	}

	return uint32(valueUint64), nil
}

// SafeUintToUint32 safely converts a uint to uint32.
// Checks if the value exceeds the uint32 range.
func SafeUintToUint32(value uint) (uint32, error) {
	if value > math.MaxUint32 {
		return 0, errors.NewProcessingError("value exceeds uint32 range: %d", value)
	}

	return uint32(value), nil
}

// SafeTimeToUint32 safely converts a time.Time's Unix timestamp to uint32.
// Checks if the timestamp is non-negative and within the uint32 range.
func SafeTimeToUint32(value time.Time) (uint32, error) {
	timestamp := value.Unix()
	if timestamp < 0 {
		return 0, errors.NewProcessingError("negative timestamp cannot be converted to uint32: %d", timestamp)
	}

	if timestamp > math.MaxUint32 {
		return 0, errors.NewProcessingError("timestamp exceeds uint32 range: %d", timestamp)
	}

	return uint32(timestamp), nil
}

// SafeUint32ToUint8 safely converts a uint32 to uint8.
// Checks if the value exceeds the uint8 range.
func SafeUint32ToUint8(value uint32) (uint8, error) {
	if value > math.MaxUint8 {
		return 0, errors.NewProcessingError("value exceeds uint8 range: %d", value)
	}

	return uint8(value), nil
}

// SafeUintptrToInt safely converts a uintptr to int.
// Checks if the value exceeds the maximum int range.
func SafeUintptrToInt(value uintptr) (int, error) {
	if value > uintptr(math.MaxInt) {
		return 0, errors.NewProcessingError("value exceeds int range: %d", value)
	}

	return int(value), nil
}

// SafeUint64ToInt64 safely converts a uint64 to int64.
// Checks if the value exceeds the maximum int64 range.
func SafeUint64ToInt64(value uint64) (int64, error) {
	if value > math.MaxInt64 {
		return 0, errors.NewProcessingError("value exceeds int64 range: %d", value)
	}

	return int64(value), nil
}

// SafeUint32ToInt32 safely converts a uint32 to int32.
// Checks if the value exceeds the maximum int32 range.
func SafeUint32ToInt32(value uint32) (int32, error) {
	if value > math.MaxInt32 {
		return 0, errors.NewProcessingError("value exceeds int32 range: %d", value)
	}

	return int32(value), nil
}

// SafeUint64ToInt32 safely converts a uint64 to int32.
// Checks if the value exceeds the int32 range or if it's negative.
func SafeUint64ToInt32(value uint64) (int32, error) {
	if value > math.MaxInt32 {
		return 0, errors.NewProcessingError("value exceeds int32 range: %d", value)
	}

	return int32(value), nil
}
