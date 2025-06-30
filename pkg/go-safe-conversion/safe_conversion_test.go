package safe_test

import (
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/pkg/go-safe-conversion"
)

func TestIntToUint32(t *testing.T) {
	tests := []struct {
		name    string
		input   int
		expect  uint32
		wantErr bool
	}{
		{"zero value", 0, 0, false},
		{"positive value", 100, 100, false},
		{"max uint32", math.MaxUint32, math.MaxUint32, false},
		{"negative value", -1, 0, true},
		{"value too large", math.MaxInt, 0, true}, // Assuming this is on a 64-bit system where MaxInt > MaxUint32
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := safe.IntToUint32(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("IntToUint32(%d) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != tt.expect {
				t.Errorf("IntToUint32(%d) = %d, want %d", tt.input, result, tt.expect)
			}
		})
	}
}

func TestUint64ToUint32(t *testing.T) {
	tests := []struct {
		name    string
		input   uint64
		expect  uint32
		wantErr bool
	}{
		{"zero value", 0, 0, false},
		{"positive value", 100, 100, false},
		{"max uint32", uint64(math.MaxUint32), math.MaxUint32, false},
		{"value too large", uint64(math.MaxUint32) + 1, 0, true},
		{"value much too large", math.MaxUint64, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := safe.Uint64ToUint32(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Uint64ToUint32(%d) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != tt.expect {
				t.Errorf("Uint64ToUint32(%d) = %d, want %d", tt.input, result, tt.expect)
			}
		})
	}
}

func TestInt64ToUint64(t *testing.T) {
	tests := []struct {
		name    string
		input   int64
		expect  uint64
		wantErr bool
	}{
		{"zero value", 0, 0, false},
		{"positive value", 100, 100, false},
		{"max int64", math.MaxInt64, uint64(math.MaxInt64), false},
		{"negative value", -1, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := safe.Int64ToUint64(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Int64ToUint64(%d) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != tt.expect {
				t.Errorf("Int64ToUint64(%d) = %d, want %d", tt.input, result, tt.expect)
			}
		})
	}
}

func TestIntToUint64(t *testing.T) {
	tests := []struct {
		name    string
		input   int
		expect  uint64
		wantErr bool
	}{
		{"zero value", 0, 0, false},
		{"positive value", 100, 100, false},
		{"max int", math.MaxInt, uint64(math.MaxInt), false},
		{"negative value", -1, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := safe.IntToUint64(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("IntToUint64(%d) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != tt.expect {
				t.Errorf("IntToUint64(%d) = %d, want %d", tt.input, result, tt.expect)
			}
		})
	}
}

func TestUint64ToInt(t *testing.T) {
	tests := []struct {
		name    string
		input   uint64
		expect  int
		wantErr bool
	}{
		{"zero value", 0, 0, false},
		{"positive value", 100, 100, false},
		{"max int", uint64(math.MaxInt), math.MaxInt, false},
		{"value too large", uint64(math.MaxInt) + 1, 0, true},
		{"value much too large", math.MaxUint64, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := safe.Uint64ToInt(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Uint64ToInt(%d) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != tt.expect {
				t.Errorf("Uint64ToInt(%d) = %d, want %d", tt.input, result, tt.expect)
			}
		})
	}
}

func TestInt64ToInt32(t *testing.T) {
	tests := []struct {
		name    string
		input   int64
		expect  int32
		wantErr bool
	}{
		{"zero value", 0, 0, false},
		{"positive value", 100, 100, false},
		{"max int32", int64(math.MaxInt32), math.MaxInt32, false},
		{"min int32", int64(math.MinInt32), math.MinInt32, false},
		{"value too large", int64(math.MaxInt32) + 1, 0, true},
		{"value too small", int64(math.MinInt32) - 1, 0, true},
		{"max int64", math.MaxInt64, 0, true},
		{"min int64", math.MinInt64, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := safe.Int64ToInt32(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Int64ToInt32(%d) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != tt.expect {
				t.Errorf("Int64ToInt32(%d) = %d, want %d", tt.input, result, tt.expect)
			}
		})
	}
}

func TestIntToInt32(t *testing.T) {
	tests := []struct {
		name    string
		input   int
		expect  int32
		wantErr bool
	}{
		{"zero value", 0, 0, false},
		{"positive value", 100, 100, false},
		{"max int32", int(math.MaxInt32), math.MaxInt32, false},
		{"min int32", int(math.MinInt32), math.MinInt32, false},
		// These tests below assume a 64-bit system where int can exceed int32 range
		{"value too large", int(math.MaxInt32) + 1, 0, true},
		{"value too small", int(math.MinInt32) - 1, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := safe.IntToInt32(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("IntToInt32(%d) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != tt.expect {
				t.Errorf("IntToInt32(%d) = %d, want %d", tt.input, result, tt.expect)
			}
		})
	}
}

func TestInt32ToUint32(t *testing.T) {
	tests := []struct {
		name    string
		input   int32
		expect  uint32
		wantErr bool
	}{
		{"zero value", 0, 0, false},
		{"positive value", 100, 100, false},
		{"max int32", math.MaxInt32, uint32(math.MaxInt32), false},
		{"negative value", -1, 0, true},
		{"min int32", math.MinInt32, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := safe.Int32ToUint32(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Int32ToUint32(%d) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != tt.expect {
				t.Errorf("Int32ToUint32(%d) = %d, want %d", tt.input, result, tt.expect)
			}
		})
	}
}

func TestInt64ToUint32(t *testing.T) {
	tests := []struct {
		name    string
		input   int64
		expect  uint32
		wantErr bool
	}{
		{"zero value", 0, 0, false},
		{"positive value", 100, 100, false},
		{"max uint32", int64(math.MaxUint32), math.MaxUint32, false},
		{"negative value", -1, 0, true},
		{"value too large", int64(math.MaxUint32) + 1, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := safe.Int64ToUint32(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Int64ToUint32(%d) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != tt.expect {
				t.Errorf("Int64ToUint32(%d) = %d, want %d", tt.input, result, tt.expect)
			}
		})
	}
}

func TestBigWordToUint32(t *testing.T) {
	tests := []struct {
		name    string
		input   big.Word
		expect  uint32
		wantErr bool
	}{
		{"zero value", big.Word(0), 0, false},
		{"positive value", big.Word(100), 100, false},
		{"max uint32", big.Word(math.MaxUint32), math.MaxUint32, false},
		// This test might not work on 32-bit systems where big.Word is uint32
		{"value too large", big.Word(uint64(math.MaxUint32) + 1), 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := safe.BigWordToUint32(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("BigWordToUint32(%v) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != tt.expect {
				t.Errorf("BigWordToUint32(%v) = %d, want %d", tt.input, result, tt.expect)
			}
		})
	}
}

func TestIntToUint16(t *testing.T) {
	tests := []struct {
		name    string
		input   int
		expect  uint16
		wantErr bool
	}{
		{"zero value", 0, 0, false},
		{"positive value", 100, 100, false},
		{"max uint16", int(math.MaxUint16), math.MaxUint16, false},
		{"negative value", -1, 0, true},
		{"value too large", int(math.MaxUint16) + 1, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := safe.IntToUint16(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("IntToUint16(%d) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != tt.expect {
				t.Errorf("IntToUint16(%d) = %d, want %d", tt.input, result, tt.expect)
			}
		})
	}
}

func TestIntToInt16(t *testing.T) {
	tests := []struct {
		name    string
		input   int
		expect  int16
		wantErr bool
	}{
		{"zero value", 0, 0, false},
		{"positive value", 100, 100, false},
		{"max int16", 32767, 32767, false},   // MaxInt16 is 32767
		{"min int16", -32768, -32768, false}, // MinInt16 is -32768
		{"value too large", 32768, 0, true},  // MaxInt16 + 1
		{"value too small", -32769, 0, true}, // MinInt16 - 1
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := safe.IntToInt16(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("IntToInt16(%d) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != tt.expect {
				t.Errorf("IntToInt16(%d) = %d, want %d", tt.input, result, tt.expect)
			}
		})
	}
}

func TestUintToUint32(t *testing.T) {
	tests := []struct {
		name    string
		input   uint
		expect  uint32
		wantErr bool
	}{
		{"zero value", 0, 0, false},
		{"positive value", 100, 100, false},
		{"max uint32", uint(math.MaxUint32), math.MaxUint32, false},
		// This test might not work on 32-bit systems where uint is uint32
		{"value too large", uint(uint64(math.MaxUint32) + 1), 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := safe.UintToUint32(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("UintToUint32(%d) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != tt.expect {
				t.Errorf("UintToUint32(%d) = %d, want %d", tt.input, result, tt.expect)
			}
		})
	}
}

func TestTimeToUint32(t *testing.T) {
	tests := []struct {
		name    string
		input   time.Time
		expect  uint32
		wantErr bool
	}{
		{"epoch start", time.Unix(0, 0), 0, false},
		{"positive timestamp", time.Unix(100, 0), 100, false},
		{"max uint32 timestamp", time.Unix(math.MaxUint32, 0), math.MaxUint32, false},
		{"negative timestamp", time.Unix(-1, 0), 0, true},
		{"timestamp too large", time.Unix(int64(math.MaxUint32)+1, 0), 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := safe.TimeToUint32(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("TimeToUint32(%v) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != tt.expect {
				t.Errorf("TimeToUint32(%v) = %d, want %d", tt.input, result, tt.expect)
			}
		})
	}
}

func TestUint32ToUint8(t *testing.T) {
	tests := []struct {
		name    string
		input   uint32
		expect  uint8
		wantErr bool
	}{
		{"zero value", 0, 0, false},
		{"positive value", 100, 100, false},
		{"max uint8", uint32(math.MaxUint8), math.MaxUint8, false},
		{"value too large", uint32(math.MaxUint8) + 1, 0, true},
		{"max uint32", math.MaxUint32, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := safe.Uint32ToUint8(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Uint32ToUint8(%d) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != tt.expect {
				t.Errorf("Uint32ToUint8(%d) = %d, want %d", tt.input, result, tt.expect)
			}
		})
	}
}

func TestUintptrToInt(t *testing.T) {
	tests := []struct {
		name    string
		input   uintptr
		expect  int
		wantErr bool
	}{
		{"zero value", 0, 0, false},
		{"positive value", 100, 100, false},
		{"max int", uintptr(math.MaxInt), math.MaxInt, false},
		{"value too large", uintptr(math.MaxInt) + 1, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := safe.UintptrToInt(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("UintptrToInt(%d) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != tt.expect {
				t.Errorf("UintptrToInt(%d) = %d, want %d", tt.input, result, tt.expect)
			}
		})
	}
}

func TestUint64ToInt64(t *testing.T) {
	tests := []struct {
		name    string
		input   uint64
		expect  int64
		wantErr bool
	}{
		{"zero value", 0, 0, false},
		{"positive value", 100, 100, false},
		{"max int64", uint64(math.MaxInt64), math.MaxInt64, false},
		{"value too large", uint64(math.MaxInt64) + 1, 0, true},
		{"max uint64", math.MaxUint64, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := safe.Uint64ToInt64(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Uint64ToInt64(%d) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != tt.expect {
				t.Errorf("Uint64ToInt64(%d) = %d, want %d", tt.input, result, tt.expect)
			}
		})
	}
}

func TestUint32ToInt32(t *testing.T) {
	tests := []struct {
		name    string
		input   uint32
		expect  int32
		wantErr bool
	}{
		{"zero value", 0, 0, false},
		{"positive value", 100, 100, false},
		{"max int32", uint32(math.MaxInt32), math.MaxInt32, false},
		{"value too large", uint32(math.MaxInt32) + 1, 0, true},
		{"max uint32", math.MaxUint32, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := safe.Uint32ToInt32(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Uint32ToInt32(%d) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != tt.expect {
				t.Errorf("Uint32ToInt32(%d) = %d, want %d", tt.input, result, tt.expect)
			}
		})
	}
}

func TestUint64ToInt32(t *testing.T) {
	tests := []struct {
		name    string
		input   uint64
		expect  int32
		wantErr bool
	}{
		{"zero value", 0, 0, false},
		{"positive value", 100, 100, false},
		{"max int32", uint64(math.MaxInt32), math.MaxInt32, false},
		{"value too large", uint64(math.MaxInt32) + 1, 0, true},
		{"max uint64", math.MaxUint64, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := safe.Uint64ToInt32(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Uint64ToInt32(%d) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != tt.expect {
				t.Errorf("Uint64ToInt32(%d) = %d, want %d", tt.input, result, tt.expect)
			}
		})
	}
}

func TestUint32ToInt64(t *testing.T) {
	tests := []struct {
		name    string
		input   uint32
		expect  int64
		wantErr bool
	}{
		{"zero value", 0, 0, false},
		{"positive value", 100, 100, false},
		{"max uint32", math.MaxUint32, int64(math.MaxUint32), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := safe.Uint32ToInt64(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Uint32ToInt64(%d) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != tt.expect {
				t.Errorf("Uint32ToInt64(%d) = %d, want %d", tt.input, result, tt.expect)
			}
		})
	}
}

func TestUint32ToUint64(t *testing.T) {
	tests := []struct {
		name    string
		input   uint32
		expect  uint64
		wantErr bool
	}{
		{"zero value", 0, 0, false},
		{"positive value", 100, 100, false},
		{"max uint32", math.MaxUint32, uint64(math.MaxUint32), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := safe.Uint32ToUint64(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Uint32ToUint64(%d) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != tt.expect {
				t.Errorf("Uint32ToUint64(%d) = %d, want %d", tt.input, result, tt.expect)
			}
		})
	}
}

func TestUint64ToUint16(t *testing.T) {
	tests := []struct {
		name    string
		input   uint64
		expect  uint16
		wantErr bool
	}{
		{"zero value", 0, 0, false},
		{"positive value", 100, 100, false},
		{"max uint16", uint64(math.MaxUint16), math.MaxUint16, false},
		{"value too large", uint64(math.MaxUint16) + 1, 0, true},
		{"max uint64", math.MaxUint64, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := safe.Uint64ToUint16(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Uint64ToUint16(%d) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != tt.expect {
				t.Errorf("Uint64ToUint16(%d) = %d, want %d", tt.input, result, tt.expect)
			}
		})
	}
}
