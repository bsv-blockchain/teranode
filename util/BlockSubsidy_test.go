package util

import (
	"testing"
)

func TestGetBlockSubsidyForHeight(t *testing.T) {
	tests := []struct {
		name            string
		height          uint32
		expectedSubsidy uint64
	}{
		{"Genesis block", 0, 5000000000},
		{"Block 1", 1, 5000000000},
		{"Last block of first period", 209999, 5000000000},
		{"First block of second period", 210000, 2500000000},
		{"Last block of second period", 419999, 2500000000},
		{"First block of third period", 420000, 1250000000},
		{"Halfway through 3rd period", 524999, 1250000000},
		{"First block of 4th period", 630000, 625000000},
		{"First block after 32 halvings", 32 * 210000, 1},
		{"Last block with non-zero subsidy", 6930000 - 1, 1},
		{"First block with zero subsidy", 6930000, 0},
		{"Block far in the future", 100000000, 0},
		{"Random check 1", 820133, 625000000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subsidy := GetBlockSubsidyForHeight(tt.height)
			if subsidy != tt.expectedSubsidy {
				t.Errorf("GetBlockSubsidyForHeight(%d) = %d, want %d",
					tt.height, subsidy, tt.expectedSubsidy)
			}
		})
	}
}

func TestSubsidyHalvingInterval(t *testing.T) {
	interval := uint32(210000)
	initialSubsidy := uint64(5000000000)

	for i := uint32(0); i < 64; i++ {
		height := i * interval
		expectedSubsidy := initialSubsidy >> i
		subsidy := GetBlockSubsidyForHeight(height)

		if subsidy != expectedSubsidy {
			t.Errorf("Incorrect subsidy at height %d: got %d, want %d",
				height, subsidy, expectedSubsidy)
		}
	}
}
