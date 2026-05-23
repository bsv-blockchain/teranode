package util

import (
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/assert"
)

func TestDetermineStorageMode(t *testing.T) {
	tests := []struct {
		name                 string
		blockPersisterHeight uint32
		bestHeight           uint32
		retentionWindow      uint32
		prunerBlockTrigger   string
		expectedMode         string
	}{
		{
			name:                 "Full node - persister at same height as best",
			blockPersisterHeight: 100,
			bestHeight:           100,
			retentionWindow:      288,
			prunerBlockTrigger:   settings.PrunerBlockTriggerOnBlockPersisted,
			expectedMode:         "full",
		},
		{
			name:                 "Full node - persister within retention window",
			blockPersisterHeight: 100,
			bestHeight:           200,
			retentionWindow:      288,
			prunerBlockTrigger:   settings.PrunerBlockTriggerOnBlockPersisted,
			expectedMode:         "full",
		},
		{
			name:                 "Full node - persister exactly at retention boundary",
			blockPersisterHeight: 100,
			bestHeight:           388, // 100 + 288 = 388
			retentionWindow:      288,
			prunerBlockTrigger:   settings.PrunerBlockTriggerOnBlockPersisted,
			expectedMode:         "full",
		},
		{
			name:                 "Pruned node - persister beyond retention window",
			blockPersisterHeight: 100,
			bestHeight:           389, // 100 + 289 = 389 (1 block beyond)
			retentionWindow:      288,
			prunerBlockTrigger:   settings.PrunerBlockTriggerOnBlockPersisted,
			expectedMode:         "pruned",
		},
		{
			name:                 "Pruned node - persister not running (height 0)",
			blockPersisterHeight: 0,
			bestHeight:           1000,
			retentionWindow:      288,
			prunerBlockTrigger:   settings.PrunerBlockTriggerOnBlockPersisted,
			expectedMode:         "pruned",
		},
		{
			name:                 "Full node - default retention window (0 means use default 288)",
			blockPersisterHeight: 100,
			bestHeight:           200,
			retentionWindow:      0, // Should use default 288
			prunerBlockTrigger:   settings.PrunerBlockTriggerOnBlockPersisted,
			expectedMode:         "full",
		},
		{
			name:                 "Pruned node - lag exceeds default retention window",
			blockPersisterHeight: 100,
			bestHeight:           500, // lag = 400, exceeds default 288
			retentionWindow:      0,   // Should use default 288
			prunerBlockTrigger:   settings.PrunerBlockTriggerOnBlockPersisted,
			expectedMode:         "pruned",
		},
		{
			name:                 "Full node - custom small retention window",
			blockPersisterHeight: 100,
			bestHeight:           110,
			retentionWindow:      10,
			prunerBlockTrigger:   settings.PrunerBlockTriggerOnBlockPersisted,
			expectedMode:         "full",
		},
		{
			name:                 "Pruned node - exceeds custom small retention window",
			blockPersisterHeight: 100,
			bestHeight:           111,
			retentionWindow:      10,
			prunerBlockTrigger:   settings.PrunerBlockTriggerOnBlockPersisted,
			expectedMode:         "pruned",
		},
		{
			name:                 "Full node - persister ahead of best height (edge case)",
			blockPersisterHeight: 200,
			bestHeight:           100,
			retentionWindow:      288,
			prunerBlockTrigger:   settings.PrunerBlockTriggerOnBlockPersisted,
			expectedMode:         "full", // lag = 0 when persister ahead
		},
		{
			name:                 "Full node - both at genesis (height 0)",
			blockPersisterHeight: 0,
			bestHeight:           0,
			retentionWindow:      288,
			prunerBlockTrigger:   settings.PrunerBlockTriggerOnBlockPersisted,
			expectedMode:         "pruned", // blockPersisterHeight 0 means not running
		},
		{
			name:                 "Full node - large retention window",
			blockPersisterHeight: 1000,
			bestHeight:           5000,
			retentionWindow:      10000,
			prunerBlockTrigger:   settings.PrunerBlockTriggerOnBlockPersisted,
			expectedMode:         "full",
		},
		{
			name:                 "Pruned node - OnBlockMined trigger mode (persister running)",
			blockPersisterHeight: 100,
			bestHeight:           200,
			retentionWindow:      288,
			prunerBlockTrigger:   settings.PrunerBlockTriggerOnBlockMined,
			expectedMode:         "pruned", // Always pruned when using OnBlockMined
		},
		{
			name:                 "Pruned node - OnBlockMined trigger mode (persister not running)",
			blockPersisterHeight: 0,
			bestHeight:           1000,
			retentionWindow:      288,
			prunerBlockTrigger:   settings.PrunerBlockTriggerOnBlockMined,
			expectedMode:         "pruned",
		},
		{
			name:                 "Pruned node - OnBlockMined trigger mode (within retention window)",
			blockPersisterHeight: 100,
			bestHeight:           150,
			retentionWindow:      288,
			prunerBlockTrigger:   settings.PrunerBlockTriggerOnBlockMined,
			expectedMode:         "pruned", // Still pruned even within retention window
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DetermineStorageMode(tt.blockPersisterHeight, tt.bestHeight, tt.retentionWindow, tt.prunerBlockTrigger)
			assert.Equal(t, tt.expectedMode, result, "Storage mode should match expected value")
		})
	}
}

// TestDetermineStorageModeAlwaysReturnsValidMode ensures the function never returns
// an empty string or invalid mode
func TestDetermineStorageModeAlwaysReturnsValidMode(t *testing.T) {
	testCases := []struct {
		persisterHeight uint32
		bestHeight      uint32
		retention       uint32
	}{
		{0, 0, 0},
		{0, 100, 0},
		{100, 0, 0},
		{100, 100, 0},
		{1000, 2000, 500},
		{^uint32(0), ^uint32(0), ^uint32(0)}, // max values
	}

	for _, tc := range testCases {
		result := DetermineStorageMode(tc.persisterHeight, tc.bestHeight, tc.retention, settings.PrunerBlockTriggerOnBlockPersisted)
		assert.NotEmpty(t, result, "Result should never be empty")
		assert.Contains(t, []string{"full", "pruned"}, result, "Result should be either 'full' or 'pruned'")
	}
}
