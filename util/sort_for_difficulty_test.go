package util

import (
	"testing"
)

// testBlock implements GetTime() uint32 for testing
type testBlock struct {
	timestamp uint32
}

func (tb testBlock) GetTime() uint32 {
	return tb.timestamp
}

func TestSortForDifficultyAdjustment(t *testing.T) {
	t.Run("already sorted", func(t *testing.T) {
		blocks := []testBlock{
			{timestamp: 100},
			{timestamp: 200},
			{timestamp: 300},
		}
		expected := []uint32{100, 200, 300}

		SortForDifficultyAdjustment(blocks)

		for i, block := range blocks {
			if block.GetTime() != expected[i] {
				t.Errorf("Expected timestamp %d at index %d, got %d", expected[i], i, block.GetTime())
			}
		}
	})

	t.Run("reverse sorted", func(t *testing.T) {
		blocks := []testBlock{
			{timestamp: 300},
			{timestamp: 200},
			{timestamp: 100},
		}
		expected := []uint32{100, 200, 300}

		SortForDifficultyAdjustment(blocks)

		for i, block := range blocks {
			if block.GetTime() != expected[i] {
				t.Errorf("Expected timestamp %d at index %d, got %d", expected[i], i, block.GetTime())
			}
		}
	})

	t.Run("random order 1", func(t *testing.T) {
		blocks := []testBlock{
			{timestamp: 200},
			{timestamp: 100},
			{timestamp: 300},
		}
		expected := []uint32{100, 200, 300}

		SortForDifficultyAdjustment(blocks)

		for i, block := range blocks {
			if block.GetTime() != expected[i] {
				t.Errorf("Expected timestamp %d at index %d, got %d", expected[i], i, block.GetTime())
			}
		}
	})

	t.Run("random order 2", func(t *testing.T) {
		blocks := []testBlock{
			{timestamp: 300},
			{timestamp: 100},
			{timestamp: 200},
		}
		expected := []uint32{100, 200, 300}

		SortForDifficultyAdjustment(blocks)

		for i, block := range blocks {
			if block.GetTime() != expected[i] {
				t.Errorf("Expected timestamp %d at index %d, got %d", expected[i], i, block.GetTime())
			}
		}
	})

	t.Run("random order 3", func(t *testing.T) {
		blocks := []testBlock{
			{timestamp: 100},
			{timestamp: 300},
			{timestamp: 200},
		}
		expected := []uint32{100, 200, 300}

		SortForDifficultyAdjustment(blocks)

		for i, block := range blocks {
			if block.GetTime() != expected[i] {
				t.Errorf("Expected timestamp %d at index %d, got %d", expected[i], i, block.GetTime())
			}
		}
	})

	t.Run("random order 4", func(t *testing.T) {
		blocks := []testBlock{
			{timestamp: 200},
			{timestamp: 300},
			{timestamp: 100},
		}
		expected := []uint32{100, 200, 300}

		SortForDifficultyAdjustment(blocks)

		for i, block := range blocks {
			if block.GetTime() != expected[i] {
				t.Errorf("Expected timestamp %d at index %d, got %d", expected[i], i, block.GetTime())
			}
		}
	})

	t.Run("equal timestamps", func(t *testing.T) {
		blocks := []testBlock{
			{timestamp: 200},
			{timestamp: 200},
			{timestamp: 200},
		}
		expected := []uint32{200, 200, 200}

		SortForDifficultyAdjustment(blocks)

		for i, block := range blocks {
			if block.GetTime() != expected[i] {
				t.Errorf("Expected timestamp %d at index %d, got %d", expected[i], i, block.GetTime())
			}
		}
	})

	t.Run("two equal timestamps", func(t *testing.T) {
		blocks := []testBlock{
			{timestamp: 200},
			{timestamp: 100},
			{timestamp: 200},
		}
		expected := []uint32{100, 200, 200}

		SortForDifficultyAdjustment(blocks)

		for i, block := range blocks {
			if block.GetTime() != expected[i] {
				t.Errorf("Expected timestamp %d at index %d, got %d", expected[i], i, block.GetTime())
			}
		}
	})

	t.Run("bitcoin style timestamps", func(t *testing.T) {
		// Test with realistic Bitcoin timestamp values
		blocks := []testBlock{
			{timestamp: 1704738582}, // Most recent
			{timestamp: 1704647599}, // Oldest
			{timestamp: 1704738369}, // Middle
		}
		expected := []uint32{1704647599, 1704738369, 1704738582}

		SortForDifficultyAdjustment(blocks)

		for i, block := range blocks {
			if block.GetTime() != expected[i] {
				t.Errorf("Expected timestamp %d at index %d, got %d", expected[i], i, block.GetTime())
			}
		}
	})
}
