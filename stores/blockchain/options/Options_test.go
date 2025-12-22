package options

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProcessStoreBlockOptions tests the ProcessStoreBlockOptions function
func TestProcessStoreBlockOptions(t *testing.T) {
	t.Run("default values", func(t *testing.T) {
		opts := ProcessStoreBlockOptions()

		require.NotNil(t, opts)
		assert.False(t, opts.MinedSet, "MinedSet should default to false")
		assert.False(t, opts.SubtreesSet, "SubtreesSet should default to false")
		assert.False(t, opts.Invalid, "Invalid should default to false")
		assert.Equal(t, uint64(0), opts.ID, "ID should default to 0")
	})

	t.Run("with no options", func(t *testing.T) {
		opts := ProcessStoreBlockOptions()

		assert.NotNil(t, opts)
		assert.False(t, opts.MinedSet)
		assert.False(t, opts.SubtreesSet)
		assert.False(t, opts.Invalid)
		assert.Equal(t, uint64(0), opts.ID)
	})

	t.Run("with empty options slice", func(t *testing.T) {
		var options []StoreBlockOption
		opts := ProcessStoreBlockOptions(options...)

		assert.NotNil(t, opts)
		assert.False(t, opts.MinedSet)
		assert.False(t, opts.SubtreesSet)
		assert.False(t, opts.Invalid)
		assert.Equal(t, uint64(0), opts.ID)
	})

	t.Run("returns new instance each time", func(t *testing.T) {
		opts1 := ProcessStoreBlockOptions()
		opts2 := ProcessStoreBlockOptions()

		assert.NotSame(t, opts1, opts2, "Each call should return a new instance")
	})
}

// TestWithMinedSet tests the WithMinedSet option
func TestWithMinedSet(t *testing.T) {
	t.Run("set to true", func(t *testing.T) {
		opts := ProcessStoreBlockOptions(WithMinedSet(true))

		assert.True(t, opts.MinedSet)
		assert.False(t, opts.SubtreesSet, "Other fields should remain default")
		assert.False(t, opts.Invalid)
		assert.Equal(t, uint64(0), opts.ID)
	})

	t.Run("set to false", func(t *testing.T) {
		opts := ProcessStoreBlockOptions(WithMinedSet(false))

		assert.False(t, opts.MinedSet)
		assert.False(t, opts.SubtreesSet)
		assert.False(t, opts.Invalid)
		assert.Equal(t, uint64(0), opts.ID)
	})

	t.Run("override default", func(t *testing.T) {
		// Start with false (default), then set to true
		opts := ProcessStoreBlockOptions(WithMinedSet(true))
		assert.True(t, opts.MinedSet)

		// Apply false
		opts2 := ProcessStoreBlockOptions(WithMinedSet(true), WithMinedSet(false))
		assert.False(t, opts2.MinedSet, "Last option should win")
	})

	t.Run("multiple applications", func(t *testing.T) {
		// Apply multiple times - last one wins
		opts := ProcessStoreBlockOptions(
			WithMinedSet(true),
			WithMinedSet(false),
			WithMinedSet(true),
		)
		assert.True(t, opts.MinedSet, "Last WithMinedSet(true) should win")
	})

	t.Run("does not affect other options", func(t *testing.T) {
		opts := ProcessStoreBlockOptions(
			WithMinedSet(true),
			WithSubtreesSet(true),
			WithInvalid(true),
			WithID(100),
		)

		assert.True(t, opts.MinedSet)
		assert.True(t, opts.SubtreesSet, "SubtreesSet should still be set")
		assert.True(t, opts.Invalid, "Invalid should still be set")
		assert.Equal(t, uint64(100), opts.ID, "ID should still be set")
	})
}

// TestWithSubtreesSet tests the WithSubtreesSet option
func TestWithSubtreesSet(t *testing.T) {
	t.Run("set to true", func(t *testing.T) {
		opts := ProcessStoreBlockOptions(WithSubtreesSet(true))

		assert.True(t, opts.SubtreesSet)
		assert.False(t, opts.MinedSet, "Other fields should remain default")
		assert.False(t, opts.Invalid)
		assert.Equal(t, uint64(0), opts.ID)
	})

	t.Run("set to false", func(t *testing.T) {
		opts := ProcessStoreBlockOptions(WithSubtreesSet(false))

		assert.False(t, opts.SubtreesSet)
		assert.False(t, opts.MinedSet)
		assert.False(t, opts.Invalid)
		assert.Equal(t, uint64(0), opts.ID)
	})

	t.Run("override default", func(t *testing.T) {
		opts := ProcessStoreBlockOptions(WithSubtreesSet(true))
		assert.True(t, opts.SubtreesSet)

		opts2 := ProcessStoreBlockOptions(WithSubtreesSet(true), WithSubtreesSet(false))
		assert.False(t, opts2.SubtreesSet, "Last option should win")
	})

	t.Run("multiple applications", func(t *testing.T) {
		opts := ProcessStoreBlockOptions(
			WithSubtreesSet(false),
			WithSubtreesSet(true),
			WithSubtreesSet(false),
		)
		assert.False(t, opts.SubtreesSet, "Last WithSubtreesSet(false) should win")
	})

	t.Run("does not affect other options", func(t *testing.T) {
		opts := ProcessStoreBlockOptions(
			WithSubtreesSet(true),
			WithMinedSet(true),
			WithInvalid(true),
			WithID(200),
		)

		assert.True(t, opts.SubtreesSet)
		assert.True(t, opts.MinedSet, "MinedSet should still be set")
		assert.True(t, opts.Invalid, "Invalid should still be set")
		assert.Equal(t, uint64(200), opts.ID, "ID should still be set")
	})
}

// TestWithInvalid tests the WithInvalid option
func TestWithInvalid(t *testing.T) {
	t.Run("set to true", func(t *testing.T) {
		opts := ProcessStoreBlockOptions(WithInvalid(true))

		assert.True(t, opts.Invalid)
		assert.False(t, opts.MinedSet, "Other fields should remain default")
		assert.False(t, opts.SubtreesSet)
		assert.Equal(t, uint64(0), opts.ID)
	})

	t.Run("set to false", func(t *testing.T) {
		opts := ProcessStoreBlockOptions(WithInvalid(false))

		assert.False(t, opts.Invalid)
		assert.False(t, opts.MinedSet)
		assert.False(t, opts.SubtreesSet)
		assert.Equal(t, uint64(0), opts.ID)
	})

	t.Run("override default", func(t *testing.T) {
		opts := ProcessStoreBlockOptions(WithInvalid(true))
		assert.True(t, opts.Invalid)

		opts2 := ProcessStoreBlockOptions(WithInvalid(true), WithInvalid(false))
		assert.False(t, opts2.Invalid, "Last option should win")
	})

	t.Run("multiple applications", func(t *testing.T) {
		opts := ProcessStoreBlockOptions(
			WithInvalid(true),
			WithInvalid(false),
			WithInvalid(true),
		)
		assert.True(t, opts.Invalid, "Last WithInvalid(true) should win")
	})

	t.Run("does not affect other options", func(t *testing.T) {
		opts := ProcessStoreBlockOptions(
			WithInvalid(true),
			WithMinedSet(true),
			WithSubtreesSet(true),
			WithID(300),
		)

		assert.True(t, opts.Invalid)
		assert.True(t, opts.MinedSet, "MinedSet should still be set")
		assert.True(t, opts.SubtreesSet, "SubtreesSet should still be set")
		assert.Equal(t, uint64(300), opts.ID, "ID should still be set")
	})
}

// TestWithID tests the WithID option
func TestWithID(t *testing.T) {
	t.Run("set to positive value", func(t *testing.T) {
		opts := ProcessStoreBlockOptions(WithID(12345))

		assert.Equal(t, uint64(12345), opts.ID)
		assert.False(t, opts.MinedSet, "Other fields should remain default")
		assert.False(t, opts.SubtreesSet)
		assert.False(t, opts.Invalid)
	})

	t.Run("set to zero", func(t *testing.T) {
		opts := ProcessStoreBlockOptions(WithID(0))

		assert.Equal(t, uint64(0), opts.ID)
		assert.False(t, opts.MinedSet)
		assert.False(t, opts.SubtreesSet)
		assert.False(t, opts.Invalid)
	})

	t.Run("set to maximum uint64", func(t *testing.T) {
		maxUint64 := ^uint64(0)
		opts := ProcessStoreBlockOptions(WithID(maxUint64))

		assert.Equal(t, maxUint64, opts.ID)
	})

	t.Run("override default", func(t *testing.T) {
		opts := ProcessStoreBlockOptions(WithID(100))
		assert.Equal(t, uint64(100), opts.ID)

		opts2 := ProcessStoreBlockOptions(WithID(100), WithID(200))
		assert.Equal(t, uint64(200), opts2.ID, "Last option should win")
	})

	t.Run("multiple applications", func(t *testing.T) {
		opts := ProcessStoreBlockOptions(
			WithID(100),
			WithID(200),
			WithID(300),
		)
		assert.Equal(t, uint64(300), opts.ID, "Last WithID(300) should win")
	})

	t.Run("does not affect other options", func(t *testing.T) {
		opts := ProcessStoreBlockOptions(
			WithID(999),
			WithMinedSet(true),
			WithSubtreesSet(true),
			WithInvalid(true),
		)

		assert.Equal(t, uint64(999), opts.ID)
		assert.True(t, opts.MinedSet, "MinedSet should still be set")
		assert.True(t, opts.SubtreesSet, "SubtreesSet should still be set")
		assert.True(t, opts.Invalid, "Invalid should still be set")
	})

	t.Run("various ID values", func(t *testing.T) {
		testCases := []uint64{
			0,
			1,
			100,
			1000,
			10000,
			100000,
			1000000,
			^uint64(0), // max uint64
		}

		for _, id := range testCases {
			opts := ProcessStoreBlockOptions(WithID(id))
			assert.Equal(t, id, opts.ID, "ID should be set to %d", id)
		}
	})
}

// TestCombinedOptions tests combining multiple options
func TestCombinedOptions(t *testing.T) {
	t.Run("all options set to true", func(t *testing.T) {
		opts := ProcessStoreBlockOptions(
			WithMinedSet(true),
			WithSubtreesSet(true),
			WithInvalid(true),
			WithID(12345),
		)

		assert.True(t, opts.MinedSet)
		assert.True(t, opts.SubtreesSet)
		assert.True(t, opts.Invalid)
		assert.Equal(t, uint64(12345), opts.ID)
	})

	t.Run("all options set to false", func(t *testing.T) {
		opts := ProcessStoreBlockOptions(
			WithMinedSet(false),
			WithSubtreesSet(false),
			WithInvalid(false),
			WithID(0),
		)

		assert.False(t, opts.MinedSet)
		assert.False(t, opts.SubtreesSet)
		assert.False(t, opts.Invalid)
		assert.Equal(t, uint64(0), opts.ID)
	})

	t.Run("mixed options", func(t *testing.T) {
		opts := ProcessStoreBlockOptions(
			WithMinedSet(true),
			WithSubtreesSet(false),
			WithInvalid(true),
			WithID(777),
		)

		assert.True(t, opts.MinedSet)
		assert.False(t, opts.SubtreesSet)
		assert.True(t, opts.Invalid)
		assert.Equal(t, uint64(777), opts.ID)
	})

	t.Run("order independence", func(t *testing.T) {
		opts1 := ProcessStoreBlockOptions(
			WithMinedSet(true),
			WithSubtreesSet(true),
			WithInvalid(true),
			WithID(100),
		)

		opts2 := ProcessStoreBlockOptions(
			WithID(100),
			WithInvalid(true),
			WithSubtreesSet(true),
			WithMinedSet(true),
		)

		assert.Equal(t, opts1.MinedSet, opts2.MinedSet)
		assert.Equal(t, opts1.SubtreesSet, opts2.SubtreesSet)
		assert.Equal(t, opts1.Invalid, opts2.Invalid)
		assert.Equal(t, opts1.ID, opts2.ID)
	})

	t.Run("overriding options", func(t *testing.T) {
		opts := ProcessStoreBlockOptions(
			WithMinedSet(false),
			WithSubtreesSet(false),
			WithInvalid(false),
			WithID(100),
			// Override all of them
			WithMinedSet(true),
			WithSubtreesSet(true),
			WithInvalid(true),
			WithID(200),
		)

		assert.True(t, opts.MinedSet, "Should be overridden to true")
		assert.True(t, opts.SubtreesSet, "Should be overridden to true")
		assert.True(t, opts.Invalid, "Should be overridden to true")
		assert.Equal(t, uint64(200), opts.ID, "Should be overridden to 200")
	})
}

// TestOptionFunctions tests that option functions work correctly
func TestOptionFunctions(t *testing.T) {
	t.Run("WithMinedSet returns function", func(t *testing.T) {
		opt := WithMinedSet(true)
		assert.NotNil(t, opt)

		// Apply to options
		opts := &StoreBlockOptions{}
		opt(opts)
		assert.True(t, opts.MinedSet)
	})

	t.Run("WithSubtreesSet returns function", func(t *testing.T) {
		opt := WithSubtreesSet(true)
		assert.NotNil(t, opt)

		opts := &StoreBlockOptions{}
		opt(opts)
		assert.True(t, opts.SubtreesSet)
	})

	t.Run("WithInvalid returns function", func(t *testing.T) {
		opt := WithInvalid(true)
		assert.NotNil(t, opt)

		opts := &StoreBlockOptions{}
		opt(opts)
		assert.True(t, opts.Invalid)
	})

	t.Run("WithID returns function", func(t *testing.T) {
		opt := WithID(999)
		assert.NotNil(t, opt)

		opts := &StoreBlockOptions{}
		opt(opts)
		assert.Equal(t, uint64(999), opts.ID)
	})

	t.Run("options can be stored and reused", func(t *testing.T) {
		// Store options in variables
		minedOpt := WithMinedSet(true)
		subtreesOpt := WithSubtreesSet(true)
		invalidOpt := WithInvalid(true)
		idOpt := WithID(456)

		// Use them multiple times
		opts1 := ProcessStoreBlockOptions(minedOpt, subtreesOpt, invalidOpt, idOpt)
		opts2 := ProcessStoreBlockOptions(minedOpt, subtreesOpt, invalidOpt, idOpt)

		assert.Equal(t, opts1.MinedSet, opts2.MinedSet)
		assert.Equal(t, opts1.SubtreesSet, opts2.SubtreesSet)
		assert.Equal(t, opts1.Invalid, opts2.Invalid)
		assert.Equal(t, opts1.ID, opts2.ID)
	})
}

// TestStoreBlockOptionsStruct tests the StoreBlockOptions struct directly
func TestStoreBlockOptionsStruct(t *testing.T) {
	t.Run("can be created manually", func(t *testing.T) {
		opts := &StoreBlockOptions{
			MinedSet:    true,
			SubtreesSet: false,
			Invalid:     true,
			ID:          789,
		}

		assert.True(t, opts.MinedSet)
		assert.False(t, opts.SubtreesSet)
		assert.True(t, opts.Invalid)
		assert.Equal(t, uint64(789), opts.ID)
	})

	t.Run("zero value struct", func(t *testing.T) {
		opts := &StoreBlockOptions{}

		assert.False(t, opts.MinedSet)
		assert.False(t, opts.SubtreesSet)
		assert.False(t, opts.Invalid)
		assert.Equal(t, uint64(0), opts.ID)
	})
}

// TestRealWorldScenarios tests realistic usage patterns
func TestRealWorldScenarios(t *testing.T) {
	t.Run("storing a mined block with subtrees", func(t *testing.T) {
		opts := ProcessStoreBlockOptions(
			WithMinedSet(true),
			WithSubtreesSet(true),
			WithID(1000),
		)

		assert.True(t, opts.MinedSet)
		assert.True(t, opts.SubtreesSet)
		assert.False(t, opts.Invalid)
		assert.Equal(t, uint64(1000), opts.ID)
	})

	t.Run("storing an invalid block", func(t *testing.T) {
		opts := ProcessStoreBlockOptions(
			WithInvalid(true),
			WithID(2000),
		)

		assert.False(t, opts.MinedSet)
		assert.False(t, opts.SubtreesSet)
		assert.True(t, opts.Invalid)
		assert.Equal(t, uint64(2000), opts.ID)
	})

	t.Run("storing a basic block with only ID", func(t *testing.T) {
		opts := ProcessStoreBlockOptions(
			WithID(3000),
		)

		assert.False(t, opts.MinedSet)
		assert.False(t, opts.SubtreesSet)
		assert.False(t, opts.Invalid)
		assert.Equal(t, uint64(3000), opts.ID)
	})

	t.Run("storing genesis block", func(t *testing.T) {
		opts := ProcessStoreBlockOptions(
			WithID(0),
			WithMinedSet(true),
			WithSubtreesSet(false),
		)

		assert.True(t, opts.MinedSet)
		assert.False(t, opts.SubtreesSet)
		assert.False(t, opts.Invalid)
		assert.Equal(t, uint64(0), opts.ID)
	})

	t.Run("batch processing with conditional options", func(t *testing.T) {
		// Simulate conditional option building
		var opts []StoreBlockOption

		shouldSetMined := true
		shouldSetSubtrees := false
		blockID := uint64(5000)

		if shouldSetMined {
			opts = append(opts, WithMinedSet(true))
		}
		if shouldSetSubtrees {
			opts = append(opts, WithSubtreesSet(true))
		}
		opts = append(opts, WithID(blockID))

		result := ProcessStoreBlockOptions(opts...)

		assert.True(t, result.MinedSet)
		assert.False(t, result.SubtreesSet)
		assert.Equal(t, blockID, result.ID)
	})
}

// TestEdgeCases tests edge cases and boundary conditions
func TestEdgeCases(t *testing.T) {
	t.Run("nil option function", func(t *testing.T) {
		// ProcessStoreBlockOptions should handle nil options gracefully
		var nilOption StoreBlockOption
		opts := ProcessStoreBlockOptions(nilOption, WithMinedSet(true), nil)
		require.NotNil(t, opts)
		assert.True(t, opts.MinedSet, "Non-nil options should still be applied")
	})

	t.Run("empty variadic arguments", func(t *testing.T) {
		opts := ProcessStoreBlockOptions()
		assert.NotNil(t, opts)
	})

	t.Run("same option applied 100 times", func(t *testing.T) {
		var options []StoreBlockOption
		for i := 0; i < 100; i++ {
			options = append(options, WithID(uint64(i)))
		}

		opts := ProcessStoreBlockOptions(options...)
		assert.Equal(t, uint64(99), opts.ID, "Last ID (99) should win")
	})

	t.Run("alternating boolean options", func(t *testing.T) {
		var options []StoreBlockOption
		for i := 0; i < 10; i++ {
			options = append(options, WithMinedSet(i%2 == 0))
		}

		opts := ProcessStoreBlockOptions(options...)
		assert.False(t, opts.MinedSet, "Last value should be false (9 % 2 != 0)")
	})

	t.Run("large ID values", func(t *testing.T) {
		largeIDs := []uint64{
			1<<32 - 1, // Max uint32
			1 << 32,   // Just above uint32
			1<<48 - 1,
			1 << 48,
			1<<63 - 1,  // Max int64 (when cast to int64)
			1 << 63,    // First bit of uint64
			^uint64(0), // Max uint64
		}

		for _, id := range largeIDs {
			opts := ProcessStoreBlockOptions(WithID(id))
			assert.Equal(t, id, opts.ID, "Should handle large ID %d", id)
		}
	})
}

// BenchmarkProcessStoreBlockOptions benchmarks option processing
func BenchmarkProcessStoreBlockOptions(b *testing.B) {
	b.Run("no options", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = ProcessStoreBlockOptions()
		}
	})

	b.Run("single option", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = ProcessStoreBlockOptions(WithID(100))
		}
	})

	b.Run("all options", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = ProcessStoreBlockOptions(
				WithMinedSet(true),
				WithSubtreesSet(true),
				WithInvalid(true),
				WithID(100),
			)
		}
	})

	b.Run("many options", func(b *testing.B) {
		options := []StoreBlockOption{
			WithMinedSet(true),
			WithMinedSet(false),
			WithSubtreesSet(true),
			WithSubtreesSet(false),
			WithInvalid(true),
			WithInvalid(false),
			WithID(100),
			WithID(200),
			WithID(300),
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = ProcessStoreBlockOptions(options...)
		}
	})
}

// BenchmarkOptionCreation benchmarks individual option creation
func BenchmarkOptionCreation(b *testing.B) {
	b.Run("WithMinedSet", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = WithMinedSet(true)
		}
	})

	b.Run("WithSubtreesSet", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = WithSubtreesSet(true)
		}
	})

	b.Run("WithInvalid", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = WithInvalid(true)
		}
	})

	b.Run("WithID", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = WithID(12345)
		}
	})
}
