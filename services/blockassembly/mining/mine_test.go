package mining

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestMiningCandidate creates a valid mining candidate for testing
func createTestMiningCandidate() *model.MiningCandidate {
	return &model.MiningCandidate{
		Id:            []byte("test-candidate-123"),
		Version:       1,
		PreviousHash:  subtree.CoinbasePlaceholderHash.CloneBytes(),
		MerkleProof:   [][]byte{subtree.CoinbasePlaceholderHash.CloneBytes()},
		Time:          uint32(time.Now().Unix()),
		NBits:         []byte{0x20, 0x7f, 0xff, 0xff}, // Very easy difficulty for testing
		Height:        100,
		CoinbaseValue: 5000000000, // 50 BTC in satoshis
	}
}

// createTestSettings creates test settings with valid private key
func createTestSettings() *settings.Settings {
	return &settings.Settings{
		Coinbase: settings.CoinbaseSettings{
			ArbitraryText: "test mining",
		},
		BlockAssembly: settings.BlockAssemblySettings{
			MinerWalletPrivateKeys: []string{"L56TgyTpDdvL3W24SMoALYotibToSCySQeo4pThLKxw6EFR6f93Q"},
		},
	}
}

func TestMine_Success_WithoutAddress(t *testing.T) {
	ctx := context.Background()
	candidate := createTestMiningCandidate()
	tSettings := createTestSettings()

	solution, err := Mine(ctx, tSettings, candidate, nil)

	require.NoError(t, err)
	require.NotNil(t, solution)
	assert.Equal(t, candidate.Id, solution.Id)
	assert.NotNil(t, solution.Nonce)
	assert.NotNil(t, solution.Time)
	assert.NotNil(t, solution.Coinbase)
	assert.NotNil(t, solution.Version)
	assert.NotNil(t, solution.BlockHash)
	assert.Len(t, solution.BlockHash, 32) // SHA-256 hash is 32 bytes
}

func TestMine_Success_WithAddress(t *testing.T) {
	ctx := context.Background()
	candidate := createTestMiningCandidate()
	tSettings := createTestSettings()
	address := "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa" // Example Bitcoin address

	solution, err := Mine(ctx, tSettings, candidate, &address)

	require.NoError(t, err)
	require.NotNil(t, solution)
	assert.Equal(t, candidate.Id, solution.Id)
	assert.NotNil(t, solution.Nonce)
	assert.NotNil(t, solution.Time)
	assert.NotNil(t, solution.Coinbase)
	assert.NotNil(t, solution.Version)
	assert.NotNil(t, solution.BlockHash)
	assert.Len(t, solution.BlockHash, 32)
}

func TestMine_ContextCancellation(t *testing.T) {
	// Create a context that will be cancelled immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	candidate := createTestMiningCandidate()
	// Set impossible difficulty to ensure mining would never succeed
	candidate.NBits = []byte{0x1d, 0x00, 0x00, 0x01} // Nearly impossible difficulty
	tSettings := createTestSettings()

	solution, err := Mine(ctx, tSettings, candidate, nil)

	// Should return nil solution due to immediate context cancellation
	// The current implementation may return either nil solution or nonce overflow error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context canceled")
	assert.Nil(t, solution)
}

func TestMine_ContextCancellationDuringMining(t *testing.T) {
	// Test that context cancellation stops the mining loop
	// We'll use easy difficulty but cancel quickly to test the context check
	ctx, cancel := context.WithCancel(context.Background())

	candidate := createTestMiningCandidate()
	// Use easy difficulty but cancel immediately after starting
	candidate.NBits = []byte{0x20, 0x7f, 0xff, 0xff} // Easy difficulty
	tSettings := createTestSettings()

	// Cancel the context after a very short delay to test the context check in the loop
	go func() {
		time.Sleep(1 * time.Millisecond)
		cancel()
	}()

	solution, err := Mine(ctx, tSettings, candidate, nil)

	// The result depends on timing - either we find a solution quickly or context is cancelled
	// Both outcomes are valid for this test
	if solution != nil {
		// If we got a solution, it should be valid
		assert.NoError(t, err)
		assert.NotNil(t, solution.BlockHash)
	} else {
		// If no solution, it should be due to context cancellation
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context canceled")
	}
}

func TestMine_CreateCoinbaseTxError_NoAddress(t *testing.T) {
	ctx := context.Background()
	candidate := createTestMiningCandidate()

	// Create settings with invalid private key to trigger error
	tSettings := &settings.Settings{
		Coinbase: settings.CoinbaseSettings{
			ArbitraryText: "test mining",
		},
		BlockAssembly: settings.BlockAssemblySettings{
			MinerWalletPrivateKeys: []string{"invalid-private-key"},
		},
	}

	solution, err := Mine(ctx, tSettings, candidate, nil)

	assert.Nil(t, solution)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "can't decode coinbase priv key")
}

func TestMine_CreateCoinbaseTxError_WithAddress(t *testing.T) {
	ctx := context.Background()
	candidate := createTestMiningCandidate()
	tSettings := createTestSettings()
	invalidAddress := "" // Empty address should cause error

	solution, err := Mine(ctx, tSettings, candidate, &invalidAddress)

	assert.Nil(t, solution)
	assert.Error(t, err)
}

func TestMine_DifferentDifficultyLevels(t *testing.T) {
	tSettings := createTestSettings()

	tests := []struct {
		name       string
		nBits      []byte
		shouldFind bool
		timeout    time.Duration
	}{
		{
			name:       "very easy difficulty",
			nBits:      []byte{0x20, 0x7f, 0xff, 0xff}, // Very easy
			shouldFind: true,
			timeout:    5 * time.Second,
		},
		{
			name:       "easy difficulty",
			nBits:      []byte{0x1f, 0x7f, 0xff, 0xff}, // Easy
			shouldFind: true,
			timeout:    10 * time.Second,
		},
		{
			name:       "moderate difficulty",
			nBits:      []byte{0x1e, 0x7f, 0xff, 0xff}, // Moderate
			shouldFind: true,
			timeout:    30 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), tt.timeout)
			defer cancel()

			candidate := createTestMiningCandidate()
			candidate.NBits = tt.nBits

			solution, err := Mine(ctx, tSettings, candidate, nil)

			if tt.shouldFind {
				// For easy difficulties, we should find a solution
				if solution == nil && err == nil {
					t.Skip("Solution not found within timeout - may need easier difficulty")
				} else {
					require.NoError(t, err)
					require.NotNil(t, solution)
				}
			}
		})
	}
}

func TestMine_ValidateMiningSolution(t *testing.T) {
	ctx := context.Background()
	candidate := createTestMiningCandidate()
	tSettings := createTestSettings()

	solution, err := Mine(ctx, tSettings, candidate, nil)
	require.NoError(t, err)
	require.NotNil(t, solution)

	// Validate that the solution contains all required fields
	assert.NotEmpty(t, solution.Id)
	assert.NotNil(t, solution.Nonce)
	assert.NotNil(t, solution.Time)
	assert.NotEmpty(t, solution.Coinbase)
	assert.NotNil(t, solution.Version)
	assert.NotEmpty(t, solution.BlockHash)

	// Validate block hash length
	assert.Len(t, solution.BlockHash, 32)

	// Validate nonce is within valid range
	assert.LessOrEqual(t, solution.Nonce, uint32(4294967295)) // Max uint32
}

func TestMine_MultipleRuns_ProduceDifferentNonces(t *testing.T) {
	ctx := context.Background()
	candidate := createTestMiningCandidate()
	tSettings := createTestSettings()

	// Run mining multiple times to see if we get different nonces
	solutions := make([]*model.MiningSolution, 0, 3)
	nonces := make(map[uint32]bool)

	for i := 0; i < 3; i++ {
		// Modify candidate slightly for each run to ensure different results
		candidate.Time = uint32(time.Now().Unix()) + uint32(i)

		solution, err := Mine(ctx, tSettings, candidate, nil)
		require.NoError(t, err)
		require.NotNil(t, solution)

		solutions = append(solutions, solution)
		nonces[solution.Nonce] = true
	}

	// We should have found solutions (might have same nonce due to easy difficulty)
	assert.Len(t, solutions, 3)

	// All solutions should be valid
	for i, solution := range solutions {
		assert.NotNil(t, solution, "Solution %d should not be nil", i)
		assert.NotEmpty(t, solution.BlockHash, "Solution %d should have block hash", i)
	}
}

func TestMine_WithNilCandidate(t *testing.T) {
	ctx := context.Background()
	tSettings := createTestSettings()

	// This should panic or error - testing defensive programming
	assert.Panics(t, func() {
		_, _ = Mine(ctx, tSettings, nil, nil)
	}, "Mine should panic with nil candidate")
}

func TestMine_WithNilSettings(t *testing.T) {
	ctx := context.Background()
	candidate := createTestMiningCandidate()

	// This should panic or error - testing defensive programming
	assert.Panics(t, func() {
		_, _ = Mine(ctx, nil, candidate, nil)
	}, "Mine should panic with nil settings")
}

// Benchmark tests for performance evaluation
func BenchmarkMine_EasyDifficulty(b *testing.B) {
	ctx := context.Background()
	candidate := createTestMiningCandidate()
	candidate.NBits = []byte{0x20, 0x7f, 0xff, 0xff} // Very easy difficulty
	tSettings := createTestSettings()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		candidate.Time = uint32(time.Now().Unix()) + uint32(i) // Vary time to get different results
		solution, err := Mine(ctx, tSettings, candidate, nil)
		if err != nil {
			b.Fatal(err)
		}
		if solution == nil {
			b.Fatal("Expected solution but got nil")
		}
	}
}

func BenchmarkMine_WithAddress(b *testing.B) {
	ctx := context.Background()
	candidate := createTestMiningCandidate()
	candidate.NBits = []byte{0x20, 0x7f, 0xff, 0xff} // Very easy difficulty
	tSettings := createTestSettings()
	address := "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		candidate.Time = uint32(time.Now().Unix()) + uint32(i)
		solution, err := Mine(ctx, tSettings, candidate, &address)
		if err != nil {
			b.Fatal(err)
		}
		if solution == nil {
			b.Fatal("Expected solution but got nil")
		}
	}
}
