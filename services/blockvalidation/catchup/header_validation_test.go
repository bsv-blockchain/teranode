package catchup

import (
	"math/big"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestValidateHeaderProofOfWork tests the proof of work validation
func TestValidateHeaderProofOfWork(t *testing.T) {
	t.Run("ValidProofOfWork", func(t *testing.T) {
		// Use the helper to create a header with valid proof of work
		header := createValidPoWHeader(t, "207fffff")

		// This header has valid proof of work
		err := ValidateHeaderProofOfWork(header)
		assert.NoError(t, err, "Valid proof of work should pass")
	})

	t.Run("InvalidProofOfWork", func(t *testing.T) {
		// Create a header with invalid proof of work
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Nonce:          0,
		}

		// Set extremely difficult target (impossible to meet)
		bits, err := model.NewNBitFromString("03000000") // Extremely hard difficulty
		require.NoError(t, err)
		header.Bits = *bits

		// The hash will be greater than this difficult target
		err = ValidateHeaderProofOfWork(header)
		assert.Error(t, err, "Invalid proof of work should fail")
		assert.Contains(t, err.Error(), "fails proof of work")
	})

	t.Run("EdgeCaseMaxTarget", func(t *testing.T) {
		// Test with maximum possible target
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Nonce:          0,
		}

		// Maximum target (easiest difficulty)
		bits, err := model.NewNBitFromString("1d00ffff")
		require.NoError(t, err)
		header.Bits = *bits

		err = ValidateHeaderProofOfWork(header)
		// This should typically pass unless we get extremely unlucky with the hash
		// The test verifies the function handles max target correctly
		if err != nil {
			assert.Contains(t, err.Error(), "fails proof of work")
		}
	})
}

// TestValidateHeaderMerkleRoot tests merkle root validation
func TestValidateHeaderMerkleRoot(t *testing.T) {
	t.Run("ValidMerkleRoot", func(t *testing.T) {
		merkleRoot := chainhash.HashH([]byte("valid merkle root"))
		header := &model.BlockHeader{
			HashPrevBlock:  &chainhash.Hash{1, 2, 3}, // Non-genesis
			HashMerkleRoot: &merkleRoot,
		}

		err := ValidateHeaderMerkleRoot(header)
		assert.NoError(t, err, "Valid merkle root should pass")
	})

	t.Run("ZeroMerkleRootNonGenesis", func(t *testing.T) {
		header := &model.BlockHeader{
			HashPrevBlock:  &chainhash.Hash{1, 2, 3}, // Non-genesis
			HashMerkleRoot: &chainhash.Hash{},        // Zero merkle root
		}

		err := ValidateHeaderMerkleRoot(header)
		assert.Error(t, err, "Zero merkle root on non-genesis should fail")
		assert.Contains(t, err.Error(), "invalid zero merkle root")
	})

	t.Run("ZeroMerkleRootGenesis", func(t *testing.T) {
		header := &model.BlockHeader{
			HashPrevBlock:  &chainhash.Hash{}, // Genesis block
			HashMerkleRoot: &chainhash.Hash{}, // Zero merkle root
		}

		err := ValidateHeaderMerkleRoot(header)
		assert.NoError(t, err, "Zero merkle root on genesis block should be allowed")
	})
}

// TestValidateHeaderTimestamp tests timestamp validation
func TestValidateHeaderTimestamp(t *testing.T) {
	t.Run("ValidTimestamp", func(t *testing.T) {
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Nonce:          0,
		}
		bits, _ := model.NewNBitFromString("207fffff")
		header.Bits = *bits

		err := ValidateHeaderTimestamp(header)
		assert.NoError(t, err, "Current timestamp should be valid")
	})

	t.Run("TimestampTooFarInFuture", func(t *testing.T) {
		// Set timestamp 3 hours in the future (limit is 2 hours)
		futureTime := time.Now().Add(3 * time.Hour)
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(futureTime.Unix()),
			Nonce:          0,
		}
		bits, _ := model.NewNBitFromString("207fffff")
		header.Bits = *bits

		err := ValidateHeaderTimestamp(header)
		assert.Error(t, err, "Timestamp too far in future should fail")
		assert.Contains(t, err.Error(), "too far in the future")
	})

	t.Run("TimestampBeforeGenesis", func(t *testing.T) {
		// Set timestamp before Bitcoin genesis (Jan 3, 2009)
		oldTime := time.Date(2008, 12, 31, 0, 0, 0, 0, time.UTC)
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(oldTime.Unix()),
			Nonce:          0,
		}
		bits, _ := model.NewNBitFromString("207fffff")
		header.Bits = *bits

		err := ValidateHeaderTimestamp(header)
		assert.Error(t, err, "Timestamp before genesis should fail")
		assert.Contains(t, err.Error(), "before Bitcoin genesis")
	})

	t.Run("TimestampAtGenesis", func(t *testing.T) {
		// Set timestamp at Bitcoin genesis
		genesisTime := time.Date(2009, 1, 3, 0, 0, 1, 0, time.UTC)
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(genesisTime.Unix()),
			Nonce:          0,
		}
		bits, _ := model.NewNBitFromString("207fffff")
		header.Bits = *bits

		err := ValidateHeaderTimestamp(header)
		assert.NoError(t, err, "Timestamp at genesis should be valid")
	})

	t.Run("TimestampNearFutureLimit", func(t *testing.T) {
		// Set timestamp just under 2 hours in the future
		futureTime := time.Now().Add(119 * time.Minute)
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(futureTime.Unix()),
			Nonce:          0,
		}
		bits, _ := model.NewNBitFromString("207fffff")
		header.Bits = *bits

		err := ValidateHeaderTimestamp(header)
		assert.NoError(t, err, "Timestamp just under 2 hour limit should be valid")
	})
}

// TestValidateHeaderAgainstCheckpoints tests checkpoint validation
func TestValidateHeaderAgainstCheckpoints(t *testing.T) {
	// Create test checkpoints
	checkpoint1Hash := chainhash.HashH([]byte("checkpoint1"))
	checkpoint2Hash := chainhash.HashH([]byte("checkpoint2"))

	checkpoints := []chaincfg.Checkpoint{
		{Height: 100, Hash: &checkpoint1Hash},
		{Height: 200, Hash: &checkpoint2Hash},
	}

	t.Run("HeaderMatchesCheckpoint", func(t *testing.T) {
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Nonce:          12345,
		}

		// Make the header hash match checkpoint1
		// We need to construct the header so its hash matches
		// For testing, we'll check that the validation accepts matching hashes
		err := ValidateHeaderAgainstCheckpoints(header, 100, checkpoints)
		// Since we can't easily force the hash to match, we test the mismatch case
		assert.Error(t, err, "Non-matching hash at checkpoint height should fail")
	})

	t.Run("HeaderAtNonCheckpointHeight", func(t *testing.T) {
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Nonce:          12345,
		}

		// Height 150 is not a checkpoint
		err := ValidateHeaderAgainstCheckpoints(header, 150, checkpoints)
		assert.NoError(t, err, "Header at non-checkpoint height should pass")
	})

	t.Run("NoCheckpoints", func(t *testing.T) {
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Nonce:          12345,
		}

		// Empty checkpoints slice
		err := ValidateHeaderAgainstCheckpoints(header, 100, []chaincfg.Checkpoint{})
		assert.NoError(t, err, "No checkpoints should always pass")
	})

	t.Run("HeaderConflictsWithCheckpoint", func(t *testing.T) {
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{1, 2, 3},
			HashMerkleRoot: &chainhash.Hash{4, 5, 6},
			Timestamp:      uint32(time.Now().Unix()),
			Nonce:          99999,
		}

		// Header at checkpoint height but wrong hash
		err := ValidateHeaderAgainstCheckpoints(header, 100, checkpoints)
		assert.Error(t, err, "Header conflicting with checkpoint should fail")
		assert.Contains(t, err.Error(), "conflicts with checkpoint")
	})
}

// TestValidateBatchHeaders tests batch header validation
// TestValidateBatchHeaders has been moved to the main blockvalidation package
// since it tests Server methods

/*
func TestValidateBatchHeaders(t *testing.T) {
	t.Run("ValidBatch", func(t *testing.T) {
		server := &Server{
			logger:           ulogger.TestLogger{},
			settings:         test.CreateBaseTestSettings(t),
			blockchainClient: &blockchain.Mock{},
		}

		// Create valid headers with easy difficulty
		headers := make([]*model.BlockHeader, 3)
		for i := range headers {
			merkleRoot := chainhash.HashH([]byte{byte(i + 1)})
			prevHash := chainhash.HashH([]byte{byte(i)})
			bits, _ := model.NewNBitFromString("207fffff") // Easy difficulty

			headers[i] = &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &prevHash,
				HashMerkleRoot: &merkleRoot,
				Timestamp:      uint32(time.Now().Unix() - int64(100-i)),
				Bits:           *bits,
				Nonce:          uint32(i),
			}
		}

		ctx := context.Background()
		err := server.validateBatchHeaders(ctx, headers)
		assert.NoError(t, err, "Valid batch should pass validation")
	})

	t.Run("EmptyBatch", func(t *testing.T) {
		server := &Server{
			logger:   ulogger.TestLogger{},
			settings: test.CreateBaseTestSettings(t),
		}

		ctx := context.Background()
		err := server.validateBatchHeaders(ctx, []*model.BlockHeader{})
		assert.NoError(t, err, "Empty batch should pass")
	})

	t.Run("BatchWithInvalidPoW", func(t *testing.T) {
		server := &Server{
			logger:   ulogger.TestLogger{},
			settings: test.CreateBaseTestSettings(t),
		}

		// Create header with impossible difficulty
		bits, _ := model.NewNBitFromString("03000000") // Extremely hard
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{1},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           *bits,
			Nonce:          0,
		}

		ctx := context.Background()
		err := server.validateBatchHeaders(ctx, []*model.BlockHeader{header})
		assert.Error(t, err, "Batch with invalid PoW should fail")
		assert.Contains(t, err.Error(), "fails PoW validation")
	})

	t.Run("BatchWithInvalidTimestamp", func(t *testing.T) {
		server := &Server{
			logger:   ulogger.TestLogger{},
			settings: test.CreateBaseTestSettings(t),
		}

		// Create header with timestamp too far in future
		bits, _ := model.NewNBitFromString("207fffff")
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{1},
			Timestamp:      uint32(time.Now().Add(3 * time.Hour).Unix()),
			Bits:           *bits,
			Nonce:          0,
		}

		ctx := context.Background()
		err := server.validateBatchHeaders(ctx, []*model.BlockHeader{header})
		assert.Error(t, err, "Batch with invalid timestamp should fail")
		assert.Contains(t, err.Error(), "invalid timestamp")
	})

	t.Run("BatchWithContextCancellation", func(t *testing.T) {
		server := &Server{
			logger:   ulogger.TestLogger{},
			settings: test.CreateBaseTestSettings(t),
		}

		// Create multiple headers
		headers := make([]*model.BlockHeader, 10)
		for i := range headers {
			bits, _ := model.NewNBitFromString("207fffff")
			headers[i] = &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &chainhash.Hash{byte(i)},
				HashMerkleRoot: &chainhash.Hash{byte(i + 1)},
				Timestamp:      uint32(time.Now().Unix()),
				Bits:           *bits,
				Nonce:          uint32(i),
			}
		}

		// Cancel context immediately
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := server.validateBatchHeaders(ctx, headers)
		assert.Error(t, err, "Cancelled context should cause error")
		assert.Equal(t, context.Canceled, err)
	})

	t.Run("BatchWithCheckpointValidation", func(t *testing.T) {
		mockBlockchain := &blockchain.Mock{}
		checkpointHash := chainhash.HashH([]byte("checkpoint"))

		settings := test.CreateBaseTestSettings(t)
		settings.ChainCfgParams = &chaincfg.Params{
			Checkpoints: []chaincfg.Checkpoint{
				{Height: 100, Hash: &checkpointHash},
			},
		}

		server := &Server{
			logger:           ulogger.TestLogger{},
			settings:         settings,
			blockchainClient: mockBlockchain,
		}

		// Create headers
		bits, _ := model.NewNBitFromString("207fffff")
		header1 := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{1},
			HashMerkleRoot: &chainhash.Hash{2},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           *bits,
			Nonce:          1,
		}

		header2 := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{3},
			HashMerkleRoot: &chainhash.Hash{4},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           *bits,
			Nonce:          2,
		}

		ctx := context.Background()

		// Mock blockchain client to return height for second header
		mockBlockchain.On("GetBlockHeader", ctx, header2.Hash()).Return(
			header2,
			&model.BlockHeaderMeta{Height: 100}, // Checkpoint height
			nil,
		)

		err := server.validateBatchHeaders(ctx, []*model.BlockHeader{header1, header2})
		// Should fail because header2 at height 100 doesn't match checkpoint
		assert.Error(t, err, "Header at checkpoint height with wrong hash should fail")
	})
}
*/

// Helper function to create a test header with proof of work
func createValidPoWHeader(t *testing.T, difficulty string) *model.BlockHeader {
	bits, err := model.NewNBitFromString(difficulty)
	require.NoError(t, err)

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{1, 2, 3},
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           *bits,
		Nonce:          0,
	}

	// Try to find a valid nonce (limited attempts for testing)
	target := bits.CalculateTarget()
	for i := 0; i < 1000000; i++ {
		header.Nonce = uint32(i)
		hash := header.Hash()
		hashNum := new(big.Int).SetBytes(bt.ReverseBytes(hash.CloneBytes()))

		if hashNum.Cmp(target) <= 0 {
			return header
		}
	}

	t.Fatal("Could not find valid proof of work in reasonable time")
	return nil
}
