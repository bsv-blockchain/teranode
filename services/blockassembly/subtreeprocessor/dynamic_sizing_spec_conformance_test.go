package subtreeprocessor

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func setSubtreeNodeSamples(stp *SubtreeProcessor, samples []int) {
	r := stp.subtreeNodeCounts
	for _, sample := range samples {
		r.Value = sample
		r = r.Next()
	}
}

func repeatInterval(d time.Duration, count int) []time.Duration {
	intervals := make([]time.Duration, count)
	for i := range intervals {
		intervals[i] = d
	}
	return intervals
}

func testBlockHeader(seed byte) *model.BlockHeader {
	prevHash := chainhash.HashH([]byte{seed, 0x01})
	merkleHash := chainhash.HashH([]byte{seed, 0x02})

	return &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &prevHash,
		HashMerkleRoot: &merkleHash,
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.GenesisBlockHeader.Bits,
		Nonce:          uint32(seed),
	}
}

func TestDynamicSizing_ZoneBoundaries_BASubtree021(t *testing.T) {
	t.Run("exactly 10 percent utilization holds size", func(t *testing.T) {
		stp := newDynamicSizingProcessor(t, 64, 4, 32768)
		stp.currentItemsPerFile.Store(64)
		setSubtreeNodeSamples(stp, []int{6, 6, 6, 6, 6, 6, 7, 7, 7, 7}) // avg=6.4 => 10%
		stp.blockIntervals = repeatInterval(200*time.Millisecond, 3)

		before := stp.currentItemsPerFile.Load()
		stp.adjustSubtreeSize()
		require.Equal(t, before, stp.currentItemsPerFile.Load(),
			"BA-SUBTREE-021: exactly 10%% utilization must keep subtree size unchanged")
	})

	t.Run("just below 10 percent utilization decreases size", func(t *testing.T) {
		stp := newDynamicSizingProcessor(t, 64, 4, 32768)
		stp.currentItemsPerFile.Store(64)
		setSubtreeNodeSamples(stp, []int{6, 6, 6, 6, 6, 6, 6, 6, 6, 6}) // avg=6 => 9.375%
		stp.blockIntervals = repeatInterval(time.Second, 3)

		stp.adjustSubtreeSize()
		require.Less(t, stp.currentItemsPerFile.Load(), int32(64),
			"BA-SUBTREE-021: utilization below 10%% must decrease subtree size")
	})

	t.Run("exactly 80 percent utilization holds size", func(t *testing.T) {
		stp := newDynamicSizingProcessor(t, 64, 4, 32768)
		stp.currentItemsPerFile.Store(64)
		setSubtreeNodeSamples(stp, []int{51, 51, 51, 51, 51, 51, 51, 51, 52, 52}) // avg=51.2 => 80%
		stp.blockIntervals = repeatInterval(100*time.Millisecond, 3)

		before := stp.currentItemsPerFile.Load()
		stp.adjustSubtreeSize()
		require.Equal(t, before, stp.currentItemsPerFile.Load(),
			"BA-SUBTREE-021: exactly 80%% utilization must keep subtree size unchanged")
	})

	t.Run("just above 80 percent with sufficient volume can increase", func(t *testing.T) {
		stp := newDynamicSizingProcessor(t, 64, 4, 32768)
		stp.currentItemsPerFile.Store(64)
		setSubtreeNodeSamples(stp, []int{52, 52, 52, 52, 52, 52, 52, 52, 52, 52}) // avg=52 => 81.25%
		stp.blockIntervals = repeatInterval(250*time.Millisecond, 3)

		before := stp.currentItemsPerFile.Load()
		stp.adjustSubtreeSize()
		require.Greater(t, stp.currentItemsPerFile.Load(), before,
			"BA-SUBTREE-021: utilization above 80%% should enter increase path when anti-creep gate permits")
	})
}

func TestDynamicSizing_IncreasePathCaps_BASubtree023(t *testing.T) {
	t.Run("ratio-based increase is capped at 2x per evaluation", func(t *testing.T) {
		stp := newDynamicSizingProcessor(t, 64, 4, 32768)
		stp.currentItemsPerFile.Store(64)
		setSubtreeNodeSamples(stp, []int{60, 60, 60, 60, 60, 60, 60, 60, 60, 60})
		stp.blockIntervals = repeatInterval(50*time.Millisecond, 3) // ratio=20

		stp.adjustSubtreeSize()
		require.Equal(t, int32(128), stp.currentItemsPerFile.Load(),
			"BA-SUBTREE-023: increase path must cap at 2x previous size")
	})

	t.Run("increase path is capped at configured maximum", func(t *testing.T) {
		stp := newDynamicSizingProcessor(t, 16384, 4, 32768)
		stp.currentItemsPerFile.Store(16384)
		setSubtreeNodeSamples(stp, []int{16000, 16050, 16100, 16150, 16200, 16000, 16050, 16100, 16150, 16200})
		stp.blockIntervals = repeatInterval(10*time.Millisecond, 3)

		stp.adjustSubtreeSize()
		require.Equal(t, int32(32768), stp.currentItemsPerFile.Load(),
			"BA-SUBTREE-023: increase path must not exceed MaximumMerkleItemsPerSubtree")
	})
}

func TestDynamicSizing_IntervalFiltering_BASubtree023(t *testing.T) {
	t.Run("ignores invalid intervals and uses only valid ones", func(t *testing.T) {
		stp := newDynamicSizingProcessor(t, 64, 4, 32768)
		stp.currentItemsPerFile.Store(64)
		setSubtreeNodeSamples(stp, []int{60, 60, 60, 60, 60, 60, 60, 60, 60, 60})
		stp.blockIntervals = []time.Duration{
			time.Millisecond,       // invalid: not > 1ms
			500 * time.Millisecond, // valid
			2 * time.Hour,          // invalid: not < 1h
			500 * time.Millisecond, // valid
		}

		stp.adjustSubtreeSize()
		require.Equal(t, int32(128), stp.currentItemsPerFile.Load(),
			"BA-SUBTREE-023: increase path must derive ratio from valid interval samples only")
	})

	t.Run("no valid intervals means no timing-driven increase", func(t *testing.T) {
		stp := newDynamicSizingProcessor(t, 64, 4, 32768)
		stp.currentItemsPerFile.Store(64)
		setSubtreeNodeSamples(stp, []int{60, 60, 60, 60, 60, 60, 60, 60, 60, 60})
		stp.blockIntervals = []time.Duration{
			0,
			time.Millisecond,
			-1 * time.Second,
			2 * time.Hour,
		}

		before := stp.currentItemsPerFile.Load()
		stp.adjustSubtreeSize()
		require.Equal(t, before, stp.currentItemsPerFile.Load(),
			"BA-SUBTREE-023: if interval samples are invalid, sizing should remain unchanged")
	})
}

func TestDynamicSizing_ReevaluatedOnFinalizeBlock_BASubtree020(t *testing.T) {
	stp := newDynamicSizingProcessor(t, 64, 4, 32768)

	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.
		On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).
		Once()
	stp.blockchainClient = mockBlockchainClient

	stp.currentItemsPerFile.Store(64)
	setSubtreeNodeSamples(stp, []int{60, 60, 60, 60, 60})
	stp.InitCurrentBlockHeader(testBlockHeader(0x10))
	stp.subtreesInBlock = 4
	stp.blockStartTime = time.Now().Add(-200 * time.Millisecond) // avg 50ms/subtree

	block := &model.Block{Header: testBlockHeader(0x20)}
	before := stp.currentItemsPerFile.Load()
	stp.finalizeBlockProcessing(context.Background(), block)

	require.Greater(t, stp.currentItemsPerFile.Load(), before,
		"BA-SUBTREE-020: finalizeBlockProcessing should trigger subtree-size re-evaluation from per-block samples")
	mockBlockchainClient.AssertExpectations(t)
}

func TestDynamicSizing_SizeBoundsAndPowerOfTwo_BASubtree025(t *testing.T) {
	t.Run("decrease path maintains power-of-two and minimum bound", func(t *testing.T) {
		stp := newDynamicSizingProcessor(t, 64, 8, 32768)
		stp.currentItemsPerFile.Store(64)
		setSubtreeNodeSamples(stp, []int{1, 1, 1, 1, 1})
		stp.blockIntervals = repeatInterval(time.Second, 2)

		stp.adjustSubtreeSize()
		got := stp.currentItemsPerFile.Load()
		require.True(t, got > 0 && got&(got-1) == 0, "BA-SUBTREE-025: adjusted size must remain a power of two")
		require.GreaterOrEqual(t, int(got), 8, "BA-SUBTREE-025: adjusted size must remain >= minimum")
	})

	t.Run("increase path maintains power-of-two and maximum bound", func(t *testing.T) {
		stp := newDynamicSizingProcessor(t, 64, 4, 128)
		stp.currentItemsPerFile.Store(64)
		setSubtreeNodeSamples(stp, []int{60, 60, 60, 60, 60})
		stp.blockIntervals = repeatInterval(50*time.Millisecond, 2)

		stp.adjustSubtreeSize()
		got := stp.currentItemsPerFile.Load()
		require.True(t, got > 0 && got&(got-1) == 0, "BA-SUBTREE-025: adjusted size must remain a power of two")
		require.LessOrEqual(t, int(got), 128, "BA-SUBTREE-025: adjusted size must remain <= maximum")
	})
}

func TestDynamicSizing_DisabledKeepsFixedSize_BAConfig005(t *testing.T) {
	stp := newDynamicSizingProcessor(t, 64, 4, 32768)
	stp.settings.BlockAssembly.UseDynamicSubtreeSize = false
	stp.currentItemsPerFile.Store(64)
	setSubtreeNodeSamples(stp, []int{60, 60, 60, 60, 60})
	stp.blockIntervals = repeatInterval(50*time.Millisecond, 3)

	before := stp.currentItemsPerFile.Load()
	stp.adjustSubtreeSize()
	require.Equal(t, before, stp.currentItemsPerFile.Load(),
		"BA-CONFIG-005: with UseDynamicSubtreeSize=false, subtree size must stay fixed")
}

func TestDynamicSizing_AntiCreepThresholdNotHardcoded_BASubtree024(t *testing.T) {
	source, err := os.ReadFile("SubtreeProcessor.go")
	require.NoError(t, err)

	require.False(t, strings.Contains(string(source), "avgNodesPerSubtree < 50"),
		"BA-SUBTREE-024: anti-creep threshold must be derived from MinimumMerkleItemsPerSubtree, not hardcoded")
}

func newProcessorWithSettings(t *testing.T, tSettings *settings.Settings) (*SubtreeProcessor, error) {
	t.Helper()

	newSubtreeChan := make(chan NewSubtreeRequest, 1)
	subtreeStore := blob_memory.New()
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	stp, err := NewSubtreeProcessor(
		ctx,
		ulogger.TestLogger{},
		tSettings,
		subtreeStore,
		&blockchain.Mock{},
		utxoStore,
		newSubtreeChan,
	)

	if stp != nil {
		t.Cleanup(func() {
			stp.Stop(context.Background())
			close(newSubtreeChan)
		})
	} else {
		t.Cleanup(func() { close(newSubtreeChan) })
	}

	return stp, err
}

func TestDynamicSizing_ConfigValidation_BAConfig001To003(t *testing.T) {
	t.Run("rejects non-power-of-two initial subtree size with setting-specific error", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 1000
		tSettings.BlockAssembly.MinimumMerkleItemsPerSubtree = 4
		tSettings.BlockAssembly.MaximumMerkleItemsPerSubtree = 32768

		_, err := newProcessorWithSettings(t, tSettings)
		require.Error(t, err, "BA-CONFIG-001: invalid InitialMerkleItemsPerSubtree must fail startup")
		require.Contains(t, err.Error(), "InitialMerkleItemsPerSubtree",
			"BA-CONFIG-001: error should name the offending setting")
	})

	t.Run("rejects non-power-of-two minimum subtree size at startup", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 1024
		tSettings.BlockAssembly.MinimumMerkleItemsPerSubtree = 1000
		tSettings.BlockAssembly.MaximumMerkleItemsPerSubtree = 32768

		_, err := newProcessorWithSettings(t, tSettings)
		require.Error(t, err, "BA-CONFIG-002: invalid MinimumMerkleItemsPerSubtree must fail startup")
	})

	t.Run("enforces minimum <= initial <= maximum ordering at startup", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 512
		tSettings.BlockAssembly.MinimumMerkleItemsPerSubtree = 1024
		tSettings.BlockAssembly.MaximumMerkleItemsPerSubtree = 32768

		_, err := newProcessorWithSettings(t, tSettings)
		require.Error(t, err, "BA-CONFIG-003: subtree-size ordering constraints must be validated at startup")
	})
}
