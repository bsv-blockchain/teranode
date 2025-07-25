package aerospikereader

import (
	"bytes"
	"context"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/stores/blob/file"
	"github.com/bitcoin-sv/teranode/stores/blockchain/options"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/aerospike"
	"github.com/bitcoin-sv/teranode/test/utils/transactions"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bitcoin-sv/teranode/util/usql"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockBlockchainStore is a mock implementation of the blockchain.Store interface
type mockBlockchainStore struct {
	blocks map[uint64]*model.Block
}

// Health returns a healthy status for the mock store.
func (m *mockBlockchainStore) Health(_ context.Context, _ bool) (int, string, error) {
	return 0, "", nil
}

// GetDB returns nil for the mock store.
func (m *mockBlockchainStore) GetDB() *usql.DB {
	return nil
}

// GetDBEngine returns nil for the mock store.
func (m *mockBlockchainStore) GetDBEngine() util.SQLEngine {
	return util.Sqlite
}

// GetHeader returns nil for the mock store.
func (m *mockBlockchainStore) GetHeader(_ context.Context, _ *chainhash.Hash) (*model.BlockHeader, error) {
	return nil, nil
}

// GetBlock returns nil and 0 for the mock store.
func (m *mockBlockchainStore) GetBlock(_ context.Context, _ *chainhash.Hash) (*model.Block, uint32, error) {
	return nil, 0, nil
}

// GetBlocks returns nil for the mock store.
func (m *mockBlockchainStore) GetBlocks(_ context.Context, _ *chainhash.Hash, _ uint32) ([]*model.Block, error) {
	return nil, nil
}

// GetBlockByHeight returns nil for the mock store.
func (m *mockBlockchainStore) GetBlockByHeight(_ context.Context, _ uint32) (*model.Block, error) {
	return nil, nil
}

// GetBlockInChainByHeightHash returns nil, false for the mock store.
func (m *mockBlockchainStore) GetBlockInChainByHeightHash(_ context.Context, _ uint32, _ *chainhash.Hash) (*model.Block, bool, error) {
	return nil, false, nil
}

// GetBlockStats returns nil for the mock store.
func (m *mockBlockchainStore) GetBlockStats(_ context.Context) (*model.BlockStats, error) {
	return nil, nil
}

// GetBlockGraphData returns nil for the mock store.
func (m *mockBlockchainStore) GetBlockGraphData(_ context.Context, _ uint64) (*model.BlockDataPoints, error) {
	return nil, nil
}

// GetLastNBlocks returns nil for the mock store.
func (m *mockBlockchainStore) GetLastNBlocks(_ context.Context, _ int64, _ bool, _ uint32) ([]*model.BlockInfo, error) {
	return nil, nil
}

// GetLastNInvalidBlocks returns nil for the mock store.
func (m *mockBlockchainStore) GetLastNInvalidBlocks(_ context.Context, _ int64) ([]*model.BlockInfo, error) {
	return nil, nil
}

// GetSuitableBlock returns nil for the mock store.
func (m *mockBlockchainStore) GetSuitableBlock(_ context.Context, _ *chainhash.Hash) (*model.SuitableBlock, error) {
	return nil, nil
}

// GetHashOfAncestorBlock returns nil for the mock store.
func (m *mockBlockchainStore) GetHashOfAncestorBlock(_ context.Context, _ *chainhash.Hash, _ int) (*chainhash.Hash, error) {
	return nil, nil
}

// GetBlockExists returns false for the mock store.
func (m *mockBlockchainStore) GetBlockExists(_ context.Context, _ *chainhash.Hash) (bool, error) {
	return false, nil
}

// GetBlockHeight returns 0 for the mock store.
func (m *mockBlockchainStore) GetBlockHeight(_ context.Context, _ *chainhash.Hash) (uint32, error) {
	return 0, nil
}

// StoreBlock returns zero values for the mock store.
func (m *mockBlockchainStore) StoreBlock(_ context.Context, _ *model.Block, _ string, _ ...options.StoreBlockOption) (uint64, uint32, error) {
	return 0, 0, nil
}

// GetBestBlockHeader returns nil for the mock store.
func (m *mockBlockchainStore) GetBestBlockHeader(_ context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	return nil, nil, nil
}

// GetBlockHeader returns nil for the mock store.
func (m *mockBlockchainStore) GetBlockHeader(_ context.Context, _ *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	return nil, nil, nil
}

// GetBlockHeaders returns nil for the mock store.
func (m *mockBlockchainStore) GetBlockHeaders(_ context.Context, _ *chainhash.Hash, _ uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return nil, nil, nil
}

// GetBlockHeadersFromTill returns nil for the mock store.
func (m *mockBlockchainStore) GetBlockHeadersFromTill(_ context.Context, _ *chainhash.Hash, _ *chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return nil, nil, nil
}

// GetForkedBlockHeaders returns nil for the mock store.
func (m *mockBlockchainStore) GetForkedBlockHeaders(_ context.Context, _ *chainhash.Hash, _ uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return nil, nil, nil
}

// GetBlockHeadersFromHeight returns nil for the mock store.
func (m *mockBlockchainStore) GetBlockHeadersFromHeight(_ context.Context, _ uint32, _ uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return nil, nil, nil
}

// GetBlockHeadersByHeight returns nil for the mock store.
func (m *mockBlockchainStore) GetBlockHeadersByHeight(_ context.Context, _ uint32, _ uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return nil, nil, nil
}

// InvalidateBlock returns nil for the mock store.
func (m *mockBlockchainStore) InvalidateBlock(_ context.Context, _ *chainhash.Hash) error {
	return nil
}

// RevalidateBlock returns nil for the mock store.
func (m *mockBlockchainStore) RevalidateBlock(_ context.Context, _ *chainhash.Hash) error {
	return nil
}

// GetBlockHeaderIDs returns nil for the mock store.
func (m *mockBlockchainStore) GetBlockHeaderIDs(_ context.Context, _ *chainhash.Hash, _ uint64) ([]uint32, error) {
	return nil, nil
}

// GetState returns nil for the mock store.
func (m *mockBlockchainStore) GetState(_ context.Context, _ string) ([]byte, error) {
	return nil, nil
}

// SetState returns nil for the mock store.
func (m *mockBlockchainStore) SetState(_ context.Context, _ string, _ []byte) error {
	return nil
}

// GetBlockIsMined returns false for the mock store.
func (m *mockBlockchainStore) GetBlockIsMined(_ context.Context, _ *chainhash.Hash) (bool, error) {
	return false, nil
}

// SetBlockMinedSet returns nil for the mock store.
func (m *mockBlockchainStore) SetBlockMinedSet(_ context.Context, _ *chainhash.Hash) error {
	return nil
}

// GetBlocksMinedNotSet returns nil for the mock store.
func (m *mockBlockchainStore) GetBlocksMinedNotSet(_ context.Context) ([]*model.Block, error) {
	return nil, nil
}

// SetBlockSubtreesSet returns nil for the mock store.
func (m *mockBlockchainStore) SetBlockSubtreesSet(_ context.Context, _ *chainhash.Hash) error {
	return nil
}

// GetBlocksSubtreesNotSet returns nil for the mock store.
func (m *mockBlockchainStore) GetBlocksSubtreesNotSet(_ context.Context) ([]*model.Block, error) {
	return nil, nil
}

// GetBlocksByTime returns nil for the mock store.
func (m *mockBlockchainStore) GetBlocksByTime(_ context.Context, _ time.Time, _ time.Time) ([][]byte, error) {
	return nil, nil
}

// CheckBlockIsInCurrentChain returns false for the mock store.
func (m *mockBlockchainStore) CheckBlockIsInCurrentChain(_ context.Context, _ []uint32) (bool, error) {
	return false, nil
}

// GetChainTips returns nil for the mock store.
func (m *mockBlockchainStore) GetChainTips(_ context.Context) ([]*model.ChainTip, error) {
	return nil, nil
}

// GetFSMState returns an empty string for the mock store.
func (m *mockBlockchainStore) GetFSMState(_ context.Context) (string, error) {
	return "", nil
}

// SetFSMState returns nil for the mock store.
func (m *mockBlockchainStore) SetFSMState(_ context.Context, _ string) error {
	return nil
}

// LocateBlockHeaders returns nil for the mock store.
func (m *mockBlockchainStore) LocateBlockHeaders(_ context.Context, _ []*chainhash.Hash, _ *chainhash.Hash, _ uint32) ([]*model.BlockHeader, error) {
	return nil, nil
}

// ExportBlockDB returns nil for the mock store.
func (m *mockBlockchainStore) ExportBlockDB(_ context.Context, _ *chainhash.Hash) (*file.File, error) {
	return nil, nil
}

// SetBlockProcessedAt returns nil for the mock store.
func (m *mockBlockchainStore) SetBlockProcessedAt(_ context.Context, _ *chainhash.Hash, _ ...bool) error {
	return nil
}

// GetBlockByID retrieves a block by its ID from the mock store, or returns an error if not found.
func (m *mockBlockchainStore) GetBlockByID(_ context.Context, id uint64) (*model.Block, error) {
	if b, ok := m.blocks[id]; ok {
		return b, nil
	}

	return nil, context.DeadlineExceeded
}

// TestAerospikeReader tests the Aerospike reader functionality.
func TestAerospikeReader(t *testing.T) {
	// Skip the test if Aerospike is not available
	t.Skip("aerospike reader test is skipped, requires Aerospike server to be running")

	// Create a logger and context for the test
	logger := ulogger.NewVerboseTestLogger(t)

	// Create a test settings object
	testingSettings := test.CreateBaseTestSettings()
	ctx := context.Background()

	// Create a new private key for the transaction
	privKey, err := bec.NewPrivateKey()
	require.NoError(t, err)

	// Create a new transaction with the private key
	tx := transactions.Create(t,
		transactions.WithCoinbaseData(100, "/test miner/"),
		transactions.WithP2PKHOutputs(1, 100000, privKey.PubKey()),
	)

	// Create the aerospike URL for the store
	var aeroURL *url.URL
	aeroURL, err = url.Parse("aerospike://localhost:3000/test?set=utxo&externalStore=file://./data/external")
	require.NoError(t, err)

	// Get the store URL from the settings
	var store utxo.Store
	store, err = aerospike.New(ctx, logger, testingSettings, aeroURL)
	require.NoError(t, err)

	// Create a new transaction in the store
	_, err = store.Create(ctx, tx, 0)
	require.NoError(t, err)

	// Set the mined block info for the transaction
	err = store.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, utxo.MinedBlockInfo{
		BlockID: 0,
	})
	require.NoError(t, err)

	// Set the mined block info for the transaction
	err = store.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, utxo.MinedBlockInfo{
		BlockID: 1,
	})
	require.NoError(t, err)

	// Log the transaction ID
	t.Logf("txid: %s", tx.TxIDChainHash().String())
}

// TestPrintBlockIDs verifies printBlockIDs prints block info and errors as expected.
func TestPrintBlockIDs(t *testing.T) {
	hashPrevBlock, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")
	hashMerkleRoot, _ := chainhash.NewHashFromStr("6a6c0ec8d4adfe242b17153b4f2723b0cb6f783b1ca0f1e17cbdaf699a813316")

	// Prepare mock data
	store := &mockBlockchainStore{
		blocks: map[uint64]*model.Block{
			1: {
				Header: &model.BlockHeader{
					Version:        0x20000000,
					HashPrevBlock:  hashPrevBlock,
					HashMerkleRoot: hashMerkleRoot,
					Timestamp:      1729251723,
					Nonce:          4,
				},
				Height: 123,
				ID:     1,
			},
			2: {
				Header: &model.BlockHeader{
					Version:        0x20000000,
					HashPrevBlock:  hashPrevBlock,
					HashMerkleRoot: hashMerkleRoot,
					Timestamp:      1729251723,
					Nonce:          4,
				},
				CoinbaseTx:       nil,
				TransactionCount: 0,
				SizeInBytes:      0,
				Subtrees:         nil,
				SubtreeSlices:    nil,
				Height:           124,
				ID:               2,
			},
		}}
	input := []interface{}{1, 2, 999} // 999 does not exist

	// Capture output
	origStdout := os.Stdout //nolint:wsl // ignore: assignments should only be cuddled with other assignments

	r, w, err := os.Pipe()
	require.NoError(t, err)

	os.Stdout = w

	// Call the function to test
	printBlockIDs(input, store)

	// Close the writer to flush the output
	err = w.Close()
	require.NoError(t, err)

	// Restore original stdout
	os.Stdout = origStdout

	// Read the output
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)
	output := buf.String()

	// Verify the output
	assert.Contains(t, output, "7a3fbac61c3adbddbd39851b19347d7105918ec73c00f35e1f821a4c9d1bc988")
	assert.Contains(t, output, "error getting block")
}

// TestPrintArray verifies printArray prints array bin values as expected.
func TestPrintArray(t *testing.T) {
	cases := []struct {
		name    string
		input   interface{}
		expects []string
	}{
		{
			name:    "nil input",
			input:   nil,
			expects: []string{"<nil>"},
		},
		{
			name:    "not array",
			input:   123,
			expects: []string{"<not array>"},
		},
		{
			name:    "empty array",
			input:   []interface{}{},
			expects: []string{"<empty>"},
		},
		{
			name:    "array of bytes and values",
			input:   []interface{}{[]byte{0x01, 0x02}, "foo", 42},
			expects: []string{"0102", "foo", "42"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer

			origStdout := os.Stdout

			r, w, err := os.Pipe()
			require.NoError(t, err)

			os.Stdout = w

			printArray("testbin", tc.input)

			err = w.Close()
			require.NoError(t, err)

			os.Stdout = origStdout

			_, err = buf.ReadFrom(r)
			require.NoError(t, err)

			output := buf.String()

			for _, expect := range tc.expects {
				assert.Contains(t, output, expect)
			}
		})
	}
}

// TestFormatExpiration verifies formatExpiration returns expected string representations.
func TestFormatExpiration(t *testing.T) {
	cases := []struct {
		name     string
		input    uint32
		expected string
	}{
		{
			name:     "never expires (0)",
			input:    0,
			expected: "0 (Never expires)",
		},
		{
			name:     "TTLDontExpire (MaxUint32)",
			input:    4294967295, // math.MaxUint32
			expected: "4294967295 (TTLDontExpire)",
		},
		{
			name:     "TTLDontUpdate (MaxUint32 - 1)",
			input:    4294967294, // math.MaxUint32 - 1
			expected: "4294967294 (TTLDontUpdate)",
		},
		{
			name:     "valid timestamp",
			input:    1234567890,
			expected: "1234567890 (2009-02-13T23:31:30Z)",
		},
		{
			name:     "future timestamp",
			input:    2147483647, // Max 32-bit signed int (year 2038)
			expected: "2147483647 (2038-01-19T03:14:07Z)",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := formatExpiration(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TestFormatUtxoHex verifies formatUtxoHex properly formats byte arrays with spaces and reverses hashes.
func TestFormatUtxoHex(t *testing.T) {
	cases := []struct {
		name     string
		input    []byte
		expected string
	}{
		{
			name:     "empty bytes",
			input:    []byte{},
			expected: "",
		},
		{
			name: "32 bytes hash (reversed)",
			input: []byte{
				0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
				0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
				0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
				0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
			},
			expected: "1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100",
		},
		{
			name:     "less than 32 bytes (not reversed)",
			input:    []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05},
			expected: "000102030405",
		},
		{
			name: "33 bytes (first 32 reversed, rest unchanged)",
			input: append([]byte{
				0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
				0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
				0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
				0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
			}, 0xBB),
			expected: "1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100 bb",
		},
		{
			name: "64 bytes (first 32 reversed, rest unchanged)",
			input: append([]byte{
				0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
				0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
				0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
				0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
			}, bytes.Repeat([]byte{0xCC}, 32)...),
			expected: "1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100 cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		},
		{
			name: "65 bytes (first 32 reversed, rest unchanged with two spaces)",
			input: append([]byte{
				0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
				0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
				0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
				0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
			}, append(bytes.Repeat([]byte{0xDD}, 32), 0xEE)...),
			expected: "1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100 dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd ee",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := formatUtxoHex(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TestPrintUtxos verifies printUtxos prints UTXO values with proper formatting.
func TestPrintUtxos(t *testing.T) {
	cases := []struct {
		name    string
		input   interface{}
		expects []string
	}{
		{
			name:    "nil input",
			input:   nil,
			expects: []string{"<nil>"},
		},
		{
			name:    "not array",
			input:   "not an array",
			expects: []string{"<not array>"},
		},
		{
			name:    "empty array",
			input:   []interface{}{},
			expects: []string{"<empty>"},
		},
		{
			name: "array with 32-byte hash (reversed)",
			input: []interface{}{
				[]byte{
					0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
					0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
					0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
					0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
				},
			},
			expects: []string{"1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100"},
		},
		{
			name: "array with 65-byte value (first 32 reversed)",
			input: []interface{}{
				append([]byte{
					0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
					0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
					0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
					0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
				}, append(bytes.Repeat([]byte{0xCD}, 32), 0xEF)...),
			},
			expects: []string{
				"1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100 cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd ef",
			},
		},
		{
			name: "mixed array",
			input: []interface{}{
				[]byte{0x01, 0x02, 0x03},
				"string value",
				42,
			},
			expects: []string{"010203", "string value", "42"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer

			origStdout := os.Stdout

			r, w, err := os.Pipe()
			require.NoError(t, err)

			os.Stdout = w

			printUtxos(tc.input)

			err = w.Close()
			require.NoError(t, err)

			os.Stdout = origStdout

			_, err = buf.ReadFrom(r)
			require.NoError(t, err)

			output := buf.String()

			for _, expect := range tc.expects {
				assert.Contains(t, output, expect)
			}
		})
	}
}

// TestPrintTxID verifies printTxID prints transaction IDs in big-endian format.
func TestPrintTxID(t *testing.T) {
	cases := []struct {
		name    string
		input   interface{}
		expects []string
	}{
		{
			name:    "nil input",
			input:   nil,
			expects: []string{"<nil>"},
		},
		{
			name:    "not bytes",
			input:   "not bytes",
			expects: []string{"<not bytes>"},
		},
		{
			name:    "invalid length (not 32 bytes)",
			input:   []byte{0x01, 0x02, 0x03},
			expects: []string{"010203 (invalid length: 3 bytes)"},
		},
		{
			name: "valid 32-byte txid (reversed)",
			input: []byte{
				0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
				0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
				0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
				0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
			},
			expects: []string{"1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer

			origStdout := os.Stdout

			r, w, err := os.Pipe()
			require.NoError(t, err)

			os.Stdout = w

			printTxID(tc.input)

			err = w.Close()
			require.NoError(t, err)

			os.Stdout = origStdout

			_, err = buf.ReadFrom(r)
			require.NoError(t, err)

			output := buf.String()

			for _, expect := range tc.expects {
				assert.Contains(t, output, expect)
			}
		})
	}
}

// TestPrintConflictingChildren verifies printConflictingChildren prints array of txids reversed.
func TestPrintConflictingChildren(t *testing.T) {
	cases := []struct {
		name    string
		input   interface{}
		expects []string
	}{
		{
			name:    "nil input",
			input:   nil,
			expects: []string{"<nil>"},
		},
		{
			name:    "not array",
			input:   "not an array",
			expects: []string{"<not array>"},
		},
		{
			name:    "empty array",
			input:   []interface{}{},
			expects: []string{"<empty>"},
		},
		{
			name: "array with single 32-byte txid",
			input: []interface{}{
				[]byte{
					0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
					0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
					0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
					0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
				},
			},
			expects: []string{"1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100"},
		},
		{
			name: "array with multiple 32-byte txids",
			input: []interface{}{
				[]byte{
					0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
					0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
					0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
					0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
				},
				[]byte{
					0x1f, 0x1e, 0x1d, 0x1c, 0x1b, 0x1a, 0x19, 0x18,
					0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, 0x10,
					0x0f, 0x0e, 0x0d, 0x0c, 0x0b, 0x0a, 0x09, 0x08,
					0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x00,
				},
			},
			expects: []string{
				"1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100",
				"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			},
		},
		{
			name: "array with invalid length txid",
			input: []interface{}{
				[]byte{0x01, 0x02, 0x03},
			},
			expects: []string{"010203 (invalid length: 3 bytes)"},
		},
		{
			name: "mixed array",
			input: []interface{}{
				[]byte{
					0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
					0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
					0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
					0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
				},
				"string value",
				42,
			},
			expects: []string{
				"1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100",
				"string value",
				"42",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer

			origStdout := os.Stdout

			r, w, err := os.Pipe()
			require.NoError(t, err)

			os.Stdout = w

			printConflictingChildren(tc.input)

			err = w.Close()
			require.NoError(t, err)

			os.Stdout = origStdout

			_, err = buf.ReadFrom(r)
			require.NoError(t, err)

			output := buf.String()

			for _, expect := range tc.expects {
				assert.Contains(t, output, expect)
			}
		})
	}
}

// TODO - create a test for printAerospikeRecord
