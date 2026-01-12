# Teranode Test Generation Guide for LLM Agents

This comprehensive guide provides everything LLM agents need to generate proper Teranode blockchain tests from plain English descriptions. It combines both tutorial patterns and complete API reference.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Test Structure & Patterns](#test-structure--patterns)
3. [Core TestDaemon API](#core-testdaemon-api)
4. [Transaction Patterns](#transaction-patterns)
5. [Mining & Blockchain Operations](#mining--blockchain-operations)
6. [RPC Operations](#rpc-operations)
7. [Verification & Assertions](#verification--assertions)
8. [Complete Test Examples](#complete-test-examples)
9. [Common Test Scenarios](#common-test-scenarios)
10. [Import Patterns](#import-patterns)
11. [Best Practices](#best-practices)
12. [API Reference](#api-reference)
13. [Advanced Testing Patterns](#advanced-testing-patterns)
14. [Bug Reproduction Testing](#bug-reproduction-testing)

## Quick Start

### Basic Test Template

```go
package smoke

import (
	"context"
	"testing"
	"encoding/hex"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/assert"
)

func TestMyScenario(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	// Setup daemon
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsContext: "dev.system.test",
	})
	defer td.Stop(t)

	// Initialize blockchain
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Test implementation...
}
```

## Test Structure & Patterns

### Key Structural Elements

1. **Package**: Always `package smoke`
2. **SharedTestLock**: Must acquire lock at start of every test
3. **Daemon Setup**: Create TestDaemon with appropriate options
4. **Defer Cleanup**: Always defer `td.Stop(t)`
5. **Error Handling**: Use `require.NoError(t, err)` for critical errors
6. **Context**: Use `td.Ctx` or `context.Background()`

## Multi-Database Backend Testing

### IMPORTANT: Database Backend Requirements

All tests MUST be tested with multiple database backends to ensure compatibility:

- **PostgreSQL** (using testcontainers)
- **Aerospike** (using testcontainers)

### Unified Container Management (RECOMMENDED)

**NEW: As of 2025, use the unified `UTXOStoreType` option in TestDaemon for automatic container management.**

This approach eliminates boilerplate container initialization code and ensures consistent setup across all tests.

#### Simple Pattern - Single Backend Test

```go
package smoke

import (
	"testing"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/stretchr/testify/require"
)

func TestMyFeature(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	// Container automatically initialized and cleaned up
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsContext: "dev.system.test",
		UTXOStoreType:   "aerospike", // "aerospike", "postgres"
	})
	defer td.Stop(t)

	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Test implementation...
}
```

#### Multi-Backend Pattern - Testing All Backends

```go
package smoke

import (
	"testing"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/stretchr/testify/require"
)

// Test with Aerospike (default, recommended for most tests)
func TestMyFeatureAerospike(t *testing.T) {
	t.Run("scenario1", func(t *testing.T) {
		testScenario1(t, "aerospike")
	})
	t.Run("scenario2", func(t *testing.T) {
		testScenario2(t, "aerospike")
	})
}

// Test with PostgreSQL
func TestMyFeaturePostgres(t *testing.T) {
	t.Run("scenario1", func(t *testing.T) {
		testScenario1(t, "postgres")
	})
	t.Run("scenario2", func(t *testing.T) {
		testScenario2(t, "postgres")
	})
}

// Shared test implementation
func testScenario1(t *testing.T, storeType string) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsContext: "dev.system.test",
		UTXOStoreType:   storeType, // Automatic container management
	})
	defer td.Stop(t)

	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Test implementation...
}
```

#### Available Store Types

- `"aerospike"` - Aerospike container (production-like, recommended)
- `"postgres"` - PostgreSQL container (production-like)
- `""` (empty) - No automatic container (uses default settings)

#### Benefits of Unified Approach

✅ **No boilerplate** - No manual container initialization
✅ **Automatic cleanup** - Containers cleaned up with `td.Stop(t)`
✅ **Consistent setup** - Same initialization across all tests
✅ **Type-safe** - Compile-time validation of store types
✅ **Less code** - ~10 lines reduced per test

### Legacy Pattern (Manual Container Setup)

**NOTE: This pattern is deprecated. Use `UTXOStoreType` option instead.**

If you encounter existing tests using manual container setup, they should be refactored to use the unified approach above.

<details>
<summary>Click to see legacy pattern (for reference only)</summary>

```go
// OLD PATTERN - Do not use for new tests
func TestMyFeatureAerospike(t *testing.T) {
	// Manual container setup (deprecated)
	utxoStore, teardown, err := aerospike.InitAerospikeContainer()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = teardown()
	})

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		SettingsContext: "dev.system.test",
		SettingsOverrideFunc: func(s *settings.Settings) {
			url, err := url.Parse(utxoStore)
			require.NoError(t, err)
			s.UtxoStore.UtxoStore = url
		},
	})
	defer td.Stop(t)
	// ...
}
```

</details>

### Best Practices for Multi-Database Testing

1. **Use UTXOStoreType option** - Always prefer the unified container management approach
2. **Test with two store types** - Aerospike and PostgreSQL
3. **Use shared test functions** - Write test logic once, parameterize with `storeType`
4. **Aerospike is default** - Most tests should use Aerospike as it's closest to production
5. **Use subtests** - Organize scenarios with `t.Run()` for better test output
6. **Consider test isolation** - Each database backend should be independent

## Core TestDaemon API

### TestDaemon Structure

```go
type TestDaemon struct {
    AssetURL              string
    BlockAssemblyClient   *blockassembly.Client
    BlockValidationClient *blockvalidation.Client
    BlockchainClient      blockchain.ClientI
    Ctx                   context.Context
    DistributorClient     *distributor.Distributor
    Logger                ulogger.Logger
    PropagationClient     *propagation.Client
    Settings              *settings.Settings
    SubtreeStore          blob.Store
    UtxoStore             utxo.Store
    P2PClient             p2p.ClientI
}
```

### TestOptions Structure

```go
type TestOptions struct {
    EnableFullLogging       bool
    EnableLegacy            bool
    EnableP2P               bool
    EnableRPC               bool
    EnableValidator         bool
    SettingsContext         string
    SettingsOverrideFunc    func(*settings.Settings)
    SkipRemoveDataDir       bool
    StartDaemonDependencies bool
    FSMState                blockchain.FSMStateType
}
```

### Daemon Setup Patterns

#### Standard Daemon Setup

```go
td := daemon.NewTestDaemon(t, daemon.TestOptions{
	EnableRPC:       true,
	EnableValidator: true,
	SettingsContext: "dev.system.test",
})
defer td.Stop(t)

// Initialize blockchain
err := td.BlockchainClient.Run(td.Ctx, "test")
require.NoError(t, err)
```

#### Daemon with Custom Settings

```go
td := daemon.NewTestDaemon(t, daemon.TestOptions{
	EnableRPC:       true,
	EnableValidator: true,
	SettingsContext: "dev.system.test",
	SettingsOverrideFunc: func(s *settings.Settings) {
		s.UtxoStore.UnminedTxRetention = 3
		s.GlobalBlockHeightRetention = 5
		// Modify chain params
		if s.ChainCfgParams != nil {
			chainParams := *s.ChainCfgParams
			chainParams.CoinbaseMaturity = 4
			s.ChainCfgParams = &chainParams
		}
	},
})
```

#### Multi-Node Setup (for reorg tests)

```go
node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
	EnableRPC: true,
	EnableP2P: true,
	SettingsContext: "docker.host.teranode1.daemon",
	FSMState: blockchain.FSMStateRUNNING,
})
defer node1.Stop(t)

node2 := daemon.NewTestDaemon(t, daemon.TestOptions{
	EnableRPC: true,
	EnableP2P: true,
	SettingsContext: "docker.host.teranode2.daemon",
	FSMState: blockchain.FSMStateRUNNING,
})
defer node2.Stop(t)

// Connect nodes
node2.ConnectToPeer(t, node1)
```

## Transaction Patterns

### Get Spendable Coinbase

```go
// Mine to maturity and get spendable coinbase (mines 101 blocks)
coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)
```

### Transaction Creation with Helper (RECOMMENDED)

```go
// Create transaction using helper
tx := td.CreateTransactionWithOptions(t,
	transactions.WithInput(coinbaseTx, 0),
	transactions.WithP2PKHOutputs(1, 1000000), // 1 output, 1M satoshis
)

// Multiple outputs with different amounts
parentTx := td.CreateTransactionWithOptions(t,
	transactions.WithInput(coinbaseTx, 0),
	transactions.WithP2PKHOutputs(1, 1_000_000),   // 0.01 BSV
	transactions.WithP2PKHOutputs(1, 2_000_000),   // 0.02 BSV
	transactions.WithP2PKHOutputs(1, 5_000_000),   // 0.05 BSV
)

// Spend from previous transaction
childTx := td.CreateTransactionWithOptions(t,
	transactions.WithInput(parentTx, 0),  // Spend output 0
	transactions.WithInput(parentTx, 1),  // Spend output 1
	transactions.WithP2PKHOutputs(1, combinedAmount),
)

// Alternative creation methods
_, parentTx, err := td.CreateAndSendTxs(t, coinbaseTx, numTxs)
require.NoError(t, err)

parentTx, err := td.CreateParentTransactionWithNOutputs(t, coinbaseTx, numOutputs)
require.NoError(t, err)
```

### Transaction Helper Options (Complete List)

```go
// Available transaction options:
transactions.WithInput(tx *bt.Tx, vout uint32, privKey ...*bec.PrivateKey)
transactions.WithP2PKHOutputs(numOutputs int, amount uint64, pubKey ...*bec.PublicKey)
transactions.WithCoinbaseData(blockHeight uint32, minerInfo string)
transactions.WithOpReturnData(data []byte)
transactions.WithOpReturnSize(size int)
transactions.WithOutput(amount uint64, script *bscript.Script)
transactions.WithChangeOutput(pubKey ...*bec.PublicKey)
transactions.WithPrivateKey(privKey *bec.PrivateKey)
transactions.WithContextForSigning(ctx context.Context)
```

### Transaction Creation Examples

```go
// Basic transaction
tx := transactions.Create(t,
    transactions.WithInput(parentTx, 0),
    transactions.WithP2PKHOutputs(1, amount),
)

// Coinbase transaction
coinbaseTx := transactions.Create(t,
    transactions.WithCoinbaseData(height, "/Test miner/"),
    transactions.WithP2PKHOutputs(1, 50e8, publicKey),
)

// Multi-input transaction
tx := transactions.Create(t,
    transactions.WithInput(parentTx, 0),
    transactions.WithInput(parentTx, 1),
    transactions.WithP2PKHOutputs(1, combinedAmount),
)

// Transaction with change
tx := transactions.Create(t,
    transactions.WithPrivateKey(privateKey),
    transactions.WithInput(parentTx, 0),
    transactions.WithP2PKHOutputs(1, amount),
    transactions.WithChangeOutput(),
)
```

### Manual Transaction Creation (Advanced)

```go
// Create transaction manually (for advanced scenarios)
newTx := bt.NewTx()

// Create UTXO from coinbase
utxo := &bt.UTXO{
	TxIDHash:      coinbaseTx.TxIDChainHash(),
	Vout:          uint32(0),
	LockingScript: coinbaseTx.Outputs[0].LockingScript,
	Satoshis:      coinbaseTx.Outputs[0].Satoshis,
}

err = newTx.FromUTXOs(utxo)
require.NoError(t, err)

// Add output
err = newTx.AddP2PKHOutputFromAddress(address.AddressString, 50000)
require.NoError(t, err)

// Sign transaction
err = newTx.FillAllInputs(td.Ctx, &unlocker.Getter{PrivateKey: privateKey})
require.NoError(t, err)

// Get transaction ID and bytes
txID := newTx.TxIDChainHash().String()
txBytes := newTx.ExtendedBytes()
txHex := hex.EncodeToString(txBytes)
```

### Transaction Submission Methods

#### Via Propagation Client (RECOMMENDED)

```go
err = td.PropagationClient.ProcessTransaction(td.Ctx, tx)
require.NoError(t, err)
```

#### Via RPC

```go
txHex := hex.EncodeToString(tx.ExtendedBytes())
_, err := td.CallRPC(td.Ctx, "sendrawtransaction", []interface{}{txHex})
require.NoError(t, err)
```

#### Via Distributor Client

```go
_, err = td.DistributorClient.SendTransaction(td.Ctx, tx)
require.NoError(t, err)
```

## Mining & Blockchain Operations

### Mining Methods

```go
// Mine to maturity and get spendable coinbase (mines 101 blocks)
coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

// Mine specific number of blocks
td.MineBlocks(t, count)

// Mine blocks and wait for confirmation
block := td.MineAndWait(t, count)

// Mine additional blocks incrementally
for i := 0; i < 10; i++ {
	td.MineAndWait(t, 1)
}
```

### Block Operations

```go
// Get block by height
block, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, height)
require.NoError(t, err)

// Get best block info
height, timestamp, err := td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
require.NoError(t, err)

// Get best block header
_, meta, err := td.BlockchainClient.GetBestBlockHeader(td.Ctx)
require.NoError(t, err)

block, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, meta.Height)
require.NoError(t, err)
```

### Block Assembly & Mining Candidates

```go
// Get mining candidate
mc, err := td.BlockAssemblyClient.GetMiningCandidate(td.Ctx)
require.NoError(t, err)

// Calculate expected subsidy
expectedSubsidy := util.GetBlockSubsidyForHeight(mc.Height, td.Settings.ChainCfgParams)

// Verify coinbase value
assert.Greater(t, mc.CoinbaseValue, expectedSubsidy)
```

### Manual Block Creation

```go
// Create test block manually
// Use a nonce constant and keep incrementing it for every subsequent block operation
nonce := uint32(100)
_, block := td.CreateTestBlock(t, previousBlock, nonce)

// Add a block to blockchain manually
err = nodeA.BlockchainClient.AddBlock(nodeA.Ctx, block, "")
require.NoError(t, err)

// Process block
// Do not add block after process operation
err = td.BlockValidationClient.ProcessBlock(td.Ctx, block, block.Height)
require.NoError(t, err)

// Validate block
// Do not add block after process operation
err = nodeA.BlockValidationClient.ValidateBlock(nodeA.Ctx, block)
require.Error(t, err)
```

## RPC Operations

### Standard RPC Calls

```go
// Make RPC calls
resp, err := td.CallRPC(td.Ctx, "method", []interface{}{params})
require.NoError(t, err)

// Common RPC calls:
// - "generate", []interface{}{numBlocks}
// - "sendrawtransaction", []interface{}{txHex}
// - "getrawmempool", []interface{}{}
// - "getblockchaininfo", []interface{}{}
// - "getrawtransaction", []interface{}{txID, verbose}
// - "getblockbyheight", []interface{}{height}

// Generate blocks via RPC
_, err := td.CallRPC(td.Ctx, "generate", []interface{}{10})
require.NoError(t, err)

// Get blockchain info
resp, err := td.CallRPC(td.Ctx, "getblockchaininfo", []interface{}{})
require.NoError(t, err)

// Get block by hash/height
resp, err := td.CallRPC(td.Ctx, "getblockbyheight", []interface{}{height})
require.NoError(t, err)
```

### Mempool Operations

```go
// Get raw mempool
resp, err := td.CallRPC(td.Ctx, "getrawmempool", []interface{}{})
require.NoError(t, err)

// Parse mempool response
var mempoolResp struct {
	Result []string `json:"result"`
}
err = json.Unmarshal([]byte(resp), &mempoolResp)
require.NoError(t, err)

// Check if transaction is in mempool
found := false
for _, txID := range mempoolResp.Result {
	if txID == expectedTxID {
		found = true
		break
	}
}
require.True(t, found, "Transaction not found in mempool")
```

### Transaction RPC Operations

```go
// Get raw transaction
resp, err := td.CallRPC(td.Ctx, "getrawtransaction", []interface{}{txID, 1})
require.NoError(t, err)

var getRawTxResp helper.GetRawTransactionResponse
err = json.Unmarshal([]byte(resp), &getRawTxResp)
require.NoError(t, err)
```

## Verification & Assertions

### Transaction Verification

```go
// Verify transaction ID
require.Equal(t, expectedTxID, tx.TxIDChainHash().String())

// Verify transaction in block
txFound := false
for _, blockTx := range block.Transactions {
	if blockTx.TxIDChainHash().String() == txID {
		txFound = true
		break
	}
}
require.True(t, txFound, "Transaction not found in block")
```

### Block Verification

```go
// Validate subtrees
err = block.GetAndValidateSubtrees(td.Ctx, td.Logger, td.SubtreeStore)
require.NoError(t, err)

// Check merkle root
err = block.CheckMerkleRoot(td.Ctx)
require.NoError(t, err)

// Verify block height
height, _, err := td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
require.NoError(t, err)
require.Equal(t, expectedHeight, height)
```

### UTXO Operations

```go
// Get transaction from UTXO store
utxoMeta, err := td.UtxoStore.Get(td.Ctx, txHash)
require.NoError(t, err)

// Get with specific fields
utxoMeta, err := td.UtxoStore.Get(td.Ctx, txHash, fields.UnminedSince)
require.NoError(t, err)
```

### Wait for Conditions

```go
// Wait for transaction in mempool (custom helper)
func waitForTransactionInMempool(t *testing.T, td *daemon.TestDaemon, txID string, timeout time.Duration) error {
	start := time.Now()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-time.After(timeout):
			return fmt.Errorf("timeout: transaction %s not found in mempool after %v", txID, timeout)
		case <-ticker.C:
			resp, err := td.CallRPC(td.Ctx, "getrawmempool", []interface{}{})
			if err != nil {
				continue
			}

			var mempoolResp struct {
				Result []string `json:"result"`
			}
			if err = json.Unmarshal([]byte(resp), &mempoolResp); err != nil {
				continue
			}

			for _, mempoolTxID := range mempoolResp.Result {
				if mempoolTxID == txID {
					elapsed := time.Since(start)
					t.Logf("Transaction %s found in mempool after %v", txID, elapsed)
					return nil
				}
			}
		}
	}
}

// Usage
err = waitForTransactionInMempool(t, td, txID, 10*time.Second)
require.NoError(t, err)
```

## Complete Test Examples

### Simple Transaction Test

```go
func TestSimpleTransaction(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsContext: "dev.system.test",
	})
	defer td.Stop(t)

	// Initialize blockchain
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Get spendable coinbase
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	// Create and submit transaction
	tx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 10000),
	)

	err = td.PropagationClient.ProcessTransaction(td.Ctx, tx)
	require.NoError(t, err)

	// Mine block to confirm
	block := td.MineAndWait(t, 1)

	// Verify transaction is in block
	require.NotNil(t, block)
	t.Logf("Transaction confirmed in block: %s", tx.TxIDChainHash())
}
```

### Multi-Output Transaction Test

```go
func TestMultiOutputTransaction(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsContext: "dev.system.test",
	})
	defer td.Stop(t)

	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Mine to maturity
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	// Create transaction with 3 outputs of different amounts
	amount1 := uint64(1_000_000)   // 0.01 BSV
	amount2 := uint64(2_000_000)   // 0.02 BSV
	amount3 := uint64(5_000_000)   // 0.05 BSV

	parentTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, amount1),
		transactions.WithP2PKHOutputs(1, amount2),
		transactions.WithP2PKHOutputs(1, amount3),
	)

	// Submit via RPC
	txHex := hex.EncodeToString(parentTx.ExtendedBytes())
	_, err = td.CallRPC(td.Ctx, "sendrawtransaction", []interface{}{txHex})
	require.NoError(t, err)

	// Create child transaction spending two outputs
	childAmount := amount1 + amount2 - 1000 // Leave fee
	childTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0),
		transactions.WithInput(parentTx, 1),
		transactions.WithP2PKHOutputs(1, childAmount),
	)

	err = td.PropagationClient.ProcessTransaction(td.Ctx, childTx)
	require.NoError(t, err)

	// Verify both in mempool
	resp, err := td.CallRPC(td.Ctx, "getrawmempool", []interface{}{})
	require.NoError(t, err)

	var mempoolResp struct {
		Result []string `json:"result"`
	}
	err = json.Unmarshal([]byte(resp), &mempoolResp)
	require.NoError(t, err)

	parentFound := false
	childFound := false
	for _, txID := range mempoolResp.Result {
		if txID == parentTx.TxIDChainHash().String() {
			parentFound = true
		}
		if txID == childTx.TxIDChainHash().String() {
			childFound = true
		}
	}

	require.True(t, parentFound, "Parent transaction not in mempool")
	require.True(t, childFound, "Child transaction not in mempool")
}
```

## Common Test Scenarios

### When Plain English Says... Generate This Pattern:

#### "Setup daemon" → Standard daemon initialization

```go
td := daemon.NewTestDaemon(t, daemon.TestOptions{
	EnableRPC:       true,
	EnableValidator: true,
	SettingsContext: "dev.system.test",
})
defer td.Stop(t)

err := td.BlockchainClient.Run(td.Ctx, "test")
require.NoError(t, err)
```

#### "Mine blocks to maturity" → Get spendable coinbase

```go
coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)
```

#### "Mine X blocks" → Specific block mining

```go
td.MineBlocks(t, X)
// OR
for i := 0; i < X; i++ {
	td.MineAndWait(t, 1)
}
```

#### "Create transaction with N outputs" → Multi-output transaction

```go
tx := td.CreateTransactionWithOptions(t,
	transactions.WithInput(coinbaseTx, 0),
	transactions.WithP2PKHOutputs(1, amount1),
	transactions.WithP2PKHOutputs(1, amount2),
	// ... more outputs
)
```

#### "Submit via RPC" → RPC submission

```go
txHex := hex.EncodeToString(tx.ExtendedBytes())
_, err := td.CallRPC(td.Ctx, "sendrawtransaction", []interface{}{txHex})
require.NoError(t, err)
```

#### "Submit transaction" → Propagation client submission

```go
err = td.PropagationClient.ProcessTransaction(td.Ctx, tx)
require.NoError(t, err)
```

#### "Wait for transaction in mempool" → Mempool verification

```go
// Check mempool via RPC
resp, err := td.CallRPC(td.Ctx, "getrawmempool", []interface{}{})
require.NoError(t, err)

var mempoolResp struct {
	Result []string `json:"result"`
}
err = json.Unmarshal([]byte(resp), &mempoolResp)
require.NoError(t, err)

// Verify transaction is in mempool
found := false
for _, txID := range mempoolResp.Result {
	if txID == expectedTxID {
		found = true
		break
	}
}
require.True(t, found, "Transaction not found in mempool")
```

#### "Verify transaction in block" → Block confirmation

```go
block := td.MineAndWait(t, 1)

// Verify subtrees and merkle root
err = block.GetAndValidateSubtrees(td.Ctx, td.Logger, td.SubtreeStore)
require.NoError(t, err)

err = block.CheckMerkleRoot(td.Ctx)
require.NoError(t, err)
```

#### "Spend from previous transaction" → Chain transactions

```go
childTx := td.CreateTransactionWithOptions(t,
	transactions.WithInput(parentTx, outputIndex),
	transactions.WithP2PKHOutputs(1, amount),
)
```

## Import Patterns

### Basic Test Imports

```go
import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/stretchr/testify/require"
)
```

### Transaction Test Imports

```go
import (
	"context"
	"testing"
	"encoding/hex"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/assert"
)
```

### RPC/JSON Test Imports

```go
import (
	"context"
	"testing"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	helper "github.com/bsv-blockchain/teranode/test/utils"
	"github.com/stretchr/testify/require"
)
```

### Advanced Test Imports

```go
import (
	"context"
	"testing"
	"time"
	"database/sql"
	"net/url"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
	helper "github.com/bsv-blockchain/teranode/test/utils"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/assert"
)
```

### Multi-Node/P2P Test Imports

```go
import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	helper "github.com/bsv-blockchain/teranode/test/utils"
	"github.com/stretchr/testify/require"
)
```

## Best Practices

### 1. Always Follow This Structure

```go
func TestName(t *testing.T) {
	SharedTestLock.Lock()           // ALWAYS first
	defer SharedTestLock.Unlock()   // ALWAYS defer

	td := daemon.NewTestDaemon(t, daemon.TestOptions{...})
	defer td.Stop(t)                // ALWAYS defer cleanup

	err := td.BlockchainClient.Run(td.Ctx, "test")  // Initialize blockchain
	require.NoError(t, err)

	// Test implementation...
}
```

### 2. Error Handling

```go
// For critical operations that must succeed
require.NoError(t, err, "Failed to create transaction")

// For operations that should fail
require.Error(t, err, "Expected transaction to be rejected")

// Check error contains specific message
require.Contains(t, err.Error(), "expected error text")

// Check specific values
require.Equal(t, expectedValue, actualValue, "Values should match")
require.True(t, condition, "Condition should be true")
require.NotNil(t, object, "Object should not be nil")

// Use assert for non-critical checks
assert.Greater(t, actualValue, expectedMinimum)
assert.Equal(t, expectedValue, actualValue)
assert.Contains(t, slice, expectedItem)
assert.Len(t, slice, expectedLength)
```

### 3. Logging

- Use `t.Log()` and `t.Logf()` for test progress
- Log important transaction IDs and block heights
- Use `td.Logger.Infof()` for daemon-level logging

### 4. Timing

- Use `time.Sleep()` sparingly, prefer proper wait mechanisms
- Implement timeout-based waits for async operations
- Use helper functions like `WaitForNodeBlockHeight()` when available

### 5. Verification

- Always verify critical operations succeeded
- Check transaction IDs match expectations
- Verify transactions appear in expected locations (mempool/blocks)
- Use assertions that provide clear failure messages

### 6. Resource Management

- Always defer cleanup: `defer td.Stop(t)`
- Close any opened connections or channels
- Clean up test data when possible

### 7. Test Isolation

- Use SharedTestLock to prevent concurrent test issues
- Each test should be independent
- Don't rely on external state

## API Reference

### TestDaemon Core Methods

- `td.MineToMaturityAndGetSpendableCoinbaseTx(t, ctx)` - Mine 101 blocks and return spendable coinbase
- `td.MineBlocks(t, count)` - Mine specified number of blocks
- `td.MineAndWait(t, count)` - Mine blocks and wait for confirmation
- `td.CreateTransactionWithOptions(t, options...)` - Create transaction with helper options
- `td.CallRPC(ctx, method, params)` - Make RPC call
- `td.Stop(t)` - Cleanup daemon (always defer this)
- `td.GetPrivateKey(t)` - Get private key for signing
- `td.ConnectToPeer(t, peer)` - Connect nodes (for multi-node tests)
- `td.LogJSON(t, description, data)` - Log JSON for debugging

### Client Access

- `td.BlockchainClient` - Blockchain operations
- `td.PropagationClient` - Transaction propagation
- `td.DistributorClient` - Transaction distribution
- `td.BlockAssemblyClient` - Block assembly operations
- `td.UtxoStore` - UTXO store operations
- `td.P2PClient` - P2P operations

### Context & Settings

- `td.Ctx` - Daemon context
- `td.Settings` - Daemon settings
- `td.Logger` - Logger instance

### Key Generation

```go
// Generate new private key
privateKey, err := bec.NewPrivateKey()
require.NoError(t, err)

// Create address from public key
address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
require.NoError(t, err)

// Create private key from WIF
privateKey, err := bec.PrivateKeyFromWif(wifString)
require.NoError(t, err)
```

---

## Usage Instructions for LLMs

When generating tests from plain English:

1. **Start with the basic structure** - always include SharedTestLock and daemon setup
2. **Map English phrases to patterns** using the "Common Test Scenarios" section
3. **Use the helper methods** rather than manual transaction creation when possible
4. **Include proper error handling** with descriptive messages
5. **Add verification steps** to confirm operations succeeded
6. **Follow the import patterns** based on what APIs you're using
7. **End with cleanup** - defer td.Stop(t)

The goal is to produce complete, working, well-structured tests that follow Teranode conventions and can be run immediately without syntax errors.

## Advanced Testing Patterns

### Daemon Restart Testing

Testing daemon restart scenarios requires special handling to preserve container state while stopping and restarting the daemon process. This is essential for testing data persistence and recovery behavior.

#### Pattern: Daemon Restart with Container Persistence

```go
func testDaemonRestartScenario(t *testing.T, utxoStoreType string) {
	// Phase 1: Initial daemon with SkipContainerCleanup
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		UTXOStoreType:        utxoStoreType,
		SkipContainerCleanup: true,  // CRITICAL: Keep container alive after Stop
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.ChainCfgParams.CoinbaseMaturity = 2
				// Custom settings...
			},
		),
	})

	// Store container manager BEFORE any Stop() call
	containerManager := td.GetContainerManager()

	// Ensure container cleanup at test end
	defer func() {
		if containerManager != nil {
			_ = containerManager.Cleanup()
		}
	}()
	defer td.Stop(t)

	// Initialize and perform operations...
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// ... create transactions, mine blocks, etc ...

	// Phase 2: Stop daemon but keep container
	td.Stop(t)
	td.ResetServiceManagerContext(t)  // CRITICAL: Reset context for new daemon

	// Phase 3: Create new daemon reusing container
	td = daemon.NewTestDaemon(t, daemon.TestOptions{
		ContainerManager:     containerManager,  // Reuse existing container
		SkipRemoveDataDir:    true,              // Preserve data directory
		SkipContainerCleanup: true,              // Don't cleanup on this Stop either
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.ChainCfgParams.CoinbaseMaturity = 2
				// Same settings as before
			},
		),
	})
	defer td.Stop(t)

	// Verify state was preserved after restart...
}
```

#### Key Options for Restart Testing

| Option | Purpose |
|--------|---------|
| `SkipContainerCleanup: true` | Prevents container from being destroyed when `td.Stop(t)` is called |
| `td.GetContainerManager()` | Returns the container manager to pass to new daemon instance |
| `td.ResetServiceManagerContext(t)` | Resets internal context after Stop, required before creating new daemon |
| `ContainerManager: containerManager` | Passes existing container to new daemon instance |
| `SkipRemoveDataDir: true` | Preserves the data directory between daemon instances |

#### Common Restart Test Scenarios

1. **Service restart recovery** - Verify unmined transactions are reloaded
2. **Block assembly restart** - Test block assembly state persistence
3. **Data integrity after crash** - Simulate crash and verify data recovery
4. **Configuration reload** - Test behavior with different settings after restart

### External Transaction Testing

External transactions are stored differently than regular transactions. A transaction becomes "external" when its UTXO count exceeds `utxoBatchSize` (len(batches) > 1).

#### Creating External Transactions

```go
const (
	lowUtxoBatchSize       = 2   // Low value to trigger external storage easily
	numOutputsForExternalTx = 5  // More than utxoBatchSize to force external
)

func testExternalTransactions(t *testing.T, utxoStoreType string) {
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		UTXOStoreType: utxoStoreType,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.ChainCfgParams.CoinbaseMaturity = 2
				// Set low batch size to force external storage with fewer outputs
				s.UtxoStore.UtxoBatchSize = lowUtxoBatchSize
			},
		),
	})
	defer td.Stop(t)

	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate blocks and get spendable coinbase
	err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 3})
	require.NoError(t, err)

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	// Create external transaction: outputs > utxoBatchSize triggers external storage
	externalTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(block1.CoinbaseTx, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 100000),  // 5 outputs > 2 batch size
	)

	t.Logf("Created external transaction %s with %d outputs",
		externalTx.TxIDChainHash().String(), len(externalTx.Outputs))

	// Submit and verify
	err = td.PropagationClient.ProcessTransaction(td.Ctx, externalTx)
	require.NoError(t, err)
}
```

#### External Transaction Characteristics

- **Storage**: Stored in blob store (external) rather than inline in UTXO records
- **Format**: Stored using `tx.ExtendedBytes()` which includes Extended Format metadata
- **Trigger condition**: `len(batches) > 1` where `batches = ceil(numOutputs / utxoBatchSize)`
- **Recovery path**: `loadUnminedTransactions` → `GetUnminedTxIterator` → `processExternalTransactionInpoints`

### Block Assembly Reset vs Daemon Restart

Understanding the difference is critical for writing correct tests:

| Operation | What it does | Data preserved | Use case |
|-----------|--------------|----------------|----------|
| `ResetBlockAssemblyFully()` | Resets block assembly state, reloads from store | In-memory caches may persist | Testing reload from UTXO store |
| Daemon restart | Full process restart, cold start | Only persisted data | Testing true restart scenarios |

**Important**: `ResetBlockAssemblyFully()` may not catch bugs that only manifest during cold restart because in-memory caches can mask issues with data parsing/loading.

### Block Assembly Verification Methods

```go
// Wait for transaction to appear in block assembly
err = td.WaitForTransactionInBlockAssembly(tx, 10*time.Second)
require.NoError(t, err)

// Verify transaction is in block assembly
td.VerifyInBlockAssembly(t, tx1, tx2, tx3)

// Verify transaction is NOT in block assembly (e.g., after mining)
td.VerifyNotInBlockAssembly(t, tx1, tx2)

// Verify transaction is on longest chain in UTXO store
td.VerifyOnLongestChainInUtxoStore(t, tx)

// Wait for specific block height
td.WaitForBlockHeight(t, block, 10*time.Second, true)
```

### Using test.ComposeSettings and test.SystemTestSettings

For sequential tests and integration tests, use the settings composition pattern:

```go
import (
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/settings"
)

td := daemon.NewTestDaemon(t, daemon.TestOptions{
	UTXOStoreType: utxoStoreType,
	SettingsOverrideFunc: test.ComposeSettings(
		test.SystemTestSettings(),  // Base system test settings
		func(s *settings.Settings) {
			// Additional customizations
			s.ChainCfgParams.CoinbaseMaturity = 2
			s.UtxoStore.UtxoBatchSize = 2
		},
	),
})
```

### UTXO Store Iterator Pattern

For testing data that's read via iterators (like unmined transactions):

```go
// Get unmined transaction iterator
it, err := td.UtxoStore.GetUnminedTxIterator(false)
require.NoError(t, err)
defer it.Close()

// Iterate through unmined transactions
for {
	unminedTx, err := it.Next(td.Ctx)
	if err != nil {
		t.Fatalf("Error iterating: %v", err)
	}
	if unminedTx == nil {
		break  // End of iteration
	}
	if unminedTx.Skip {
		continue  // Skip flagged entries
	}

	// Access transaction data
	t.Logf("Found tx %s with %d parent hashes",
		unminedTx.Hash.String(),
		len(unminedTx.TxInpoints.ParentTxHashes))
}
```

## Bug Reproduction Testing

### Writing Tests That Reproduce Bugs

When writing tests to reproduce bugs, follow these principles:

#### 1. Understand the Bug Mechanism

Before writing a test, understand:
- **What triggers the bug** - specific conditions, data states, or sequences
- **What the bug causes** - error, silent data corruption, or wrong behavior
- **The code path involved** - which functions are called

#### 2. Silent Data Corruption vs Explicit Errors

Some bugs cause silent data corruption rather than errors:

```go
// BAD: This test may pass even with the bug present
// because no error is returned - just wrong data
err = someOperation()
require.NoError(t, err)  // Passes, but data may be corrupted!

// GOOD: Explicitly verify the data is correct
err = someOperation()
require.NoError(t, err)
// Actually check the data values
require.Equal(t, expectedValue, actualValue,
	"Data should be correct. If this fails, the parsing bug is present.")
```

#### 3. Test Downstream Impact

Instead of testing the buggy function directly, test where its corruption causes visible failures:

```go
// Example: Bug corrupts parent tx hashes during parsing
// Direct test might miss it if caching masks the issue

// Better: Test the downstream operation that fails due to corruption
// e.g., subtree serialization fails when parent hashes are empty
err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 1})
// This will fail with "cannot serialize, parent tx hashes are not set"
// if the bug is present
require.NoError(t, err)
```

#### 4. Create Parent-Child Transaction Chains

For testing transaction processing bugs, create chains where children depend on parents:

```go
// Parent transaction (external, triggers the bug condition)
parentTx := td.CreateTransactionWithOptions(t,
	transactions.WithInput(coinbaseTx, 0),
	transactions.WithP2PKHOutputs(numOutputsForExternalTx, 100000),
)

// Child transaction spending from parent
childTx := td.CreateTransactionWithOptions(t,
	transactions.WithInput(parentTx, 0),
	transactions.WithP2PKHOutputs(numOutputsForExternalTx, 1000),
)

// Submit both
require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, parentTx))
require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, childTx))

// The bug manifests when processing the child's reference to parent
```

#### 5. Restart Testing for Parser Bugs

Parser bugs often only manifest after restart when data is re-read from storage:

```go
// Phase 1: Store data (works fine)
err = td.PropagationClient.ProcessTransaction(td.Ctx, tx)
require.NoError(t, err)

// Reset doesn't catch it (in-memory cache masks bug)
err = td.BlockAssemblyClient.ResetBlockAssemblyFully(td.Ctx)
require.NoError(t, err)  // May pass due to caching

// Phase 2: Full restart forces re-parsing from storage
td.Stop(t)
td.ResetServiceManagerContext(t)
td = daemon.NewTestDaemon(t, daemon.TestOptions{
	ContainerManager:     containerManager,
	SkipRemoveDataDir:    true,
	SkipContainerCleanup: true,
	// ... same settings ...
})

// NOW the bug manifests because data must be parsed from scratch
err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 1})
// Fails on buggy code, passes on fixed code
require.NoError(t, err)
```

### Complete Bug Reproduction Test Template

```go
func TestBugReproduction_DescriptiveName(t *testing.T) {
	t.Run("scenario that triggers bug", func(t *testing.T) {
		testBugScenario(t, "aerospike")
	})
}

func testBugScenario(t *testing.T, utxoStoreType string) {
	// Setup with options that trigger the bug condition
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		UTXOStoreType:        utxoStoreType,
		SkipContainerCleanup: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				// Settings that create bug-triggering conditions
				s.SomeSetting = bugTriggeringValue
			},
		),
	})

	containerManager := td.GetContainerManager()
	defer func() {
		if containerManager != nil {
			_ = containerManager.Cleanup()
		}
	}()
	defer td.Stop(t)

	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Setup: Create conditions that expose the bug
	// ... create transactions, mine blocks, etc ...

	// Trigger: Action that causes the bug to manifest
	td.Stop(t)
	td.ResetServiceManagerContext(t)

	td = daemon.NewTestDaemon(t, daemon.TestOptions{
		ContainerManager:     containerManager,
		SkipRemoveDataDir:    true,
		SkipContainerCleanup: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.SomeSetting = bugTriggeringValue
			},
		),
	})
	defer td.Stop(t)

	// Verify: Operation that fails on buggy code, passes on fixed code
	err = td.SomeOperation()
	require.NoError(t, err, "This should pass on fixed code, fail on buggy code")

	// Additional verification of correct behavior
	td.VerifySomeCondition(t)
}
```

### Sequential Test Package Structure

For tests that require specific ordering or isolation, use the sequential test package:

```go
package block_assembly  // or other sequential test package

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// Note: Sequential tests don't use SharedTestLock - they run in isolation
func TestSequentialScenario(t *testing.T) {
	t.Run("scenario name", func(t *testing.T) {
		testScenario(t, "aerospike")
	})
}
```

## Double Spend Testing with External Transactions

### Overview

External transactions are multi-UTXO record transactions that exceed `utxoBatchSize`, causing them to be stored externally rather than inline in UTXO records. Testing double spend detection with external transactions ensures that the conflict detection works correctly when transaction data spans multiple records.

### Key Concepts

1. **External Transaction Trigger**: A transaction becomes "external" when `len(batches) > 1` where `batches = ceil(numOutputs / utxoBatchSize)`
2. **Low Batch Size**: Setting `s.UtxoStore.UtxoBatchSize = 2` triggers external storage with just 3+ outputs
3. **Multi-Record Spending**: Double spends that touch different UTXO records test the full conflict detection path

### External Transaction Helper Pattern

```go
const (
	lowUtxoBatchSize        = 2      // Trigger external storage easily
	numOutputsForExternalTx = 5      // More than utxoBatchSize to force external
	outputAmount            = uint64(100000)
)

// Settings function for external transaction tests
func externalTxSettingsFunc() func(*settings.Settings) {
	return test.ComposeSettings(
		test.SystemTestSettings(),
		func(s *settings.Settings) {
			s.UtxoStore.UtxoBatchSize = lowUtxoBatchSize
		},
	)
}
```

### Setup Function for Double Spend Tests with External TXs

```go
func setupExternalTxDoubleSpendTest(t *testing.T, utxoStoreType string, blockOffset ...uint32) (
	td *daemon.TestDaemon,
	coinbaseTx1, txOriginal, txDoubleSpend *bt.Tx,
	block102 *model.Block,
	tx2 *bt.Tx,
) {
	td = daemon.NewTestDaemon(t, daemon.TestOptions{
		UTXOStoreType:        utxoStoreType,
		SettingsOverrideFunc: externalTxSettingsFunc(),
	})

	// Initialize blockchain
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 101})
	require.NoError(t, err)

	// Get coinbase to spend
	blockHeight := uint32(1)
	if len(blockOffset) > 0 && blockOffset[0] > 0 && blockOffset[0] <= 100 {
		blockHeight = blockOffset[0]
	}

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, blockHeight)
	require.NoError(t, err)
	coinbaseTx1 = block1.CoinbaseTx

	// Create external transactions (5 outputs > batch size of 2)
	txOriginal = td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx1, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, outputAmount),
	)

	txDoubleSpend = td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx1, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, outputAmount),
	)

	// Submit original (succeeds) and double spend (fails)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txOriginal))
	require.Error(t, td.PropagationClient.ProcessTransaction(td.Ctx, txDoubleSpend))

	// Mine a block
	err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 1})
	require.NoError(t, err)

	block102, err = td.BlockchainClient.GetBlockByHeight(td.Ctx, 102)
	require.NoError(t, err)

	// ... create tx2 from another block ...

	return td, coinbaseTx1, txOriginal, txDoubleSpend, block102, tx2
}
```

### Creating External Transaction Chains

```go
// Each transaction in the chain has multiple outputs, stored externally
txA1 := td.CreateTransactionWithOptions(t,
	transactions.WithInput(txA0, 0),
	transactions.WithP2PKHOutputs(numOutputsForExternalTx, outputAmount/2),
)
txA2 := td.CreateTransactionWithOptions(t,
	transactions.WithInput(txA1, 0),
	transactions.WithP2PKHOutputs(numOutputsForExternalTx, outputAmount/4),
)
// Continue chain...
```

### Conflicting External Blocks Pattern

```go
// Create a block with conflicting external transactions
func createConflictingExternalBlock(t *testing.T, td *daemon.TestDaemon, originalBlock *model.Block,
	blockTxs []*bt.Tx, originalTxs []*bt.Tx, nonce uint32) *model.Block {

	previousBlock, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, originalBlock.Height-1)
	require.NoError(t, err)

	newBlockSubtree, newBlock := td.CreateTestBlock(t, previousBlock, nonce, blockTxs...)

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, newBlock, newBlock.Height, "", "legacy"))

	// Verify original block is still winning
	td.WaitForBlockHeight(t, originalBlock, blockWait, true)

	// Verify conflict status
	td.VerifyConflictingInUtxoStore(t, false, originalTxs...)
	td.VerifyConflictingInUtxoStore(t, true, blockTxs...)

	return newBlock
}
```

### Test Scenarios with External Transactions

#### 1. Single Double Spend (External TXs)

```go
// Setup: txA0 and txB0 both spend same coinbase (both external with 5 outputs)
// Result: txB0 rejected as double spend
// Fork test: Create block102b with txB0, make it longer, verify txA0 becomes conflicting
```

#### 2. Multiple Conflicting TXs (External)

```go
// Setup: txA0, txB0, txC0 all spend same coinbase (triple spend, all external)
// Create blocks 102a, 102b, 102c each with one of the transactions
// Make chain B longest, verify txA0 and txC0 are conflicting
```

#### 3. Conflicting Chains (External)

```go
// Chain A: txA0 -> txA1 -> txA2 -> txA3 (all external)
// Chain B: txB1 -> txB2 -> txB3 (txB1 conflicts with txA1, all external)
// Test that entire chain is marked conflicting when losing
```

#### 4. Triple Fork (External)

```go
// Three chains: A, B, C (all transactions external)
// Multiple reorganizations: A wins -> B wins -> C wins
// Verify all txs in A and B are conflicting, C txs are not
```

### Best Practices for External TX Double Spend Tests

1. **Use consistent batch size**: Set `s.UtxoStore.UtxoBatchSize = 2` for all external TX tests
2. **Create enough outputs**: Use `numOutputsForExternalTx = 5` to ensure external storage
3. **Test multiple scenarios**: Single, multiple, chains, and forks
4. **Verify both directions**: Check conflict status after each reorg
5. **Use separate files**: Create one file per test scenario for easy debugging
6. **Log transaction details**: Include txid and output count in logs for debugging

### File Structure for External TX Double Spend Tests

```
test/sequentialtest/double_spend/
├── double_spend_test.go                     # Original tests (non-external)
├── helpers.go                               # Original helpers
├── external_tx_helpers.go                   # External TX helpers
├── 01_single_double_spend_external_tx_test.go
├── 02_multiple_conflicting_external_tx_test.go
├── 03_conflicting_chains_external_tx_test.go
├── 04_double_spend_fork_external_tx_test.go
├── 05_triple_forked_chain_external_tx_test.go
├── 06_grandparent_child_conflict_test.go
├── 07_grandparent_multi_output_conflict_test.go
├── 08_same_chain_grandparent_double_spend_test.go
└── 09_forked_chain_grandparent_double_spend_test.go
```

## Complete Guide to Writing Double Spend Tests

This section provides comprehensive instructions for LLMs to write double spend tests based on plain English descriptions.

### Understanding Double Spend Scenarios

Double spend tests verify that the system correctly:
1. **Rejects invalid transactions** that spend already-spent outputs
2. **Tracks conflicting transactions** across competing chains
3. **Updates conflict status** during chain reorganizations
4. **Cascades conflicts** to dependent transactions

### Test Structure Template

All double spend tests follow this structure:

```go
package doublespendtest

import (
	"testing"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// Test with PostgreSQL
func TestMyScenarioPostgres(t *testing.T) {
	t.Run("scenario_name", func(t *testing.T) {
		testMyScenario(t, "postgres")
	})
}

// Test with Aerospike
func TestMyScenarioAerospike(t *testing.T) {
	t.Run("scenario_name", func(t *testing.T) {
		testMyScenario(t, "aerospike")
	})
}

func testMyScenario(t *testing.T, utxoStore string) {
	// Setup
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		UTXOStoreType:        utxoStore,
		SettingsOverrideFunc: externalTxSettingsFunc(),
	})
	defer func() {
		td.Stop(t)
	}()

	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Get spendable coinbase (CoinbaseMaturity=1, so this mines 2 blocks)
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	// Test implementation...
}
```

### Key Constants and Helpers

```go
const (
	lowUtxoBatchSize        = 2              // Triggers external storage
	numOutputsForExternalTx = 5              // Creates external transactions
	outputAmount            = uint64(100000) // Satoshis per output
	blockWait               = 5 * time.Second
)

// externalTxSettingsFunc configures low batch size for external TX testing
func externalTxSettingsFunc() func(*settings.Settings) {
	return test.ComposeSettings(
		test.SystemTestSettings(),
		func(s *settings.Settings) {
			s.UtxoStore.UtxoBatchSize = lowUtxoBatchSize
		},
	)
}
```

### Block Height Reference

With `CoinbaseMaturity = 1` (set by TestDaemon):
- `MineToMaturityAndGetSpendableCoinbaseTx` mines 2 blocks (heights 1, 2)
- Returns coinbase from block 1
- Next mined block will be height 3

### Common Patterns

#### Pattern 1: Creating Multi-Output Transactions

```go
// Grandparent with 5 outputs (external transaction)
grandparent := td.CreateTransactionWithOptions(t,
	transactions.WithInput(coinbaseTx, 0),
	transactions.WithP2PKHOutputs(numOutputsForExternalTx,
		coinbaseTx.Outputs[0].Satoshis/numOutputsForExternalTx-100),
)

// Parent spending specific grandparent outputs
parent := td.CreateTransactionWithOptions(t,
	transactions.WithInput(grandparent, 0), // Spend output 0
	transactions.WithInput(grandparent, 1), // Spend output 1
	transactions.WithP2PKHOutputs(numOutputsForExternalTx,
		grandparent.Outputs[0].Satoshis/numOutputsForExternalTx-100),
)
```

#### Pattern 2: Creating Conflicting Transactions

```go
// txA and txB both spend the same output - they conflict!
txA := td.CreateTransactionWithOptions(t,
	transactions.WithInput(parentTx, 0),  // Spends output 0
	transactions.WithP2PKHOutputs(numOutputsForExternalTx, amount),
)

txB := td.CreateTransactionWithOptions(t,
	transactions.WithInput(parentTx, 0),  // Also spends output 0 - CONFLICT!
	transactions.WithP2PKHOutputs(numOutputsForExternalTx, amount),
)
```

#### Pattern 3: Creating Forked Chains

```go
// Get the fork point block
forkPoint, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, forkHeight)
require.NoError(t, err)

// Create block on chain A (main chain via propagation + mining)
require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txA))
td.MineAndWait(t, 1)
blockA, _ := td.BlockchainClient.GetBlockByHeight(td.Ctx, forkHeight+1)

// Create block on chain B (fork via CreateTestBlock)
_, blockB := td.CreateTestBlock(t, forkPoint, nonce, txB)
require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, blockB, blockB.Height, "", "legacy"))
```

#### Pattern 4: Making a Fork Win (Trigger Reorg)

```go
// Chain A is at height 5, Chain B is at height 4
// Add empty blocks to Chain B to make it longer

_, block5b := td.CreateTestBlock(t, block4b, 10502)
require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block5b, block5b.Height, "", "legacy"))

_, block6b := td.CreateTestBlock(t, block5b, 10602)
require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block6b, block6b.Height, "", "legacy"))

// Wait for reorg to complete
td.WaitForBlockHeight(t, block6b, blockWait, true)
```

### Verification Methods

```go
// Verify transaction is on longest chain (not conflicting)
td.VerifyOnLongestChainInUtxoStore(t, tx)

// Verify transaction conflict status in UTXO store
td.VerifyConflictingInUtxoStore(t, true, conflictingTx)   // Should be conflicting
td.VerifyConflictingInUtxoStore(t, false, winningTx)      // Should NOT be conflicting

// Verify transaction is NOT in block assembly (mined or conflicting)
td.VerifyNotInBlockAssembly(t, tx)

// Verify conflict status in subtrees
td.VerifyConflictingInSubtrees(t, block.Subtrees[0], conflictingTxs...)

// Wait for specific block height
td.WaitForBlockHeight(t, block, blockWait, true)
```

### Translating Plain English to Test Code

#### "Grandparent has N outputs"
```go
grandparent := td.CreateTransactionWithOptions(t,
	transactions.WithInput(coinbaseTx, 0),
	transactions.WithP2PKHOutputs(N, amountPerOutput),
)
```

#### "Parent spends grandparent outputs X and Y"
```go
parent := td.CreateTransactionWithOptions(t,
	transactions.WithInput(grandparent, X),
	transactions.WithInput(grandparent, Y),
	transactions.WithP2PKHOutputs(numOutputs, amount),
)
```

#### "Child spends parent output A and grandparent outputs B, C"
```go
child := td.CreateTransactionWithOptions(t,
	transactions.WithInput(parent, A),
	transactions.WithInput(grandparent, B),
	transactions.WithInput(grandparent, C),
	transactions.WithP2PKHOutputs(numOutputs, amount),
)
```

#### "Transaction should be rejected / invalid"
```go
err = td.PropagationClient.ProcessTransaction(td.Ctx, invalidTx)
require.Error(t, err, "Expected transaction to be rejected")

// Or for blocks:
err = td.BlockValidationClient.ProcessBlock(td.Ctx, invalidBlock, invalidBlock.Height, "", "legacy")
require.Error(t, err, "Expected block to be rejected")
```

#### "Create a fork at block N"
```go
forkPoint, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, N)
require.NoError(t, err)

_, forkBlock := td.CreateTestBlock(t, forkPoint, nonce, txs...)
require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, forkBlock, forkBlock.Height, "", "legacy"))
```

#### "Make chain B win / longer"
```go
// Add enough empty blocks to make chain B longer than chain A
for i := 0; i < numBlocksNeeded; i++ {
	_, nextBlock := td.CreateTestBlock(t, previousBlock, nonce+uint32(i))
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, nextBlock, nextBlock.Height, "", "legacy"))
	previousBlock = nextBlock
}
td.WaitForBlockHeight(t, previousBlock, blockWait, true)
```

#### "Verify X is conflicting / not conflicting"
```go
td.VerifyConflictingInUtxoStore(t, true, conflictingTx)   // Is conflicting
td.VerifyConflictingInUtxoStore(t, false, validTx)        // Not conflicting
```

### Complex Scenario Example: Grandparent Multi-Output Conflict

**Plain English:**
> Create a test where grandparent has 5 outputs. Parent spends GP:0 and GP:4.
> Child1 spends parent:0 and GP:1. Child2 spends parent:1 and GP:2.
> ParentSibling spends GP:3. Then a fork where parentB spends GP:0,1,3,4 wins.

**Test Structure:**

```go
func testComplexForkGrandparentConflict(t *testing.T, utxoStore string) {
	// Setup daemon
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		UTXOStoreType:        utxoStore,
		SettingsOverrideFunc: externalTxSettingsFunc(),
	})
	defer td.Stop(t)

	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	// Create grandparent with 5 outputs
	grandparent := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(5, coinbaseTx.Outputs[0].Satoshis/5-100),
	)

	// Mine grandparent
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, grandparent))
	td.MineAndWait(t, 1)

	block3gp, _ := td.BlockchainClient.GetBlockByHeight(td.Ctx, 3)

	// Chain A: parent spends GP:0, GP:4
	parent := td.CreateTransactionWithOptions(t,
		transactions.WithInput(grandparent, 0),
		transactions.WithInput(grandparent, 4),
		transactions.WithP2PKHOutputs(5, grandparent.Outputs[0].Satoshis/5-100),
	)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, parent))
	td.MineAndWait(t, 1)

	// Chain A: child1 spends parent:0, GP:1
	child1 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parent, 0),
		transactions.WithInput(grandparent, 1),
		transactions.WithP2PKHOutputs(5, parent.Outputs[0].Satoshis/5-100),
	)

	// Chain A: child2 spends parent:1, GP:2
	child2 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parent, 1),
		transactions.WithInput(grandparent, 2),
		transactions.WithP2PKHOutputs(5, parent.Outputs[1].Satoshis/5-100),
	)

	// Chain A: parentSibling spends GP:3
	parentSibling := td.CreateTransactionWithOptions(t,
		transactions.WithInput(grandparent, 3),
		transactions.WithP2PKHOutputs(5, grandparent.Outputs[3].Satoshis/5-100),
	)

	// Submit and mine Chain A transactions
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, child1))
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, child2))
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, parentSibling))
	td.MineAndWait(t, 1)

	block5a, _ := td.BlockchainClient.GetBlockByHeight(td.Ctx, 5)

	// Verify Chain A is winning
	td.VerifyOnLongestChainInUtxoStore(t, parent)
	td.WaitForBlockHeight(t, block5a, blockWait, true)

	// Chain B: parentB spends GP:0,1,3,4 (conflicts with parent, child1, parentSibling)
	parentB := td.CreateTransactionWithOptions(t,
		transactions.WithInput(grandparent, 0),
		transactions.WithInput(grandparent, 1),
		transactions.WithInput(grandparent, 3),
		transactions.WithInput(grandparent, 4),
		transactions.WithP2PKHOutputs(5, grandparent.Outputs[0].Satoshis/5-100),
	)

	// Create fork from block 3 (grandparent block)
	_, block4b := td.CreateTestBlock(t, block3gp, 10302, parentB)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block4b, block4b.Height, "", "legacy"))

	// Verify parentB is conflicting (losing chain)
	td.VerifyConflictingInUtxoStore(t, true, parentB)

	// Make Chain B longer to trigger reorg
	_, block5b := td.CreateTestBlock(t, block4b, 10402)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block5b, block5b.Height, "", "legacy"))

	_, block6b := td.CreateTestBlock(t, block5b, 10502)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block6b, block6b.Height, "", "legacy"))

	td.WaitForBlockHeight(t, block6b, blockWait, true)

	// After reorg: verify conflict status
	td.VerifyConflictingInUtxoStore(t, false, parentB)         // Now winning
	td.VerifyConflictingInUtxoStore(t, true, parent)           // GP:0,4 conflict
	td.VerifyConflictingInUtxoStore(t, true, child1)           // GP:1 conflict + cascade
	td.VerifyConflictingInUtxoStore(t, true, child2)           // Cascade (parent:1 gone)
	td.VerifyConflictingInUtxoStore(t, true, parentSibling)    // GP:3 conflict
	td.VerifyConflictingInUtxoStore(t, false, grandparent)     // In both chains
}
```

### Same-Chain Double Spend Test (Invalid Transaction)

**Plain English:**
> Test where parent spends GP:0,1 and child tries to spend parent:0 + GP:0 + GP:4.
> The child should be rejected because GP:0 is already spent by parent.

```go
func testSameChainGrandparentDoubleSpend(t *testing.T, utxoStore string) {
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		UTXOStoreType:        utxoStore,
		SettingsOverrideFunc: externalTxSettingsFunc(),
	})
	defer td.Stop(t)

	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	// Grandparent with 5 outputs
	grandparent := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(5, coinbaseTx.Outputs[0].Satoshis/5-100),
	)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, grandparent))
	td.MineAndWait(t, 1)

	// Parent spends GP:0 and GP:1
	parent := td.CreateTransactionWithOptions(t,
		transactions.WithInput(grandparent, 0),
		transactions.WithInput(grandparent, 1),
		transactions.WithP2PKHOutputs(5, grandparent.Outputs[0].Satoshis/5-100),
	)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, parent))
	td.MineAndWait(t, 1)

	// Invalid child: tries to spend parent:0 + GP:0 (already spent!) + GP:4
	invalidChild := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parent, 0),
		transactions.WithInput(grandparent, 0),  // INVALID: already spent by parent!
		transactions.WithInput(grandparent, 4),
		transactions.WithP2PKHOutputs(5, grandparent.Outputs[0].Satoshis/5-100),
	)

	// Should be rejected via propagation
	err = td.PropagationClient.ProcessTransaction(td.Ctx, invalidChild)
	require.Error(t, err, "Expected rejection - GP:0 already spent")

	// Should also be rejected as a block
	block4, _ := td.BlockchainClient.GetBlockByHeight(td.Ctx, 4)
	_, invalidBlock := td.CreateTestBlock(t, block4, 10501, invalidChild)
	err = td.BlockValidationClient.ProcessBlock(td.Ctx, invalidBlock, invalidBlock.Height, "", "legacy")
	require.Error(t, err, "Expected block rejection - contains double spend")
}
```

### Checklist for Writing Double Spend Tests

1. **Setup:**
   - [ ] Use `externalTxSettingsFunc()` for external transaction tests
   - [ ] Call `td.BlockchainClient.Run(td.Ctx, "test")`
   - [ ] Use `MineToMaturityAndGetSpendableCoinbaseTx` for spendable coins
   - [ ] Always `defer td.Stop(t)`

2. **Transaction Creation:**
   - [ ] Use correct output indices when spending
   - [ ] Calculate amounts properly (input amount / num outputs - fee)
   - [ ] Log transaction IDs and output counts for debugging

3. **Chain Building:**
   - [ ] Use `PropagationClient.ProcessTransaction` + `MineAndWait` for main chain
   - [ ] Use `CreateTestBlock` + `ProcessBlock` for forks
   - [ ] Use unique nonces for each block (increment by test ID + block number)

4. **Verification:**
   - [ ] Verify winning chain with `WaitForBlockHeight`
   - [ ] Check conflict status with `VerifyConflictingInUtxoStore`
   - [ ] Verify mined transactions with `VerifyNotInBlockAssembly`
   - [ ] Check cascade conflicts (transactions depending on conflicting parents)

5. **Documentation:**
   - [ ] Add clear ASCII art showing chain structure
   - [ ] Document which outputs each transaction spends
   - [ ] Explain the conflict (which outputs are double-spent)
   - [ ] Note cascade effects (child becomes conflicting when parent does)
