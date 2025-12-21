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
