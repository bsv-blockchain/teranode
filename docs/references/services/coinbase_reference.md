# Coinbase Reference Documentation

## Overview

In the current Teranode implementation, coinbase functionality is integrated within the BlockAssembly service rather than existing as a standalone service. Coinbase transactions, which are special transactions that miners use to collect block rewards, are created during the block assembly and mining process.

## Types

### MiningCandidate

The `MiningCandidate` structure (defined in `/model/model.pb.go`) includes coinbase-related fields:

```go
type MiningCandidate struct {
    Id                  []byte
    PreviousHash        []byte
    CoinbaseValue       uint64
    Version             uint32
    NBits               []byte
    Time                uint32
    Height              uint32
    MerkleProof         [][]byte
    SubtreeCount        uint32
    NumTxs              uint32
    SizeWithoutCoinbase uint64
    SubtreeHashes       [][]byte
    // Other fields omitted for brevity
}
```

The `CoinbaseValue` field represents the total amount available for the coinbase transaction in satoshis, including block rewards and transaction fees.

## Functions

### MiningCandidate Methods

#### CreateCoinbaseTxCandidate

```go
func (mc *MiningCandidate) CreateCoinbaseTxCandidate(tSettings *settings.Settings, p2pk ...bool) (*bt.Tx, error)
```

Creates a coinbase transaction for the mining candidate. The optional `p2pk` parameter specifies if the coinbase output should use pay-to-public-key (P2PK) instead of pay-to-public-key-hash (P2PKH).

#### CreateCoinbaseTxCandidateForAddress

```go
func (mc *MiningCandidate) CreateCoinbaseTxCandidateForAddress(tSettings *settings.Settings, address *string) (*bt.Tx, error)
```

Creates a coinbase transaction that pays to a specific address.

### Helper Functions

#### CreateCoinbase

```go
func CreateCoinbase(height uint32, coinbaseValue uint64, arbitraryText string, addresses []string) (*bt.Tx, error)
```

Creates a coinbase transaction with the given parameters:
- `height`: Block height
- `coinbaseValue`: Total amount in satoshis
- `arbitraryText`: Optional text to include in the coinbase
- `addresses`: Recipient addresses for the coinbase reward

## Key Processes

### Coinbase Transaction Creation

1. During mining or block assembly, a coinbase transaction is created using `CreateCoinbaseTxCandidate` or `CreateCoinbaseTxCandidateForAddress`.
2. The transaction includes the block reward and collected transaction fees.
3. The coinbase transaction becomes the first transaction in a new block.

### Mining Integration

Coinbase transaction handling is integrated with mining operations in the BlockAssembly service:

1. When `GetMiningCandidate` is called, a block template is created with the coinbase value calculated from block subsidy and transaction fees.
2. When mining, the coinbase transaction is created using the candidate's methods.
3. When a mining solution is submitted, the coinbase transaction is included in the block.

## Configuration

Coinbase-related settings are configured in the Teranode settings:

- `Settings.Coinbase.ArbitraryText`: Optional text to include in the coinbase transaction
- `Settings.BlockAssembly.MinerWalletPrivateKeys`: Private keys used for coinbase outputs
- `coinbase_wallet_private_key`: Private key for the coinbase wallet
- `distributor_backoff_duration`: Backoff duration for the transaction distributor
- `distributor_max_retries`: Maximum number of retries for transaction distribution
- `distributor_failure_tolerance`: Failure tolerance for transaction distribution
- `blockchain_store_dbTimeoutMillis`: Timeout for database operations
- `coinbase_wait_for_peers`: Whether to wait for peers before processing
- `blockvalidation_maxPreviousBlockHeadersToCheck`: Number of previous block headers to check for confirmations

## Dependencies

The Coinbase Service depends on several other components and services:

- Blockchain Client
- Blockchain Store
- Transaction Distributor
- Peer Sync Service
- SQL Database (PostgreSQL or SQLite)

These dependencies are injected into the `Server` and `Coinbase` structures during initialization.

## Database Schema

The service uses two main tables:

1. `coinbase_utxos`: Stores coinbase UTXOs
2. `spendable_utxos`: Stores UTXOs that are available for spending

Additional tables and functions are created for PostgreSQL to optimize balance tracking and UTXO management.

## Error Handling

The service implements robust error handling, particularly in database operations and transaction processing. Errors are wrapped with custom error types to provide context and aid in debugging.

## Concurrency

- The service uses goroutines and error groups to process blocks and UTXOs concurrently.
- A lock-free queue is used for processing new blocks and catching up with the blockchain.

## Metrics

The service initializes Prometheus metrics for monitoring various aspects of its operation, including:

- Health check responses
- Fund requests
- Transaction distribution
- Balance retrieval

These metrics can be used to monitor the performance and health of the Coinbase Service.

## Security

- The service uses a private key to sign transactions.
- Only P2PKH (Pay to Public Key Hash) coinbase outputs are supported.
- The service implements a mechanism to wait for peer synchronization before processing blocks, enhancing security in a distributed environment.
