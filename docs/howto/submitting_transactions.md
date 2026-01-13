# How to Submit Transactions to Teranode

This guide explains how to submit Bitcoin transactions to a Teranode node and covers the different transaction formats supported by the system.

## Supported Transaction Formats

Teranode accepts transactions in two formats:

### 1. Standard Bitcoin Transaction Format

The traditional Bitcoin transaction format used by most Bitcoin implementations:

- **No special markers or extensions required**
- **Compatible with all existing Bitcoin libraries** (libbitcoin, bitcoin-js, go-bt, etc.)
- **Smaller transaction size** for network transmission (no duplicate previous output data)
- **Universally supported** across the Bitcoin ecosystem

**When to use:** This is the recommended format for most use cases. It's simpler to implement, widely supported, and Teranode handles the rest automatically.

**Example using go-bt:**

```go
package main

import (
    "github.com/libsv/go-bt/v2"
)

func main() {
    // Create a new transaction
    tx := bt.NewTx()

    // Add input (standard format - no previous output data)
    err := tx.From(
        "previous_txid_here",
        0,  // output index
        "locking_script_hex",
        1000000, // satoshis
    )
    if err != nil {
        panic(err)
    }

    // Add output
    err = tx.PayTo("address_or_script", 900000)
    if err != nil {
        panic(err)
    }

    // Sign the transaction
    // ... signing logic ...

    // Serialize to standard Bitcoin format
    txBytes := tx.Bytes()

    // Submit txBytes to Teranode
}
```

### 2. Extended Format (BIP-239)

Enhanced transaction format that includes additional metadata in each input:

- **Includes previous output satoshi values** in each input
- **Includes previous output locking scripts** in each input
- **Marked with special header** (`0000000000EF` after version number)
- **Slightly faster validation** (skips UTXO lookup step)

**When to use:** Use extended format when:

- You need offline transaction validation without UTXO store access
- You're building high-throughput submission tools where every millisecond counts
- You're implementing BIP-239-specific features

**Example using go-bt (conceptual):**

```go
package main

import (
    "github.com/libsv/go-bt/v2"
)

func main() {
    // Create a new transaction
    tx := bt.NewTx()

    // Add input with extended format data
    input := &bt.Input{
        PreviousTxID:       previousTxIDBytes,
        PreviousTxOutIndex: 0,
        UnlockingScript:    unlockingScript,
        SequenceNumber:     0xFFFFFFFF,
        // Extended format fields
        PreviousTxSatoshis: 1000000,
        PreviousTxScript:   lockingScript,
    }
    tx.AddInput(input)

    // Add output
    tx.AddOutput(&bt.Output{
        Satoshis:      900000,
        LockingScript: outputScript,
    })

    // Serialize to extended format
    // Note: Actual API may vary by library version
    txBytes := tx.ExtendedBytes()

    // Submit txBytes to Teranode
}
```

## Submission Methods

### HTTP API

Teranode's Propagation Service provides HTTP endpoints for transaction submission.

#### Single Transaction

```bash
curl -X POST http://localhost:8090/tx \
  -H "Content-Type: application/octet-stream" \
  --data-binary @transaction.bin
```

**Response:**

- `200 OK`: Transaction accepted
- `400 Bad Request`: Malformed transaction
- `422 Unprocessable Entity`: Transaction validation failed

#### Multiple Transactions (Batch)

```bash
curl -X POST http://localhost:8090/txs \
  -H "Content-Type: application/octet-stream" \
  --data-binary @transactions.bin
```

The batch endpoint expects transactions concatenated together. Each transaction result is returned in order.

**Go Example:**

```go
package main

import (
    "bytes"
    "context"
    "fmt"
    "io"
    "net/http"

    "github.com/libsv/go-bt/v2"
)

func submitTransaction(tx *bt.Tx, nodeURL string) error {
    txBytes := tx.Bytes()

    resp, err := http.Post(
        fmt.Sprintf("%s/tx", nodeURL),
        "application/octet-stream",
        bytes.NewReader(txBytes),
    )
    if err != nil {
        return fmt.Errorf("failed to submit transaction: %w", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        body, _ := io.ReadAll(resp.Body)
        return fmt.Errorf("transaction rejected: %s", string(body))
    }

    return nil
}
```

### gRPC API

For higher performance and lower latency, use the gRPC API.

**Go Example:**

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/libsv/go-bt/v2"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"

    propagation_api "github.com/bsv-blockchain/teranode/services/propagation/propagation_api"
)

func submitTransactionGRPC(tx *bt.Tx, nodeAddr string) error {
    // Connect to Teranode
    conn, err := grpc.Dial(nodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        return fmt.Errorf("failed to connect: %w", err)
    }
    defer conn.Close()

    // Create propagation client
    client := propagation_api.NewPropagationAPIClient(conn)

    // Submit transaction
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    response, err := client.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
        Tx: tx.Bytes(),
    })
    if err != nil {
        return fmt.Errorf("failed to process transaction: %w", err)
    }

    if response.Status != propagation_api.Status_SUCCESS {
        return fmt.Errorf("transaction rejected: %s", response.RejectReason)
    }

    return nil
}
```

## Format Recommendation

### For Most Use Cases: Use Standard Bitcoin Format

**Reasons:**

1. **Simplicity**: Easier to implement with existing Bitcoin libraries
2. **Compatibility**: Works with all Bitcoin tools and wallets
3. **Smaller size**: Reduces network bandwidth usage
4. **No changes needed**: Existing applications work without modification

### Performance Comparison

In Teranode's architecture, the performance difference between formats is **negligible**:

- **Extended format advantage**: Saves ~0.1-1ms per transaction (skips UTXO lookup)
- **Teranode's UTXO lookup**: Highly optimized with Aerospike/SQL + txmeta cache
- **Typical UTXO lookup time**: Sub-millisecond (0.1-0.5ms)

**Conclusion**: Unless you're processing millions of transactions per second, the format choice won't significantly impact performance.

## Error Handling

### Common Errors

#### 1. "transaction with no inputs" or "malformed transaction"

**Cause**: Transaction binary data is corrupted or improperly formatted.

**Solution**: Verify serialization logic and ensure transaction is properly constructed.

#### 2. "parent transaction not found"

**Cause**: The transaction references a parent UTXO that hasn't been validated yet.

**Solution**:

- Ensure parent transactions are submitted and validated first
- For transaction chains (CPFP), submit parent before child
- Wait a moment and retry

#### 3. "insufficient fee"

**Cause**: Transaction fee doesn't meet the node's minimum fee policy.

**Solution**: Increase the transaction fee to meet the node's requirements.

#### 4. "double spend detected"

**Cause**: One or more inputs have already been spent.

**Solution**: Verify UTXO status before creating transactions. The first transaction to reach the node wins.

### Error Response Example

```json
{
  "status": "REJECTED",
  "reject_reason": "transaction validation failed: parent transaction 5f3a... not found",
  "tx_id": "abc123...",
  "error_code": "MISSING_PARENT"
}
```

## Best Practices

### 1. Always Include Transaction Fees

Teranode validates transaction fees. Ensure your transaction includes sufficient fees based on size and complexity.

```go
// Calculate fee based on transaction size
txSize := len(tx.Bytes())
feeRate := 50 // satoshis per byte
minimumFee := txSize * feeRate
```

### 2. Check Parent Transactions

Before submitting a transaction, verify that all parent transactions (UTXOs being spent) are confirmed or have been submitted.

### 3. Handle Retries Gracefully

Network conditions may require transaction resubmission:

```go
func submitWithRetry(tx *bt.Tx, nodeURL string, maxRetries int) error {
    var err error
    for i := 0; i < maxRetries; i++ {
        err = submitTransaction(tx, nodeURL)
        if err == nil {
            return nil
        }

        // Check if error is retryable
        if isRetryableError(err) {
            time.Sleep(time.Duration(i+1) * time.Second)
            continue
        }

        // Non-retryable error, fail immediately
        return err
    }
    return fmt.Errorf("max retries exceeded: %w", err)
}
```

### 4. Monitor Transaction Status

After submission, track transaction status using the Asset Server API:

```bash
# Check transaction status
curl http://localhost:8090/tx/abc123.../json

# Check UTXO status
curl http://localhost:8090/utxo/output_hash/json
```

### 5. Batch Submissions for High Volume

For high-throughput applications, use batch submission:

```go
func submitBatch(txs []*bt.Tx, nodeURL string) error {
    var buffer bytes.Buffer

    for _, tx := range txs {
        buffer.Write(tx.Bytes())
    }

    resp, err := http.Post(
        fmt.Sprintf("%s/txs", nodeURL),
        "application/octet-stream",
        &buffer,
    )
    // ... handle response
}
```

## Transaction Lifecycle

1. **Submission**: Transaction sent to Propagation Service (HTTP/gRPC)
2. **Storage**: Transaction stored in blob store
3. **Format Check**: Validator checks if transaction is extended
   - If not extended: Automatically extended in-memory using UTXO store
   - If extended: Proceeds directly to validation
4. **Validation**: Full consensus and policy validation
5. **UTXO Update**: UTXOs marked as spent, new outputs created
6. **Block Assembly**: Valid transactions forwarded to mining
7. **Mining**: Transaction included in a block
8. **Confirmation**: Block validated and added to blockchain

## Testing

### Local Testing Setup

```bash
# Start Teranode in development mode
SETTINGS_CONTEXT=dev make dev-teranode

# Submit a test transaction
curl -X POST http://localhost:8090/tx \
  -H "Content-Type: application/octet-stream" \
  --data-binary @test_tx.bin
```

### Integration Testing

For automated testing with Teranode, refer to the test utilities in the codebase:

- **Transaction helpers**: `test/utils/transaction_helper.go` provides functions like `GenerateNewValidSingleInputTransaction()`
- **Test environment setup**: `test/utils/testenv.go` contains the `TeranodeTestClient` structure for integration tests
- **E2E test examples**: See `test/e2e/` directory for complete end-to-end test examples

Example test pattern from the codebase:

```go
import (
    "github.com/bsv-blockchain/teranode/test/utils"
    "github.com/bsv-blockchain/go-bt/v2"
)

func TestTransactionSubmission(t *testing.T) {
    // Setup test client (see test/utils/testenv.go)
    node := utils.NewTeranodeTestClient()

    // Generate a valid transaction using test helpers
    tx, err := utils.GenerateNewValidSingleInputTransaction(node)
    require.NoError(t, err)

    // Submit via distributor client
    _, err = node.DistributorClient.SendTransaction(context.Background(), tx)
    require.NoError(t, err)
}
```

For more detailed testing patterns, examine the existing tests in `test/e2e/`, `test/longtest/`, and `test/sequentialtest/` directories.

## Additional Resources

- [Transaction Data Model](../topics/datamodel/transaction_data_model.md): Detailed explanation of both transaction formats
- [Propagation Service](../topics/services/propagation.md): How transactions are received and distributed
- [Validator Service](../topics/services/validator.md): Transaction validation process including automatic format extension
- [BIP-239 Reference](../misc/BIP-239.md): Extended Transaction Format specification

## Summary

- **Teranode accepts both standard and extended transaction formats**
- **Standard format is recommended for most use cases**
- **Automatic extension happens transparently during validation**
- **HTTP and gRPC APIs are available for submission**
- **Performance difference between formats is negligible**
- **Proper error handling and retries are essential**

For questions or issues, refer to the Teranode documentation or contact the development team.
