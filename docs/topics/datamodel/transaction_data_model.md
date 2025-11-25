# Teranode Data Model - Transaction Format Support

Teranode supports both standard Bitcoin transaction format and Extended Transaction Format (as defined in BIP-239). The Extended Format includes additional metadata to facilitate processing and validation.

## Transaction Format Flexibility

**Teranode accepts transactions in either format:**

1. **Standard Bitcoin Format**: Traditional Bitcoin transactions without extended input data
2. **Extended Format (BIP-239)**: Transactions with additional input metadata (previous output satoshis and locking scripts)

When a transaction arrives in standard format, Teranode automatically extends it during validation by looking up the required input data from the UTXO store. This on-demand extension approach provides:

- **Backward compatibility** with existing Bitcoin wallets and applications
- **Storage efficiency** by storing transactions in compact non-extended format
- **Validation flexibility** by accepting both formats seamlessly

**For wallet developers:** You may send either format to Teranode. Extended format may provide slightly faster initial validation (skips UTXO lookup), but both are fully supported.

## Transaction Format Specification

_Bitcoin Transaction format:_

| Field           | Description                                          | Size                                             |
|-----------------|------------------------------------------------------|--------------------------------------------------|
| Version no      | currently 2                                          | 4 bytes                                          |
| In-counter      | positive integer VI = [[VarInt]]                     | 1 - 9 bytes                                      |
| list of inputs  | Transaction Input  Structure                         | <in-counter> qty with variable length per input  |
| Out-counter     | positive integer VI = [[VarInt]]                     | 1 - 9 bytes                                      |
| list of outputs | Transaction Output Structure                         | <out-counter> qty with variable length per output |
| nLocktime       | if non-zero and sequence numbers are < 0xFFFFFFFF: block height or timestamp when transaction is final | 4 bytes                                          |

The Extended Format adds a marker to the transaction format:

| Field           | Description                                                                                            | Size                                              |
|-----------------|--------------------------------------------------------------------------------------------------------|---------------------------------------------------|
| Version no      | currently 2                                                                                            | 4 bytes                                           |
| **EF marker**   | **marker for extended format**                                                                         | **0000000000EF**                                  |
| In-counter      | positive integer VI = [[VarInt]]                                                                       | 1 - 9 bytes                                       |
| list of inputs  | **Extended Format** transaction Input Structure                                                        | <in-counter> qty with variable length per input   |
| Out-counter     | positive integer VI = [[VarInt]]                                                                       | 1 - 9 bytes                                       |
| list of outputs | Transaction Output Structure                                                                           | <out-counter> qty with variable length per output |
| nLocktime       | if non-zero and sequence numbers are < 0xFFFFFFFF: block height or timestamp when transaction is final | 4 bytes                                           |

The Extended Format marker allows a library that supports the format to recognize that it is dealing with a transaction in extended format, while a library that does not support extended format will read the transaction as having 0 inputs, 0 outputs and a future nLock time. This has been done to minimize the possible problems a legacy library will have when reading the extended format. It can in no way be recognized as a valid transaction.

The input structure is the only additional thing that is changed in the Extended Format. The original input structure looks like this:

| Field                     | Description                                                                                 | Size                          |
|---------------------------|---------------------------------------------------------------------------------------------|-------------------------------|
| Previous Transaction hash | TXID of the transaction the output was created in                                           | 32 bytes                      |
| Previous Txout-index      | Index of the output (Non negative integer)                                                  | 4 bytes                       |
| Txin-script length        | Non negative integer VI = VarInt                                                            | 1 - 9 bytes                   |
| Txin-script / scriptSig   | Script                                                                                      | <in-script length>-many bytes |
| Sequence_no               | Used to iterate inputs inside a payment channel. Input is final when nSequence = 0xFFFFFFFF | 4 bytes                       |

In the Extended Format, we extend the input structure to include the previous locking script and satoshi outputs:

| Field                          | Description                                                                                 | Size                            |
|--------------------------------|---------------------------------------------------------------------------------------------|---------------------------------|
| Previous Transaction hash      | TXID of the transaction the output was created in                                           | 32 bytes                        |
| Previous Txout-index           | Index of the output (Non negative integer)                                                  | 4 bytes                         |
| Txin-script length             | Non negative integer VI = VarInt                                                            | 1 - 9 bytes                     |
| Txin-script / scriptSig        | Script                                                                                      | <in-script length>-many bytes   |
| Sequence_no                    | Used to iterate inputs inside a payment channel. Input is final when nSequence = 0xFFFFFFFF | 4 bytes                         |
| **Previous TX satoshi output** | **Output value in satoshis of previous input**                                              | **8 bytes**                     |
| **Previous TX script length**  | **Non negative integer VI = VarInt**                                                        | **1 - 9 bytes**                 |
| **Previous TX locking script** | **Script**                                                                                  | **script length - many bytes**  |

The Extended Format is not backwards compatible, but has been designed in such a way that existing software should not read a transaction in Extend Format as a valid (partial) transaction. The Extended Format header (0000000000EF) will be read as an empty transaction with a future nLock time in a library that does not support the Extended Format.

## Comparison with the historical Bitcoin transactions

Bitcoin Transactions are broadcast and included in blocks as they are found.

_Current Transaction format:_

| Field           | Description                                                                                            | Size                                              |
|-----------------|--------------------------------------------------------------------------------------------------------|---------------------------------------------------|
| Version no      | currently 2                                                                                            | 4 bytes                                           |
| In-counter      | positive integer VI = [[VarInt]]                                                                       | 1 - 9 bytes                                       |
| list of inputs  | Transaction Input  Structure                                                                           | <in-counter> qty with variable length per input   |
| Out-counter     | positive integer VI = [[VarInt]]                                                                       | 1 - 9 bytes                                       |
| list of outputs | Transaction Output Structure                                                                           | <out-counter> qty with variable length per output |
| nLocktime       | if non-zero and sequence numbers are < 0xFFFFFFFF: block height or timestamp when transaction is final | 4 bytes                                           |

As opposed to that, the Extended Format includes additional metadata, and is bundled within subtrees containers for efficient processing and propagation.

## How Teranode Handles Transaction Formats

### Ingress (Receiving Transactions)

The Propagation Service accepts transactions in any valid Bitcoin format:

- Standard Bitcoin transaction format
- Extended Format (BIP-239) with the `0000000000EF` marker

No validation or rejection occurs based on format at ingress. Transactions are stored in their received form to the blob store, and format handling is delegated to the Validator Service.

### Validation Process

During validation, the Validator Service automatically handles format conversion when necessary:

1. **Check if Extended**: The validator checks `tx.IsExtended()` status on the transaction
2. **Automatic Extension**: If not extended, the validator:

    - Queries the UTXO store for each input's parent transaction
    - Extracts the referenced output's satoshi value and locking script
    - Decorates the transaction inputs with this data in-memory
    - Marks the transaction as extended for the validation process

3. **Validation**: Proceeds with full validation including script verification using the extended data

This extension process happens transparently at multiple checkpoints throughout the validation pipeline:

- `Validator.Validate()` in `services/validator/Validator.go` - Main validation entry point
- `Validator.validateTransaction()` in `services/validator/Validator.go` - Before transaction format validation
- `Validator.validateTransactionScripts()` in `services/validator/Validator.go` - Before script validation
- `BlockValidation.quickValidateBlock()` in `services/blockvalidation/quick_validate.go` - During block validation for historical blocks
- `BlockValidation.ExtendTransaction()` in `services/blockvalidation/quick_validate.go` - During block processing when transactions are not extended

**Key implementation details:**

- Parent transactions are looked up in **parallel batches** using Go's errgroup for optimal performance
- UTXO store queries are batched based on configuration (`UtxoStore.GetBatcherSize`)
- Already-decorated inputs are skipped (idempotent operation)
- Extension is performed entirely in-memory with no disk writes

### Storage Format

**Transactions are stored in their received format**, preserving the format in which they arrived. This design decision provides:

- **Space efficiency**: Eliminates duplicate data since previous outputs are already stored in the UTXO set
- **Compatibility**: Standard format is universally readable by all Bitcoin tools
- **Performance**: Smaller transaction sizes improve I/O performance across the system

The storage layer uses the go-bt library's `SerializeBytes()` method, which preserves the received format (extended or standard). When standard format transactions need to be extended (during validation or when serving API requests), the extension happens on-demand in memory.

### Performance Considerations

**Extended Format Advantages:**

- Slightly faster initial validation (skips UTXO lookup step for input decoration)
- Useful for offline validation scenarios where UTXO store access is not available
- Beneficial for high-throughput transaction submission where every millisecond counts

**Standard Format Advantages:**

- Smaller network transmission size (no duplicate previous output data)
- Compatible with all existing Bitcoin tools and libraries
- No change required for existing wallet implementations
- Widely supported across the Bitcoin ecosystem

**Recommendation:** Use whichever format is most convenient for your application. The performance difference is negligible in Teranode's architecture due to highly optimized UTXO lookups. Teranode's Aerospike and SQL-based UTXO stores, combined with the txmeta cache, make parent transaction lookups extremely fast (typically sub-millisecond).

## Teranode's BIP-239 Implementation

Teranode implements BIP-239 (Extended Transaction Format) with several enhancements specific to its high-performance, distributed architecture:

### Flexible Format Acceptance

Unlike a strict BIP-239 implementation that might require extended format, Teranode:

- **Accepts both standard and extended formats** at all ingress points (HTTP, gRPC, UDP)
- **Does not reject** transactions based on format at the network edge
- **Handles format conversion transparently** during the validation pipeline
- **Maintains compatibility** with standard Bitcoin transaction processing

### Automatic Extension Mechanism

When Teranode receives a standard format transaction, it automatically extends it during validation:

1. The validator detects non-extended transactions using the `IsExtended()` check
2. For each input, the system queries the UTXO store to retrieve parent transaction data
3. Input decoration happens in-memory using the parent output's satoshi value and locking script
4. The extended transaction is used for validation but **never written back to storage**

This automatic extension is highly optimized:

- Parallel batch queries minimize latency
- In-memory operation with no disk I/O overhead
- Idempotent design allows safe retries
- Negligible performance impact compared to native extended format

### Storage Optimization Strategy

Teranode's storage layer differs from a naive BIP-239 implementation:

- **Transactions stored in received format** regardless of ingress format
- Format flexibility allows clients to choose optimal format for their use case
- Extended format transactions avoid UTXO lookup during validation
- Standard format transactions save storage space
- Extension performed on-demand when needed for validation (if transaction arrives in standard format)

### Error Handling

If parent transactions cannot be found during extension:

- Returns `ErrTxMissingParent` error to the client
- Transaction validation fails gracefully
- Provides clear error message indicating which parent is missing
- Client can retry after ensuring parent transactions are confirmed

Common scenarios requiring parent transactions:

- Child-pays-for-parent (CPFP) transaction patterns
- Transaction chains where parent hasn't been validated yet
- Block validation where transactions reference outputs from the same block

### Comparison with BIP-239 Specification

| Aspect | BIP-239 Spec | Teranode Implementation |
|--------|--------------|------------------------|
| **Format requirement** | Extended format recommended | Both formats accepted |
| **Storage** | Not specified | Received format preserved |
| **Extension** | Manual by client | Automatic by validator |
| **Performance** | Faster validation | Negligible difference |
| **Compatibility** | Limited to BIP-239 aware clients | Full Bitcoin ecosystem compatibility |

### Implementation References

For developers interested in the implementation details:

- Extension logic: `Validator.extendTransaction()` method in `services/validator/Validator.go`
- UTXO decoration: `PreviousOutputsDecorate()` method in `stores/utxo/Interface.go` and implementations
- Storage serialization: Uses go-bt library's `SerializeBytes()` method (preserves received format)

## Additional Resources

To know more about the Extended Transaction Format, please refer to the [Bitcoin Improvement Proposal 239 (09 November 2022)](https://github.com/bitcoin-sv/arc/blob/b6296d1f775e7f3568f915e13d8f03bfe8fd3c32/doc/BIP-239.md).

Other:

- [Overall System Design](../architecture/teranode-overall-system-design.md)
- [Block](./block_data_model.md)
- [Subtree](./subtree_data_model.md)
