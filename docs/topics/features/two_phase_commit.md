# Two-Phase Transaction Commit Process

## Index

1. [Overview](#1-overview)
2. [Purpose and Benefits](#2-purpose-and-benefits)
3. [Implementation Details](#3-implementation-details)
    - [3.1. Phase 1: Initial Transaction Creation with Locked Flag](#31-phase-1-initial-transaction-creation-with-locked-flag)
    - [3.2. Phase 2: Unsetting the Locked Flag](#32-phase-2-unsetting-the-locked-flag)
    - [3.3. Special Case: Transactions from Blocks](#33-special-case-transactions-from-blocks)
4. [Service Interaction](#4-service-interaction)
    - [4.1. Validator Service Role](#41-validator-service-role)
    - [4.2. Block Validation Service Role](#42-block-validation-service-role)
5. [Data Model Impact](#5-data-model-impact)
6. [Configuration Options](#6-configuration-options)
7. [Flow Diagrams](#7-flow-diagrams)
8. [Related Documentation](#8-related-documentation)

## 1. Overview

The Two-Phase Transaction Commit process is a mechanism implemented in Teranode to ensure atomicity and data consistency during transaction processing across the system's microservice architecture. This process uses an "locked" flag to temporarily lock transaction outputs until they are safely included in block assembly, and then makes them available for spending after confirmation.

When a Bitcoin transaction is processed, it both spends existing outputs (inputs) and creates new outputs that can be spent in the future. In Teranode's distributed architecture, transaction processing spans multiple services - particularly the UTXO store (where transaction data is persisted) and Block Assembly (where transactions are prepared for inclusion in blocks). The two-phase commit process ensures atomicity across these services, preventing scenarios where a transaction might exist in one component but not another.

This approach ensures transaction consistency across the entire system, preventing situations where a transaction might be included in a block but not properly recorded in the UTXO store, or vice versa. It maintains system integrity in a distributed environment by coordinating state changes across microservices.

## 2. Purpose and Benefits

The Two-Phase Transaction Commit process addresses several critical concerns that arise in a distributed transaction processing system:

- **Atomicity in a Microservice Architecture**: The process ensures that a transaction is either fully processed across all components or effectively doesn't exist at all. This prevents serious inconsistencies between the UTXO store and Block Assembly.

- **Prevention of Data Corruption Scenarios**:

   - **Scenario 1 - Block Assembly Without UTXO Storage**: Without two-phase commit, if a transaction were added to Block Assembly but failed to be stored in the UTXO store, when that block is mined, future transactions trying to spend its outputs would be rejected because the outputs don't exist in the UTXO store. This could cause a chain fork.
   - **Scenario 2 - UTXO Storage Without Block Assembly**: If a transaction is stored in the UTXO store but not added to Block Assembly, it creates "ghost money" that exists in the database but isn't in the blockchain.

- **Safe Failure Mode**: In case of system failures, the worst case is that money temporarily can't be spent (rather than creating invalid money or losing funds). This preserves the integrity of the monetary system.

- **System Consistency**: Ensures that the UTXO database state and block assembly state are synchronized, maintaining a consistent view of transactions across the system.

- **Transaction Dependency Management**: Ensures proper handling of transaction dependencies in a distributed processing environment.

- **Improved Validation**: Provides a clear state transition mechanism that helps track the status of transactions throughout the system.

### 2.1. Critical Assumptions

The Two-Phase Transaction Commit process is built on a critical assumption that must be understood to grasp how the system prevents double-spending and maintains consistency:

> **Critical Assumption**: Parent transactions are either mined, in block template, or in previous block. No other option is possible.

This means that for any transaction being processed,  **all input transactions (parent transactions) must be in one of these states**:

    - Already mined in a confirmed block (transactions in blocks with multiple confirmations)
   - Currently in a block template (pending mining, not yet in a block)
   - In the immediately previous block (transactions in the most recently mined block)

   > **Note**: The distinction between "already mined in a confirmed block" and "in the immediately previous block" is important. The former refers to transactions with multiple confirmations that are considered stable, while the latter refers specifically to transactions in the most recently added block that have only one confirmation and may still be subject to reorganization.

**Implications**:

    - This assumption ensures that all parent transactions are either confirmed or in the process of being confirmed
   - It prevents the system from processing transactions that refer to parent transactions that are still in an intermediate state
   - It creates a clean dependency chain where transactions build upon others that are already securely in the system

**Security Benefits**:

    - Prevents transaction graph inconsistencies
   - Eliminates scenarios where transaction outputs could be spent before their parent transactions are fully committed
   - Supports the effectiveness of the locked flag mechanism

This assumption is foundational to the system's security model and crucial for the proper functioning of the two-phase commit process.

## 3. Implementation Details

### 3.1. Phase 1: Initial Transaction Creation with Locked Flag

When a transaction is validated and accepted by the system:

1. The Validator service processes and validates the transaction according to Bitcoin SV consensus rules.

2. Upon successful validation, new UTXOs generated by the transaction are created in the UTXO store with the "locked" flag set to `true`.

3. This flag prevents these UTXOs from being spent in subsequent transactions while they're still being processed through the system.

4. The transaction is marked as valid and forwarded to Block Assembly for inclusion in a block.

### 3.2. Phase 2: Unsetting the Locked Flag

The locked flag is unset in two key scenarios:

#### 3.2.1. Scenario 1: After Successful Addition to Block Assembly

1. When a transaction is successfully validated and added to block assembly:

2. The Validator service detects the successful addition and immediately unsets the "locked" flag (changes it to `false`).

3. This makes the transaction outputs available for spending in subsequent transactions, even before the transaction is mined in a block.

#### 3.2.2. Scenario 2: When Mined in a Block

As a fallback mechanism, the locked flag is also unset when a transaction is mined in a block:

1. When a block containing the transaction is validated, the Block Validation service processes the block and identifies all transactions within it.

2. As part of the `SetMinedMulti` operation during block processing, the Block Validation service updates the transaction's metadata in the UTXO store, which includes unsetting the "locked" flag (changing it to `false`) if it hasn't been unset already.

3. The transaction is now fully committed and integrated into the blockchain.

### 3.3. Special Case: Transactions from Blocks

For transactions that are received as part of a block (rather than through the transaction validation process):

1. The Validator can be configured to ignore the locked flag when validating transactions that are part of a received block.

2. This is controlled via the `WithIgnoreLocked` option in the Validator service.

3. This approach allows the system to accept transactions that are already part of a valid block, even if they would spend outputs that are marked as locked in the local UTXO store.

4. This mechanism is essential for handling block synchronization and reorgs properly.

## 4. Service Interaction

### 4.1. Validator Service Role

The Validator service is responsible for:

- Validating incoming transactions against consensus rules
- Setting the locked flag on newly created UTXOs (Phase 1)
- Forwarding valid transactions to Block Assembly (while keeping them marked as locked)
- Handling the special case of ignoring the locked flag for transactions in blocks

### 4.2. Block Validation Service Role

The Block Validation service is responsible for:

- Validating blocks and their transactions
- Updating transaction metadata when marking transactions as mined
- Unsetting the locked flag as part of the SetMinedMulti operation when a transaction is mined in a block (Phase 2)
- Ensuring that the second phase of the commit process is completed

## 5. Data Model Impact

The Two-Phase Transaction Commit process impacts the UTXO data model by using the "locked" flag field in the UTXO records. This flag is stored in the UTXO metadata and is used to track the state of transaction outputs throughout the system.

**UTXO Table:**
```
| Field Name         | Type    | Description                                             |
|--------------------|---------|---------------------------------------------------------|
| locked             | boolean | Indicates if the UTXO is temporarily locked             |
```

**UTXO MetaData Table:**
```
| Field Name         | Type    | Description                                             |
|--------------------|---------|---------------------------------------------------------|
| locked             | boolean | Flag to prevent spending during the two-phase commit    |
```

## 6. Configuration Options

The behavior of the Two-Phase Transaction Commit process can be configured through the following options:

**Validator Service:**
```
WithIgnoreLocked(bool) - When set to true, the validator will ignore the locked flag when processing transactions that are part of a block
```

In the Validator Options struct:
```go
type Options struct {
    IgnoreLocked bool
}
```

## 7. Flow Diagrams

### Transaction Validation (Phase 1)

![tx_validation_post_process.svg](../services/img/plantuml/validator/tx_validation_post_process.svg)

### Transaction Mining (Phase 2)

![block_validation_set_tx_mined.svg](../services/img/plantuml/blockvalidation/block_validation_set_tx_mined.svg)

## 8. Related Documentation

- [Validator Service Documentation](../services/validator.md#271-two-phase-transaction-commit-process)
- [Block Validation Service Documentation](../services/blockValidation.md#23-marking-txs-as-mined)
- [UTXO Data Model Documentation](../datamodel/utxo_data_model.md)
