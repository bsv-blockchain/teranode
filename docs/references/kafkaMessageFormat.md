# Kafka Message Format Reference Documentation

This document provides comprehensive information about the message formats used in Kafka topics within the Teranode ecosystem. All message formats are defined using Protocol Buffers (protobuf), providing a structured and efficient serialization mechanism.

## Index

- [Protobuf Overview](#protobuf-overview)
- [Block Notification Message Format](#block-notification-message-format)
    - [Block Topic](#block-topic)
    - [Message Structure](#message-structure)
    - [Field Specifications](#field-specifications)
        - [hash](#hash)
        - [URL](#url)
    - [Example](#example)
    - [Code Examples](#code-examples)
        - [Sending Messages](#sending-messages)
        - [Receiving Messages](#receiving-messages)
- [Subtree Notification Message Format](#subtree-notification-message-format)
    - [Subtree Topic](#subtree-topic)
    - [Message Structure](#message-structure)
    - [Field Specifications](#field-specifications)
        - [hash](#hash)
        - [URL](#url)
    - [Example](#example)
    - [Code Examples](#code-examples)
        - [Sending Messages](#sending-messages)
        - [Receiving Messages](#receiving-messages)
    - [Error Cases](#error-cases)
- [Transaction Validation Message Format](#transaction-validation-message-format)
    - [Transaction Validation Topic](#transaction-validation-topic)
    - [Message Structure](#message-structure)
    - [Field Specifications](#field-specifications)
        - [tx](#tx)
        - [height](#height)
        - [options](#options)
    - [Example](#example)
    - [Code Examples](#code-examples)
        - [Sending Messages](#sending-messages)
        - [Receiving Messages](#receiving-messages)
    - [Error Cases](#error-cases)
- [Transaction Metadata Message Format](#transaction-metadata-message-format)
    - [TxMeta Topic](#txmeta-topic)
    - [Message Structure](#message-structure)
    - [Field Specifications](#field-specifications)
        - [txHash](#txhash)
        - [action](#action)
        - [content](#content)
    - [Transaction Metadata](#transaction-metadata)
    - [Example](#example)
    - [Code Examples](#code-examples)
        - [Sending Messages](#sending-messages)
        - [Receiving Messages](#receiving-messages)
    - [Error Cases](#error-cases)
- [Rejected Transaction Message Format](#rejected-transaction-message-format)
    - [Rejected Transaction Topic](#rejected-transaction-topic)
    - [Message Structure](#message-structure)
    - [Field Specifications](#field-specifications)
        - [txHash](#txhash)
        - [reason](#reason)
    - [Example](#example)
    - [Code Examples](#code-examples)
        - [Sending Messages](#sending-messages)
        - [Receiving Messages](#receiving-messages)
    - [Error Cases](#error-cases)
- [Inventory Message Format](#inventory-message-format)
    - [Inventory Topic](#inventory-topic)
    - [Message Structure](#message-structure)
    - [Field Specifications](#field-specifications)
        - [peerAddress](#peeraddress)
        - [inv](#inv)
    - [Example](#example)
    - [Code Examples](#code-examples)
        - [Sending Messages](#sending-messages)
        - [Receiving Messages](#receiving-messages)
    - [Error Cases](#error-cases)
- [Final Block Message Format](#final-block-message-format)
    - [Final Block Topic](#final-block-topic)
    - [Message Structure](#message-structure)
    - [Field Specifications](#field-specifications)
        - [header](#header)
        - [transaction_count](#transaction_count)
        - [size_in_bytes](#size_in_bytes)
        - [subtree_hashes](#subtree_hashes)
        - [coinbase_tx](#coinbase_tx)
        - [height](#height)
    - [Example](#example)
    - [Code Examples](#code-examples)
        - [Sending Messages](#sending-messages)
        - [Receiving Messages](#receiving-messages)
    - [Error Cases](#error-cases)
- [General Code Examples](#general-code-examples)
    - [Serializing Messages](#serializing-messages)
    - [Deserializing Messages](#deserializing-messages)
- [Other Resources](#other-resources)

## Protobuf Overview

Protocol Buffers (protobuf) is a language-neutral, platform-neutral, extensible mechanism for serializing structured data. In Teranode, all Kafka messages are defined and serialized using protobuf.

The protobuf definitions for Kafka messages are located in `util/kafka/kafka_message/kafka_messages.proto`.

## Block Notification Message Format

### Block Topic

`kafka_blocksConfig` is the Kafka topic used for broadcasting block notifications. This topic notifies subscribers about new blocks as they are added to the blockchain.

### Message Structure

The block notification message is defined in protobuf as `KafkaBlockTopicMessage`:

```protobuf
message KafkaBlockTopicMessage {
  string hash = 1;  // Block hash (as hex string)
  string URL = 2;  // URL pointing to block data
}
```

### Field Specifications

#### hash

- Type: string
- Description: Hexadecimal string representation of the BSV block hash
- Required: Yes

#### URL

- Type: string
- Description: URL pointing to the location where the full block data can be retrieved
- Required: Yes

### Example

Here's a JSON representation of the message content (for illustration purposes only; actual messages are protobuf-encoded):

```json
{
  "hash": "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
  "URL": "https://datahub.example.com/blocks/123"
}
```

### Code Examples

#### Sending Messages

```go
// Create the block notification message
blockHash := block.Hash() // assumes this returns *chainhash.Hash
dataHubUrl := "https://datahub.example.com/blocks/123"

// Create a new protobuf message
message := &kafkamessage.KafkaBlockTopicMessage{
    Hash: blockHash.String(), // convert the hash to a string
    URL:  dataHubUrl,
}

// Serialize to protobuf format
data, err := proto.Marshal(message)
if err != nil {
    return fmt.Errorf("failed to serialize block message: %w", err)
}

// Send to Kafka
producer.Publish(&kafka.Message{
    Value: data,
})
```

#### Receiving Messages

```go
// Handle incoming block notification message
func handleBlockMessage(msg *kafka.Message) error {
    if msg == nil {
        return nil
    }

    // Deserialize from protobuf format
    blockMessage := &kafkamessage.KafkaBlockTopicMessage{}
    if err := proto.Unmarshal(msg.Value, blockMessage); err != nil {
        return fmt.Errorf("failed to deserialize block message: %w", err)
    }

    // Convert string hash to chainhash.Hash
    blockHash, err := chainhash.NewHashFromStr(blockMessage.Hash)
    if err != nil {
        return fmt.Errorf("invalid block hash: %w", err)
    }

    // Extract DataHub URL
    dataHubUrl := blockMessage.URL

    // Process the block notification...
    log.Printf("Received block notification for %s, data at: %s", blockHash.String(), dataHubUrl)
    return nil
}
```

### Error Cases

- Invalid message format: Message cannot be unmarshaled to KafkaBlockTopicMessage
- Empty or malformed hash: Hash is not a valid hexadecimal string representation of a block hash
- Invalid URL: DataHub URL is empty or not properly formatted

---

## Subtree Notification Message Format

### Subtree Topic

`kafka_subtreesConfig` is the Kafka topic used for broadcasting subtree notifications. This topic notifies subscribers about new subtrees as they are created.

### Message Structure

The subtree notification message is defined in protobuf as `KafkaSubtreeTopicMessage`:

```protobuf
message KafkaSubtreeTopicMessage {
  string hash = 1;  // Subtree hash (as hex string)
  string URL = 2;  // URL pointing to subtree data
}
```

### Field Specifications

#### hash

- Type: string
- Description: Hexadecimal string representation of the BSV subtree hash
- Required: Yes

#### URL

- Type: string
- Description: URL pointing to the location where the full subtree data can be retrieved
- Required: Yes

### Example

Here's a JSON representation of the message content (for illustration purposes only; actual messages are protobuf-encoded):

```json
{
  "hash": "45a2b856743012ce25a4dabddd5f5bdf534c27c9347b34862bca5a14176d07",
  "URL": "https://datahub.example.com/subtrees/123"
}
```

### Code Examples

#### Sending Messages

```go
// Create the subtree notification message
subtreeHash := subtree.Hash() // assumes this returns *chainhash.Hash
dataHubUrl := "https://datahub.example.com/subtrees/123"

// Create a new protobuf message
message := &kafkamessage.KafkaSubtreeTopicMessage{
    Hash: subtreeHash.String(), // convert the hash to a string
    URL:  dataHubUrl,
}

// Serialize to protobuf format
data, err := proto.Marshal(message)
if err != nil {
    return fmt.Errorf("failed to serialize subtree message: %w", err)
}

// Send to Kafka
producer.Publish(&kafka.Message{
    Value: data,
})
```

#### Receiving Messages

```go
// Handle incoming subtree notification message
func handleSubtreeMessage(msg *kafka.Message) error {
    if msg == nil {
        return nil
    }

    // Deserialize from protobuf format
    subtreeMessage := &kafkamessage.KafkaSubtreeTopicMessage{}
    if err := proto.Unmarshal(msg.Value, subtreeMessage); err != nil {
        return fmt.Errorf("failed to deserialize subtree message: %w", err)
    }

    // Convert string hash to chainhash.Hash
    subtreeHash, err := chainhash.NewHashFromStr(subtreeMessage.Hash)
    if err != nil {
        return fmt.Errorf("invalid subtree hash: %w", err)
    }

    // Extract DataHub URL
    dataHubUrl := subtreeMessage.URL

    // Process the subtree notification...
    log.Printf("Received subtree notification for %s, data at: %s", subtreeHash.String(), dataHubUrl)
    return nil
}
```

### Error Cases

- Invalid message format: Message cannot be unmarshaled to KafkaSubtreeTopicMessage
- Empty or malformed hash: Hash is not a valid hexadecimal string representation of a subtree hash
- Invalid URL: DataHub URL is empty or not properly formatted

---

## Transaction Validation Message Format

### Transaction Validation Topic

`kafka_validatortxsConfig` is the Kafka topic used for sending transactions from the Propagation service to the Validator for validation.

### Message Structure

The transaction validation message is defined in protobuf as `KafkaTxValidationTopicMessage`:

```protobuf
message KafkaTxValidationTopicMessage {
  bytes tx = 1;                     // Complete BSV transaction
  uint32 height = 2;                // Current blockchain height
  KafkaTxValidationOptions options = 3;  // Optional validation options
}

message KafkaTxValidationOptions {
  bool skipUtxoCreation = 1;        // Skip UTXO creation if true
  bool addTXToBlockAssembly = 2;    // Add transaction to block assembly if true
  bool skipPolicyChecks = 3;        // Skip policy checks if true
  bool createConflicting = 4;       // Allow conflicting transactions if true
}
```

### Field Specifications

#### tx

- Type: bytes
- Description: Raw bytes of the complete BSV transaction
- Required: Yes

#### height

- Type: uint32
- Description: Current blockchain height, used for validation rules that depend on height
- Required: Yes

#### options

- Type: KafkaTxValidationOptions
- Description: Special options that modify the validation behavior
- Required: No (if not provided, default values are used)

##### KafkaTxValidationOptions

###### skipUtxoCreation

- Type: bool
- Description: When true, the validator will not create UTXO entries for this transaction
- Default: false

###### addTXToBlockAssembly

- Type: bool
- Description: When true, the validated transaction will be added to block assembly
- Default: true

###### skipPolicyChecks

- Type: bool
- Description: When true, certain policy validation checks will be skipped
- Default: false

###### createConflicting

- Type: bool
- Description: When true, the validator may create a transaction that conflicts with existing UTXOs
- Default: false

### Example

Here's a JSON representation of the message content (for illustration purposes only; actual messages are protobuf-encoded):

```json
{
  "tx": "<binary data - variable length>",
  "height": 12345,
  "options": {
    "skipUtxoCreation": false,
    "addTXToBlockAssembly": true,
    "skipPolicyChecks": false,
    "createConflicting": false
  }
}
```

### Code Examples

#### Sending Messages

```go
// Create the transaction validation message
transactionBytes := tx.Serialize() // serialized transaction bytes
currentHeight := uint32(12345)     // current blockchain height

// Create options (using defaults in this example)
options := &kafkamessage.KafkaTxValidationOptions{
    SkipUtxoCreation:     false,
    AddTXToBlockAssembly: true,
    SkipPolicyChecks:     false,
    CreateConflicting:    false,
}

// Create a new protobuf message
message := &kafkamessage.KafkaTxValidationTopicMessage{
    Tx:      transactionBytes,
    Height:  currentHeight,
    Options: options,
}

// Serialize to protobuf format
data, err := proto.Marshal(message)
if err != nil {
    return fmt.Errorf("failed to serialize transaction validation message: %w", err)
}

// Send to Kafka
producer.Publish(&kafka.Message{
    Value: data,
})
```

#### Receiving Messages

```go
// Handle incoming transaction validation message
func handleTxValidationMessage(msg *kafka.Message) error {
    if msg == nil {
        return nil
    }

    // Deserialize from protobuf format
    txValidationMessage := &kafkamessage.KafkaTxValidationTopicMessage{}
    if err := proto.Unmarshal(msg.Value, txValidationMessage); err != nil {
        return fmt.Errorf("failed to deserialize transaction validation message: %w", err)
    }

    // Extract transaction data
    txBytes := txValidationMessage.Tx
    height := txValidationMessage.Height
    options := txValidationMessage.Options

    // Parse the transaction
    tx, err := bsvutil.NewTxFromBytes(txBytes)
    if err != nil {
        return fmt.Errorf("invalid transaction data: %w", err)
    }

    // Process the transaction with the provided options...
    skipUtxoCreation := options.SkipUtxoCreation
    addToBlockAssembly := options.AddTXToBlockAssembly
    skipPolicyChecks := options.SkipPolicyChecks
    createConflicting := options.CreateConflicting

    // Perform validation based on options...
    return validateTransaction(tx, height, skipUtxoCreation, addToBlockAssembly, skipPolicyChecks, createConflicting)
}
```

### Error Cases

- Invalid message format: Message cannot be unmarshaled to KafkaTxValidationTopicMessage
- Empty or invalid transaction: Transaction bytes cannot be parsed
- Invalid height: Height is too high compared to the current blockchain height

---

## Transaction Metadata Message Format

### TxMeta Topic

`kafka_txmetaConfig` is the Kafka topic used for broadcasting transaction metadata for validated transactions. This topic allows the Validator to either add new transaction metadata or request deletion of previously shared metadata.

### Message Structure

The transaction metadata message is defined in protobuf as `KafkaTxMetaTopicMessage`:

```protobuf
enum KafkaTxMetaActionType {
  ADD = 0;    // Add or update transaction metadata
  DELETE = 1; // Delete transaction metadata
}

message KafkaTxMetaTopicMessage {
  bytes txHash = 1;                 // Transaction hash (32 bytes)
  KafkaTxMetaActionType action = 2; // Action type (add or delete)
  bytes content = 3;                // Serialized transaction metadata (only used for ADD)
}
```

### Field Specifications

#### txHash

- Type: bytes
- Description: Raw bytes representation of the transaction hash (32 bytes)
- Required: Yes

#### action

- Type: KafkaTxMetaActionType (enum)
- Description: Specifies whether to add/update metadata (ADD) or delete metadata (DELETE)
- Required: Yes
- Values:

    - ADD (0): Add or update transaction metadata
    - DELETE (1): Delete transaction metadata

#### content

- Type: bytes
- Description: Serialized transaction metadata
- Required: Only when action is ADD; should be empty when action is DELETE
- Content: Serialized transaction metadata that includes transaction details, transaction input outpoints (TxInpoints), block IDs, fees, and other relevant information

### Transaction Metadata

The content field contains serialized transaction metadata, which typically includes:

- Complete transaction content
- Transaction input outpoints (TxInpoints) - containing parent transaction hashes and output indices
- Block heights where the transaction appears
- Transaction fee
- Size in bytes
- Flags (e.g., whether it's a coinbase transaction)
- Lock time

### Example

Here's a JSON representation of an ADD message (for illustration purposes only; actual messages are protobuf-encoded):

```json
{
  "txHash": "<binary data - 32 bytes>",
  "action": 0,  // ADD
  "content": "<binary data - serialized transaction metadata>"
}
```

Here's a JSON representation of a DELETE message:

```json
{
  "txHash": "<binary data - 32 bytes>",
  "action": 1,  // DELETE
  "content": ""  // Empty for DELETE operations
}
```

### Code Examples

#### Sending Messages

```go
// Example 1: Send ADD transaction metadata message
txHash := tx.TxID().Bytes() // returns 32-byte transaction hash
metadataContent := serializeMetadata(metadata) // serialize transaction metadata

// Create a new protobuf message for adding metadata
addMessage := &kafkamessage.KafkaTxMetaTopicMessage{
    TxHash: txHash,
    Action: kafkamessage.KafkaTxMetaActionType_ADD,
    Content: metadataContent,
}

// Serialize to protobuf format
addData, err := proto.Marshal(addMessage)
if err != nil {
    return fmt.Errorf("failed to serialize ADD metadata message: %w", err)
}

// Send to Kafka
producer.Publish(&kafka.Message{
    Value: addData,
})

// Example 2: Send DELETE transaction metadata message
txHash := tx.TxID().Bytes() // returns 32-byte transaction hash

// Create a new protobuf message for deleting metadata
deleteMessage := &kafkamessage.KafkaTxMetaTopicMessage{
    TxHash: txHash,
    Action: kafkamessage.KafkaTxMetaActionType_DELETE,
    // Content is empty for DELETE operations
}

// Serialize to protobuf format
deleteData, err := proto.Marshal(deleteMessage)
if err != nil {
    return fmt.Errorf("failed to serialize DELETE metadata message: %w", err)
}

// Send to Kafka
producer.Publish(&kafka.Message{
    Value: deleteData,
})
```

#### Receiving Messages

```go
// Handle incoming transaction metadata message
func handleTxMetaMessage(msg *kafka.Message) error {
    if msg == nil {
        return nil
    }

    // Deserialize from protobuf format
    txMetaMessage := &kafkamessage.KafkaTxMetaTopicMessage{}
    if err := proto.Unmarshal(msg.Value, txMetaMessage); err != nil {
        return fmt.Errorf("failed to deserialize transaction metadata message: %w", err)
    }

    // Extract transaction hash
    var txHash chainhash.Hash
    copy(txHash[:], txMetaMessage.TxHash)

    // Process based on action type
    switch txMetaMessage.Action {
    case kafkamessage.KafkaTxMetaActionType_ADD:
        // Handle ADD operation
        metadata := deserializeMetadata(txMetaMessage.Content)
        return handleAddMetadata(txHash, metadata)

    case kafkamessage.KafkaTxMetaActionType_DELETE:
        // Handle DELETE operation
        return handleDeleteMetadata(txHash)

    default:
        return fmt.Errorf("unknown action type: %d", txMetaMessage.Action)
    }
}
```

### Error Cases

- Invalid message format: Message cannot be unmarshaled to KafkaTxMetaTopicMessage
- Empty or invalid transaction hash: Hash does not contain exactly 32 bytes
- Unknown action type: Action is not a recognized KafkaTxMetaActionType enum value
- Missing content for ADD: Content field is empty when Action is ADD

---

## Rejected Transaction Message Format

### Rejected Transaction Topic

`kafka_rejectedTxConfig` is the Kafka topic used for broadcasting rejected transactions. This topic notifies subscribers about transactions that have been rejected during validation.

### Message Structure

The rejected transaction message is defined in protobuf as `KafkaRejectedTxTopicMessage`:

```protobuf
message KafkaRejectedTxTopicMessage {
  bytes txHash = 1;  // Transaction hash (32 bytes)
  string reason = 2; // Rejection reason
}
```

### Field Specifications

#### txHash

- Type: bytes
- Description: Raw bytes of the transaction hash (32 bytes), computed as double SHA256 hash
- Computation: `sha256(sha256(transaction_bytes))`
- Required: Yes

#### reason

- Type: string
- Description: Human-readable description of why the transaction was rejected
- Required: Yes

### Example

Here's a JSON representation of the message content (for illustration purposes only; actual messages are protobuf-encoded):

```json
{
  "txHash": "<binary data - 32 bytes>",
  "reason": "Insufficient fee for transaction size"
}
```

### Code Examples

#### Sending Messages

```go
// Create the rejected transaction message
txHash := tx.TxID().Bytes() // returns 32-byte transaction hash
reasonStr := "Insufficient fee for transaction size"

// Create a new protobuf message
message := &kafkamessage.KafkaRejectedTxTopicMessage{
    TxHash: txHash,
    Reason: reasonStr,
}

// Serialize to protobuf format
data, err := proto.Marshal(message)
if err != nil {
    return fmt.Errorf("failed to serialize rejected transaction message: %w", err)
}

// Send to Kafka
producer.Publish(&kafka.Message{
    Value: data,
})
```

#### Receiving Messages

```go
// Handle incoming rejected transaction message
func handleRejectedTxMessage(msg *kafka.Message) error {
    if msg == nil {
        return nil
    }

    // Deserialize from protobuf format
    rejectedTxMessage := &kafkamessage.KafkaRejectedTxTopicMessage{}
    if err := proto.Unmarshal(msg.Value, rejectedTxMessage); err != nil {
        return fmt.Errorf("failed to deserialize rejected transaction message: %w", err)
    }

    // Extract transaction hash
    var txHash chainhash.Hash
    copy(txHash[:], rejectedTxMessage.TxHash)

    // Extract rejection reason
    reason := rejectedTxMessage.Reason

    // Process the rejected transaction notification...
    log.Printf("Transaction %s was rejected: %s", txHash.String(), reason)
    return nil
}
```

### Error Cases

- Invalid message format: Message cannot be unmarshaled to KafkaRejectedTxTopicMessage
- Empty or invalid transaction hash: Hash does not contain exactly 32 bytes
- Missing reason: Reason field is empty

## Inventory Message Format

### Inventory Topic

The inventory message topic is used for broadcasting inventory vectors between components. This allows components to notify each other about available blocks, transactions, and other data.

### Message Structure

The inventory message is defined in protobuf as `KafkaInvTopicMessage`:

```protobuf
message KafkaInvTopicMessage {
  string peerAddress = 1;  // Address of the peer
  repeated Inv inv = 2;    // List of inventory items
}

message Inv {
  InvType type = 1;  // Type of inventory item
  bytes hash = 2;    // Hash of the inventory item
}

enum InvType {
  Error         = 0;
  Tx            = 1;
  Block         = 2;
  FilteredBlock = 3;
}
```

### Field Specifications

#### peerAddress

- Type: string
- Description: Network address of the peer that has the inventory item
- Required: Yes

#### inv

- Type: repeated Inv
- Description: List of inventory items
- Required: Yes

##### Inv

###### type

- Type: InvType (enum)
- Description: Type of inventory item
- Required: Yes
- Values:

    - Error (0): Error or unknown type
    - Tx (1): Transaction
    - Block (2): Block
    - FilteredBlock (3): Filtered block

###### hash

- Type: bytes
- Description: Hash of the inventory item (32 bytes)
- Required: Yes

### Example

Here's a JSON representation of the message content (for illustration purposes only; actual messages are protobuf-encoded):

```json
{
  "peerAddress": "192.168.1.10:8333",
  "inv": [
    {
      "type": 1,  // Tx
      "hash": "<binary data - 32 bytes>"
    },
    {
      "type": 2,  // Block
      "hash": "<binary data - 32 bytes>"
    }
  ]
}
```

### Code Examples

#### Sending Messages

```go
// Create the inventory message
peerAddress := "192.168.1.10:8333"

// Create inventory items
invItems := []*kafkamessage.Inv{
    {
        Type: kafkamessage.InvType_Tx,
        Hash: txHash[:], // 32-byte transaction hash
    },
    {
        Type: kafkamessage.InvType_Block,
        Hash: blockHash[:], // 32-byte block hash
    },
}

// Create a new protobuf message
message := &kafkamessage.KafkaInvTopicMessage{
    PeerAddress: peerAddress,
    Inv:         invItems,
}

// Serialize to protobuf format
data, err := proto.Marshal(message)
if err != nil {
    return fmt.Errorf("failed to serialize inventory message: %w", err)
}

// Send to Kafka
producer.Publish(&kafka.Message{
    Value: data,
})
```

#### Receiving Messages

```go
// Handle incoming inventory message
func handleInvMessage(msg *kafka.Message) error {
    if msg == nil {
        return nil
    }

    // Deserialize from protobuf format
    invMessage := &kafkamessage.KafkaInvTopicMessage{}
    if err := proto.Unmarshal(msg.Value, invMessage); err != nil {
        return fmt.Errorf("failed to deserialize inventory message: %w", err)
    }

    // Extract peer address
    peerAddr := invMessage.PeerAddress

    // Process inventory items
    for _, inv := range invMessage.Inv {
        var hash chainhash.Hash
        copy(hash[:], inv.Hash)

        switch inv.Type {
        case kafkamessage.InvType_Tx:
            log.Printf("Received transaction inventory from %s: %s", peerAddr, hash.String())
            // Process transaction inventory...

        case kafkamessage.InvType_Block:
            log.Printf("Received block inventory from %s: %s", peerAddr, hash.String())
            // Process block inventory...

        case kafkamessage.InvType_FilteredBlock:
            log.Printf("Received filtered block inventory from %s: %s", peerAddr, hash.String())
            // Process filtered block inventory...

        default:
            log.Printf("Received unknown inventory type %d from %s: %s", inv.Type, peerAddr, hash.String())
        }
    }

    return nil
}
```

### Error Cases

- Invalid message format: Message cannot be unmarshaled to KafkaInvTopicMessage
- Empty peer address: PeerAddress field is empty
- Invalid inventory item: Hash does not contain exactly 32 bytes or Type is unrecognized

## Final Block Message Format

### Final Block Topic

`kafka_blocksFinalConfig` is the Kafka topic used for broadcasting finalized blocks. This topic notifies subscribers about blocks that have been fully validated and accepted into the blockchain.

### Message Structure

The final block message is defined in protobuf as `KafkaBlocksFinalTopicMessage`:

```protobuf
message KafkaBlocksFinalTopicMessage {
    bytes header = 1;                    // Block header bytes
    uint64 transaction_count = 2;        // Number of transactions in block
    uint64 size_in_bytes = 3;            // Size of block in bytes
    repeated bytes subtree_hashes = 4;   // Merkle tree subtree hashes
    bytes coinbase_tx = 5;               // Coinbase transaction bytes
    uint32 height = 6;                   // Block height
}
```

### Field Specifications

#### header

- Type: bytes
- Description: Serialized block header
- Required: Yes

#### transaction_count

- Type: uint64
- Description: Total number of transactions in the block
- Required: Yes

#### size_in_bytes

- Type: uint64
- Description: Total size of the block in bytes
- Required: Yes

#### subtree_hashes

- Type: repeated bytes
- Description: List of Merkle tree subtree hashes that compose the block
- Required: Yes

#### coinbase_tx

- Type: bytes
- Description: Serialized coinbase transaction
- Required: Yes

#### height

- Type: uint32
- Description: Block height in the blockchain
- Required: Yes

### Example

Here's a JSON representation of the message content (for illustration purposes only; actual messages are protobuf-encoded):

```json
{
  "header": "<binary data - 80 bytes>",
  "transaction_count": 2500,
  "size_in_bytes": 1048576,
  "subtree_hashes": [
    "<binary data - 32 bytes>",
    "<binary data - 32 bytes>",
    "<binary data - 32 bytes>"
  ],
  "coinbase_tx": "<binary data - variable length>",
  "height": 12345
}
```

### Code Examples

#### Sending Messages

```go
// Create the final block message
blockHeader := block.Header.Serialize() // serialized block header bytes
txCount := uint64(block.Transactions.Len())
blockSize := uint64(block.SerializedSize())

// Get subtree hashes
subtreeHashes := make([][]byte, len(merkleTree.SubTrees))
for i, subtree := range merkleTree.SubTrees {
    subtreeHashes[i] = subtree.Hash()[:]
}

// Get coinbase transaction
coinbaseTx := block.Transactions[0].Serialize()
blockHeight := uint32(block.Height)

// Create a new protobuf message
message := &kafkamessage.KafkaBlocksFinalTopicMessage{
    Header:          blockHeader,
    TransactionCount: txCount,
    SizeInBytes:     blockSize,
    SubtreeHashes:   subtreeHashes,
    CoinbaseTx:      coinbaseTx,
    Height:          blockHeight,
}

// Serialize to protobuf format
data, err := proto.Marshal(message)
if err != nil {
    return fmt.Errorf("failed to serialize final block message: %w", err)
}

// Send to Kafka
producer.Publish(&kafka.Message{
    Value: data,
})
```

#### Receiving Messages

```go
// Handle incoming final block message
func handleFinalBlockMessage(msg *kafka.Message) error {
    if msg == nil {
        return nil
    }

    // Deserialize from protobuf format
    finalBlockMessage := &kafkamessage.KafkaBlocksFinalTopicMessage{}
    if err := proto.Unmarshal(msg.Value, finalBlockMessage); err != nil {
        return fmt.Errorf("failed to deserialize final block message: %w", err)
    }

    // Parse block header
    header := wire.BlockHeader{}
    if err := header.Deserialize(bytes.NewReader(finalBlockMessage.Header)); err != nil {
        return fmt.Errorf("invalid block header: %w", err)
    }

    // Extract other fields
    txCount := finalBlockMessage.TransactionCount
    blockSize := finalBlockMessage.SizeInBytes
    subtreeHashes := finalBlockMessage.SubtreeHashes
    coinbaseTxBytes := finalBlockMessage.CoinbaseTx
    height := finalBlockMessage.Height

    // Parse coinbase transaction
    coinbaseTx, err := bsvutil.NewTxFromBytes(coinbaseTxBytes)
    if err != nil {
        return fmt.Errorf("invalid coinbase transaction: %w", err)
    }

    // Process the final block...
    log.Printf("Received final block at height %d with %d transactions (size: %d bytes)",
              height, txCount, blockSize)

    // Process subtree hashes...
    for i, hashBytes := range subtreeHashes {
        var hash chainhash.Hash
        copy(hash[:], hashBytes)
        log.Printf("  Subtree hash %d: %s", i, hash.String())
    }

    return nil
}
```

### Error Cases

- Invalid message format: Message cannot be unmarshaled to KafkaBlocksFinalTopicMessage
- Invalid block header: Header bytes cannot be deserialized to a valid block header
- Invalid coinbase transaction: Coinbase transaction bytes cannot be parsed
- Missing subtree hashes: No subtree hashes provided

## General Code Examples

### Serializing Messages

Here's a general example of how to serialize a protobuf message for Kafka:

```go
// Create a new message
message := &kafkamessage.KafkaBlockTopicMessage{
    Hash: blockHash[:],
    URL:  datahubUrl,
}

// Serialize the message to protobuf format
data, err := proto.Marshal(message)
if err != nil {
    return fmt.Errorf("failed to serialize message: %w", err)
}

// Send to Kafka
producer.Publish(&kafka.Message{
    Value: data,
})
```

### Deserializing Messages

Here's a general example of how to deserialize a protobuf message from Kafka:

```go
func handleBlockMessage(msg *kafka.Message) error {
    if msg == nil {
        return nil
    }

    // Create a new message container
    blockMessage := &kafkamessage.KafkaBlockTopicMessage{}

    // Deserialize the message from protobuf format
    if err := proto.Unmarshal(msg.Value, blockMessage); err != nil {
        return fmt.Errorf("failed to deserialize message: %w", err)
    }

    // Extract block hash
    var blockHash chainhash.Hash
    copy(blockHash[:], blockMessage.Hash)

    // Extract DataHub URL
    dataHubUrl := blockMessage.URL

    // Process the message...
    return nil
}
```

---

## Other Resources

- [Understanding Kafka Role in Teranode](../topics/kafka/kafka.md)
- [The Block data model](../topics/datamodel/block_data_model.md)
- [The Block Header data model](../topics/datamodel/block_header_data_model.md)
- [The Subtree data model](../topics/datamodel/subtree_data_model.md)
- [The Transaction data model](../topics/datamodel/transaction_data_model.md)
- [The UTXO data model](../topics/datamodel/utxo_data_model.md)
