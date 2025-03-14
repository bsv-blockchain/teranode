# Kafka Message Format Reference Documentation


## Index


- [Block Notification Message Format](#block-notification-message-format)
    - [Block Topic](#block-topic)
    - [Message Structure](#message-structure)
    - [Field Specifications](#field-specifications)
        - [Block Hash](#block-hash)
        - [DataHub URL](#datahub-url)
    - [Example (Pseudo representation)](#example-pseudo-representation)
    - [Code Examples](#code-examples)
        - [Sending Messages](#sending-messages)
        - [Receiving Messages](#receiving-messages)
- [Subtree Notification Message Format](#subtree-notification-message-format)
    - [Subtree Topic](#subtree-topic)
    - [Message Structure](#message-structure)
    - [Field Specifications](#field-specifications)
        - [Subtree Hash](#subtree-hash)
        - [DataHub URL](#datahub-url)
    - [Message Format Example](#message-format-example)
    - [Code Examples](#code-examples)
        - [Sending Messages](#sending-messages)
        - [Receiving Messages](#receiving-messages)
    - [Error Cases](#error-cases)
- [Transaction Validation Message Format](#transaction-validation-message-format)
    - [Transaction Validation Topic](#transaction-validation-topic)
    - [Message Structure](#message-structure)
    - [Field Specifications](#field-specifications)
        - [Height](#height)
        - [Transaction](#transaction)
    - [Message Format Example](#message-format-example)
    - [Code Examples](#code-examples)
        - [Sending Messages](#sending-messages)
        - [Receiving Messages](#receiving-messages)
    - [Error Cases](#error-cases)
- [Transaction Metadata Delete Message Format](#transaction-metadata-delete-message-format)
    - [TxMeta Topic](#txmeta-topic)
    - [Message Structure](#message-structure)
    - [Field Specifications](#field-specifications)
        - [Transaction Hash](#transaction-hash)
        - [Content](#content)
- [Data Structures](#data-structures)
    - [Transaction Metadata](#transaction-metadata)
    - [Message Format Example](#message-format-example)
    - [Code Examples](#code-examples)
        - [Sending Messages](#sending-messages)
        - [Receiving Messages](#receiving-messages)
- [Rejected Transaction Message Format](#rejected-transaction-message-format)
- [Rejected Transaction Topic](#rejected-transaction-topic)
    - [Message Structure](#message-structure)
    - [Field Specifications](#field-specifications)
        - [Transaction Hash](#transaction-hash)
        - [Error Message](#error-message)
    - [Message Format Example](#message-format-example)
    - [Code Examples](#code-examples)
        - [Sending Messages](#sending-messages)
        - [Receiving Messages](#receiving-messages)
    - [Downstream Message Structure](#downstream-message-structure)
- [Other Resources](#other-resources)


## Block Notification Message Format

### Block Topic

`kafka_blocksConfig` is the Kafka topic used for broadcasting block notifications. This topic is used to notify subscribers of new blocks as they are added to the blockchain.

### Message Structure
The Kafka message consists of a binary value with two concatenated fields:

| Field      | Size    | Format      | Description |
|------------|---------|-------------|-------------|
| Block Hash | 32 bytes| Raw bytes   | BSV block hash |
| DataHub URL| Variable| UTF-8 string| URL pointing to the block data |

### Field Specifications

#### Block Hash
- Fixed size: 32 bytes
- Format: Raw bytes representation of the BSV block hash
- Validation: Must be exactly 32 bytes long

#### DataHub URL
- Size: Variable length
- Format: UTF-8 encoded string
- Required: Yes
- Content: Valid URL pointing to block data location

### Example (Pseudo representation)

```hex
// Message value (hex representation)
<32 bytes of block hash><variable length DataHub URL>

Example:
7d1c7a5c9f6e4b3a2d1c7a5c9f6e4b3a2d1c7a5c9f6e4b3a2d1c7a5c9f6e4b3a + https://datahub.example.com/blocks/123
 ```

### Code Examples

#### Sending Messages
```go
// Prepare message
hash := block.Hash() // assumes this returns *chainhash.Hash
dataHubUrl := "https://datahub.example.com/blocks/123"

// Construct message value
value := make([]byte, 0, chainhash.HashSize+len(dataHubUrl))
value = append(value, hash.CloneBytes()...)
value = append(value, []byte(dataHubUrl)...)

// Send to Kafka
producer.Publish(&kafka.Message{
Value: value,
})
```

#### Receiving Messages
```go
// Handle incoming message
func handleMessage(msg *kafka.Message) error {
if msg == nil {
return nil
}

// Extract block hash (first 32 bytes)
hash, err := chainhash.NewHash(msg.Value[:32])
if err != nil {
return fmt.Errorf("invalid block hash: %w", err)
}

// Extract DataHub URL (remaining bytes)
dataHubUrl := string(msg.Value[32:])

// Process the message...
return nil
}
```

---

## Subtree Notification Message Format

### Subtree Topic

`kafka_subtreesConfig` is the Kafka topic used for broadcasting subtree notifications. This topic is used to notify subscribers of new subtrees as they are created.

### Message Structure
The Kafka message consists of a binary value with two concatenated fields:

| Field        | Size     | Format       | Description                      |
|--------------|----------|--------------|----------------------------------|
| Subtree Hash | 32 bytes | Raw bytes    | BSV subtree hash                 |
| DataHub URL  | Variable | UTF-8 string | URL pointing to the subtree data |

### Field Specifications

#### Subtree Hash
- Fixed size: 32 bytes
- Format: Raw bytes representation of the BSV subtree hash
- Validation: Must be exactly 32 bytes long

#### DataHub URL
- Size: Variable length
- Format: UTF-8 encoded string
- Required: Yes
- Content: Valid URL pointing to subtree data location

### Message Format Example
```
[32 bytes subtree hash][variable length DataHub URL]

Example:
7d1c7a5c9f6e4b3a2d1c7a5c9f6e4b3a2d1c7a5c9f6e4b3a2d1c7a5c9f6e4b3a + https://datahub.example.com/subtrees/123
```

### Code Examples

#### Sending Messages
```go
// Prepare message
hash := subtree.Hash() // assumes this returns *chainhash.Hash
dataHubUrl := "https://datahub.example.com/subtrees/123"

// Construct message value
value := make([]byte, 0, chainhash.HashSize+len(dataHubUrl))
value = append(value, hash.CloneBytes()...)
value = append(value, []byte(dataHubUrl)...)

// Send to Kafka
producer.Publish(&kafka.Message{
    Value: value,
})
```

#### Receiving Messages
```go
// Handle incoming message
func handleMessage(msg *kafka.Message) error {
    if msg == nil {
        return nil
    }

    // Validate minimum message length
    if len(msg.Value) < 32 {
        return fmt.Errorf("invalid message length: got %d bytes, want at least 32", len(msg.Value))
    }

    // Extract subtree hash (first 32 bytes)
    hash, err := chainhash.NewHash(msg.Value[:32])
    if err != nil {
        return fmt.Errorf("invalid subtree hash: %w", err)
    }

    // Extract DataHub URL (remaining bytes)
    dataHubUrl := string(msg.Value[32:])

    // Process the message...
    return nil
}
```

### Error Cases
- Message length less than 32 bytes: ERR_INVALID_ARGUMENT
- Invalid hash format: ERR_INVALID_ARGUMENT

---


## Transaction Validation Message Format

### Transaction Validation Topic

`kafka_validatortxsConfig` is the Kafka topic used for broadcasting transaction validation messages. This topic is used to notify subscribers of transactions that require validation.

### Message Structure
The Kafka message consists of a binary value containing transaction data and validation height:

| Field        | Size       | Format        | Description                 |
|--------------|------------|---------------|-----------------------------|
| Height       | 4 bytes    | uint32 (LE)   | Block height for validation |
| Transaction  | Variable   | Raw bytes     | Raw transaction data        |

### Field Specifications

#### Height
- Fixed size: 4 bytes
- Format: Unsigned 32-bit integer, little-endian encoding
- Purpose: Specifies the block height at which validation rules should be applied

#### Transaction
- Size: Variable length
- Format: Raw bytes of the BSV transaction
- Required: Yes
- Content: Complete serialized transaction data in extended format

### Message Format Example
```
[4 bytes height][variable length transaction data]

Example:
00001000 (height 4096 in little-endian) + [raw transaction bytes]
```

### Code Examples

#### Sending Messages
```go
// Prepare message
validatorData := &validator.TxValidationData{
    Tx:     transactionBytes,
    Height: chaincfg.GenesisActivationHeight,
}

// Send to Kafka
producer.Publish(&kafka.Message{
    Value: validatorData.Bytes(),
})
```

#### Receiving Messages
```go
// Handle incoming message
func handleMessage(bytes []byte) (*TxValidationData, error) {
    // Check minimum length requirement for height field
    if len(bytes) < 4 {
        return nil, errors.New(errors.ERR_ERROR, "input bytes too short")
    }

    data := &TxValidationData{}

    // Extract height from first 4 bytes (little-endian)
    data.Height = binary.LittleEndian.Uint32(bytes[:4])

    // Extract transaction data (remaining bytes)
    if len(bytes) > 4 {
        data.Tx = make([]byte, len(bytes[4:]))
        copy(data.Tx, bytes[4:])
    }

    return data, nil
}
```

### Error Cases
- Message length less than 4 bytes: ERR_ERROR ("input bytes too short")

---


## Transaction Metadata Delete Message Format

### TxMeta Topic

`kafka_txmetaConfig` is the Kafka topic used for broadcasting transaction metadata for validated transactions. This topic is used to notify subscribers of transactions that require deletion from the metadata cache.

### Message Structure
The Kafka message consists of a binary value containing a transaction hash followed by either:
- The complete transaction metadata (if we intend the consumers to store or cache this new metadata), or
- A delete command (if we intend the consumers to delete previously shared metadata).

| Field        | Size    | Format      | Description                 |
|--------------|---------|-------------|-----------------------------|
| TX Hash      | 32 bytes| Raw bytes   | Transaction hash            |
| Content      | Variable| Binary/Text | Either metadata or "delete" |

### Field Specifications

#### Transaction Hash
- Fixed size: 32 bytes (chainhash.HashSize)
- Format: Raw bytes representation of the transaction hash
- Validation: Must be exactly 32 bytes long

#### Content
Either:
1. Delete Command:
    - Fixed value: "delete" (UTF-8 encoded)
    - Size: 6 bytes

2. Transaction Metadata:
    - Variable size
    - Serialized `Data` structure containing:
     * Complete transaction (`bt.Tx`)
     * Parent transaction hashes
     * Block IDs
     * Fee
     * Size in bytes
     * Coinbase flag
     * Lock time

## Data Structures

#### Transaction Metadata
```go
type Data struct {
    Tx             *bt.Tx            // Complete transaction
    ParentTxHashes []chainhash.Hash  // Parent transaction IDs
    BlockIDs       []uint32          // Block heights
    Fee            uint64            // Transaction fee in satoshis
    SizeInBytes    uint64            // Serialized size
    IsCoinbase     bool             // Coinbase indicator
    LockTime       uint32            // Lock time
}

type Tx struct {
    Inputs   []*Input
    Outputs  []*Output
    Version  uint32
    LockTime uint32
}
```

### Message Format Example
```
[32 bytes tx hash][either "delete" or serialized metadata]

Examples:
1. Delete command:
   7d1c7a5c9f6e4b3a2d1c7a5c9f6e4b3a2d1c7a5c9f6e4b3a2d1c7a5c9f6e4b3a + delete

2. Metadata:
   7d1c7a5c9f6e4b3a2d1c7a5c9f6e4b3a2d1c7a5c9f6e4b3a2d1c7a5c9f6e4b3a + [serialized metadata]
```

### Code Examples

#### Sending Messages
```go
// Send delete command
producer.Publish(&kafka.Message{
    Value: append(txHash.CloneBytes(), []byte("delete")...),
})

// Send metadata
data, err := utxoStore.Create(ctx, tx, blockHeight)
if err != nil {
    return err
}
producer.Publish(&kafka.Message{
    Value: append(tx.TxIDChainHash().CloneBytes(), data.MetaBytes()...),
})
```

#### Receiving Messages
```go
func handleMessage(msg *kafka.Message) error {
    if msg == nil || len(msg.Value) <= chainhash.HashSize {
        return nil
    }

    hash := chainhash.Hash(msg.Value[:chainhash.HashSize])
    content := msg.Value[chainhash.HashSize:]

    if bytes.Equal(content, []byte("delete")) {
        return handleDelete(&hash)
    }

    return handleMetadata(&hash, content)
}
```

---

## Rejected Transaction Message Format

`kafka_rejectedTxConfig` is the Kafka topic used for broadcasting rejected transactions. This topic is used to notify subscribers of transactions that have been rejected during validation.

## Rejected Transaction Topic

### Message Structure
The Kafka message consists of a binary value containing a transaction hash and an error message:

| Field        | Size    | Format      | Description                       |
|--------------|---------|-------------|-----------------------------------|
| TX Hash      | 32 bytes| Raw bytes   | Double SHA256 hash of transaction |
| Error Message| Variable| UTF-8 string| Rejection reason                  |


### Field Specifications

#### Transaction Hash
- Fixed size: 32 bytes (chainhash.HashSize)
- Format: Raw bytes of double SHA256 hash
- Computation: `sha256(sha256(transaction_bytes))`
- Validation: Must be exactly 32 bytes long

#### Error Message
- Size: Variable length
- Format: UTF-8 encoded string
- Content: Description of why the transaction was rejected

### Message Format Example
```
[32 bytes tx hash][variable length error message]

Example:
7d1c7a5c9f6e4b3a2d1c7a5c9f6e4b3a2d1c7a5c9f6e4b3a2d1c7a5c9f6e4b3a + "insufficient fee"
```

### Code Examples

#### Sending Messages
```go
// Prepare and send rejected transaction message
txHash := tx.TxIDChainHash() // returns double-hashed transaction ID
producer.Publish(&kafka.Message{
    Value: append(txHash.CloneBytes(), err.Error()...),
})
```

#### Receiving Messages
```go
func handleMessage(msg *kafka.Message) error {
    if msg == nil || len(msg.Value) <= chainhash.HashSize {
        return nil
    }

    // Extract transaction hash (first 32 bytes)
    hash, err := chainhash.NewHash(msg.Value[:chainhash.HashSize])
    if err != nil {
        return fmt.Errorf("error getting chainhash: %w", err)
    }

    // Extract rejection reason
    reason := string(msg.Value[chainhash.HashSize:])

    // Create rejection message
    rejectedTxMessage := p2p.RejectedTxMessage{
        TxId:   hash.String(),
        Reason: reason,
        PeerId: p2pNodeID,
    }

    // Process the rejection...
    return nil
}
```

### Downstream Message Structure

After processing, the rejection is formatted as JSON:
```json
{
    "txId": "hash_in_hex_string",
    "reason": "rejection_reason",
    "peerId": "p2p_node_identifier"
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
