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
- [Invalid Block Notification Message Format](#invalid-block-notification-message-format)
    - [Invalid Block Topic](#invalid-block-topic)
    - [Message Structure](#message-structure)
    - [Field Specifications](#field-specifications)
        - [blockHash](#blockhash)
        - [reason](#reason)
    - [Example](#example)
    - [Code Examples](#code-examples)
        - [Sending Messages](#sending-messages)
        - [Receiving Messages](#receiving-messages)
    - [Error Cases](#error-cases)
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
- [Invalid Subtree Notification Message Format](#invalid-subtree-notification-message-format)
    - [Invalid Subtree Topic](#invalid-subtree-topic)
    - [Message Structure](#message-structure)
    - [Field Specifications](#field-specifications)
        - [subtreeHash](#subtreehash)
        - [peerUrl](#peerurl)
        - [reason](#reason)
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
    - [Wire Format](#wire-format)
        - [v1 (legacy)](#v1-legacy)
        - [v2 (partition-aware)](#v2-partition-aware)
    - [Wire-Format Detection (Receiver Side)](#wire-format-detection-receiver-side)
    - [Action Values](#action-values)
    - [Content Payload](#content-payload)
    - [Code Examples](#code-examples)
        - [Producing Messages](#producing-messages)
        - [Consuming Messages](#consuming-messages)
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
  string peer_id = 3; // P2P peer identifier for peerMetrics tracking
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

#### peer_id

- Type: string
- Description: P2P peer identifier used for peer metrics tracking
- Required: Yes

### Example

Here's a JSON representation of the message content (for illustration purposes only; actual messages are protobuf-encoded):

```json
{
  "hash": "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
  "URL": "https://datahub.example.com/blocks/123",
  "peer_id": "peer_12345"
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
    PeerId: "peer_12345", // P2P peer identifier
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

    // Extract DataHub URL and peer ID
    dataHubUrl := blockMessage.URL
    peerID := blockMessage.PeerId

    // Process the block notification...
    log.Printf("Received block notification for %s from peer %s, data at: %s", blockHash.String(), peerID, dataHubUrl)
    return nil
}
```

### Error Cases

- Invalid message format: Message cannot be unmarshaled to KafkaBlockTopicMessage
- Empty or malformed hash: Hash is not a valid hexadecimal string representation of a block hash
- Invalid URL: DataHub URL is empty or not properly formatted

---

## Invalid Block Notification Message Format

### Invalid Block Topic

`kafka_invalidBlocksConfig` is the Kafka topic used for broadcasting invalid block notifications. This topic allows services to notify other components when a block has been determined to be invalid.

### Message Structure

The invalid block message is defined in protobuf as `KafkaInvalidBlockTopicMessage`:

```protobuf
message KafkaInvalidBlockTopicMessage {
  string blockHash = 1;
  string reason = 2;
}
```

### Field Specifications

#### blockHash

- Type: string
- Description: Hexadecimal string representation of the invalid BSV block hash
- Required: Yes
- Format: 64-character hexadecimal string (256-bit hash)
- Example: `"00000000000000000007abd8d2a16a69c1c45a1c3b0d1a6b2e0c8b4e8f9a1b2c3"`

#### reason

- Type: string
- Description: Human-readable explanation of why the block was determined to be invalid
- Required: Yes
- Example: `"Block contains invalid transaction"`

### Example

```json
{
  "blockHash": "00000000000000000007abd8d2a16a69c1c45a1c3b0d1a6b2e0c8b4e8f9a1b2c3",
  "reason": "Block contains invalid transaction"
}
```

### Code Examples

#### Sending Messages

```go
// Create invalid block message
invalidBlockMessage := &kafkamessage.KafkaInvalidBlockTopicMessage{
    BlockHash: blockHash.String(),
    Reason:    "Block validation failed: invalid merkle root",
}

// Serialize to protobuf
data, err := proto.Marshal(invalidBlockMessage)
if err != nil {
    return fmt.Errorf("failed to marshal invalid block message: %w", err)
}

// Send to Kafka
producerMessage := &sarama.ProducerMessage{
    Topic: "kafka_invalidBlock",
    Value: sarama.ByteEncoder(data),
}

partition, offset, err := producer.SendMessage(producerMessage)
if err != nil {
    return fmt.Errorf("failed to send invalid block message: %w", err)
}
```

#### Receiving Messages

```go
// Handle incoming invalid block message
func handleInvalidBlockMessage(msg *kafka.Message) error {
    if msg == nil {
        return nil
    }

    var invalidBlockMessage kafkamessage.KafkaInvalidBlockTopicMessage
    if err := proto.Unmarshal(msg.Value, &invalidBlockMessage); err != nil {
        return fmt.Errorf("failed to unmarshal invalid block message: %w", err)
    }

    blockHash := invalidBlockMessage.BlockHash
    reason := invalidBlockMessage.Reason

    // Process the invalid block notification...
    log.Printf("Block %s marked as invalid: %s", blockHash, reason)
    return nil
}
```

### Error Cases

- Invalid message format: Message cannot be unmarshaled to KafkaInvalidBlockTopicMessage
- Empty or malformed blockHash: Hash is not a valid hexadecimal string representation of a block hash
- Empty reason: Reason field is empty or not provided

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
  string peer_id = 3;  // Originator peer ID
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

#### peer_id

- Type: string
- Description: Originator peer identifier that created or provided this subtree
- Required: Yes

### Example

Here's a JSON representation of the message content (for illustration purposes only; actual messages are protobuf-encoded):

```json
{
  "hash": "45a2b856743012ce25a4dabddd5f5bdf534c27c9347b34862bca5a14176d07",
  "URL": "https://datahub.example.com/subtrees/123",
  "peer_id": "peer_67890"
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
    PeerId: "peer_67890", // Originator peer identifier
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

    // Extract DataHub URL and peer ID
    dataHubUrl := subtreeMessage.URL
    peerID := subtreeMessage.PeerId

    // Process the subtree notification...
    log.Printf("Received subtree notification for %s from peer %s, data at: %s", subtreeHash.String(), peerID, dataHubUrl)
    return nil
}
```

### Error Cases

- Invalid message format: Message cannot be unmarshaled to KafkaSubtreeTopicMessage
- Empty or malformed hash: Hash is not a valid hexadecimal string representation of a subtree hash
- Invalid URL: DataHub URL is empty or not properly formatted

---

## Invalid Subtree Notification Message Format

### Invalid Subtree Topic

`kafka_invalidSubtreesConfig` is the Kafka topic used for broadcasting invalid subtree notifications. This topic allows services to notify other components when a subtree has been determined to be invalid.

### Message Structure

The invalid subtree message is defined in protobuf as `KafkaInvalidSubtreeTopicMessage`:

```protobuf
message KafkaInvalidSubtreeTopicMessage {
  string subtreeHash = 1;
  string peerUrl = 2;
  string reason = 3;
}
```

### Field Specifications

#### subtreeHash

- Type: string
- Description: Hexadecimal string representation of the invalid subtree hash
- Required: Yes
- Format: 64-character hexadecimal string (256-bit hash)
- Example: `"a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456"`

#### peerUrl

- Type: string
- Description: URL of the peer that provided the invalid subtree
- Required: Yes
- Format: Valid URL string
- Example: `"http://peer1.example.com:8080"`

#### reason

- Type: string
- Description: Human-readable explanation of why the subtree was determined to be invalid
- Required: Yes
- Example: `"Subtree contains invalid transaction merkle proof"`

### Example

```json
{
  "subtreeHash": "a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456",
  "peerUrl": "http://peer1.example.com:8080",
  "reason": "Subtree contains invalid transaction merkle proof"
}
```

### Code Examples

#### Sending Messages

```go
// Create invalid subtree message
invalidSubtreeMessage := &kafkamessage.KafkaInvalidSubtreeTopicMessage{
    SubtreeHash: subtreeHash.String(),
    PeerUrl:     "http://peer1.example.com:8080",
    Reason:      "Subtree validation failed: invalid merkle proof",
}

// Serialize to protobuf
data, err := proto.Marshal(invalidSubtreeMessage)
if err != nil {
    return fmt.Errorf("failed to marshal invalid subtree message: %w", err)
}

// Send to Kafka
producerMessage := &sarama.ProducerMessage{
    Topic: "kafka_invalidSubtree",
    Value: sarama.ByteEncoder(data),
}

partition, offset, err := producer.SendMessage(producerMessage)
if err != nil {
    return fmt.Errorf("failed to send invalid subtree message: %w", err)
}
```

#### Receiving Messages

```go
// Handle incoming invalid subtree message
func handleInvalidSubtreeMessage(msg *kafka.Message) error {
    if msg == nil {
        return nil
    }

    var invalidSubtreeMessage kafkamessage.KafkaInvalidSubtreeTopicMessage
    if err := proto.Unmarshal(msg.Value, &invalidSubtreeMessage); err != nil {
        return fmt.Errorf("failed to unmarshal invalid subtree message: %w", err)
    }

    subtreeHash := invalidSubtreeMessage.SubtreeHash
    peerUrl := invalidSubtreeMessage.PeerUrl
    reason := invalidSubtreeMessage.Reason

    // Process the invalid subtree notification...
    log.Printf("Subtree %s from peer %s marked as invalid: %s", subtreeHash, peerUrl, reason)
    return nil
}
```

### Error Cases

- Invalid message format: Message cannot be unmarshaled to KafkaInvalidSubtreeTopicMessage
- Empty or malformed subtreeHash: Hash is not a valid hexadecimal string representation of a subtree hash
- Invalid peerUrl: URL is empty or not properly formatted
- Empty reason: Reason field is empty or not provided

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

Unlike the other Teranode Kafka topics, the txmeta topic does **not** use protobuf. Each Kafka record carries a custom, length-prefixed binary batch of many entries so the producer can amortise the per-record overhead at high transaction rates (the throughput target is well into the millions of transactions per second). The wire-format constants are defined once in [`stores/txmetacache/wire.go`](../../stores/txmetacache/wire.go) and imported by the producer (`services/validator`) and every consumer (`services/subtreevalidation`, `services/legacy/netsync`).

### Wire Format

Two batch formats coexist on the topic, distinguished at the receiver by a multi-byte signature at the start of the record. The producer chooses the format based on the `validator_txmeta_wireFormat` setting (`v1` by default; `v2` for partition-aligned writes).

#### v1 (legacy)

```text
+--------+-------+--------+----------+---------+
| count  | hash  | action | contLen  | content |
| u32 LE | 32 B  | 1 B    | u32 LE   | N B     |
+--------+-------+--------+----------+---------+
\__hdr__/ \________ per-entry ___________ ... /
```

Header (4 bytes): little-endian `uint32` entry count.
Per entry: 32-byte tx hash · 1-byte action · 4-byte LE content length · `contLen` bytes content (0 bytes for `DELETE`).

#### v2 (partition-aware)

```text
+-------+--------+----------+--------+--------+-------+--------+---------+---------+
| magic | versn  | reserved | count  | xxhash | hash  | action | contLen | content |
| 0xFF  | 0x02   | 2 B = 0  | u32 LE | u64 LE | 32 B  | 1 B    | u32 LE  | N B     |
+-------+--------+----------+--------+--------+-------+--------+---------+---------+
\__________________ 8-byte header _______________/  \________ per-entry ____________ ... /
```

v2 adds an 8-byte fixed-prefix header followed by entries that carry the pre-computed `xxhash(tx hash)` on the wire. Receivers can use the wire-side xxhash to skip rehashing for cache writes; partition routing is also aligned with the xxhash so that per-partition consumer goroutines touch disjoint cache buckets.

### Wire-Format Detection (Receiver Side)

A receiver cannot rely on the magic byte alone: a v1 message with entry count `255`, `511`, `767`, ... has a little-endian low byte of `0xFF`, identical to the v2 magic byte. For count `767` the first four bytes of the v1 message are exactly `[0xFF, 0x02, 0x00, 0x00]`, aliasing the full v2 header signature.

To avoid misclassifying these v1 messages, every consumer follows the same detection rule:

1. Verify all four header bytes: `data[0] == 0xFF`, `data[1] == 0x02`, `data[2] == 0`, `data[3] == 0`.
2. Read the candidate v2 entry count from `data[4:8]`.
3. Check plausibility: `candidateCount * minV2EntrySize ≤ len(data) - 8` (where `minV2EntrySize = 45` bytes = xxhash + hash + action + contentLen).
4. **Only if all three pass**, parse as v2.
5. **Otherwise**, fall through to v1 parsing — the message is *never* dropped just because its first byte happens to be `0xFF`.

A symmetric plausibility check on the v1 path (`entries * minV1EntrySize ≤ len(data) - 4`, where `minV1EntrySize = 37`) bounds the receiver's pool-buffer allocation against a malformed wire-side count.

### Action Values

The 1-byte action enum is defined in `stores/txmetacache/wire.go`:

| Constant | Value | Meaning |
|----------|-------|---------|
| `WireActionADD` | `0x00` | Add or replace cached metadata for the given tx hash. `content` carries the serialized `meta.Data`. |
| `WireActionDELETE` | `0x01` | Remove the entry for the given tx hash from the cache. `contLen` is `0` and no content follows. |

### Content Payload

For `ADD` entries, `content` is the serialized `meta.Data` for the transaction — produced by `(*meta.Data).MetaBytes()` and parsed by `meta.NewMetaDataFromBytes`. It includes:

- Complete transaction bytes (extended format)
- Transaction input outpoints (parent tx hashes + output indices)
- Block IDs the transaction has been mined into
- Transaction fee (satoshis)
- Size in bytes
- Coinbase flag
- Lock time

### Code Examples

#### Producing Messages

The producer is `services/validator/Validator.go` — see `serializeTxMetaBatch` (v1) and `serializeTxMetaBatchV2` (v2). The wire-format constants come from the shared package:

```go
import "github.com/bsv-blockchain/teranode/stores/txmetacache"

// v1 entry: hash, action, content length, content
buf := make([]byte, 4)
binary.LittleEndian.PutUint32(buf, uint32(len(entries)))
for _, e := range entries {
    buf = append(buf, e.hash[:]...)
    if e.isDelete {
        buf = append(buf, txmetacache.WireActionDELETE, 0, 0, 0, 0)
    } else {
        buf = append(buf, txmetacache.WireActionADD)
        lenBuf := make([]byte, 4)
        binary.LittleEndian.PutUint32(lenBuf, uint32(len(e.metaBytes)))
        buf = append(buf, lenBuf...)
        buf = append(buf, e.metaBytes...)
    }
}
```

#### Consuming Messages

Receivers detect the format and dispatch accordingly. See `services/subtreevalidation/txmetaHandler.go` and `services/legacy/netsync/manager.go` for the canonical implementations:

```go
import "github.com/bsv-blockchain/teranode/stores/txmetacache"

// Speculative v2 detection — full signature + entry-count plausibility.
isV2 := false
var entries uint32
var offset int

if len(data) >= txmetacache.WireV2HeaderLen &&
    data[0] == txmetacache.WireV2Magic &&
    data[1] == txmetacache.WireV2Version &&
    data[2] == 0 && data[3] == 0 {
    candidate := binary.LittleEndian.Uint32(data[4:])
    remaining := uint64(len(data) - txmetacache.WireV2HeaderLen)
    if uint64(candidate)*uint64(txmetacache.WireV2MinEntrySize) <= remaining {
        entries, offset, isV2 = candidate, txmetacache.WireV2HeaderLen, true
    }
}

if !isV2 {
    entries = binary.LittleEndian.Uint32(data[:4])
    offset = 4
}

// ... iterate entries, reading [xxhash (v2 only)][hash][action][contLen][content]
```

### Error Cases

- **Truncated header**: buffer shorter than the minimum header (4 bytes for v1, 8 bytes for v2). Logged + acked.
- **Truncated entry**: per-entry header or content runs off the end of the buffer. Logged + acked at the truncation point; preceding entries are still processed.
- **Implausible entry count**: wire-side count exceeds what the buffer can hold at the minimum entry size. Logged + acked; nothing is dispatched. Protects the pool buffer from oversized pre-allocation.
- **Unknown action byte** (not `0x00` or `0x01`): the entry is logged and skipped; remaining entries continue to be processed.

In all of the above the message is acknowledged (`return nil`) rather than re-delivered — a single corrupt message must not stall the topic.

---

## Rejected Transaction Message Format

### Rejected Transaction Topic

`kafka_rejectedTxConfig` is the Kafka topic used for broadcasting rejected transactions. This topic notifies subscribers about transactions that have been rejected during validation.

### Message Structure

The rejected transaction message is defined in protobuf as `KafkaRejectedTxTopicMessage`:

```protobuf
message KafkaRejectedTxTopicMessage {
  string txHash = 1;  // Transaction hash (as hex string)
  string reason = 2; // Rejection reason
  string peer_id = 3;  // Empty = internal rejection, non-empty = external peer
}
```

### Field Specifications

#### txHash

- Type: string
- Description: Hexadecimal string representation of the transaction hash, computed as double SHA256 hash
- Computation: `sha256(sha256(transaction_bytes))`
- Required: Yes

#### reason

- Type: string
- Description: Human-readable description of why the transaction was rejected
- Required: Yes

#### peer_id

- Type: string
- Description: Peer identifier indicating the source of the rejection. Empty string indicates internal rejection, non-empty indicates rejection from an external peer
- Required: No (can be empty)

### Example

Here's a JSON representation of the message content (for illustration purposes only; actual messages are protobuf-encoded):

```json
{
  "txHash": "a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456",
  "reason": "Insufficient fee for transaction size",
  "peer_id": ""
}
```

### Code Examples

#### Sending Messages

```go
// Create the rejected transaction message
txHash := tx.TxID().String() // returns hex string representation
reasonStr := "Insufficient fee for transaction size"

// Create a new protobuf message
message := &kafkamessage.KafkaRejectedTxTopicMessage{
    TxHash: txHash,
    Reason: reasonStr,
    PeerId: "", // Empty string indicates internal rejection
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

    // Extract transaction hash, reason, and peer ID
    txHashStr := rejectedTxMessage.TxHash
    reason := rejectedTxMessage.Reason
    peerID := rejectedTxMessage.PeerId

    // Convert hex string to chainhash.Hash if needed
    txHash, err := chainhash.NewHashFromStr(txHashStr)
    if err != nil {
        return fmt.Errorf("invalid transaction hash: %w", err)
    }

    // Determine rejection source
    rejectionSource := "internally"
    if peerID != "" {
        rejectionSource = fmt.Sprintf("by peer %s", peerID)
    }

    // Process the rejected transaction notification...
    if peerID == "" {
        log.Printf("Transaction %s was rejected internally: %s", txHash.String(), reason)
    } else {
        log.Printf("Transaction %s was rejected by peer %s: %s", txHash.String(), peerID, reason)
    }
    return nil
}
```

### Error Cases

- Invalid message format: Message cannot be unmarshaled to KafkaRejectedTxTopicMessage
- Empty or invalid transaction hash: Hash is not a valid hexadecimal string
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
  string hash = 2;   // Hash of the inventory item (as hex string)
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
- Description: List of inventory items (see Inv message structure defined above)
- Required: Yes

### Example

Here's a JSON representation of the message content (for illustration purposes only; actual messages are protobuf-encoded):

```json
{
  "peerAddress": "192.168.1.10:8333",
  "inv": [
    {
      "type": 1,  // Tx
      "hash": "a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456"
    },
    {
      "type": 2,  // Block
      "hash": "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
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
        Hash: txHash.String(), // hex string representation
    },
    {
        Type: kafkamessage.InvType_Block,
        Hash: blockHash.String(), // hex string representation
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
        // Convert hex string to chainhash.Hash
        hash, err := chainhash.NewHashFromStr(inv.Hash)
        if err != nil {
            return fmt.Errorf("invalid inventory hash: %w", err)
        }

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
- Invalid inventory item: Hash is not a valid hexadecimal string or Type is unrecognized

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
    Hash: blockHash.String(), // convert hash to hex string
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

    // Convert string hash to chainhash.Hash
    blockHash, err := chainhash.NewHashFromStr(blockMessage.Hash)
    if err != nil {
        return fmt.Errorf("invalid block hash: %w", err)
    }

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
