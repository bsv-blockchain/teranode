# 🐘️ Kafka in Teranode

## Table of Contents

1. [Description](#1-description)
2. [Use Cases](#2-use-cases)
    - [Propagation Service](#propagation-service)
    - [Validator Component](#validator-component)
    - [P2P Service](#p2p-service)
    - [Blockchain](#blockchain)
3. [Reliability and Recoverability](#3-reliability-and-recoverability)
4. [Configuration](#4-configuration)
5. [Operational Guidelines](#5-operational-guidelines)
    - [Performance Tuning](#performance-tuning)
    - [Reliability Considerations](#reliability-considerations)
    - [Monitoring](#monitoring)
6. [Kafka URL Configuration Parameters](#6-kafka-url-configuration-parameters)
    - [Consumer Configuration Parameters](#consumer-configuration-parameters)
    - [Producer Configuration Parameters](#producer-configuration-parameters)
7. [Service-Specific Kafka Settings](#7-service-specific-kafka-settings)
    - [Auto-Commit Behavior by Service Criticality](#auto-commit-behavior-by-service-criticality)
    - [Service-Specific Performance Settings](#service-specific-performance-settings)
    - [Configuration Examples by Service](#configuration-examples-by-service)
8. [Other Resources](#8-other-resources)

## 1. Description

Kafka is a high-throughput, distributed messaging system designed to store and process large volumes of data. Its key features include scalability, fault-tolerance, and high availability, making it an ideal choice for real-time data processing and analytics in complex systems like Teranode.

In the Teranode ecosystem, Kafka plays a crucial role in facilitating communication between various components, such as the Validator, BlockValidation, and Blockchain. It enables these components to exchange messages, notifications, and data reliably and efficiently, ensuring smooth operation of the entire system.

It's important to note that Kafka is a third-party dependency in Teranode. As such, there is no specific installation or configuration process provided within the Teranode framework. Users are expected to have a properly configured Kafka setup running before initiating the Teranode services. This approach allows for flexibility in Kafka configuration based on specific deployment needs and existing infrastructure.

## 2. Use Cases

### Propagation Service

After initial sanity check tests, the propagation service endorses transactions to the validator. This is done by sending transaction notifications to the validator via the `kafka_validatortxsConfig` topic.

![kafka_propagation_validator.svg](img/plantuml/kafka_propagation_validator.svg)

- **kafka_validatortxsConfig**: This Kafka topic is used to transmit new transaction notifications from the Propagation component to the Validator.

### Validator Component

![kafka_validator_to_other_services.svg](img/plantuml/kafka_validator_to_other_services.svg)

This diagram illustrates the central role of the Validator in processing new transactions, and how it uses Kafka:

1. The Validator receives new transactions from the Propagation component via the `kafka_validatortxsConfig` topic.

2. Valid transactions are forwarded to the Block Assembly component using **direct gRPC calls** (not Kafka). The Validator uses the `blockAssembler.Store()` method for synchronous transaction processing required for mining candidate generation.

3. The Validator sends new UTXO (Unspent Transaction Output) metadata to the Subtree Validation component through the `kafka_txmetaConfig` topic for inclusion in new subtrees. Should a reversal be required, the same topic is  used to notify a deletion ("delete" command).

4. If a transaction is rejected, the Validator notifies the P2P component via the `kafka_rejectedTxConfig` topic, allowing the network (other peers) to be informed about invalid transactions.

### P2P Service

![kafka_p2p_service_to_others.svg](img/plantuml/kafka_p2p_service_to_others.svg)

The P2P (Peer-to-Peer) service is responsible from peer-to-peer communication, receiving and sending data to other nodes in the network. Here's how it interacts with other components using Kafka:

1. It receives notifications about rejected transactions from the Validator through the `kafka_rejectedTxConfig` topic, allowing it to inform other nodes in the network.

2. The P2P component propagates new blocks (as received from other peers in the network) to the Block Validation component via the `kafka_blocksConfig` topic, initiating the block validation process.

3. New subtrees (as received from other peers in the network) are sent from the P2P component to the Subtree Validation component using the `kafka_subtreesConfig` topic, enabling efficient validation of large transaction sets.

### Blockchain

![kafka_blockchain_to_others2.svg](img/plantuml/kafka_blockchain_to_others2.svg)

This diagram shows the final stage of block processing:

- The Blockchain component sends newly finalized blocks to the Blockpersister component using the `kafka_blocksFinalConfig` topic. This ensures that validated and accepted blocks are permanently stored in the blockchain.

## 3. Reliability and Recoverability

Kafka's role as a critical component in the Teranode system cannot be overstated. Its central position in facilitating the communication of new transactions, remote subtrees, and blocks makes it indispensable for the node's operation.

To maintain system integrity, Teranode is designed to pause operations when Kafka is in an unreliable state. This means:

1. The system will not process new transactions, blocks, or subtrees until Kafka is available and functioning correctly.
2. During Kafka downtime or unreliability, the node enters a safe state, preventing potential data inconsistencies or processing errors.
3. Once Kafka is reported as healthy again, the node automatically resumes normal operation without manual intervention.

## 4. Configuration

For comprehensive configuration documentation including all settings, defaults, and interactions, see the [Kafka Settings Reference](../../references/settings/kafka_settings.md).

## 5. Operational Guidelines

### Performance Tuning

1. **Partition Optimization**
    - Each partition can only be consumed by one consumer in a consumer group
    - Increase partitions to increase parallelism, but avoid over-partitioning
    - General guideline: Start with partitions = number of consumers * 2

2. **Resource Allocation**
    - Kafka is memory-intensive; ensure sufficient RAM
    - Disk I/O is critical; use fast storage (SSDs recommended)
    - Network bandwidth should be sufficient for peak message volumes

3. **Producer Tuning**
    - Batch messages when possible by adjusting `flush_*` parameters
    - Monitor producer queue size and adjust if messages are being dropped

### Reliability Considerations

1. **Replication Factor**
    - Minimum recommended for production: 3
    - Ensures data survives broker failures

2. **Consumer Group Design**
    - Critical services should use dedicated consumer groups
    - Monitor consumer lag to detect processing issues

3. **Error Handling**
    - Services have different retry policies based on criticality
    - Block and subtree validation use manual commits to ensure exactly-once processing

### Monitoring

Key metrics to monitor:

1. **Broker Metrics**
    - CPU, memory, disk usage
    - Network throughput

2. **Topic Metrics**
    - Message rate
    - Byte throughput
    - Partition count

3. **Consumer Metrics**
    - Consumer lag
    - Processing time
    - Error rate

4. **Producer Metrics**
    - Send success rate
    - Retry rate
    - Queue size

## 6. Kafka URL Configuration Parameters

### Consumer Configuration Parameters

When configuring Kafka consumers via URL, the following query parameters are supported:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `partitions` | int | 1 | Number of topic partitions to consume from |
| `consumer_ratio` | int | 1 | Ratio for scaling consumer count (partitions/consumer_ratio) |
| `replay` | int | 1 | Whether to replay messages from beginning (1=true, 0=false) |
| `group_id` | string | - | Consumer group identifier for coordination |

**Example Consumer URL:**

```text
kafka://localhost:9092/transactions?partitions=4&consumer_ratio=2&replay=0&group_id=validator-group
```

### Producer Configuration Parameters

When configuring Kafka producers via URL, the following query parameters are supported:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `partitions` | int | 1 | Number of topic partitions to create |
| `replication` | int | 1 | Replication factor for topic |
| `retention` | string | "600000" | Message retention period (ms) |
| `segment_bytes` | string | "1073741824" | Segment size in bytes (1GB) |
| `flush_bytes` | int | varies | Flush threshold in bytes (1MB async, 1KB sync) |
| `flush_messages` | int | 50000 | Number of messages before flush |
| `flush_frequency` | string | "10s" | Time-based flush frequency |

**Example Producer URL:**

```text
kafka://localhost:9092/blocks?partitions=2&replication=3&retention=3600000&flush_frequency=5s
```

## 7. Service-Specific Kafka Settings

### Auto-Commit Behavior by Service Criticality

Auto-commit in Kafka is a consumer configuration that determines when and how message offsets are committed (marked as processed) back to Kafka. When auto-commit is enabled, Kafka automatically commits message offsets at regular intervals (default is every 5 seconds). When auto-commit is disabled, it is the responsibility of the application to manually commit offsets after successfully processing messages.

Kafka consumer auto-commit behavior varies by service based on processing criticality:

#### Auto-Commit Enabled Services

These services can tolerate potential message loss for performance:

- **TxMeta Cache (Subtree Validation)**: `autoCommit=true`
  - Rationale: Metadata can be regenerated if lost
  - Performance priority over strict delivery guarantees

- **Rejected Transactions (P2P)**: `autoCommit=true`
  - Rationale: Rejection notifications are not critical for consistency
  - Network efficiency prioritized

#### Auto-Commit Disabled Services

These services require exactly-once processing guarantees:

- **Subtree Validation**: `autoCommit=false`
  - Rationale: Transaction processing must be atomic
  - Manual commit after successful processing

- **Block Persister**: `autoCommit=false`
  - Rationale: Block finalization is critical for blockchain integrity
  - Manual commit ensures durability

- **Block Validation**: `autoCommit=false`
  - Rationale: Block processing affects consensus
  - Manual commit prevents duplicate processing

### Kafka Consumer Concurrency

**Important**: Unlike what the service-specific `kafkaWorkers` settings might suggest, Kafka consumer concurrency in Teranode is actually controlled through the `consumer_ratio` URL parameter for each topic. The actual number of consumers is calculated as:

```text
consumerCount = partitions / consumer_ratio
```

Common consumer ratios in use:

- `consumer_ratio=1`: One consumer per partition (maximum parallelism)
- `consumer_ratio=4`: One consumer per 4 partitions (balanced approach)

### Service-Specific Performance Settings

#### Propagation Service Settings

- **`validator_kafka_maxMessageBytes`**: Size threshold for routing decisions
  - **Purpose**: Determines when to use HTTP fallback vs Kafka
  - **Default**: 1048576 (1MB)
  - **Usage**: Large transactions routed via HTTP to avoid Kafka message size limits

#### Validator Service Settings

- **`validator_kafkaWorkers`**: Number of concurrent Kafka processing workers
  - **Purpose**: Controls parallel transaction processing capacity
  - **Tuning**: Should match CPU cores and expected transaction volume
  - **Integration**: Works with Block Assembly via direct gRPC (not Kafka)

#### Block Validation Service Settings

**Note**: Kafka consumer concurrency is actually controlled via the `consumer_ratio` URL parameter, not through service-specific worker settings. The formula is:

```text
consumerCount = partitions / consumer_ratio
```

For example, with 8 partitions and `consumer_ratio=4`, you get 2 consumers.

### Configuration Examples by Service

#### High-Throughput Service (Propagation)

```text
kafka_validatortxsConfig=kafka://localhost:9092/validator-txs?partitions=8&consumer_ratio=2&flush_frequency=1s
validator_kafka_maxMessageBytes=1048576  # 1MB threshold
```

#### Critical Processing Service (Block Validation)

```text
kafka_blocksConfig=kafka://localhost:9092/blocks?partitions=4&consumer_ratio=1&replay=0
blockvalidation_kafkaWorkers=4
autoCommit=false  # Manual commit for reliability
```

#### Metadata Service (Subtree Validation)

```text
kafka_txmetaConfig=kafka://localhost:9092/txmeta?partitions=2&consumer_ratio=1&replay=1
autoCommit=true   # Performance over strict guarantees
```

## 8. Other Resources

- [Kafka Message Format](../../references/kafkaMessageFormat.md)
- [Block Data Model](../datamodel/block_data_model.md): Contain lists of subtree identifiers.
- [Subtree Data Model](../datamodel/subtree_data_model.md): Contain lists of transaction IDs and their Merkle root.
- [Extended Transaction Data Model](../datamodel/transaction_data_model.md): Includes additional metadata to facilitate processing.
