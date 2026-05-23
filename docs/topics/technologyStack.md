# 🔧 Technology Stack

## Go (1.26.0 or above)

Teranode is written in **Go**, leveraging its efficiency and simplicity to build scalable, concurrent microservices. **Go** was chosen for its high performance, especially when dealing with concurrent processes, which is crucial for handling the scale of blockchain validation and transaction processing within Teranode.

### Specific Usage in Teranode

1. **Concurrency with Goroutines**: In Teranode, Go's goroutines are extensively used to manage concurrent processes like transaction validation and block propagation. This allows the system to handle multiple tasks simultaneously without compromising speed.

2. **Efficient Resource Utilization**: Go's memory management and garbage collection are vital for managing large amounts of data efficiently.

3. **Cross-Platform Compilation**: Since Teranode is designed to operate across various environments, Go's cross-platform capabilities ensure that binaries can be compiled for different systems seamlessly.

For more details on Go, visit the [official site](https://go.dev/).

---

## gRPC

**gRPC** is employed in Teranode to enable efficient and fast communication between its various microservices. As Teranode is composed of multiple independent services (e.g., block validation, transaction propagation), gRPC ensures low-latency, high-throughput communication, which is essential for a system processing blockchain transactions in real-time.

### Specific Usage in Teranode

1. **Service Definitions in Protobuf**: Teranode defines its RPC services using Protobuf files, which are compiled into Go code to generate gRPC stubs for the client and server.

2. **Bidirectional Streaming**: Certain operations in Teranode, such as transaction streaming or real-time blockchain updates, use gRPC's bidirectional streaming capabilities to maintain a continuous flow of data between nodes or services, enhancing performance.

Read more on gRPC [here](https://grpc.io/docs/what-is-grpc/introduction/).

---

## Protobuf

**Protocol Buffers (Protobuf)** is the serialization format used in Teranode to define the messages exchanged between services. This ensures that data (such as blocks, transactions, and validation results) is serialized in a compact binary format, minimizing the payload size for efficient communication.

### Specific Usage in Teranode

1. **Data Serialization**: In Teranode, Protobuf is used to define the data structures for blockchain transactions, blocks, and validation requests. These definitions are stored in `.proto` files and compiled into Go, allowing the system to serialize and deserialize these structures efficiently.

2. **Backward Compatibility**: Teranode leverages Protobuf’s backward compatibility to evolve the blockchain system without breaking older services.

You can find more on Protobuf [here](https://developers.google.com/protocol-buffers).

---

## P2P Networking (libp2p)

**libp2p** is the modular peer-to-peer networking stack used in Teranode for node discovery and communication. It provides a robust foundation for building distributed systems with features like peer discovery, secure communication, and publish-subscribe messaging.

### Specific Usage in Teranode

1. **Peer Discovery with Kademlia DHT**: Teranode uses Kademlia Distributed Hash Table for discovering and maintaining connections with other nodes in the network, ensuring a well-connected mesh topology.

2. **GossipSub for Message Broadcasting**: The GossipSub protocol is employed for efficient message propagation across the network, allowing nodes to broadcast blocks and transactions to multiple peers simultaneously.

3. **Persistent Peer Identity**: Each node maintains a persistent private key for its peer ID and encryption, ensuring consistent identity across restarts and secure communications.

4. **Future IPv6 Multicast Support**: While currently using libp2p, Teranode's architecture is designed to potentially benefit from IPv6 multicast in the future for even more efficient transaction dissemination, though this is currently limited by AWS platform constraints.

---

## Stores

Teranode employs various storage technologies, each selected for its specific strengths in terms of speed, scalability, and reliability. The choice of storage depends on the service's needs, such as real-time transaction processing, persistent blockchain storage, or temporary data caching.

### Specific Usage in Teranode

1. **Aerospike**:

    - Teranode uses **Aerospike** as a distributed, high-performance NoSQL database for UTXO (Unspent Transaction Output) storage
    - Provides fast, consistent reads and writes for transaction validation
    - Handles UTXO state management including spent/unspent/frozen states
    - Supports cleanup operations for transaction lifecycle management

2. **PostgreSQL**:

    - Used for blockchain metadata storage requiring complex queries
    - Stores block headers, chain state, and FSM (Finite State Machine) state
    - Provides SQL querying capabilities for blockchain analysis
    - Maintains block statistics and chain organization data

3. **Blob Storage**:

    - Multiple implementations for flexible deployment:

        - **S3**: AWS S3 for cloud-based blob storage
        - **File System**: Local file system for development/testing
        - **HTTP**: HTTP-based blob storage service
        - **Memory**: In-memory storage for caching and testing
    - Stores blocks, transactions, and Merkle subtrees
    - Supports concurrent access with batching capabilities

4. **Shared Storage**:

     - Enables services to share large datasets across multiple nodes
     - Used for block and transaction persistence in distributed deployments
     - Lustre file systems (provided via S3-backed shared volumes) offer high-throughput storage solution, and it is a recommended shared storage option

------

## Kafka

**Apache Kafka** is employed in Teranode for event-driven architectures, enabling real-time data pipelines. Kafka handles the ingestion and distribution of blockchain-related events, ensuring that all nodes are updated with the latest state of the network.

### Specific Usage in Teranode

1. **Event Streaming**: Kafka is used to stream real-time events like transaction creation and block mining. These events are then consumed by different microservices to update the blockchain state or trigger validations.

2. **High Availability**: Kafka’s fault tolerance and replication ensure that event streams are durable and highly available, which is essential for the integrity of a distributed system like Teranode.

------

## Containerization & Orchestration

Teranode uses containerization as a crucial part of its deployment and scaling strategy, with **Docker** for packaging services and **Docker Compose** for local / test orchestration, while supporting **Kubernetes** for production deployments.

### Docker

1. **Service Isolation**: Each microservice (validator, block assembly, blockchain, etc.) is packaged into its own Docker container, ensuring isolated runtime environments and easier management.

2. **Multi-stage Builds**: Uses optimized multi-stage Dockerfile with separate build and runtime images to minimize container size and improve security.

3. **Portability**: Docker ensures services run consistently across development, testing, and production environments.

### Docker Compose

1. **Local Development & Testing**: Primary orchestration tool for local development and testing environments

2. **Multi-node Testing**: Supports complex multi-node setups for integration testing (e.g., 3-node test configurations)

3. **Service Dependencies**: Manages service startup order and inter-service dependencies

### Kubernetes (Production)

1. **Production Orchestration**: Kubernetes can be used for production deployments, providing automatic scaling, load balancing, and self-healing capabilities.

2. **Service Discovery**: Ensures seamless communication between microservices using built-in service discovery mechanisms.

3. **Scalability**: Enables horizontal scaling of services based on workload demands.

---

## Real-time Communication

### WebSocket & Centrifuge

Teranode uses **WebSocket** connections and the **Centrifuge** library for real-time bidirectional communication between clients and services.

### Specific Usage in Teranode

1. **Real-time Updates**: WebSocket connections enable real-time blockchain updates, transaction notifications, and state changes to be pushed to connected clients.

2. **Centrifuge Framework**: Provides a scalable real-time messaging server with features like:

    - Channel subscriptions for different event types
    - Presence information for connected clients
    - Automatic reconnection handling
    - Message history and recovery

3. **Asset Service Integration**: The Asset Server uses WebSocket/Centrifuge for streaming blockchain data to clients in real-time.

---

## Observability & Monitoring

Teranode implements comprehensive observability to monitor system health, performance, and debug issues in the distributed environment.

### Prometheus

1. **Metrics Collection**: Collects and stores time-series metrics from all services
2. **Custom Metrics**: Tracks blockchain-specific metrics like transaction throughput, block validation times, UTXO set size
3. **Alerting**: Configured alerts for system health and performance thresholds

### OpenTelemetry & Jaeger

1. **Distributed Tracing**: Traces requests across multiple services to identify bottlenecks
2. **Span Collection**: Collects detailed timing information for each operation
3. **Jaeger Backend**: Provides UI for visualizing and analyzing traces
4. **Context Propagation**: Maintains trace context across gRPC and HTTP calls

### Logging

1. **Structured Logging**: Uses structured JSON logging for easy parsing and analysis
2. **Log Aggregation**: Centralized log collection for debugging distributed issues
3. **Log Levels**: Configurable log levels per service for detailed debugging

---

## Testing Infrastructure

### TestContainers

Teranode uses **TestContainers** for integration and end-to-end testing, providing isolated, reproducible test environments.

1. **Container Management**: Automatically starts and stops Docker containers for tests
2. **Service Dependencies**: Manages complex test setups with multiple services (Aerospike, PostgreSQL, Kafka)
3. **Network Isolation**: Creates isolated networks for each test suite
4. **Cleanup**: Ensures proper cleanup of resources after test completion

### Testing Framework

1. **Multi-node Testing**: Supports testing with multiple Teranode instances
2. **State Management**: Provides consistent initial blockchain state for tests
3. **Mock Services**: Includes mock implementations for testing service interactions
4. **Performance Testing**: Infrastructure for load testing and benchmarking

---
