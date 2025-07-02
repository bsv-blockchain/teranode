# 🔧 Technology Stack

## Go

Teranode is written in **Go**, leveraging its efficiency and simplicity to build scalable, concurrent microservices. **Go** was chosen for its high performance, especially when dealing with concurrent processes, which is crucial for handling the scale of blockchain validation and transaction processing within Teranode.

### Specific Usage in Teranode:

1. **Concurrency with Goroutines**: In Teranode, Go's goroutines are extensively used to manage concurrent processes like transaction validation and block propagation. This allows the system to handle multiple tasks simultaneously without compromising speed.

2. **Efficient Resource Utilization**: Go’s memory management and garbage collection are vital for managing large amounts of data efficiently.

3. **Cross-Platform Compilation**: Since Teranode is designed to operate across various environments, Go’s cross-platform capabilities ensure that binaries can be compiled for different systems seamlessly.

For more details on Go, visit the [official site](https://go.dev/).

---

## gRPC

**gRPC** is employed in Teranode to enable efficient and fast communication between its various microservices. As Teranode is composed of multiple independent services (e.g., block validation, transaction propagation), gRPC ensures low-latency, high-throughput communication, which is essential for a system processing blockchain transactions in real-time.

### Specific Usage in Teranode:

1. **Service Definitions in Protobuf**: Teranode defines its RPC services using Protobuf files, which are compiled into Go code to generate gRPC stubs for the client and server.

2. **Bidirectional Streaming**: Certain operations in Teranode, such as transaction streaming or real-time blockchain updates, use gRPC's bidirectional streaming capabilities to maintain a continuous flow of data between nodes or services, enhancing performance.

Read more on gRPC [here](https://grpc.io/docs/what-is-grpc/introduction/).

---

## Protobuf

**Protocol Buffers (Protobuf)** is the serialization format used in Teranode to define the messages exchanged between services. This ensures that data (such as blocks, transactions, and validation results) is serialized in a compact binary format, minimizing the payload size for efficient communication.

### Specific Usage in Teranode:

1. **Data Serialization**: In Teranode, Protobuf is used to define the data structures for blockchain transactions, blocks, and validation requests. These definitions are stored in `.proto` files and compiled into Go, allowing the system to serialize and deserialize these structures efficiently.

2. **Backward Compatibility**: Teranode leverages Protobuf’s backward compatibility to evolve the blockchain system without breaking older services.

You can find more on Protobuf [here](https://developers.google.com/protocol-buffers).

---

## IPv6 Multicast

**IPv6 Multicast** is considered in Teranode for propagating transactions across the network. Although it is currently not supported due to limitations in the hosting platform (AWS), the concept is vital for future scalability. Multicast allows a node to broadcast transactions to multiple peers simultaneously, making it highly efficient for distributing blockchain data.

### Specific Usage in Teranode:

1. **Transaction Propagation**: The idea behind using IPv6 multicast in Teranode is to enable a single node to broadcast a transaction to all connected nodes in the multicast group, drastically reducing the overhead required to propagate transactions individually.

2. **Network Efficiency**: Although AWS does not yet support IPv6 multicast, Teranode's architecture is designed to benefit from multicast as a future optimization to reduce network traffic and improve transaction dissemination speeds.

---

## Stores

Teranode employs various storage technologies, each selected for its specific strengths in terms of speed, scalability, and reliability. The choice of storage depends on the service's needs, such as real-time transaction processing, persistent blockchain storage, or temporary data caching.

### Specific Usage in Teranode:

1. **Aerospike**:

    - Teranode uses **Aerospike** for its distributed, high-performance NoSQL database capabilities. Aerospike is particularly useful for handling large volumes of transactions that require fast, consistent reads and writes.
    - Example: Transaction metadata, such as UTXOs (Unspent Transaction Outputs), are stored in Aerospike for fast retrieval during validation.

2. **Memory (In-Memory Store)**:

    - In-memory storage is used for caching frequently accessed data, providing the fastest access time but without persistence.

3. **SQL**:

    - For services that require robust querying capabilities, Teranode uses SQL databases like PostgreSQL. SQL is ideal for storing large sets of structured data, such as blockchain metadata, that need complex querying capabilities.
    - Example: Block metadata is stored in SQL for easy querying and analysis.

4. **Shared Storage**:

    - Teranode also uses shared filesystems, especially for services that need to share large datasets across multiple nodes. Lustre file systems, combined with S3-backed shared volumes, offer a high-throughput storage solution for services such as block and transaction persistence.

------


## Kafka

**Apache Kafka** is employed in Teranode for event-driven architectures, enabling real-time data pipelines. Kafka handles the ingestion and distribution of blockchain-related events, ensuring that all nodes are updated with the latest state of the network.

### Specific Usage in Teranode:

1. **Event Streaming**: Kafka is used to stream real-time events like transaction creation and block mining. These events are then consumed by different microservices to update the blockchain state or trigger validations.

2. **High Availability**: Kafka’s fault tolerance and replication ensure that event streams are durable and highly available, which is essential for the integrity of a distributed system like Teranode.


------


## Containerization

In Teranode, containerization is a crucial part of the deployment and scaling strategy. The system uses **Docker** for packaging services into isolated containers, ensuring consistency across different environments. **Kubernetes** is used for orchestrating these containers, allowing for automatic scaling, load balancing, and self-healing.

### Docker:
1. **Service Isolation**: Each microservice, like the block validation service, is packaged into its own Docker container. This ensures isolated runtime environments, enabling easier management and updates.

2. **Portability**: Docker ensures that the service runs consistently across development, testing, and production environments, minimizing deployment issues.

### Kubernetes:
1. **Orchestration**: Kubernetes is used to orchestrate and manage the various containers that make up the Teranode system. It ensures that the system scales automatically based on traffic and workload, and that containers are restarted if they fail.

2. **Service Discovery and Load Balancing**: Kubernetes ensures that the communication between microservices is seamless, using built-in service discovery mechanisms. It also provides load balancing to distribute traffic evenly across services.

---
