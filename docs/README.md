# 🚀 UBSV
> Unbounded Bitcoin Satoshi Vision

## Index

1. [Introduction](#1-introduction)
2. [Getting Started](#2-getting-started)
- [2.1. Pre-requisites and Installation](#21-pre-requisites-and-installation)
- [Running the Node and Individual Services Locally for Development](#running-the-node-and-individual-services-locally-for-development)
3. [Advanced Usage](#3-advanced-usage)
- [3.1. Settings](#31-settings)
- [3.2. Makefile](#32-makefile)
- [3.3. Proto buffers (protoc)](#33-proto-buffers-protoc)
- [3.4. Running Tests](#34-running-tests)
- [3.5. gRPC Logging](#35-grpc-logging)
4. [Running the Node in Production](#4-running-the-node-in-production)
5. [Architecture](#5-architecture)
6. [Micro-Services](#6-micro-services)
7. [Technology](#7-technology)
- [7.1. Go](#71-go)
- [7.2. gRPC](#72-grpc)
- [7.3. fRPC and DRPC](#73-frpc-and-drpc)
- [7.4. Protobuf](#74-protobuf)
- [7.5. IPv6 Multicast](#75-ipv6-multicast)
- [7.6. Stores](#76-stores)
8. [Project Structure and Coding Conventions](#8-project-structure-and-coding-conventions)
- [8.1 Directory Structure and Descriptions:](#81-directory-structure-and-descriptions)
- [8.2. Coding Conventions](#82-coding-conventions)
- [8.3. Error Handling](#83-error-handling)
9. [License](#9-license)


## 1. Introduction

---

The Bitcoin (BTC) _scalability issue_ refers to the challenge faced by the historical Bitcoin network in processing a large number of transactions efficiently. Originally, the Bitcoin block size, where transactions are recorded, was limited to 1 megabyte. This limitation meant that the network could only handle an average of **3.3 to 7 transactions per second**. As Bitcoin's popularity grew, this has led to delayed transaction processing and higher fees.

With BSV, the block size limit was increased to 4 gigabyte, thereby also increasing the performance of the network. However, the current BSV node software (SV Node) has its own limitations, especially related to scalability and performance, limiting the current network to several thousand transactions per second.

**UBSV** is BSV’s solution to the challenges of vertical scaling by instead spreading the workload across multiple machines. This horizontal scaling approach, coupled with an unbound block size, enables network capacity to grow with increasing demand through the addition of cluster nodes, allowing for BSV scaling to be truly unbounded.

UBSV provides a robust node processing system for BSV that can consistently handle over **1M transactions per second**, while strictly adhering to the Bitcoin whitepaper.
The node has been designed as a collection of microservices, each handling specific functionalities of the BSV network.

---

## 2. Getting Started

---

### 2.1. Pre-requisites and Installation

To be able to run the node locally, please check the [Installation Guide for Developers and Contributors](docs/developerSetup.md).

### Running the Node and Individual Services Locally for Development

Please refer to the [Locally Running Services Documentation](docs/locallyRunningServices.md) for detailed instructions on how to run the node and / or individual services locally in development.

---

## 3. Advanced Usage

### 3.1. Settings

All services accept settings allowing local and remote servers to have their own specific configuration.

For more information on how to create and use settings, please check the [Settings Documentation](docs/settings.md).

### 3.2. Makefile

The Makefile facilitates a variety of development and build tasks for the UBSV project.

Check the [Makefile Documentation](docs/makefile.md) for detailed documentation. Some use cases will be highlighted here:

### 3.3. Proto buffers (protoc)

You can generate the protobuf files by running the following command:

```shell
make gen
```

You can read more about proto buffers in the Technology section.

For additional make commands, please check the [Makefile Documentation](docs/makefile.md).

### 3.4. Running Tests

There are 2 commands to run tests:

```shell
make test  # Executes Go tests excluding the playground and PoC directories.
```

```shell
make testall  # Executes Go tests excluding the playground and PoC directories.
```

### 3.5. gRPC Logging

Additional logs can be produced when the node is run with the following environment variables set: `GRPC_VERBOSITY=debug GRPC_TRACE=client_channel,round_robin`



---

## 4. Running the Node in Production

-- TODO

---


---

## 5. Architecture

---

Please check the [Architecture Documentation](docs/architecture/teranode-architecture.md) for an introduction to the overall architecture of the node.

Additional documentation on the Network Consensus Rules can be found in the [Network Consensus Rules](docs%2FnetworkConsensusRules.md) document.

---

## 6. Micro-Services

---

Detailed Node Service documentation:

+ [Asset Server](docs/services/assetServer.md)

+ [Propagation Service](docs/services/propagation.md)

+ [Validator Service](docs/services/validator.md)

+ [Subtree Validation Service](docs/services/subtreeValidation.md)

+ [Block Validation Service](docs/services/blockValidation.md)

+ [Block Assembly Service](docs/services/blockAssembly.md)

+ [Blockchain Service](docs/services/blockchain.md)

Store Documentation:

+ [Blob Server](docs/stores/blob.md)

+ [UTXO Store](docs/stores/utxo.md)

+ [TXMeta Service](docs/stores/txmeta.md)

Overlay Service documentation:

+ [Block Persister Service](docs/services/blockPersister.md)
+ [P2P Service](docs/services/p2p.md)
+ [P2P Bootstrap Service](docs/services/p2pBootstrap.md)
+ [P2P Legacy Service](docs/services/p2pLegacy.md)
+ [Bootstrap (Deprecated)](docs/services/bootstrap.md)

Additionally, the system has a number of test / development utilities:

+ [TX Blaster](docs/commands/txBlaster.md)
+ [UTXO Blaster](docs/commands/utxoBlaster.md)
+ [Propagation Blaster](docs/commands/propagationBlaster.md)
+ [Chain Integrity](docs/commands/chainIntegrity.md)

---

## 7. Technology

---

### 7.1. Go

The project is written in Go. Go, often referred to as Golang, is an open-source programming language created at Google in 2007 and officially announced in 2009. Go was designed with a focus on simplicity, efficiency, and productivity, making it well-suited for building scalable and reliable software systems.

Key characteristics and features of the Go programming language include:

1. **Concurrency:** Go includes built-in support for concurrent programming through goroutines and channels. Goroutines are lightweight threads that allow developers to write concurrent code easily. Channels facilitate communication and synchronization between goroutines.

2. **Efficiency:** Go is known for its high performance and efficient resource utilization. It compiles to native machine code, and its runtime system is designed to manage memory efficiently.

3. **Simplicity:** Go has a clean and minimalistic syntax, which makes it easy to read and write code. The language was intentionally designed to be simple and straightforward to reduce complexity and bugs.

4. **Strong Typing:** Go is statically typed, which means that variable types are determined at compile-time. This helps catch many errors early in the development process.

5. **Garbage Collection:** Go includes a garbage collector that automatically manages memory allocation and deallocation, relieving developers from the burden of manual memory management.

6. **Standard Library:** Go has a rich standard library that covers a wide range of tasks, from networking and file I/O to web development and more. This reduces the need for third-party libraries in many cases.

7. **Cross-Platform:** Go is designed to be cross-platform, and it supports various operating systems and architectures. You can write Go code on one platform and compile it for another without modification.

8. **Open Source:** Go is open source, which means that the source code is freely available for anyone to use, modify, and distribute.

9. **Community:** The Go programming language has a strong and active community of developers, which results in a wealth of resources, documentation, and third-party libraries.

Read more on: https://go.dev/


### 7.2. gRPC

**gRPC** is an open-source framework for efficient and high-performance communication between services in distributed systems. Key features include:

1. **Language Agnostic:** Use Protocol Buffers for language-agnostic service definitions.

2. **Efficiency:** Built on HTTP/2 for faster, multiplexed communication.

3. **Strong Typing:** Clearly defined service contracts and data types.

4. **Bidirectional Streaming:** Supports both unary and bidirectional streaming.

5. **Security:** Pluggable authentication and authorization mechanisms.

6. **Code Generation:** Auto-generates client and server code in multiple languages.

7. **Middleware:** Easily add interceptors and middleware for cross-cutting concerns.

8. **Load Balancing:** Automatic client-side load balancing for reliability.

gRPC is widely used for building microservices. It offers efficient and strongly typed communication between services.

Read more on: https://grpc.io/docs/what-is-grpc/introduction/


### 7.3. fRPC and DRPC

fRPC (or Frisbee RPC), is an RPC Framework (similar to gRPC or Apache Thrift) that’s designed from the ground up to be lightweight, extensible, and extremely performant. It allows autogeneration from your existing Protobuf definition files.

Read more on: https://frpc.io/


DRPC is a lightweight, drop-in, protocol buffer-based gRPC replacement. DRPC is small, extensible, efficient, and can still be autogenerated from your existing Protobuf definition files.

Read more on: https://storj.github.io/drpc/

Both fRPC and DRPC are widely used as an experimental technology in the project. gRPC is still the reference RPC implementation for the project.

### 7.4. Protobuf


**Protocol Buffers (Protobuf)** is a language-agnostic data serialization format developed by Google. Key features include:

1. **Efficiency:** Compact binary format for efficient data transmission and storage.
2. **Strong Typing:** Enforces well-defined data types for early error detection.
3. **Code Generation:** Generates code for data serialization and deserialization.
4. **Backward Compatibility:** Supports evolving data schemas without breaking existing code.
5. **Extensibility:** Allows adding fields and options to messages without compatibility issues.

You can learn more and find detailed documentation about Protobuf on the official website:
[Google's Protocol Buffers](https://developers.google.com/protocol-buffers)

Also, read about how to generate this project's protobuf files in the [3.3. Proto buffers (protoc)](#33-proto-buffers-protoc) section.

### 7.5. IPv6 Multicast

IPv6 multicast is a communication method in IPv6 networks that allows one-to-many and many-to-many communication. It is used for sending data packets from one source to multiple receivers efficiently, especially in scenarios where multiple recipients want to receive the same data simultaneously. IPv6 multicast is an improvement over IPv4 multicast and offers several advantages, including a larger address space and simplified addressing.

In terms of the UBSV project, IPv6 multicast is used for the propagation of transactions across the network. An originator will broadcast a transaction to a multicast group, and all nodes that have joined that group will receive the transaction. This is a much more efficient way of propagating transactions across the network, as opposed to the traditional unicast method, where the originator would have to send the transaction to each node individually.

Here are some key points about IPv6 multicast:

1. **Multicast Addressing:** IPv6 multicast uses a specific range of addresses called "IPv6 multicast addresses." These addresses begin with the prefix `ff00::/8`. The last 120 bits of the address are used to specify the multicast group, and the remaining 8 bits are used for flags.

2. **Multicast Groups:** Devices that want to receive multicast traffic join specific multicast groups by expressing interest in those groups. They do this by sending a multicast group membership report to their local router. Routers use this information to forward multicast traffic only to devices interested in a particular group.

3. **Efficiency:** IPv6 multicast is efficient because it eliminates the need for sending multiple unicast copies of data to individual recipients. Instead, a single copy of the data is sent to a multicast group, and routers replicate and forward the data only to devices that have joined that group.

4. **Scope:** IPv6 multicast addresses have different scopes, indicating the range of the multicast group's visibility. Common multicast address scopes include link-local, site-local, and global scopes. The scope determines how far the multicast traffic can propagate in the network.

5. **Applications:** IPv6 multicast is commonly used in various applications, such as video streaming, online gaming, content distribution networks (CDNs), network management, and more. It is particularly useful for delivering real-time data to multiple recipients, reducing network congestion and improving scalability.

6. **Routing:** Multicast routing protocols, such as Protocol Independent Multicast (PIM), are used to manage and control the forwarding of multicast traffic within an IPv6 network. These protocols ensure that multicast data reaches the intended recipients efficiently.

**NOTE:** IPv6 Multicast support is currently not enabled or configured in the test infrastructure as our current hosting platform, Amazon Web Services, does not support the correct features we require. Specifically, there is no support for MLDv1 or MLDv2 within a VPC subnet, no multicast packets over VPC peering connections without using a cumbersome and costly service, and also strict restrictions on network service architecture which would impose additional costs and require complex engineering to workaround. We have multicast support written inside the codebase but this needs more implementation and testing to be effective as a true solution.

### 7.6. Stores

The system uses a number of different store technologies to store data. Different microservices allow different storage options, please check the specific service documentation for more details. The following stores are the most commonly used:

1. **Aerospike**:
  - A high-performance, NoSQL distributed database.
  - Suitable for environments requiring high throughput and low latency.
  - Handles large volumes of data with fast read/write capabilities.
  - https://aerospike.com.

2. **Memory (In-Memory Store)**:
  - Stores data directly in the application's memory.
  - Offers the fastest access times but lacks persistence; data is lost if the service restarts.
  - Useful for development or testing purposes.

3. **Redis**:
  - An in-memory data structure store, used as a database, cache, and message broker.
  - Offers fast data access and can persist data to disk.
  - Useful for scenarios requiring rapid access combined with the ability to handle volatile or transient data.
  - https://redis.com.

4. **SQL**:
  - SQL-based relational database implementations like PostgreSQL, SQLite, or SQLite in-memory variant.
  - PostgreSQL: Offers robustness, advanced features, and strong consistency, suitable for complex queries and large datasets.
    - https://www.postgresql.org.

5. **Shared Storage**:
  - Clustered, high-availability, low-latency shared filesystems provided by AWS FSx for Lustre service.
  - Subtree Store: For shared subtree data
    - /data/subtreestore
  - Block Store: For shared block data
    - /data/blockstore
  - These volumes are meant to be temporary holding locations for short-lived file-based data that needs to be shared quickly between various services.
  - Files are deleted from the base directory of each file store after 360 minutes (6 hours).
  - No file-locking is provided, so the application must be aware of when a file-write is completed and it is safe to be read by other services.
  - Filesystem consistent-state latency should be in the sub-millisecond range.
  - There is a /s3 subfolder in each filesystem that facilitates automatic transfer of data to an associated S3 bucket.
  - File data on all objects within the /s3 subfolder are released from the filesystem every hour, within a 30 minute flexible time window. The filesystem metadata will still be available, but the actual file contents will be cleared.
  - Once data is archived off to S3, the best way to read it again is using S3 direct API calls
  - Linked S3 archive buckets:
    - s3://ap-ubsv-subtree-store
    - s3://ap-ubsv-block-store
    - s3://eu-ubsv-subtree-store
    - s3://eu-ubsv-block-store
    - s3://us-ubsv-subtree-store
    - s3://us-ubsv-block-store


---

## 8. Project Structure and Coding Conventions


### 8.1 Directory Structure and Descriptions:

```
ubsv/
│
├── main.go                       # Start the services.
│
├── main_native.go                # Start the services in native secp256k1 mode.
│
├── Makefile                      # This Makefile facilitates a variety of development and build tasks for our project.
│
├── settings.conf                 # Global settings
│
├── settings_local.conf           # Local overridden settings
│
├── certs/                        # Project dev self-signed and ca certificates
│
├── cmd/                          # Directory containing all different Commands
│   ├── chainintegrity/           # Utility to verify the integrity of the blockchain.
│   ├── propagation_blaster/      # Utility to load test the Propagation service
│   ├── s3_blaster/               # Utility to load test the S3 service
│   ├── seeder_blaster/           # Utility to load test the Seeder service
│   ├── sutos_blaster/            # Utility to load test the SUTOS service
│   ├── txblaster_blaster/        # Utility to load test the TxBlaster service
│   └── utxostore_blaster/        # Utility to load test the UTXO Store service
│
├── data/                         # Local node data directory, as required by local databases
│
├── deploy/                       # Deployment scripts for the project (Docker, k8s, Kafka, others)
│
├── docs/                         # Documentation for the project
│
├── k8sresolver/                  # Kubernetes resolver for gRPC.
│
├── model/                        # Key model definitions for the project
│
├── native/                       # Native signature implementation for secp256k1
│
├── scripts/                      # Various scripts
│
├── services/                     # Directory containing all different Services
│   ├── blobserver/               # Blob Server Service
│   ├── blockassembly/            # Block Assembly Service
│   ├── blockchain/               # Blockchain Service
│   ├── blockpersister/           # Block Persister Service
│   ├── blockvalidation/          # Block Validation Service
│   ├── legacy/                   # P2P Legacy Service
│   ├── coinbase/                 # Coinbase Service
│   ├── miner/                    # Miner Service
│   ├── p2p/                      # P2P Service
│   ├── propagation/              # Propagation Service
│   ├── subtreevalidation/        # Subtree Validation Service
│   └── validator/                # Validator Service
│
├── stores/                       # This directory contains the different stores used by the node.
│   ├── blob/                     # A collection of supported or experimental stores for the Blob service.
│   ├── blockchain/               # A collection of supported or experimental stores for the Blockchain service.
│   ├── txmeta/                   # A collection of supported or experimental stores for the TXMeta service.
│   └── utxo/                     # A collection of supported or experimental stores for the UTXO service.
│
├── tracing/                      # Tracing, Stats and Metric utilities
│
├── ui/
│   └── dashboard/                # Teranode Dashboard UI
│
└── util/                         # Utilities

```


### 8.2. Coding Conventions

For naming conventions please check the [Naming Conventions](docs/guidelines/namingConventions.md).

### 8.3. Error Handling

-- TODO


---


## 9. License

---
**Copyright © 2024 BSV Blockchain Org. All rights reserved.**

No part of this software may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the author.

_Unauthorized duplication, distribution, or modification of this software, in whole or in part, is strictly prohibited._

---
