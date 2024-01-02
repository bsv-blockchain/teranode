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
4. [Running the Node in Production  [ TO-DO ]](#4-running-the-node-in-production---to-do-)
5. [Architecture](#5-architecture)
6. [Micro-Services](#6-micro-services)
7. [Technology](#7-technology)
8. [Project Structure and Coding Conventions](#8-project-structure-and-coding-conventions)
- [8.1 Directory Structure and Descriptions:](#81-directory-structure-and-descriptions)
- [8.2. Coding Conventions](#82-coding-conventions)
- [8.3. Error Handling](#83-error-handling)
- [8.4. Logging](#84-logging)
- [8.5. Testing Conventions](#85-testing-conventions)
9. [License](#9-license)



## 1. Introduction

---

The Bitcoin (BTC) _scalability issue_ refers to the challenge faced by the historical Bitcoin network in processing a large number of transactions efficiently. Originally, the Bitcoin block size, where transactions are recorded, was limited to 1 megabyte. This limitation meant that the network could only handle an average of **3.3 to 7 transactions per second**. As Bitcoin's popularity grew, this has led to delayed transaction processing and higher fees.

**UBSV** is BSV’s solution to the challenges of vertical scaling by instead spreading the workload across multiple machines. This horizontal scaling approach, coupled with an unbound block size, enables network capacity to grow with increasing demand through the addition of cluster nodes, allowing for BSV scaling to be truly unbounded.

UBSV provides a robust node processing system for BSV that can consistently handle over **1M transactions per second**, white strictly adhering to the Bitcoin whitepaper.
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

## 4. Running the Node in Production  [ TO-DO ]

---


---

## 5. Architecture

---

Please check the [Architecture Documentation](docs/architecture/architecture.md) for an introduction to the overall architecture of the node.


---

## 6. Micro-Services

---

Detailed Node Service documentation:

+ [Asset Server](docs/services/assetServer.md)

+ [Propagation Service](docs/services/propagation.md)

+ [Validator Service](docs/services/validator.md)

+ [Block Validation Service](docs/services/blockValidation.md)

+ [Block Assembly Service](docs/services/blockAssembly.md)

+ [Blockchain Service](docs/services/blockchain.md)

Store Documentation:

+ [Blob Server](docs/stores/blob.md)

+ [UTXO Store](docs/stores/utxo.md)

+ [TXMeta Service](docs/stores/txmeta.md)

Overlay Service documentation:

+ [Coinbase](docs/services/coinbase.md)

+ [P2P](docs/services/p2p.md)
+ [Bootstrap (Deprecated)](docs/services/bootstrap.md)


---

## 7. Technology

---




* grpc
  -- gRPC vs IPV6 multicast
  -- — https://grpc.io/docs/what-is-grpc/introduction/
* protobuf
* Stores (options)
* Docker
* Kubernetes
  * [Kubernetes Resolver for gRPC](k8sresolver/README.md)


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
│   ├── blockvalidation/          # Block Validation Service
│   ├── bootstrap/                # Bootstrap Service
│   ├── coinbase/                 # Coinbase Service
│   ├── miner/                    # Miner Service
│   ├── p2p/                      # P2P Service
│   ├── propagation/              # Propagation Service
│   ├── seeder/                   # Seeder Service
│   ├── txmeta/                   # TXMeta Service
│   ├── utxo/                     # UTXO Service
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

### 8.4. Logging

### 8.5. Testing Conventions


---


## 9. License

---
**Copyright © 2024 BSV Blockchain Org. All rights reserved.**

No part of this software may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the author.

_Unauthorized duplication, distribution, or modification of this software, in whole or in part, is strictly prohibited._

---
