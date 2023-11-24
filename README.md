# 🚀 UBSV
> Unbounded Bitcoin Satoshi Vision

## Index

- [Introduction](#introduction)
- [Getting Started](#getting-started)
  - [Pre-requisites and Installation](#pre-requisites-and-installation)
  - [Running the Node and Individual Services Locally for Development](#running-the-node-and-individual-services-locally-for-development)
- [Advanced Usage  [ TO-DO ]](#advanced-usage---to-do-)
  - [Settings](#settings)
  - [Makefile](#makefile)
    - [Proto buffers (protoc)](#proto-buffers-protoc)
    - [Running Tests](#running-tests)
  - [gRPC Logging](#grpc-logging)
- [Running the Node in Production  [ TO-DO ]](#running-the-node-in-production---to-do-)
- [Architecture](#architecture)
- [Micro-Services](#micro-services)
- [Technology  [ TO-DO ]](#technology---to-do-)
- [Project Structure and Coding Conventions](#project-structure-and-coding-conventions)
  - [Project Structure](#project-structure)
  - [Directory Structure and Descriptions:](#directory-structure-and-descriptions)
  - [Coding Conventions](#coding-conventions)
  - [TODO ERROR HANDLING, LOGGING, ...](#todo---error-handling-logging-)
  - [Testing Conventions [TODO]](#testing-conventions---todo)
- [License](#license)





## Introduction

---

The Bitcoin (BTC) _scalability issue_ refers to the challenge faced by the historical Bitcoin network in processing a large number of transactions efficiently. Originally, the Bitcoin block size, where transactions are recorded, was limited to 1 megabyte. This limitation meant that the network could only handle an average of **3.3 to 7 transactions per second**. As Bitcoin's popularity grew, this has led to delayed transaction processing and higher fees.

**UBSV** is BSV’s solution to the challenges of vertical scaling by instead spreading the workload across multiple machines. This horizontal scaling approach, coupled with an unbound block size, enables network capacity to grow with increasing demand through the addition of cluster nodes, allowing for BSV scaling to be truly unbounded.

UBSV provides a robust node processing system for BSV that can consistently handle over **1M transactions per second**, white strictly adhering to the Bitcoin whitepaper.
The node has been designed as a collection of microservices, each handling specific functionalities of the BSV network.

---

## Getting Started

---

### Pre-requisites and Installation

To be able to run the node locally, please check the [Installation Guide for Developers and Contributors](docs/developerSetup.md).

### Running the Node and Individual Services Locally for Development

Please refer to the [Locally Running Services Documentation](docs/locallyRunningServices.md) for detailed instructions on how to run the node and / or individual services locally in development.

---

## Advanced Usage  [ TO-DO ]

### Settings

All services accept settings allowing local and remote servers to have their own specific configuration.

For more information on how to create and use settings, please check the [Settings Documentation](docs/settings.md).

### Makefile

The Makefile facilitates a variety of development and build tasks for the UBSV project.

Check the [Makefile Documentation](docs/makefile.md) for detailed documentation. Some use cases will be highlighted here:

#### Proto buffers (protoc)

You can generate the protobuf files by running the following command:

```shell
make gen
```

You can read more about proto buffers in the Technology section.

For additional make commands, please check the [Makefile Documentation](docs/makefile.md).

#### Running Tests

There are 2 commands to run tests:

```shell
make test  # Executes Go tests excluding the playground and PoC directories.
```

```shell
make testall  # Executes Go tests excluding the playground and PoC directories.
```

### gRPC Logging

Additional logs can be produced when the node is run with the following environment variables set: `GRPC_VERBOSITY=debug GRPC_TRACE=client_channel,round_robin`



---

## Running the Node in Production  [ TO-DO ]

---


---

## Architecture

---

Please check the [Architecture Documentation](docs/architecture/architecture.md) for an introduction to the overall architecture of the node.


---

## Micro-Services

---

Detailed Node Service documentation:

+ [Asset Server - TODO](docs/services/assetServer.md)

+ [Propagation Service - TODO](docs/services/propagation.md)

+ [Validator Service - TODO](docs/services/validator.md)

+ [Block Validation Service - TODO](docs/services/blockValidation.md)

+ [Block Assembly Service - TODO](docs/services/blockAssembly.md)

+ [Blockchain Service - TODO](docs/services/blockchain.md)

Store Documentation:

+ [UTXO Store](docs/stores/utxo.md)

+ [TXMeta Service](docs/stores/txmeta.md)

Overlay Service documentation:

+ [Coinbase - TODO](docs/services/coinbase.md)

+ [P2P - TODO](docs/services/p2p.md)
+ [Bootstrap (Deprecated)](docs/services/bootstrap.md)


---

## Technology  [ TO-DO ]

---


* Go
* grpc
  -- gRPC vs IPV6 multicast
  -- — https://grpc.io/docs/what-is-grpc/introduction/
* protobuf
* Stores (options)
* Docker
* Kubernetes
  * [Kubernetes Resolver for gRPC](k8sresolver/README.md)


---

## Project Structure and Coding Conventions

---

### Project Structure

Documenting a set of directories in Markdown can be efficiently done using a combination of nested lists and descriptions. Here's a structure that many developers find readable and straightforward:

### Directory Structure and Descriptions:

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


### Coding Conventions

For naming conventions please check the [Naming Conventions](docs/guidelines/namingConventions.md).

### TODO - ERROR HANDLING, LOGGING, ...

xxx

### Testing Conventions - [TODO]

xxxx

---


## License

---
**Copyright © 2023 BSV Blockchain Org. All rights reserved.**

No part of this software may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the author.

_Unauthorized duplication, distribution, or modification of this software, in whole or in part, is strictly prohibited._

---
