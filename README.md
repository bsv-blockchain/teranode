# 🚀 Teranode
> Unbounded BSV

## Index

- [Tutorials](#tutorials)
- [How-to Guides](#how-to-guides)
   - [Development](#development)
   - [Test](#test)
   - [Production](#production)
- [Key Topics](#key-topics)
- [Reference](#reference)
- [Additional Resources](#additional-resources)

## Tutorials
1. Getting Started with Teranode
    - Prerequisites and Installation
    - Running Your First Teranode Instance
    - Basic Configuration

## How-to Guides

### Development

1. [Setting Up for Development](docs/howto/developerSetup.md)
2. [Running Services Locally](docs/howto/locallyRunningServices.md)
3. [Using the Makefile](docs/howto/makefile.md)
4. [Running Tests](docs/howto/runningTests.md)
5. [Generating Protobuf Files](docs/howto/generatingProtobuf.md)
6. [Adding new Protobuf Services](docs/howto/addingNewProtobufServices.md)
7. [Configuring gRPC Logging](docs/howto/configuringGrpcLogging.md)

### Test

1. [Standard Operating Procedures - Running Teranode in Test](docs/sop/StandardOperatingProcedures-docker-compose-milestone-3-alpha-testing.md)

### Production

1. [Standard Operating Procedures - Running Teranode in Production](docs/sop/StandardOperatingProcedures-kubernetes-operator-milestone-3-alpha-testing.md)


## Key Topics
1. Introduction to Teranode
    - [What is Teranode?](docs/topics/teranodeIntro.md)

2. Teranode Architecture

    - [Overall System Design](docs/architecture/teranode-architecture.md)

    - Microservices Overview

    - Core Services:
      - [Asset Server](docs/services/assetServer.md)
      - [Propagation Service](docs/services/propagation.md)
      - [Validator Service](docs/services/validator.md)
      - [Subtree Validation Service](docs/services/subtreeValidation.md)
      - [Block Validation Service](docs/services/blockValidation.md)
      - [Block Assembly Service](docs/services/blockAssembly.md)
      - [Blockchain Service](docs/services/blockchain.md)
      - [Alert Service](docs/services/alert.md)

    - Overlay Services:
      - [Block Persister Service](docs/services/blockPersister.md)
      - [UTXO Persister Service](docs/services/utxoPersister.md)
      - [P2P Service](docs/services/p2p.md)
      - [P2P Bootstrap Service](docs/services/p2pBootstrap.md)
      - [P2P Legacy Service](docs/services/p2pLegacy.md)

    - Stores:
      - [Blob Server](docs/stores/blob.md)
      - [UTXO Store](docs/stores/utxo.md)

    - Messaging:
      - [Kafka](docs/kafka/kafka.md)

    - Commands:
      - [Seeder Service](docs/commands/seeder.md)
      - [UTXO Blaster](docs/commands/utxoBlaster.md)
      - [TX Blaster](docs/commands/txBlaster.md)
      - [Propagation Blaster](docs/commands/propagationBlaster.md)
      - [Chain Integrity](docs/commands/chainIntegrity.md)

3. [Technology Stack](docs/topics/technologyStack.md)

## Reference

1. Microservices
    - [Alert Service Reference Documentation](docs/references/services/alert_reference.md)
    - [Asset Service Reference Documentation](docs/references/services/asset_reference.md)
    - [Block Assembly Reference Documentation](docs/references/services/blockassembly_reference.md)
    - [Blockchain Server Reference Documentation](docs/references/services/blockchain_reference.md)
    - [Block Persister Service Reference Documentation](docs/references/services/blockpersister_reference.md)
    - [Block Validation Service Reference Documentation](docs/references/services/blockvalidation_reference.md)
    - [Coinbase Service Reference Documentation](docs/references/services/coinbase_reference.md)
    - [Legacy Server Reference Documentation](docs/references/services/legacy_reference.md)
    - [P2P Server Reference Documentation](docs/references/services/p2p_reference.md)
    - [Propagation Server Reference Documentation](docs/references/services/propagation_reference.md)
    - [RPC Server Reference Documentation](docs/references/services/rpc_reference.md)
    - [Subtree Validation Reference Documentation](docs/references/services/subtreevalidation_reference.md)
    - [UTXO Persister Service Reference Documentation](docs/references/services/utxopersister_reference.md)
    - [TX Validator Service Reference Documentation](docs/references/services/validator_reference.md)

2. Stores
    - [Blob Store](docs/references/stores/blob_reference.md)
    - [UTXO Store](docs/references/stores/utxo_reference.md)

3. gRPC API Documentation
    - [Alert gRPC API](docs/references/protobuf_docs/alertProto.md)
    - [Block Assembly gRPC API](docs/references/protobuf_docs/blockassemblyProto.md)
    - [Blockchain gRPC API](docs/references/protobuf_docs/blockchainProto.md)
    - [Block Validation gRPC API](docs/references/protobuf_docs/blockvalidationProto.md)
    - [Coinbase gRPC API](docs/references/protobuf_docs/coinbaseProto.md)
    - [Propagation gRPC API](docs/references/protobuf_docs/propagationProto.md)
    - [Subtree Validation gRPC API](docs/references/protobuf_docs/subtreevalidationProto.md)
    - [Validator gRPC API](docs/references/protobuf_docs/validatorProto.md)

4. [Project Structure](docs/references/projectStructure.md)
5. [Naming Conventions](docs/references/namingConventions.md)
6. [Error Handling Guidelines](docs/references/errorHandling.md)
7. [Configuration Settings](docs/references/settings.md)
8. [Network Consensus Rules](docs/references/networkConsensusRules.md)

## Additional Resources
1. Glossary
2. Troubleshooting Guide
3. FAQ
4. Contributing to Teranode
5. License Information

---

**Copyright © 2024 BSV Blockchain Org. All rights reserved.**

No part of this software may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the author.

_Unauthorized duplication, distribution, or modification of this software, in whole or in part, is strictly prohibited._

---
