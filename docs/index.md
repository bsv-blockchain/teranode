# Teranode Documentation

## Index


- [Tutorials](#tutorials)
    - [Development Tutorials](#development-tutorials)
    - [Miner Tutorials](#miner-tutorials)
- [How-to Guides](#how-to-guides)
    - [Development](#development)
    - [Miners](#miners)
- [Key Topics](#key-topics)
    - [Introduction](#introduction)
    - [Architecture](#architecture)
        - [Core Services](#core-services)
        - [Overlay Services](#overlay-services)
        - [Infrastructure Components](#infrastructure-components)
    - [Additional Topics](#additional-topics)
- [Reference](#reference)
    - [Service Documentation](#service-documentation)
    - [Store Documentation](#store-documentation)
    - [Data Model](#data-model)
    - [API Documentation](#api-documentation)
    - [Additional Reference](#additional-reference)
- [Additional Resources](#additional-resources)

-----

## Tutorials

### Development Tutorials
- [Setting Up for Development](./tutorials/developers/developerSetup.md)
- [Git Commit Signing Setup Guide](./tutorials/developers/gitCommitSigningSetupGuide.md)

### Miner Tutorials

- [Initial Setup Walkthrough](./tutorials/miners/minersGettingStarted.md)

-----

## How-to Guides

### Development

1. [Running Services Locally](./howto/locallyRunningServices.md)
2. [Using the Makefile](./howto/makefile.md)
3. [Running Tests](./howto/runningTests.md)
4. [Setting Up Automated Test Environment](./howto/automatedTestingHowTo.md)
5. [Generating Protobuf Files](./howto/generatingProtobuf.md)
6. [Adding new Protobuf Services](./howto/addingNewProtobufServices.md)
7. [Configuring gRPC Logging](./howto/configuringGrpcLogging.md)
8. [Kubernetes - Remote Debugging Guide](./howto/howToRemoteDebugTeranode.md)

### Miners

#### Docker Compose Setup

1. [Installation Guide](./howto/miners/docker/minersHowToInstallation.md)
2. [Starting and Stopping Teranode](./howto/miners/docker/minersHowToStopStartDockerTeranode.md)
3. [Configuration Guide](./howto/miners/docker/minersHowToConfigureTheNode.md)
4. [Update Procedures](./howto/miners/docker/minersUpdatingTeranode.md)
5. [Troubleshooting Guide](./howto/miners/docker/minersHowToTroubleshooting.md)
6. [Security Best Practices](./howto/miners/docker/minersSecurityBestPractices.md)

#### Kubernetes Deployment

1. [Installation with Kubernetes Operator](./howto/miners/kubernetes/minersHowToInstallation.md)
2. [Starting and Stopping Teranode](./howto/miners/kubernetes/minersHowToStopStartKubernetesTeranode.md)
3. [Configuration Guide](./howto/miners/kubernetes/minersHowToConfigureTheNode.md)
4. [Update Procedures](./howto/miners/kubernetes/minersUpdatingTeranode.md)
5. [Backup Procedures](./howto/miners/kubernetes/minersHowToBackup.md)
6. [Troubleshooting Guide](./howto/miners/kubernetes/minersHowToTroubleshooting.md)
7. [Security Best Practices](./howto/miners/kubernetes/minersSecurityBestPractices.md)


#### Common Tasks

1. [Interacting with Asset Server](./howto/miners/minersHowToInteractWithAssetServer.md)
2. [Interacting with RPC Server](./howto/miners/minersHowToInteractWithRPCServer.md)
3. [Interacting with the FSM via RPC](./howto/miners/minersHowToInteractWithFSM.md)
4. [Interacting with the Teranode CLI](./howto/miners/minersHowToTeranodeCLI.md)
5. [Managing Disk Space](./howto/miners/minersManagingDiskSpace.md)
6. [Aerospike Configuration Considerations](./howto/miners/minersHowToAerospikeTuning.md)
7. [How To Reset Teranode](./howto/miners/minersHowToResetTeranode.md)
8. [Blockchain Synchronization](./howto/miners/minersHowToSyncTheNode.md)

-----

## Key Topics

### Introduction
- [What is Teranode?](./topics/teranodeIntro.md)

### Architecture
- [Overall System Design](./topics/architecture/teranode-overall-system-design.md)
- [Microservices Overview](./topics/architecture/teranode-microservices-overview.md)

#### Core Services
- [Asset Server](./topics/services/assetServer.md)
- [Propagation Service](./topics/services/propagation.md)
- [Validator Service](./topics/services/validator.md)
- [Subtree Validation Service](./topics/services/subtreeValidation.md)
- [Block Validation Service](./topics/services/blockValidation.md)
- [Block Assembly Service](./topics/services/blockAssembly.md)
- [Blockchain Service](./topics/services/blockchain.md)
- [Alert Service](./topics/services/alert.md)

#### Overlay Services
- [Block Persister Service](./topics/services/blockPersister.md)
- [UTXO Persister Service](./topics/services/utxoPersister.md)
- [P2P Service](./topics/services/p2p.md)
- [P2P Bootstrap Service](./topics/services/p2pBootstrap.md)
- [P2P Legacy Service](./topics/services/p2pLegacy.md)
- [RPC Server](./topics/services/rpc.md)

#### Infrastructure Components
- **Stores**
    - [Blob Server](./topics/stores/blob.md)
    - [UTXO Store](./topics/stores/utxo.md)
- **Messaging**
    - [Kafka](./topics/kafka/kafka.md)
- **Utilities**
    - [UTXO Seeder](./topics/commands/seeder.md)
    - [TX Blaster](./topics/commands/txBlaster.md)
    - [Propagation Blaster](./topics/commands/propagationBlaster.md)
    - [Chain Integrity](./topics/commands/chainIntegrity.md)

### Additional Topics
- [Technology Stack](./topics/technologyStack.md)
- [Testing Framework](./topics/understandingTheTestingFramework.md)
- [QA Guide & Instructions for Functional Requirement Tests](test/README.md)
- [Double Spends](./topics/architecture/understandingDoubleSpends.md)

-----

## Reference

### Service Documentation
- [Alert Service](./references/services/alert_reference.md)
- [Asset Service](./references/services/asset_reference.md)
- [Block Assembly](./references/services/blockassembly_reference.md)
- [Blockchain Server](./references/services/blockchain_reference.md)
- [Block Persister](./references/services/blockpersister_reference.md)
- [Block Validation](./references/services/blockvalidation_reference.md)
- [Coinbase Service](./references/services/coinbase_reference.md)
- [Legacy Server](./references/services/legacy_reference.md)
- [P2P Server](./references/services/p2p_reference.md)
- [Propagation Server](./references/services/propagation_reference.md)
- [RPC Server](./references/services/rpc_reference.md)
- [Subtree Validation](./references/services/subtreevalidation_reference.md)
- [UTXO Persister](./references/services/utxopersister_reference.md)
- [TX Validator](./references/services/validator_reference.md)

### Store Documentation
- [Blob Store](./references/stores/blob_reference.md)
- [UTXO Store](./references/stores/utxo_reference.md)

### Data Model
- [Block Data Model](./topics/datamodel/block_data_model.md)
- [Block Header Data Model](./topics/datamodel/block_header_data_model.md)
- [Subtree Data Model](./topics/datamodel/subtree_data_model.md)
- [Transaction Data Model](./topics/datamodel/transaction_data_model.md)
- [UTXO Data Model](./topics/datamodel/utxo_data_model.md)

### API Documentation
- [Alert gRPC API](./references/protobuf_docs/alertProto.md)
- [Block Assembly gRPC API](./references/protobuf_docs/blockassemblyProto.md)
- [Blockchain gRPC API](./references/protobuf_docs/blockchainProto.md)
- [Block Validation gRPC API](./references/protobuf_docs/blockvalidationProto.md)
- [Coinbase gRPC API](./references/protobuf_docs/coinbaseProto.md)
- [Propagation gRPC API](./references/protobuf_docs/propagationProto.md)
- [Subtree Validation gRPC API](./references/protobuf_docs/subtreevalidationProto.md)
- [Validator gRPC API](./references/protobuf_docs/validatorProto.md)

### Additional Reference
- [Third Party Software Requirements](./references/thirdPartySoftwareRequirements.md)
- [Project Structure](./references/projectStructure.md)
- [Naming Conventions](./references/namingConventions.md)
- [Error Handling Guidelines](./references/errorHandling.md)
- [Configuration Settings](./references/settings.md)
- [Testing Framework Technical Reference](./references/testingTechnicalReference.md)
- [Teranode Daemon Reference](./references/teranodeDaemonReference.md)
- [Prometheus Metrics](./references/prometheusMetrics.md)
- [Network Consensus Rules](./references/networkConsensusRules.md)

## Additional Resources
1. [Glossary](./references/glossary.md)
2. Contributing to Teranode
3. [License Information](./references/licenseInformation.md)

---

**Copyright © 2025 BSV Blockchain org.**

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this software except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
