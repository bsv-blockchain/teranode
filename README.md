# Teranode Documentation

## Table of Contents

1. [Tutorials](#tutorials)
   - [Miners](#miner-tutorials)
2. [How-to Guides](#how-to-guides)
   - [Development](#development)
   - [Miners](#miners)
3. [Common Tasks](#common)
4. [Key Topics](#key-topics)
5. [Reference](#reference)
6. [Additional Resources](#additional-resources)

-----

## Tutorials

### Miner Tutorials

- [Initial Setup Walkthrough](docs/tutorials/miners/minersGettingStarted.md)

-----

## How-to Guides

### Development

1. [Setting Up for Development](docs/howto/developerSetup.md)
2. [Running Services Locally](docs/howto/locallyRunningServices.md)
3. [Using the Makefile](docs/howto/makefile.md)
4. [Running Tests](docs/howto/runningTests.md)
5. [Setting Up Automated Test Environment](docs/howto/automatedTestingHowTo.md)
6. [Generating Protobuf Files](docs/howto/generatingProtobuf.md)
7. [Adding new Protobuf Services](docs/howto/addingNewProtobufServices.md)
8. [Configuring gRPC Logging](docs/howto/configuringGrpcLogging.md)

### Miners

#### Docker Compose Setup

1. [Installation Guide](docs/howto/miners/docker/minersHowToInstallation.md)
2. [Starting and Stopping Teranode](docs/howto/miners/docker/minersHowToStopStartDockerTeranode.md)
3. [Configuration Guide](docs/howto/miners/docker/minersHowToConfigureTheNode.md)
4. [Blockchain Synchronization](docs/howto/miners/docker/minersHowToSyncTheNode.md)
5. [Update Procedures](docs/howto/miners/docker/minersUpdatingTeranode.md)
6. [Troubleshooting Guide](docs/howto/miners/docker/minersHowToTroubleshooting.md)
7. [Security Best Practices](docs/howto/miners/docker/minersSecurityBestPractices.md)

#### Kubernetes Deployment

1. [Installation with Kubernetes Operator](docs/howto/miners/kubernetes/minersHowToInstallation.md)
2. [Starting and Stopping Teranode](docs/howto/miners/kubernetes/minersHowToStopStartKubernetesTeranode.md)
3. [Configuration Guide](docs/howto/miners/kubernetes/minersHowToConfigureTheNode.md)
4. [Blockchain Synchronization](docs/howto/miners/kubernetes/minersHowToSyncTheNode.md)
5. [Update Procedures](docs/howto/miners/kubernetes/minersUpdatingTeranode.md)
6. [Backup Procedures](docs/howto/miners/kubernetes/minersHowToBackup.md)
7. [Troubleshooting Guide](docs/howto/miners/kubernetes/minersHowToTroubleshooting.md)
8. [Security Best Practices](docs/howto/miners/kubernetes/minersSecurityBestPractices.md)
9. [Remote Debugging Guide](docs/howto/miners/kubernetes/minersHowToRemoteDebugTeranode.md)

### Common Tasks

1. [Interacting with Asset Server](docs/howto/miners/minersHowToInteractWithAssetServer.md)
2. [Interacting with RPC Server](docs/howto/miners/minersHowToInteractWithRPCServer.md)
3. [Managing Disk Space](docs/howto/miners/minersManagingDiskSpace.md)

-----

## Key Topics

### Introduction
- [What is Teranode?](docs/topics/teranodeIntro.md)

### Architecture
- [Overall System Design](docs/architecture/teranode-overall-system-design.md)
- [Microservices Overview](docs/architecture/teranode-microservices-overview.md)

#### Core Services
- [Asset Server](docs/services/assetServer.md)
- [Propagation Service](docs/services/propagation.md)
- [Validator Service](docs/services/validator.md)
- [Subtree Validation Service](docs/services/subtreeValidation.md)
- [Block Validation Service](docs/services/blockValidation.md)
- [Block Assembly Service](docs/services/blockAssembly.md)
- [Blockchain Service](docs/services/blockchain.md)
- [Alert Service](docs/services/alert.md)

#### Overlay Services
- [Block Persister Service](docs/services/blockPersister.md)
- [UTXO Persister Service](docs/services/utxoPersister.md)
- [P2P Service](docs/services/p2p.md)
- [P2P Bootstrap Service](docs/services/p2pBootstrap.md)
- [P2P Legacy Service](docs/services/p2pLegacy.md)
- [RPC Server](docs/services/rpc.md)

#### Infrastructure Components
- **Stores**
   - [Blob Server](docs/stores/blob.md)
   - [UTXO Store](docs/stores/utxo.md)
- **Messaging**
   - [Kafka](docs/kafka/kafka.md)
- **Utilities**
   - [UTXO Seeder](docs/commands/seeder.md)
   - [UTXO Blaster](docs/commands/utxoBlaster.md)
   - [TX Blaster](docs/commands/txBlaster.md)
   - [Propagation Blaster](docs/commands/propagationBlaster.md)
   - [Chain Integrity](docs/commands/chainIntegrity.md)

### Additional Topics
- [Technology Stack](docs/topics/technologyStack.md)
- [Testing Framework](docs/topics/understandingTheTestingFramework.md)

-----

## Reference

### Service Documentation
- [Alert Service](docs/references/services/alert_reference.md)
- [Asset Service](docs/references/services/asset_reference.md)
- [Block Assembly](docs/references/services/blockassembly_reference.md)
- [Blockchain Server](docs/references/services/blockchain_reference.md)
- [Block Persister](docs/references/services/blockpersister_reference.md)
- [Block Validation](docs/references/services/blockvalidation_reference.md)
- [Coinbase Service](docs/references/services/coinbase_reference.md)
- [Legacy Server](docs/references/services/legacy_reference.md)
- [P2P Server](docs/references/services/p2p_reference.md)
- [Propagation Server](docs/references/services/propagation_reference.md)
- [RPC Server](docs/references/services/rpc_reference.md)
- [Subtree Validation](docs/references/services/subtreevalidation_reference.md)
- [UTXO Persister](docs/references/services/utxopersister_reference.md)
- [TX Validator](docs/references/services/validator_reference.md)

### Store Documentation
- [Blob Store](docs/references/stores/blob_reference.md)
- [UTXO Store](docs/references/stores/utxo_reference.md)

### API Documentation
- [Alert gRPC API](docs/references/protobuf_docs/alertProto.md)
- [Block Assembly gRPC API](docs/references/protobuf_docs/blockassemblyProto.md)
- [Blockchain gRPC API](docs/references/protobuf_docs/blockchainProto.md)
- [Block Validation gRPC API](docs/references/protobuf_docs/blockvalidationProto.md)
- [Coinbase gRPC API](docs/references/protobuf_docs/coinbaseProto.md)
- [Propagation gRPC API](docs/references/protobuf_docs/propagationProto.md)
- [Subtree Validation gRPC API](docs/references/protobuf_docs/subtreevalidationProto.md)
- [Validator gRPC API](docs/references/protobuf_docs/validatorProto.md)

### Additional Reference
- [Third Party Software Requirements](docs/references/thirdPartySoftwareRequirements.md)
- [Project Structure](docs/references/projectStructure.md)
- [Naming Conventions](docs/references/namingConventions.md)
- [Error Handling Guidelines](docs/references/errorHandling.md)
- [Configuration Settings](docs/references/settings.md)
- [Testing Framework Technical Reference](docs/references/testingTechnicalReference.md)
- [Network Consensus Rules](docs/references/networkConsensusRules.md)

## Additional Resources
1. [Glossary](docs/references/glossary.md)
2. Contributing to Teranode
3. [License Information](docs/references/licenseInformation.md)

---

**Copyright © 2024 BSV Blockchain Org.**

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this software except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
