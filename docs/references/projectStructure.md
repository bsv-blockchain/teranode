# Teranode Project Structure


The Teranode project is structured as follows:

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
│   ├── aerospike_reader/         # Command related to Aerospike reader functionality
│   ├── bare/                     # Simple command setup
│   ├── bitcoin2utxoset/          # Bitcoin to UTXO set utility
│   ├── blockassembly_blaster/    # Load testing for blockassembly
│   ├── blockchainstatus/         # Utility for checking blockchain status
│   ├── blockpersisterintegrity/  # Utility for verifying block persister integrity
│   ├── chainintegrity/           # Utility to verify the integrity of the blockchain.
│   ├── propagation_blaster/      # Utility to load test the Propagation service
│   ├── s3_blaster/               # Utility to load test the S3 service
│   ├── seeder/                   # Seeder command
│   ├── utxopersister/            # Utility to load test the UTXO Store service
│   └── various other utilities like txblaster, peer-cli, etc.
│
├── data/                         # Local node data directory
│
├── deploy/                       # Deployment scripts for the project
│   ├── dev/                      # Development deployment configurations
│   ├── docker/                   # Docker deployment scripts
│   └── operator/                 # Kubernetes operator deployment scripts
│
├── docs/                         # Documentation for the project
│   ├── architecture/             # Architectural diagrams and related docs
│   ├── services/                 # Service documentation including protobuf docs
│   └── other documentation folders for Kafka, sharding, SOP, etc.
│
├── errors/                       # Error handling and testing tools
│
├── k8sresolver/                  # Kubernetes resolver for gRPC
│
├── model/                        # Key model definitions for the project
│
├── modules/                      # Various modules, including p2pBootstrap
│
├── scripts/                      # Various scripts
│
├── services/                     # Directory containing all different Services
│   ├── alert/                    # Alert Service
│   ├── asset/                    # Asset Service
│   ├── blockassembly/            # Block Assembly Service
│   ├── blockchain/               # Blockchain Service
│   ├── blockvalidation/          # Block Validation Service
│   ├── legacy/                   # Legacy P2P services
│   ├── miner/                    # Miner Service
│   ├── propagation/              # Propagation Service
│   ├── subtreevalidation/        # Subtree Validation Service
│   └── utxopersister/            # UTXO Persister Service
│
├── stores/                       # Stores used by various services
│   ├── blob/                     # Blob store implementations
│   ├── blockchain/               # Blockchain store implementations
│   └── utxo/                     # UTXO store implementations
│
├── test/                         # Test-related scripts and data
│
├── tracing/                      # Tracing, Stats and Metric utilities
│
├── ui/
│   └── dashboard/                # Teranode Dashboard UI
│
└── util/                         # Utilities
```
