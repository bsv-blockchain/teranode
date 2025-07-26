# Teranode Project Structure


The Teranode project is structured as follows:

```
teranode/
│
├── main.go                       # Entry point to start the services
│
├── Makefile                      # Facilitates a variety of development and build tasks for the project
│
├── settings.conf                 # Global settings with sensible defaults for all environments
│
├── settings_local.conf           # Developer-specific and deployment-specific settings. Overrides settings.conf. Not tracked in source control.
│
├── Dockerfile                    # Main Dockerfile for containerization
├── docker-compose.yml            # Docker Compose configuration
│
├── cmd/                          # Directory containing command-line tools and utilities
│   ├── aerospikereader/          # Command related to Aerospike reader functionality
│   ├── bitcointoutxoset/         # Bitcoin to UTXO set utility
│   ├── checkblocktemplate/       # Tool to check block templates
│   ├── filereader/               # Utility for reading files
│   ├── getfsmstate/              # Tool to get FSM state
│   ├── keygen/                   # Key generation utility
│   ├── peercli/                  # Peer network command-line interface
│   ├── seeder/                   # Seeder functionality
│   ├── setfsmstate/              # Tool to set FSM state
│   ├── settings/                 # Settings management tools
│   ├── teranodecli/              # Teranode command-line interface
│   └── utxopersister/            # UTXO persistence utility
│
├── services/                     # Core service implementations
│   ├── alert/                    # Alert service
│   ├── asset/                    # Asset service
│   ├── blockassembly/            # Block assembly service
│   ├── blockchain/               # Blockchain service
│   ├── blockpersister/           # Block persister service
│   ├── blockvalidation/          # Block validation service
│   ├── legacy/                   # Legacy services
│   ├── p2p/                      # Peer-to-peer networking service
│   ├── propagation/              # Transaction propagation service
│   ├── rpc/                      # RPC service
│   ├── subtreevalidation/        # Subtree validation service
│   ├── utxopersister/            # UTXO persister service
│   └── validator/                # Transaction validator service
│
├── stores/                       # Data storage implementations
│   ├── blob/                     # Blob storage implementation
│   ├── blockchain/               # Blockchain storage implementation
│   ├── cleanup/                  # Cleanup storage utilities
│   ├── txmetacache/             # Transaction metadata cache implementation
│   └── utxo/                     # UTXO storage implementation
│
├── docs/                         # Documentation for the project
│   ├── architecture/             # Architectural diagrams
│   ├── references/               # Reference documentation
│   │   ├── protobuf_docs/        # Protobuf API documentation
│   │   ├── services/            # Service reference documentation
│   │   ├── stores/              # Store reference documentation
│   │   └── kafkaMessageFormat.md # Kafka message format documentation
│   └── images/                   # Documentation images
│
├── errors/                       # Error handling and definitions
│
├── k8sresolver/                  # Kubernetes resolver for gRPC
│
├── model/                        # Data models
│
├── chaincfg/                     # Chain configuration parameters
│
├── compose/                      # Docker compose configurations
│
├── daemon/                       # Daemon implementation
│
├── scripts/                      # Various utility scripts
│
├── settings/                     # Settings management implementation
│
├── test/                         # Test utilities and integration tests
│
├── testutil/                     # Test utilities
│
├── tracing/                      # Tracing and metrics utilities
│
├── ui/                           # User interface components
│   └── dashboard/                # Teranode Dashboard UI
│
├── ulogger/                      # Unified logging implementation
│
└── util/                         # Common utilities
```
