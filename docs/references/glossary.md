# Teranode BSV Glossary

**Alert Service**: A system that provides network alert functionality, allowing for UTXO freezing/unfreezing, UTXO reassignment, peer management, and block invalidation for network security and compliance purposes.

**Aerospike**: A high-performance, distributed NoSQL database used in Teranode as the primary storage backend for the UTXO Store, providing low-latency access to unspent transaction outputs.

**Asset Service**: Provides HTTP and WebSocket APIs for blockchain data access, serving transactions, subtrees, blocks, block headers, and UTXOs.

**Block**: A container of grouped subtrees, including a coinbase transaction and a header, forming the blockchain.

**Block Assembly Service**: Manages block creation including transaction selection, block template generation, and mining integration.

**Block Header**: Metadata about a block containing the previous block hash, Merkle root, timestamp, difficulty target, and nonce. The block header is used to link blocks in the chain and provides the Proof-of-Work puzzle solution.

**Block Persister Service**: Persists blocks and related data (transactions, UTXOs, subtrees) to storage, ensuring data consistency.

**Block Validation Service**: Ensures the integrity and consistency of each block before it's added to the blockchain.

**Blob Store**: A generic datastore that stores transactions and subtrees with support for multiple backend options (file system, Amazon S3, HTTP, in-memory). Provides the foundation for TX Store and Subtree Store implementations.

**Bitcoin Script**: A stack-based, Forth-like scripting language used in Bitcoin transactions to define spending conditions. BSV supports the full original Bitcoin script with restored opcodes and unbounded script sizes.

**Blockchain Service**: Implements a local Bitcoin SV blockchain service, maintaining the blockchain state as understood by the node. The service uses a Finite State Machine (FSM) to manage node states and coordinate operations across all Teranode services.

**BSV**: Bitcoin Satoshi Vision, the blockchain network that Teranode is designed to support.

**Checkpoint**: A known valid block height used as a trust anchor for validation optimization. Blocks below checkpoints can use quick validation since they are known to be valid.


**Coinbase Transaction**: The first transaction in a block that creates new coins as a reward for the miner.

**Consensus Rules**: The set of validation rules that all nodes must follow to determine which blocks and transactions are valid. These rules include script validation, transaction format requirements, block size limits, and proof-of-work verification.

**Docker**: A platform used to develop, ship, and run applications inside containers.

**Docker Compose**: A tool for defining and running multi-container Docker applications.

**Daemon**: The main Teranode process that initializes and coordinates all microservices, stores, and system components. It manages service lifecycle, configuration, and inter-service dependencies.

**Extended Transaction Format (BIP-239)**: A transaction format that includes additional metadata (previous output satoshis and locking scripts) in each input to facilitate faster validation. Teranode accepts transactions in both standard Bitcoin format and Extended Format. When standard format transactions are received, Teranode automatically extends them during validation by retrieving input data from the UTXO store. All transactions are stored in non-extended format for storage efficiency, with extension performed in-memory on-demand during validation. This dual-format support ensures backward compatibility with existing Bitcoin wallets while enabling optimized validation when extended format is provided.

**FSM (Finite State Machine)**: A model that manages the Blockchain Service states and transitions. The FSM controls node behavior through states (Idle, LegacySyncing, Running, CatchingBlocks) and events (LegacySync, Run, CatchupBlocks, Stop), determining which operations are permitted at each stage.

**gRPC**: A high-performance, open-source universal RPC framework.

**Initial Sync**: The process of downloading and validating the entire blockchain when setting up a new node.

**Invalid Block**: A block that has failed validation and is marked in the blockchain store. Invalid blocks are tracked to prevent reprocessing and their children inherit the invalid status.

**Kademlia**: A distributed hash table used for efficient routing and peer discovery in P2P networks.

**Kafka**: A distributed streaming platform used in Teranode for asynchronous event-driven communication between microservices. Kafka topics handle block notifications, subtree events, and transaction propagation across the system.

**Legacy Service**: Bridges the gap between traditional BSV nodes and advanced Teranode-BSV nodes, ensuring seamless communication and data translation.

**libp2p**: A modular peer-to-peer networking framework used by Teranode's P2P Bootstrap Service for peer discovery and network communication, providing features like NAT traversal and multiple transport protocols.

**Lustre Fs**: A parallel distributed file system used for high-performance, large-scale data storage and workloads in Teranode.

**Microservices**: An architectural style that structures an application as a collection of loosely coupled services.

**Miner**: A node on the network that processes transactions and creates new blocks.

**Mining Candidate**: A potential block that includes all known subtrees up to a certain time, built on top of the longest honest chain.

**Merkle Tree**: A binary tree of cryptographic hashes used in Bitcoin to efficiently verify transaction inclusion in a block. Each leaf node contains a transaction hash, and parent nodes contain hashes of their children, culminating in a single Merkle root stored in the block header.

**Orphan Block**: A valid block that is not part of the main blockchain because its parent block is unknown or not yet received. Orphan blocks are temporarily stored until their parent arrives or they are discarded if they don't connect to the main chain.

**P2P Bootstrap Service**: Helps new nodes discover peers and join the network, using libp2p and Kademlia.

**P2P Service**: Allows peers to subscribe and receive blockchain notifications about new blocks and subtrees in the network.

**PostgreSQL**: An open-source relational database used in Teranode for storing blockchain data.

**Pre-allocated Block ID**: A unique block identifier reserved in advance through GetNextBlockID, enabling parallel block processing and optimized validation scenarios.

**Propagation Service**: Handles the propagation of transactions across the peer-to-peer Teranode network.

**Proof-of-Work (PoW)**: A consensus mechanism where miners must solve a computationally difficult puzzle to create new blocks. The puzzle involves finding a nonce that produces a block hash below a target difficulty threshold, ensuring security and preventing spam.

**Quick Validation**: An optimized validation path for blocks below checkpoints that skips expensive script validation, providing approximately 10x faster processing for historical blocks.

**RPC Service**: Provides compatibility with the Bitcoin RPC interface, allowing clients to interact with the Teranode node using standard Bitcoin RPC commands.

**Reorganization (Reorg)**: A process where the blockchain switches from one chain to a longer competing chain, causing previously confirmed blocks to become orphaned. During a reorg, transactions from orphaned blocks are returned to the transaction pool unless they conflict with the new chain.

**Subtree**: An intermediate data structure that holds batches of transaction IDs and their corresponding Merkle root. Subtrees are components of the block's overall Merkle Tree structure, enabling efficient parallel processing and validation.

**Subtree Validation Service**: Ensures the integrity and consistency of each received subtree before it is added to the subtree store.

**Teranode**: A high-performance implementation of the Bitcoin protocol designed to handle a massive scale of transactions.

**Transaction Metadata Cache (TX Metadata Cache)**: A caching layer that stores transaction metadata to optimize validation performance and reduce database lookups during transaction processing.

**TX Validator (Transaction Validator)**: Responsible for validating new transactions, persisting data into the UTXO store, and propagating transactions to other services.

**UTXO (Unspent Transaction Output)**: Represents a piece of cryptocurrency that can be spent in future transactions.

**UTXO Persister Service**: Creates and maintains an up-to-date Unspent Transaction Output (UTXO) file set for each block in the blockchain.

**UTXO Store**: A datastore of UTXOs, tracking unspent transaction outputs that can be used as inputs in new transactions.

**Validator Service**: Validates new transactions (using the Transaction Validator) and coordinates with the UTXO store for transaction processing.

This glossary covers the main terms and components of the Teranode BSV system as described in the documentation you provided. It should help readers quickly reference and understand key concepts throughout the documentation.
