# Teranode BSV Glossary

**Alert Service**: Implements Bitcoin SV alert functionality for UTXO freezing/unfreezing, peer banning/unbanning, and confiscation transaction processing.

**Asset Service**: Provides HTTP and WebSocket APIs for blockchain data access, serving transactions, subtrees, blocks, block headers, and UTXOs.

**Block**: A container of grouped subtrees, including a coinbase transaction and a header, forming the blockchain.

**Block Assembly Service**: Manages block creation including transaction selection, block template generation, and mining integration.

**Block Header**: Metadata about a block, used to connect blocks in the blockchain and contains proof-of-work information.

**Block Persister Service**: Persists blocks and related data (transactions, UTXOs, subtrees) to storage, ensuring data consistency.

**Block Validation Service**: Ensures the integrity and consistency of each block before it's added to the blockchain.

**Blockchain Service**: Implements a local Bitcoin SV blockchain service, maintaining the blockchain as understood by the node.

**BSV**: Bitcoin Satoshi Vision, the blockchain network that Teranode is designed to support.

**Checkpoint**: A known valid block height used as a trust anchor for validation optimization. Blocks below checkpoints can use quick validation since they are known to be valid.


**Coinbase Transaction**: The first transaction in a block that creates new coins as a reward for the miner.

**Docker**: A platform used to develop, ship, and run applications inside containers.

**Docker Compose**: A tool for defining and running multi-container Docker applications.

**Extended Transaction Format (BIP-239)**: A transaction format that includes additional metadata (previous output satoshis and locking scripts) in each input to facilitate faster validation. Teranode accepts transactions in both standard Bitcoin format and Extended Format. When standard format transactions are received, Teranode automatically extends them during validation by retrieving input data from the UTXO store. All transactions are stored in non-extended format for storage efficiency, with extension performed in-memory on-demand during validation. This dual-format support ensures backward compatibility with existing Bitcoin wallets while enabling optimized validation when extended format is provided.

**gRPC**: A high-performance, open-source universal RPC framework.

**Initial Sync**: The process of downloading and validating the entire blockchain when setting up a new node.

**Invalid Block**: A block that has failed validation and is marked in the blockchain store. Invalid blocks are tracked to prevent reprocessing and their children inherit the invalid status.

**Kademlia**: A distributed hash table used for efficient routing and peer discovery in P2P networks.

**Kafka**: A distributed streaming platform used in Teranode for handling real-time data feeds.

**Lustre Fs**: A parallel distributed file system used for high-performance, large-scale data storage and workloads in Teranode.

**Microservices**: An architectural style that structures an application as a collection of loosely coupled services.

**Miner**: A node on the network that processes transactions and creates new blocks.

**Mining Candidate**: A potential block that includes all known subtrees up to a certain time, built on top of the longest honest chain.

**P2P Bootstrap Service**: Helps new nodes discover peers and join the network, using libp2p and Kademlia.

**Legacy Service**: Bridges the gap between traditional BSV nodes and advanced Teranode-BSV nodes, ensuring seamless communication and data translation.

**P2P Service**: Allows peers to subscribe and receive blockchain notifications about new blocks and subtrees in the network.

**PostgreSQL**: An open-source relational database used in Teranode for storing blockchain data.

**Pre-allocated Block ID**: A unique block identifier reserved in advance through GetNextBlockID, enabling parallel block processing and optimized validation scenarios.

**Propagation Service**: Handles the propagation of transactions across the peer-to-peer Teranode network.

**Quick Validation**: An optimized validation path for blocks below checkpoints that skips expensive script validation, providing approximately 10x faster processing for historical blocks.

**RPC Service**: Provides compatibility with the Bitcoin RPC interface, allowing clients to interact with the Teranode node using standard Bitcoin RPC commands.

**Subtree**: An intermediate data structure that holds batches of transaction IDs and their corresponding Merkle root.

**Subtree Validation Service**: Ensures the integrity and consistency of each received subtree before it is added to the subtree store.

**Teranode**: A high-performance implementation of the Bitcoin protocol designed to handle a massive scale of transactions.

**UTXO (Unspent Transaction Output)**: Represents a piece of cryptocurrency that can be spent in future transactions.

**UTXO Persister Service**: Creates and maintains an up-to-date Unspent Transaction Output (UTXO) file set for each block in the blockchain.

**UTXO Store**: A datastore of UTXOs, tracking unspent transaction outputs that can be used as inputs in new transactions.

**Validator Service**: Validates new transactions and coordinates with the UTXO store for transaction processing.

This glossary covers the main terms and components of the Teranode BSV system as described in the documentation you provided. It should help readers quickly reference and understand key concepts throughout the documentation.
