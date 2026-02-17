# P2P Server Reference Documentation

## Overview

The P2P Server facilitates peer-to-peer communication within the Bitcoin SV network, managing the distribution of blocks, transactions, and network-related data. The server integrates with blockchain services, validation systems, and Kafka messaging to ensure efficient data propagation across the network. It implements both WebSocket and HTTP interfaces for peer communication while maintaining secure, scalable connections.

## Core Components

### Server Structure

The P2P Server is implemented through the Server struct, which coordinates all peer-to-peer communication:

```go
type Server struct {
    p2p_api.UnimplementedPeerServiceServer
    P2PClient                         p2pMessageBus.P2PClient   // The P2P network client from github.com/bsv-blockchain/go-p2p-message-bus
    logger                            ulogger.Logger            // Logger instance for the server
    settings                          *settings.Settings       // Configuration settings
    bitcoinProtocolVersion            string                    // Bitcoin protocol version identifier
    blockchainClient                  blockchain.ClientI        // Client for blockchain interactions
    blockAssemblyClient               blockassembly.ClientI     // Client for block assembly operations
    AssetHTTPAddressURL               string                    // HTTP address URL for assets
    PropagationURL                    string                    // URL for peers to use for propagating txs (defaults to AssetHTTPAddressURL)
    e                                 *echo.Echo                // Echo server instance
    notificationCh                    chan *notificationMsg     // Channel for notifications
    rejectedTxKafkaConsumerClient     kafka.KafkaConsumerGroupI // Kafka consumer for rejected transactions
    invalidBlocksKafkaConsumerClient  kafka.KafkaConsumerGroupI // Kafka consumer for invalid blocks
    invalidSubtreeKafkaConsumerClient kafka.KafkaConsumerGroupI // Kafka consumer for invalid subtrees
    subtreeKafkaProducerClient        kafka.KafkaAsyncProducerI // Kafka producer for subtrees
    blocksKafkaProducerClient         kafka.KafkaAsyncProducerI // Kafka producer for blocks
    banList                           BanListI                  // List of banned peers
    banChan                           chan BanEvent             // Channel for ban events
    banManager                        PeerBanManagerI           // Manager for peer banning
    gCtx                              context.Context
    blockTopicName                    string
    subtreeTopicName                  string
    rejectedTxTopicName               string
    invalidBlocksTopicName            string             // Kafka topic for invalid blocks
    invalidSubtreeTopicName           string             // Kafka topic for invalid subtrees
    nodeStatusTopicName               string             // pubsub topic for node status messages
    topicPrefix                       string             // Chain identifier prefix for topic validation
    blockPeerMap                      sync.Map           // Map to track which peer sent each block (hash -> peerMapEntry)
    subtreePeerMap                    sync.Map           // Map to track which peer sent each subtree (hash -> peerMapEntry)
    startTime                         time.Time          // Server start time for uptime calculation
    peerRegistry                      *PeerRegistry      // Central registry for all peer information
    peerSelector                      *PeerSelector      // Stateless peer selection logic
    syncCoordinator                   *SyncCoordinator   // Orchestrates sync operations
    syncConnectionTimes               sync.Map           // Map to track when we first connected to each sync peer (peerID -> timestamp)

    // Cleanup configuration
    peerMapCleanupTicker              *time.Ticker       // Ticker for periodic cleanup of peer maps
    peerMapMaxSize                    int                // Maximum number of entries in peer maps
    peerMapTTL                        time.Duration      // Time-to-live for peer map entries
    registryCacheSaveTicker           *time.Ticker       // Ticker for periodic saving of peer registry cache
}
```

The server manages several key components, each serving a specific purpose in the P2P network:

- The P2PClient handles direct peer connections and message routing through the p2pMessageBus.P2PClient interface from the github.com/bsv-blockchain/go-p2p-message-bus package
- The bitcoinProtocolVersion contains the node's protocol version identifier
- The various Kafka clients manage message distribution across the network
- The ban system maintains network security by managing peer access through BanListI and PeerBanManager
- The notification channel handles real-time event propagation
- The peerRegistry, peerSelector, and syncCoordinator provide comprehensive peer management and synchronization capabilities

### P2P Client Interface

The P2P Server uses the p2pMessageBus.P2PClient interface from the github.com/bsv-blockchain/go-p2p-message-bus package. This interface-based design enables better testability and allows external developers to create custom P2P implementations that integrate with Teranode's messaging system.

## Server Operations

### Server Initialization

The server initializes through the NewServer function:

```go
func NewServer(
    ctx context.Context,
    logger ulogger.Logger,
    tSettings *settings.Settings,
    blockchainClient blockchain.ClientI,
    blockAssemblyClient blockassembly.ClientI,
    rejectedTxKafkaConsumerClient kafka.KafkaConsumerGroupI,
    invalidBlocksKafkaConsumerClient kafka.KafkaConsumerGroupI,
    invalidSubtreeKafkaConsumerClient kafka.KafkaConsumerGroupI,
    subtreeKafkaProducerClient kafka.KafkaAsyncProducerI,
    blocksKafkaProducerClient kafka.KafkaAsyncProducerI,
) (*Server, error)
```

This function establishes the server with required settings and dependencies, including:

- P2P network configuration (IP, port, topics)
- Topic name generation for various message types
- Ban list initialization
- Connection to blockchain services
- Kafka producer and consumer setup

### Health Management

The server implements comprehensive health checking through:

```go
func (s *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

This method performs two types of health verification:

For liveness checks, it verifies basic server operation without dependency checks.

For readiness checks, it verifies:

- Kafka broker connectivity
- Blockchain client functionality
- FSM state verification
- Block validation client status

### Server Lifecycle Management

The server manages its lifecycle through several key methods:

The Init method prepares the server for operation:

```go
func (s *Server) Init(ctx context.Context) (err error)
```

It verifies and adjusts HTTP/HTTPS settings based on security requirements and establishes necessary connection URLs.

The Start method initiates server operations:

```go
func (s *Server) Start(ctx context.Context, readyCh chan<- struct{}) error
```

It begins:

- Kafka message processing
- Block validation client setup
- HTTP/WebSocket server operation
- P2P network communication
- Blockchain subscription monitoring
- Ban event processing
- Once initialization is complete, it signals readiness by closing the readyCh channel

The Stop method ensures graceful shutdown:

```go
func (s *Server) Stop(ctx context.Context) error
```

It manages the orderly shutdown of all server components and connections.

### Ban Management

The P2P Server implements a sophisticated peer banning system through several components:

```go
type BanEventHandler interface {
    OnPeerBanned(peerID string, until time.Time, reason string)
}
```

The `BanEventHandler` interface allows the system to react to ban events, which is implemented by the P2P Server.

```go
type BanListI interface {
    // IsBanned checks if a peer is banned by its IP address
    IsBanned(ipStr string) bool

    // Add adds an IP or subnet to the ban list with an expiration time
    Add(ctx context.Context, ipOrSubnet string, expirationTime time.Time) error

    // Remove removes an IP or subnet from the ban list
    Remove(ctx context.Context, ipOrSubnet string) error

    // ListBanned returns a list of all currently banned IP addresses and subnets
    ListBanned() []string

    // Subscribe returns a channel to receive ban events
    Subscribe() chan BanEvent

    // Unsubscribe removes a subscription to ban events
    Unsubscribe(ch chan BanEvent)

    // Init initializes the ban list and starts any background processes
    Init(ctx context.Context) error

    // Clear removes all entries from the ban list and cleans up resources
    Clear()
}
```

The `BanListI` interface defines the contract for managing banned peers by IP address or subnet, with methods for adding, removing, listing, and checking ban status. The system uses an SQL-backed implementation that persists ban information.

```go
type PeerBanManager struct {
    ctx           context.Context
    mu            sync.RWMutex
    peerBanScores map[string]*BanScore
    reasonPoints  map[BanReason]int
    banThreshold  int
    banDuration   time.Duration
    decayInterval time.Duration
    decayAmount   int
    handler       BanEventHandler
}
```

The `PeerBanManager` implements the `PeerBanManagerI` interface and maintains scores for peers, automatically banning them when they exceed a threshold. It provides methods like:

- `AddScore`: Increments a peer's score for specific violations
- `GetBanScore`: Retrieves the current ban score and status
- `IsBanned`: Checks if a peer is currently banned

The system defines standard ban reasons with associated scoring:

```go
type BanReason int

const (
    ReasonUnknown BanReason = iota
    ReasonInvalidSubtree     // 10 points
    ReasonProtocolViolation  // 20 points
    ReasonSpam              // 50 points
    ReasonInvalidBlock      // 10 points
)
```

Peer scores automatically decay over time to allow for recovery from temporary issues.

### Public API Methods

```go
func (s *Server) GetPeers(ctx context.Context, _ *emptypb.Empty) (*p2p_api.GetPeersResponse, error)
```

Returns a list of connected peers with their connection information and status.

```go
func (s *Server) BanPeer(ctx context.Context, peer *p2p_api.BanPeerRequest) (*p2p_api.BanPeerResponse, error)
```

Bans a peer by their peer ID, preventing future connections from that peer.

```go
func (s *Server) UnbanPeer(ctx context.Context, peer *p2p_api.UnbanPeerRequest) (*p2p_api.UnbanPeerResponse, error)
```

Removes a ban on a specific peer, allowing them to reconnect.

```go
func (s *Server) IsBanned(ctx context.Context, peer *p2p_api.IsBannedRequest) (*p2p_api.IsBannedResponse, error)
```

Checks if a specific peer ID is currently banned.

```go
func (s *Server) ListBanned(ctx context.Context, _ *emptypb.Empty) (*p2p_api.ListBannedResponse, error)
```

Returns a list of all currently banned peer IDs and IP addresses.

```go
func (s *Server) ClearBanned(ctx context.Context, _ *emptypb.Empty) (*p2p_api.ClearBannedResponse, error)
```

Clears all bans from the ban list, allowing all previously banned peers to reconnect.

```go
func (s *Server) AddBanScore(ctx context.Context, req *p2p_api.AddBanScoreRequest) (*p2p_api.AddBanScoreResponse, error)
```

Increments a peer's ban score. When the ban score reaches a threshold, the peer is automatically banned.

```go
func (s *Server) ConnectPeer(ctx context.Context, req *p2p_api.ConnectPeerRequest) (*p2p_api.ConnectPeerResponse, error)
```

Initiates a connection to a peer using multiaddr format.

```go
func (s *Server) DisconnectPeer(ctx context.Context, req *p2p_api.DisconnectPeerRequest) (*p2p_api.DisconnectPeerResponse, error)
```

Disconnects from a currently connected peer.

#### Catchup Metrics and Reputation Endpoints

```go
func (s *Server) RecordCatchupAttempt(ctx context.Context, req *p2p_api.RecordCatchupAttemptRequest) (*p2p_api.RecordCatchupAttemptResponse, error)
```

Records that a catchup attempt was initiated with a peer. This increments the interaction attempt counter.

**Parameters**:

- `peer_id` (string): The peer identifier

```go
func (s *Server) RecordCatchupSuccess(ctx context.Context, req *p2p_api.RecordCatchupSuccessRequest) (*p2p_api.RecordCatchupSuccessResponse, error)
```

Records a successful catchup operation with a peer. This increments success counters and updates the peer's reputation score positively.

**Parameters**:

- `peer_id` (string): The peer identifier
- `duration_ms` (int64): Duration of the catchup operation in milliseconds

```go
func (s *Server) RecordCatchupFailure(ctx context.Context, req *p2p_api.RecordCatchupFailureRequest) (*p2p_api.RecordCatchupFailureResponse, error)
```

Records a failed catchup attempt with a peer. This increments failure counters and reduces reputation.

**Parameters**:

- `peer_id` (string): The peer identifier

**Impact**: Decreases peer reputation score and applies recent failure penalty.

```go
func (s *Server) RecordCatchupMalicious(ctx context.Context, req *p2p_api.RecordCatchupMaliciousRequest) (*p2p_api.RecordCatchupMaliciousResponse, error)
```

Marks a peer as malicious after detecting invalid data during catchup. This severely penalizes the peer's reputation.

**Parameters**:

- `peer_id` (string): The peer identifier

```go
func (s *Server) UpdateCatchupReputation(ctx context.Context, req *p2p_api.UpdateCatchupReputationRequest) (*p2p_api.UpdateCatchupReputationResponse, error)
```

Directly sets a peer's reputation score. Use sparingly as this bypasses the normal reputation calculation algorithm.

**Parameters**:

- `peer_id` (string): The peer identifier
- `score` (double): Reputation score value (0-100 range)

```go
func (s *Server) UpdateCatchupError(ctx context.Context, req *p2p_api.UpdateCatchupErrorRequest) (*p2p_api.UpdateCatchupErrorResponse, error)
```

Records the last error message from a catchup attempt with a peer.

**Parameters**:

- `peer_id` (string): The peer identifier
- `error_msg` (string): Error message describing the failure

```go
func (s *Server) ResetReputation(ctx context.Context, req *p2p_api.ResetReputationRequest) (*p2p_api.ResetReputationResponse, error)
```

Resets reputation for one or all peers back to neutral (50.0). This implements the reputation recovery mechanism.

**Parameters**:

- `peer_id` (string, optional): If provided, resets reputation for specific peer. If omitted, resets all peers

```go
func (s *Server) GetPeersForCatchup(ctx context.Context, req *p2p_api.GetPeersForCatchupRequest) (*p2p_api.GetPeersForCatchupResponse, error)
```

Returns a list of peers suitable for catchup operations, ranked by reputation and filtered by selection criteria.

**Parameters**: None (can optionally filter in the future)

**Returns**: Array of `PeerInfoForCatchup` containing peer ID, height, block hash, data hub URL, catchup metrics

#### Validation Reporting Endpoints

```go
func (s *Server) ReportValidSubtree(ctx context.Context, req *p2p_api.ReportValidSubtreeRequest) (*p2p_api.ReportValidSubtreeResponse, error)
```

Reports that a peer provided a valid subtree. This increments the peer's positive interaction counters.

**Parameters**:

- `peer_id` (string): The peer identifier
- `subtree_hash` (string): Hash of the validated subtree

**Returns**: `success` (bool) and `message` (string) indicating validation result

```go
func (s *Server) ReportValidBlock(ctx context.Context, req *p2p_api.ReportValidBlockRequest) (*p2p_api.ReportValidBlockResponse, error)
```

Reports that a peer provided a valid block. This increments the peer's positive interaction counters.

**Parameters**:

- `peer_id` (string): The peer identifier
- `block_hash` (string): Hash of the validated block

**Returns**: `success` (bool) and `message` (string) indicating validation result

#### Peer Status and Query Endpoints

```go
func (s *Server) IsPeerMalicious(ctx context.Context, req *p2p_api.IsPeerMaliciousRequest) (*p2p_api.IsPeerMaliciousResponse, error)
```

Checks if a peer has been marked as malicious.

**Parameters**:

- `peer_id` (string): The peer identifier

**Returns**: `is_malicious` (bool) and optional `reason` (string) explaining why peer is malicious

```go
func (s *Server) IsPeerUnhealthy(ctx context.Context, req *p2p_api.IsPeerUnhealthyRequest) (*p2p_api.IsPeerUnhealthyResponse, error)
```

Checks if a peer is considered unhealthy based on reputation score and recent failures.

**Parameters**:

- `peer_id` (string): The peer identifier

**Returns**: `is_unhealthy` (bool), optional `reason` (string), and `reputation_score` (float)

```go
func (s *Server) GetPeerRegistry(ctx context.Context, _ *emptypb.Empty) (*p2p_api.GetPeerRegistryResponse, error)
```

Returns comprehensive information about all peers in the registry, including full reputation metrics and interaction history.

**Returns**: Array of `PeerRegistryInfo` containing:

- **Identity**: ID, client name, connection status
- **Blockchain**: Height, block hash, storage mode, data hub URL
- **Ban Info**: Ban score, is banned status
- **Connection**: Connected at timestamp, bytes received
- **Timing**: Last block time, last message time
- **Metrics**: Interaction attempts/successes/failures, malicious count, average response time
- **Reputation**: Reputation score (0-100)
- **Errors**: Last catchup error message and timestamp

```go
func (s *Server) GetPeer(ctx context.Context, req *p2p_api.GetPeerRequest) (*p2p_api.GetPeerResponse, error)
```

Returns detailed information for a single peer by peer ID.

**Parameters**:

- `peer_id` (string): The peer identifier to query

**Returns**: `PeerRegistryInfo` object with full peer details, and `found` (bool) indicating if peer exists

#### Metrics Tracking Endpoints

```go
func (s *Server) RecordBytesDownloaded(ctx context.Context, req *p2p_api.RecordBytesDownloadedRequest) (*p2p_api.RecordBytesDownloadedResponse, error)
```

Records bytes downloaded from a peer via HTTP (typically from their DataHub).

**Parameters**:

- `peer_id` (string): The peer identifier that provided the data
- `bytes_downloaded` (uint64): Number of bytes downloaded from the peer

### Message Handlers

- `handleBlockTopic`: Handles incoming block messages and validates block announcements.
- `handleSubtreeTopic`: Handles incoming subtree messages and processes subtree data.
- `handleRejectedTxTopic`: Handles rejected transaction notifications from peers.
- `handleNodeStatusTopic`: Handles incoming node status update messages.
- `invalidBlockHandler`: Processes notifications about invalid blocks from Kafka.
- `invalidSubtreeHandler`: Processes notifications about invalid subtrees from Kafka.
- `rejectedTxHandler`: Processes rejected transaction notifications from Kafka.

### Message Structures

The P2P service uses JSON-encoded messages for network communication:

#### BlockMessage

Announces the availability of a new block to the network:

```json
{
  "Hash": "block_hash_hex",
  "Height": 123456,
  "DataHubURL": "http://node-address:port",
  "PeerID": "peer_identifier",
  "Header": "block_header_hex"  // NEW: Raw block header in hexadecimal format
}
```

The `Header` field was added to enable peers to quickly validate block properties without fetching the full block data. This reduces network latency and allows for faster block propagation decisions.

#### SubtreeMessage

Announces the availability of a subtree (transaction batch):

```json
{
  "Hash": "subtree_hash_hex",
  "DataHubURL": "http://node-address:port",
  "PeerID": "peer_identifier"
}
```

#### BestBlockMessage

Requests or announces the current best block:

```json
{
  "Height": 123456,
  "Hash": "best_block_hash_hex"
}
```

#### MiningOnMessage

Notifies that mining has started on a new block:

```json
{
  "Height": 123457,
  "PreviousHash": "previous_block_hash_hex"
}
```

#### HandshakeMessage

Used for version/verack exchanges during peer connection:

```json
{
  "type": "version",  // or "verack"
  "peerID": "peer_identifier",
  "bestHeight": 123456,
  "bestHash": "best_block_hash_hex",
  "dataHubURL": "http://node-address:port",
  "userAgent": "teranode/bitcoin/v1.0.0",
  "services": 1,
  "topicPrefix": "mainnet"  // Chain identifier for network isolation
}
```

The `topicPrefix` field ensures that nodes only connect to peers on the same chain (e.g., mainnet, testnet). This prevents accidental cross-chain connections and maintains network integrity.

### Handshake Protocol

The P2P handshake protocol establishes connections between peers and exchanges version information:

1. **Version Message**: When a peer connects, it sends a version message containing:

    - `UserAgent`: The node's identifier in format "teranode/bitcoin/{version}"
    - `BestHeight`: The peer's current blockchain height
    - `BestHash`: The hash of the peer's best block
    - `PeerID`: The peer's unique identifier
    - `TopicPrefix`: The chain identifier prefix (e.g., "mainnet", "testnet") for network isolation
    - `DataHubURL`: The URL where the peer's data can be accessed
    - `Services`: Bitmap of services offered by the peer

2. **Verack Message**: Upon receiving a version message, the peer responds with a verack (version acknowledgment) containing similar information.

3. **Topic Prefix Validation**: During handshake, peers validate that they share the same `TopicPrefix`:

    - If topic prefixes don't match, the handshake is rejected
    - This ensures nodes on different chains (mainnet vs testnet) don't accidentally connect
    - The topic prefix is configured via `ChainCfgParams.TopicPrefix` in the settings

4. **Dynamic Version**: The version in the UserAgent field is automatically determined at build time:

    - Tagged releases use the Git tag (e.g., "teranode/bitcoin/v1.2.3")
    - Development builds use a pseudo-version (e.g., "teranode/bitcoin/v0.0.0-20250731141601-18714b9")

## Key Processes

### P2P Communication

The server uses a P2P node for communication with other peers in the network. It publishes and subscribes to various topics such as best blocks, blocks, subtrees, and mining-on notifications.

### Blockchain Interaction

The server interacts with the blockchain client to retrieve and validate block information. It also subscribes to blockchain notifications to propagate relevant information to peers.

### Kafka Integration

The server uses Kafka producers and consumers for efficient distribution of block and subtree data across the network. Kafka connections support TLS/SSL encryption for secure communication in production environments (see Kafka TLS Configuration section for details).

## Configuration

The following settings can be configured for the p2p service:

### Chain Configuration

- `ChainCfgParams.TopicPrefix`: **REQUIRED** - Chain identifier prefix (e.g., "mainnet", "testnet", "stn")
    - Used during P2P handshake to ensure network isolation
    - Peers with different topic prefixes will reject connections
    - Prevents accidental cross-chain connections
    - Must match across all nodes in the same network

### Network Configuration

- `p2p_listen_addresses`: Specifies the IP addresses for the P2P service to bind to.
- `p2p_advertise_addresses`: Addresses to advertise to other peers in the network. Each address can be specified with or without a port (e.g., `192.168.1.1` or `example.com:9906`). When a port is not specified, the system will use the value from `p2p_port` as the default. Both IP addresses and domain names are supported. Format examples: `192.168.1.1`, `example.com:9906`, `node.local:8001`.
- `p2p_port`: **REQUIRED** - Defines the port number on which the P2P service listens.
- `p2p_block_topic`: **REQUIRED** - The topic name used for block-related messages in the P2P network.
- `p2p_subtree_topic`: **REQUIRED** - Specifies the topic for subtree-related messages within the P2P network.
- `p2p_handshake_topic`: **REQUIRED** - Defines the topic for peer handshake messages, used for version and verack exchanges.
- `p2p_mining_on_topic`: **REQUIRED** - The topic used for messages related to the start of mining a new block.
- `p2p_rejected_tx_topic`: **REQUIRED** - Specifies the topic for broadcasting information about rejected transactions.
- `p2p_node_status_topic`: Topic for node status update messages.
- `p2p_shared_key`: A shared key for securing P2P communications, required for private network configurations.
- `p2p_dht_protocol_id`: Identifier for the DHT protocol used by the P2P network.
- `p2p_dht_use_private`: A boolean flag indicating whether a private Distributed Hash Table (DHT) should be used, enhancing network privacy.
- `p2p_optimise_retries`: A boolean setting to optimize retry behavior in P2P communications, potentially improving network efficiency.
- `p2p_static_peers`: A list of static peer addresses to connect to, ensuring the P2P node can always reach known peers.
- `p2p_private_key`: The private key for the P2P node, used for secure communications within the network. If not provided, a new Ed25519 key is automatically generated and persistently stored in the blockchain database.
- `p2p_http_address`: Specifies the HTTP address for external clients to connect to the P2P service.
- `p2p_http_listen_address`: Specifies the HTTP listen address for the P2P service, enabling HTTP-based interactions.
- `p2p_grpc_address`: Specifies the gRPC address for external clients to connect to the P2P service.
- `p2p_grpc_listen_address`: Specifies the gRPC listen address for the P2P service.
- `p2p_ban_threshold`: Score threshold at which peers are banned from the network.
- `p2p_ban_duration`: Duration of time a peer remains banned after exceeding the ban threshold.
- `securityLevelHTTP`: Defines the security level for HTTP communications, where a higher level might enforce HTTPS.
- `server_certFile` and `server_keyFile`: These settings specify the paths to the SSL certificate and key files, respectively, required for setting up HTTPS.
- `p2p_ban_default_duration`: Specifies the default duration for peer bans (defaults to 24 hours if not set).
- `p2p_ban_persist_path`: Defines the path where ban list information is stored persistently.
- `p2p_ban_max_entries`: Sets the maximum number of entries allowed in the ban list to prevent memory exhaustion.

## Dependencies

The P2P Server depends on several components:

- `blockchain.ClientI`: Interface for blockchain operations
- `blockvalidation.Interface`: Interface for block validation operations
- `blockassembly.ClientI`: Interface for block assembly operations
- `p2pMessageBus.P2PClient`: P2P client interface from the `github.com/bsv-blockchain/go-p2p-message-bus` package
- Kafka producers and consumers for message distribution

These dependencies are injected into the `Server` struct during initialization.

The use of the go-p2p-message-bus package enables external developers to:

- Create custom P2P client implementations that are compatible with Teranode
- Build applications that can directly integrate with Teranode's P2P messaging system
- Extend P2P functionality while maintaining compatibility with the standard interface

## Error Handling

Errors are logged using the provided logger. Critical errors may result in the server shutting down or specific components failing to start.

## Concurrency

The server uses goroutines for handling concurrent operations, such as message processing, HTTP server, and blockchain subscription listening. It also uses contexts for cancellation and timeout management.

## Security

The server supports both HTTP and HTTPS configurations based on the `securityLevelHTTP` setting. When using HTTPS, it requires certificate and key files to be specified in the configuration.

## Related Documents

- [P2P Topic Guide](../../topics/services/p2p.md)
- [P2P Settings](../settings/services/p2p_settings.md)
- [P2P Protobuf Reference](../protobuf_docs/p2pProto.md)
- [Prometheus Metrics](../prometheusMetrics.md)
