<!-- markdownlint-disable MD051 -->
# GRPC Documentation - PeerService

<a name="top"></a>

## Table of Contents

- [p2p_api.proto](#p2p_api.proto)
    - [AddBanScoreRequest](#AddBanScoreRequest)
    - [AddBanScoreResponse](#AddBanScoreResponse)
    - [BanPeerRequest](#BanPeerRequest)
    - [BanPeerResponse](#BanPeerResponse)
    - [ClearBannedResponse](#ClearBannedResponse)
    - [ConnectPeerRequest](#ConnectPeerRequest)
    - [ConnectPeerResponse](#ConnectPeerResponse)
    - [DisconnectPeerRequest](#DisconnectPeerRequest)
    - [DisconnectPeerResponse](#DisconnectPeerResponse)
    - [GetPeerRegistryResponse](#GetPeerRegistryResponse)
    - [GetPeerRequest](#GetPeerRequest)
    - [GetPeerResponse](#GetPeerResponse)
    - [GetPeersForCatchupRequest](#GetPeersForCatchupRequest)
    - [GetPeersForCatchupResponse](#GetPeersForCatchupResponse)
    - [GetPeersResponse](#GetPeersResponse)
    - [IsBannedRequest](#IsBannedRequest)
    - [IsBannedResponse](#IsBannedResponse)
    - [IsPeerMaliciousRequest](#IsPeerMaliciousRequest)
    - [IsPeerMaliciousResponse](#IsPeerMaliciousResponse)
    - [IsPeerUnhealthyRequest](#IsPeerUnhealthyRequest)
    - [IsPeerUnhealthyResponse](#IsPeerUnhealthyResponse)
    - [ListBannedResponse](#ListBannedResponse)
    - [Peer](#Peer)
    - [PeerInfoForCatchup](#PeerInfoForCatchup)
    - [PeerRegistryInfo](#PeerRegistryInfo)
    - [RecordBytesDownloadedRequest](#RecordBytesDownloadedRequest)
    - [RecordBytesDownloadedResponse](#RecordBytesDownloadedResponse)
    - [RecordCatchupAttemptRequest](#RecordCatchupAttemptRequest)
    - [RecordCatchupAttemptResponse](#RecordCatchupAttemptResponse)
    - [RecordCatchupFailureRequest](#RecordCatchupFailureRequest)
    - [RecordCatchupFailureResponse](#RecordCatchupFailureResponse)
    - [RecordCatchupMaliciousRequest](#RecordCatchupMaliciousRequest)
    - [RecordCatchupMaliciousResponse](#RecordCatchupMaliciousResponse)
    - [RecordCatchupSuccessRequest](#RecordCatchupSuccessRequest)
    - [RecordCatchupSuccessResponse](#RecordCatchupSuccessResponse)
    - [ReportValidBlockRequest](#ReportValidBlockRequest)
    - [ReportValidBlockResponse](#ReportValidBlockResponse)
    - [ReportValidSubtreeRequest](#ReportValidSubtreeRequest)
    - [ReportValidSubtreeResponse](#ReportValidSubtreeResponse)
    - [ResetReputationRequest](#ResetReputationRequest)
    - [ResetReputationResponse](#ResetReputationResponse)
    - [UnbanPeerRequest](#UnbanPeerRequest)
    - [UnbanPeerResponse](#UnbanPeerResponse)
    - [UpdateCatchupErrorRequest](#UpdateCatchupErrorRequest)
    - [UpdateCatchupErrorResponse](#UpdateCatchupErrorResponse)
    - [UpdateCatchupReputationRequest](#UpdateCatchupReputationRequest)
    - [UpdateCatchupReputationResponse](#UpdateCatchupReputationResponse)

    - [PeerService](#PeerService)

- [Scalar Value Types](#scalar-value-types)

<a name="p2p_api.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## p2p_api.proto

> Package p2p_api defines the gRPC service interface for P2P networking operations including peer management, banning, catchup tracking, and reputation management.

<a name="AddBanScoreRequest"></a>

### AddBanScoreRequest

Represents a request to add a ban score to a peer.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| peer_id | [string](#string) |  | The peer ID to add ban score to |
| reason | [string](#string) |  | Reason for adding the ban score |

<a name="AddBanScoreResponse"></a>

### AddBanScoreResponse

Represents the response from adding a ban score.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | Indicates whether the operation succeeded |

<a name="BanPeerRequest"></a>

### BanPeerRequest

Represents a request to ban a peer.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| addr | [string](#string) |  | The address of the peer to ban |
| until | [int64](#int64) |  | Unix timestamp indicating when the ban expires |

<a name="BanPeerResponse"></a>

### BanPeerResponse

Represents the response from banning a peer.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | Indicates whether the ban operation succeeded |

<a name="ClearBannedResponse"></a>

### ClearBannedResponse

Represents the response from clearing all banned peers.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | Indicates whether the clear operation succeeded |

<a name="ConnectPeerRequest"></a>

### ConnectPeerRequest

Represents a request to connect to a peer.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| peer_address | [string](#string) |  | The peer address in multiaddr format (e.g., /ip4/127.0.0.1/tcp/9005/p2p/12D3KooW...) |

<a name="ConnectPeerResponse"></a>

### ConnectPeerResponse

Represents the response from connecting to a peer.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| success | [bool](#bool) |  | Indicates whether the connection succeeded |
| error | [string](#string) |  | Error message if the connection failed |

<a name="DisconnectPeerRequest"></a>

### DisconnectPeerRequest

Represents a request to disconnect from a peer.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| peer_id | [string](#string) |  | The peer ID to disconnect from |

<a name="DisconnectPeerResponse"></a>

### DisconnectPeerResponse

Represents the response from disconnecting from a peer.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| success | [bool](#bool) |  | Indicates whether the disconnection succeeded |
| error | [string](#string) |  | Error message if the disconnection failed |

<a name="GetPeerRegistryResponse"></a>

### GetPeerRegistryResponse

Represents the response containing comprehensive peer registry data.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| peers | [PeerRegistryInfo](#PeerRegistryInfo) | repeated | List of all peers with full registry metadata |

<a name="GetPeerRequest"></a>

### GetPeerRequest

Represents a request to get information about a specific peer.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| peer_id | [string](#string) |  | The peer ID to query |

<a name="GetPeerResponse"></a>

### GetPeerResponse

Represents the response containing information about a specific peer.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| peer | [PeerRegistryInfo](#PeerRegistryInfo) |  | Peer information (null/empty if peer not found) |
| found | [bool](#bool) |  | Indicates whether the peer was found |

<a name="GetPeersForCatchupRequest"></a>

### GetPeersForCatchupRequest

Represents a request to get peers suitable for catchup operations.

This message is currently empty but can be extended with filtering parameters in the future.

<a name="GetPeersForCatchupResponse"></a>

### GetPeersForCatchupResponse

Represents the response containing peers suitable for catchup.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| peers | [PeerInfoForCatchup](#PeerInfoForCatchup) | repeated | List of peers with catchup-relevant information |

<a name="GetPeersResponse"></a>

### GetPeersResponse

Represents the response containing all connected peers.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| peers | [Peer](#Peer) | repeated | List of all connected peers |

<a name="IsBannedRequest"></a>

### IsBannedRequest

Represents a request to check if an IP or subnet is banned.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ipOrSubnet | [string](#string) |  | The IP address or subnet to check |

<a name="IsBannedResponse"></a>

### IsBannedResponse

Represents the response indicating whether an IP or subnet is banned.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| isBanned | [bool](#bool) |  | Indicates whether the IP or subnet is banned |

<a name="IsPeerMaliciousRequest"></a>

### IsPeerMaliciousRequest

Represents a request to check if a peer is considered malicious.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| peer_id | [string](#string) |  | The peer ID to check |

<a name="IsPeerMaliciousResponse"></a>

### IsPeerMaliciousResponse

Represents the response indicating whether a peer is malicious.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| is_malicious | [bool](#bool) |  | Indicates whether the peer is considered malicious |
| reason | [string](#string) |  | Optional reason why the peer is considered malicious |

<a name="IsPeerUnhealthyRequest"></a>

### IsPeerUnhealthyRequest

Represents a request to check if a peer is unhealthy.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| peer_id | [string](#string) |  | The peer ID to check |

<a name="IsPeerUnhealthyResponse"></a>

### IsPeerUnhealthyResponse

Represents the response indicating whether a peer is unhealthy.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| is_unhealthy | [bool](#bool) |  | Indicates whether the peer is considered unhealthy |
| reason | [string](#string) |  | Optional reason why the peer is considered unhealthy |
| reputation_score | [float](#float) |  | Current reputation score of the peer |

<a name="ListBannedResponse"></a>

### ListBannedResponse

Represents the response containing all banned addresses.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| banned | [string](#string) | repeated | List of all banned IP addresses or subnets |

<a name="Peer"></a>

### Peer

Represents detailed information about a connected peer.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | Peer identifier |
| addr | [string](#string) |  | Remote address (host:port) |
| addrLocal | [string](#string) |  | Local address used for this connection |
| services | [string](#string) |  | Services supported by the peer |
| lastSend | [int64](#int64) |  | Time of last message sent (seconds since epoch) |
| lastRecv | [int64](#int64) |  | Time of last message received (seconds since epoch) |
| sendSize | [int64](#int64) |  | Size of send buffer |
| recvSize | [int64](#int64) |  | Size of receive buffer |
| sendMemory | [int64](#int64) |  | Memory used by send buffer |
| pauseSend | [bool](#bool) |  | Whether sending is paused |
| unpauseSend | [bool](#bool) |  | Whether sending is unpaused |
| bytesSent | [uint64](#uint64) |  | Total bytes sent to this peer |
| bytesReceived | [uint64](#uint64) |  | Total bytes received from this peer |
| avgRecvBandwidth | [int64](#int64) |  | Average receive bandwidth |
| assocId | [string](#string) |  | Association identifier |
| streamPolicy | [string](#string) |  | Stream policy used for this connection |
| inbound | [bool](#bool) |  | Whether this is an inbound connection |
| connTime | [int64](#int64) |  | Connection time (seconds since epoch) |
| pingTime | [int64](#int64) |  | Last ping time in microseconds |
| timeOffset | [int64](#int64) |  | Time offset from this peer |
| version | [uint32](#uint32) |  | Protocol version of the peer |
| subVer | [string](#string) |  | User agent string of the peer |
| startingHeight | [uint32](#uint32) |  | Block height when connection was established |
| currentHeight | [uint32](#uint32) |  | Current block height of the peer |
| banscore | [int32](#int32) |  | Ban score of this peer |
| whitelisted | [bool](#bool) |  | Whether this peer is whitelisted |
| feeFilter | [int64](#int64) |  | Minimum fee filter for transactions |

<a name="PeerInfoForCatchup"></a>

### PeerInfoForCatchup

Represents peer information relevant for catchup operations.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | Peer identifier |
| height | [uint32](#uint32) |  | Current block height of the peer |
| block_hash | [string](#string) |  | Hash of the peer's current block |
| data_hub_url | [string](#string) |  | URL of the peer's data hub service |
| catchup_reputation_score | [double](#double) |  | Reputation score for catchup operations |
| catchup_attempts | [int64](#int64) |  | Number of catchup attempts with this peer |
| catchup_successes | [int64](#int64) |  | Number of successful catchup operations |
| catchup_failures | [int64](#int64) |  | Number of failed catchup operations |

<a name="PeerRegistryInfo"></a>

### PeerRegistryInfo

Represents comprehensive peer information with all registry metadata.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | Peer identifier |
| height | [uint32](#uint32) |  | Current block height of the peer |
| block_hash | [string](#string) |  | Hash of the peer's current block |
| data_hub_url | [string](#string) |  | URL of the peer's data hub service |
| ban_score | [int32](#int32) |  | Ban score of this peer |
| is_banned | [bool](#bool) |  | Whether the peer is currently banned |
| is_connected | [bool](#bool) |  | Whether the peer is currently connected |
| connected_at | [int64](#int64) |  | Connection time (Unix timestamp) |
| bytes_received | [uint64](#uint64) |  | Total bytes received from this peer |
| last_block_time | [int64](#int64) |  | Time of last block received (Unix timestamp) |
| last_message_time | [int64](#int64) |  | Time of last message received (Unix timestamp) |
| interaction_attempts | [int64](#int64) |  | Number of interaction attempts |
| interaction_successes | [int64](#int64) |  | Number of successful interactions |
| interaction_failures | [int64](#int64) |  | Number of failed interactions |
| last_interaction_attempt | [int64](#int64) |  | Time of last interaction attempt (Unix timestamp) |
| last_interaction_success | [int64](#int64) |  | Time of last successful interaction (Unix timestamp) |
| last_interaction_failure | [int64](#int64) |  | Time of last failed interaction (Unix timestamp) |
| reputation_score | [double](#double) |  | Overall reputation score |
| malicious_count | [int64](#int64) |  | Number of times peer was flagged as malicious |
| avg_response_time_ms | [int64](#int64) |  | Average response time in milliseconds |
| storage | [string](#string) |  | Storage type used by the peer |
| client_name | [string](#string) |  | Human-readable name of the client software |
| last_catchup_error | [string](#string) |  | Last error message from catchup attempt |
| last_catchup_error_time | [int64](#int64) |  | Time of last catchup error (Unix timestamp) |

<a name="RecordBytesDownloadedRequest"></a>

### RecordBytesDownloadedRequest

Represents a request to record bytes downloaded from a peer via HTTP.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| peer_id | [string](#string) |  | Peer ID that provided the data |
| bytes_downloaded | [uint64](#uint64) |  | Number of bytes downloaded |

<a name="RecordBytesDownloadedResponse"></a>

### RecordBytesDownloadedResponse

Represents the response from recording bytes downloaded.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | Indicates whether the operation succeeded |

<a name="RecordCatchupAttemptRequest"></a>

### RecordCatchupAttemptRequest

Represents a request to record a catchup attempt with a peer.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| peer_id | [string](#string) |  | Peer ID for the catchup attempt |

<a name="RecordCatchupAttemptResponse"></a>

### RecordCatchupAttemptResponse

Represents the response from recording a catchup attempt.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | Indicates whether the operation succeeded |

<a name="RecordCatchupFailureRequest"></a>

### RecordCatchupFailureRequest

Represents a request to record a failed catchup attempt.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| peer_id | [string](#string) |  | Peer ID for the failed catchup |

<a name="RecordCatchupFailureResponse"></a>

### RecordCatchupFailureResponse

Represents the response from recording a catchup failure.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | Indicates whether the operation succeeded |

<a name="RecordCatchupMaliciousRequest"></a>

### RecordCatchupMaliciousRequest

Represents a request to record a malicious catchup attempt.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| peer_id | [string](#string) |  | Peer ID flagged as malicious |

<a name="RecordCatchupMaliciousResponse"></a>

### RecordCatchupMaliciousResponse

Represents the response from recording a malicious catchup.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | Indicates whether the operation succeeded |

<a name="RecordCatchupSuccessRequest"></a>

### RecordCatchupSuccessRequest

Represents a request to record a successful catchup.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| peer_id | [string](#string) |  | Peer ID for the successful catchup |
| duration_ms | [int64](#int64) |  | Duration of the catchup in milliseconds |

<a name="RecordCatchupSuccessResponse"></a>

### RecordCatchupSuccessResponse

Represents the response from recording a catchup success.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | Indicates whether the operation succeeded |

<a name="ReportValidBlockRequest"></a>

### ReportValidBlockRequest

Represents a request to report reception of a valid block from a peer.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| peer_id | [string](#string) |  | Peer ID that provided the block |
| block_hash | [string](#string) |  | Hash of the valid block |

<a name="ReportValidBlockResponse"></a>

### ReportValidBlockResponse

Represents the response from reporting a valid block.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| success | [bool](#bool) |  | Indicates whether the report was successful |
| message | [string](#string) |  | Optional message about the result |

<a name="ReportValidSubtreeRequest"></a>

### ReportValidSubtreeRequest

Represents a request to report reception of a valid subtree from a peer.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| peer_id | [string](#string) |  | Peer ID that provided the subtree |
| subtree_hash | [string](#string) |  | Hash of the valid subtree |

<a name="ReportValidSubtreeResponse"></a>

### ReportValidSubtreeResponse

Represents the response from reporting a valid subtree.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| success | [bool](#bool) |  | Indicates whether the report was successful |
| message | [string](#string) |  | Optional message about the result |

<a name="ResetReputationRequest"></a>

### ResetReputationRequest

Represents a request to reset reputation scores for peers.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| peer_id | [string](#string) |  | Peer ID to reset (empty string means reset all peers) |

<a name="ResetReputationResponse"></a>

### ResetReputationResponse

Represents the response from resetting reputation scores.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | Indicates whether the operation succeeded |
| peers_reset | [int32](#int32) |  | Number of peers whose reputation was reset |

<a name="UnbanPeerRequest"></a>

### UnbanPeerRequest

Represents a request to unban a peer.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| addr | [string](#string) |  | The address of the peer to unban |

<a name="UnbanPeerResponse"></a>

### UnbanPeerResponse

Represents the response from unbanning a peer.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | Indicates whether the unban operation succeeded |

<a name="UpdateCatchupErrorRequest"></a>

### UpdateCatchupErrorRequest

Represents a request to update the catchup error message for a peer.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| peer_id | [string](#string) |  | Peer ID to update |
| error_msg | [string](#string) |  | Error message to record |

<a name="UpdateCatchupErrorResponse"></a>

### UpdateCatchupErrorResponse

Represents the response from updating a catchup error.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | Indicates whether the operation succeeded |

<a name="UpdateCatchupReputationRequest"></a>

### UpdateCatchupReputationRequest

Represents a request to update the catchup reputation score for a peer.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| peer_id | [string](#string) |  | Peer ID to update |
| score | [double](#double) |  | Reputation score between 0-100 |

<a name="UpdateCatchupReputationResponse"></a>

### UpdateCatchupReputationResponse

Represents the response from updating catchup reputation.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | Indicates whether the operation succeeded |

<!-- end messages -->

<!-- end enums -->

<!-- end HasExtensions -->

<a name="PeerService"></a>

### PeerService

Service provides methods for P2P peer management, including connection handling, ban management, catchup tracking, and reputation management.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| GetPeers | [.google.protobuf.Empty](#google-protobuf-Empty) | [GetPeersResponse](#p2p_api-GetPeersResponse) | Retrieves a list of all connected peers with their detailed information. |
| BanPeer | [BanPeerRequest](#p2p_api-BanPeerRequest) | [BanPeerResponse](#p2p_api-BanPeerResponse) | Bans a peer by address until the specified time. |
| UnbanPeer | [UnbanPeerRequest](#p2p_api-UnbanPeerRequest) | [UnbanPeerResponse](#p2p_api-UnbanPeerResponse) | Removes a ban for a peer address. |
| IsBanned | [IsBannedRequest](#p2p_api-IsBannedRequest) | [IsBannedResponse](#p2p_api-IsBannedResponse) | Checks if an IP address or subnet is currently banned. |
| ListBanned | [.google.protobuf.Empty](#google-protobuf-Empty) | [ListBannedResponse](#p2p_api-ListBannedResponse) | Lists all currently banned addresses. |
| ClearBanned | [.google.protobuf.Empty](#google-protobuf-Empty) | [ClearBannedResponse](#p2p_api-ClearBannedResponse) | Clears all banned addresses. |
| AddBanScore | [AddBanScoreRequest](#p2p_api-AddBanScoreRequest) | [AddBanScoreResponse](#p2p_api-AddBanScoreResponse) | Adds a ban score to a peer. Peers with high ban scores may be automatically banned. |
| ConnectPeer | [ConnectPeerRequest](#p2p_api-ConnectPeerRequest) | [ConnectPeerResponse](#p2p_api-ConnectPeerResponse) | Initiates a connection to a peer using multiaddr format. |
| DisconnectPeer | [DisconnectPeerRequest](#p2p_api-DisconnectPeerRequest) | [DisconnectPeerResponse](#p2p_api-DisconnectPeerResponse) | Disconnects from a peer by peer ID. |
| RecordCatchupAttempt | [RecordCatchupAttemptRequest](#p2p_api-RecordCatchupAttemptRequest) | [RecordCatchupAttemptResponse](#p2p_api-RecordCatchupAttemptResponse) | Records an attempt to perform catchup with a peer. Used for reputation tracking. |
| RecordCatchupSuccess | [RecordCatchupSuccessRequest](#p2p_api-RecordCatchupSuccessRequest) | [RecordCatchupSuccessResponse](#p2p_api-RecordCatchupSuccessResponse) | Records a successful catchup operation including duration. Improves peer reputation. |
| RecordCatchupFailure | [RecordCatchupFailureRequest](#p2p_api-RecordCatchupFailureRequest) | [RecordCatchupFailureResponse](#p2p_api-RecordCatchupFailureResponse) | Records a failed catchup operation. Negatively affects peer reputation. |
| RecordCatchupMalicious | [RecordCatchupMaliciousRequest](#p2p_api-RecordCatchupMaliciousRequest) | [RecordCatchupMaliciousResponse](#p2p_api-RecordCatchupMaliciousResponse) | Records a malicious catchup attempt by a peer. Severely damages peer reputation. |
| UpdateCatchupReputation | [UpdateCatchupReputationRequest](#p2p_api-UpdateCatchupReputationRequest) | [UpdateCatchupReputationResponse](#p2p_api-UpdateCatchupReputationResponse) | Updates the catchup reputation score for a peer (0-100). |
| UpdateCatchupError | [UpdateCatchupErrorRequest](#p2p_api-UpdateCatchupErrorRequest) | [UpdateCatchupErrorResponse](#p2p_api-UpdateCatchupErrorResponse) | Records an error message from a catchup attempt for debugging. |
| ResetReputation | [ResetReputationRequest](#p2p_api-ResetReputationRequest) | [ResetReputationResponse](#p2p_api-ResetReputationResponse) | Resets reputation scores for a peer or all peers. |
| GetPeersForCatchup | [GetPeersForCatchupRequest](#p2p_api-GetPeersForCatchupRequest) | [GetPeersForCatchupResponse](#p2p_api-GetPeersForCatchupResponse) | Retrieves peers that are suitable for catchup operations with their reputation scores. |
| ReportValidSubtree | [ReportValidSubtreeRequest](#p2p_api-ReportValidSubtreeRequest) | [ReportValidSubtreeResponse](#p2p_api-ReportValidSubtreeResponse) | Reports reception of a valid subtree from a peer. Improves peer reputation. |
| ReportValidBlock | [ReportValidBlockRequest](#p2p_api-ReportValidBlockRequest) | [ReportValidBlockResponse](#p2p_api-ReportValidBlockResponse) | Reports reception of a valid block from a peer. Improves peer reputation. |
| IsPeerMalicious | [IsPeerMaliciousRequest](#p2p_api-IsPeerMaliciousRequest) | [IsPeerMaliciousResponse](#p2p_api-IsPeerMaliciousResponse) | Checks if a peer is flagged as malicious and returns the reason if so. |
| IsPeerUnhealthy | [IsPeerUnhealthyRequest](#p2p_api-IsPeerUnhealthyRequest) | [IsPeerUnhealthyResponse](#p2p_api-IsPeerUnhealthyResponse) | Checks if a peer is considered unhealthy based on reputation score and interaction history. |
| GetPeerRegistry | [.google.protobuf.Empty](#google-protobuf-Empty) | [GetPeerRegistryResponse](#p2p_api-GetPeerRegistryResponse) | Retrieves comprehensive information about all peers with full registry metadata. |
| RecordBytesDownloaded | [RecordBytesDownloadedRequest](#p2p_api-RecordBytesDownloadedRequest) | [RecordBytesDownloadedResponse](#p2p_api-RecordBytesDownloadedResponse) | Records bytes downloaded via HTTP from a peer for bandwidth tracking. |
| GetPeer | [GetPeerRequest](#p2p_api-GetPeerRequest) | [GetPeerResponse](#p2p_api-GetPeerResponse) | Retrieves comprehensive information about a single peer by peer ID. |

<!-- end services -->

## Scalar Value Types

| .proto Type | Notes | C++ | Java | Python | Go | C# | PHP | Ruby |
| ----------- | ----- | --- | ---- | ------ | -- | -- | --- | ---- |
| <a name="double" /> double |  | double | double | float | float64 | double | float | Float |
| <a name="float" /> float |  | float | float | float | float32 | float | float | Float |
| <a name="int32" /> int32 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="int64" /> int64 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="uint32" /> uint32 | Uses variable-length encoding. | uint32 | int | int/long | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="uint64" /> uint64 | Uses variable-length encoding. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum or Fixnum (as required) |
| <a name="sint32" /> sint32 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sint64" /> sint64 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="fixed32" /> fixed32 | Always four bytes. More efficient than uint32 if values are often greater than 2^28. | uint32 | int | int | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="fixed64" /> fixed64 | Always eight bytes. More efficient than uint64 if values are often greater than 2^56. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum |
| <a name="sfixed32" /> sfixed32 | Always four bytes. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sfixed64" /> sfixed64 | Always eight bytes. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="bool" /> bool |  | bool | boolean | boolean | bool | bool | boolean | TrueClass/FalseClass |
| <a name="string" /> string | A string must always contain UTF-8 encoded or 7-bit ASCII text. | string | String | str/unicode | string | string | string | String (UTF-8) |
| <a name="bytes" /> bytes | May contain any arbitrary sequence of bytes. | string | ByteString | str | []byte | ByteString | string | String (ASCII-8BIT) |

---
