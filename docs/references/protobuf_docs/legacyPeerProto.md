<!-- markdownlint-disable MD051 -->
# GRPC Documentation - Legacy PeerService

<a name="top"></a>

## Table of Contents

- [peer_api.proto](#peer_api.proto)
    - [BanPeerRequest](#BanPeerRequest)
    - [BanPeerResponse](#BanPeerResponse)
    - [ClearBannedResponse](#ClearBannedResponse)
    - [GetPeerCountResponse](#GetPeerCountResponse)
    - [GetPeersResponse](#GetPeersResponse)
    - [IsBannedRequest](#IsBannedRequest)
    - [IsBannedResponse](#IsBannedResponse)
    - [ListBannedResponse](#ListBannedResponse)
    - [Peer](#Peer)
    - [UnbanPeerRequest](#UnbanPeerRequest)
    - [UnbanPeerResponse](#UnbanPeerResponse)

    - [PeerService](#PeerService)

- [Scalar Value Types](#scalar-value-types)

<a name="peer_api.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## peer_api.proto

> Package peer_api defines the gRPC service interface for legacy peer management compatible with Bitcoin RPC interfaces.

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

<a name="GetPeerCountResponse"></a>

### GetPeerCountResponse

Represents the response containing the count of connected peers.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| count | [int32](#int32) |  | The number of currently connected peers |

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

<a name="ListBannedResponse"></a>

### ListBannedResponse

Represents the response containing all banned addresses.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| banned | [string](#string) | repeated | List of all banned IP addresses or subnets |

<a name="Peer"></a>

### Peer

Represents detailed information about a connected peer compatible with Bitcoin RPC format.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [int32](#int32) |  | Peer identifier |
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
| streamPolicy | [string](#string) |  | Stream policy used for this connection (e.g., "BlockPriority") |
| inbound | [bool](#bool) |  | Whether this is an inbound connection |
| connTime | [int64](#int64) |  | Connection time (seconds since epoch) |
| pingTime | [int64](#int64) |  | Last ping time in microseconds |
| timeOffset | [int64](#int64) |  | Time offset from this peer |
| version | [uint32](#uint32) |  | Protocol version of the peer |
| subVer | [string](#string) |  | User agent string of the peer |
| startingHeight | [int32](#int32) |  | Block height when connection was established |
| currentHeight | [int32](#int32) |  | Current block height of the peer |
| banscore | [int32](#int32) |  | Ban score of this peer |
| whitelisted | [bool](#bool) |  | Whether this peer is whitelisted |
| feeFilter | [int64](#int64) |  | Minimum fee filter for transactions |

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

<!-- end messages -->

<!-- end enums -->

<!-- end HasExtensions -->

<a name="PeerService"></a>

### PeerService

Service provides legacy peer management methods compatible with Bitcoin RPC interfaces.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| GetPeers | [.google.protobuf.Empty](#google-protobuf-Empty) | [GetPeersResponse](#peer_api-GetPeersResponse) | Retrieves a list of all connected peers with their detailed information. |
| BanPeer | [BanPeerRequest](#peer_api-BanPeerRequest) | [BanPeerResponse](#peer_api-BanPeerResponse) | Bans a peer by address until the specified time. |
| UnbanPeer | [UnbanPeerRequest](#peer_api-UnbanPeerRequest) | [UnbanPeerResponse](#peer_api-UnbanPeerResponse) | Removes a ban for a peer address. |
| IsBanned | [IsBannedRequest](#peer_api-IsBannedRequest) | [IsBannedResponse](#peer_api-IsBannedResponse) | Checks if an IP address or subnet is currently banned. |
| ListBanned | [.google.protobuf.Empty](#google-protobuf-Empty) | [ListBannedResponse](#peer_api-ListBannedResponse) | Lists all currently banned addresses. |
| ClearBanned | [.google.protobuf.Empty](#google-protobuf-Empty) | [ClearBannedResponse](#peer_api-ClearBannedResponse) | Clears all banned addresses. |
| GetPeerCount | [.google.protobuf.Empty](#google-protobuf-Empty) | [GetPeerCountResponse](#peer_api-GetPeerCountResponse) | Returns the count of currently connected peers. |

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
