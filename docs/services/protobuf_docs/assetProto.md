# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [asset_api.proto](#asset_apiproto)
    - [GetBlockHeaderRequest](#getblockheaderrequest)
    - [GetBlockHeaderResponse](#getblockheaderresponse)
    - [GetBlockHeadersRequest](#getblockheadersrequest)
    - [GetBlockHeadersResponse](#getblockheadersresponse)
    - [GetBlockRequest](#getblockrequest)
    - [GetBlockResponse](#getblockresponse)
    - [GetNodesResponse](#getnodesresponse)
    - [GetSubtreeRequest](#getsubtreerequest)
    - [GetSubtreeResponse](#getsubtreeresponse)
    - [HealthResponse](#healthresponse)
    - [Notification](#notification)
    - [SetSubtreeRequest](#setsubtreerequest)
    - [SetSubtreeTTLRequest](#setsubtreettlrequest)
    - [SubscribeRequest](#subscriberequest)
    - [Type](#type)
    - [AssetAPI](#assetapi)
- [Scalar Value Types](#scalar-value-types)



<a name="asset_api-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## asset_api.proto



<a name="asset_api-GetBlockHeaderRequest"></a>

### GetBlockHeaderRequest
Request format for getting a block header.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHash | [bytes](#bytes) |  | Hash of the block. |






<a name="asset_api-GetBlockHeaderResponse"></a>

### GetBlockHeaderResponse
Response format for getting a block header.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHeader | [bytes](#bytes) |  | Block header. |
| height | [uint32](#uint32) |  | Block height. |
| tx_count | [uint64](#uint64) |  | Number of transactions in the block. |
| size_in_bytes | [uint64](#uint64) |  | Size of the block in bytes. |
| miner | [string](#string) |  | Miner address. |






<a name="asset_api-GetBlockHeadersRequest"></a>

### GetBlockHeadersRequest
Request format for getting multiple block headers.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| startHash | [bytes](#bytes) |  | Hash of the first block header. |
| numberOfHeaders | [uint64](#uint64) |  | Number of block headers to retrieve. |






<a name="asset_api-GetBlockHeadersResponse"></a>

### GetBlockHeadersResponse
Response format for getting multiple block headers.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHeaders | [bytes](#bytes) | repeated | Block headers. |
| heights | [uint32](#uint32) | repeated | Block heights. |






<a name="asset_api-GetBlockRequest"></a>

### GetBlockRequest
Request format for getting a block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  | Hash of the block. |






<a name="asset_api-GetBlockResponse"></a>

### GetBlockResponse
Response format for getting a block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [bytes](#bytes) |  | Block header. |
| height | [uint32](#uint32) |  | Block height. |
| coinbase_tx | [bytes](#bytes) |  | Coinbase transaction. |
| transaction_count | [uint64](#uint64) |  | Number of transactions in the block. |
| subtree_hashes | [bytes](#bytes) | repeated | Hashes of subtrees in the block. |
| size_in_bytes | [uint64](#uint64) |  | Size of the block in bytes. |






<a name="asset_api-GetNodesResponse"></a>

### GetNodesResponse
Response format for getting node information.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| nodes | [string](#string) | repeated | List of node URLs. |






<a name="asset_api-GetSubtreeRequest"></a>

### GetSubtreeRequest
Request format for getting a subtree.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  | Hash of the subtree. |






<a name="asset_api-GetSubtreeResponse"></a>

### GetSubtreeResponse
Response format for getting a subtree.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| subtree | [bytes](#bytes) |  | Subtree. |






<a name="asset_api-HealthResponse"></a>

### HealthResponse
Contains the health status of the service.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | True if the service is healthy. |
| details | [string](#string) |  | Additional details about the health status. |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | Timestamp of the health check. |






<a name="asset_api-Notification"></a>

### Notification
Notification message format.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Type](#asset_api-Type) |  | Type of the notification - see the Type enum. |
| hash | [bytes](#bytes) |  | Hash of the subtree or block. |
| base_url | [string](#string) |  | Base URL of the node that sent the notification. |






<a name="asset_api-SetSubtreeRequest"></a>

### SetSubtreeRequest
Request format for persisting a subtree.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  | Hash of the subtree. |
| subtree | [bytes](#bytes) |  | Subtree. |
| ttl | [uint32](#uint32) |  | Time To Live (TTL) of the subtree. |






<a name="asset_api-SetSubtreeTTLRequest"></a>

### SetSubtreeTTLRequest
Request format for setting the TTL of a subtree.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  | Hash of the subtree. |
| ttl | [uint32](#uint32) |  | Time To Live (TTL) of the subtree. |






<a name="asset_api-SubscribeRequest"></a>

### SubscribeRequest
Request format for subscription.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| source | [string](#string) |  | Source of the subscription - a free text identifier of the source. |








<a name="asset_api-Type"></a>

### Type
Enumeration of notification types.

| Name | Number | Description |
| ---- | ------ | ----------- |
| PING | 0 | Ping notification. |
| Subtree | 1 | Subtree notification. |
| Block | 2 | Block notification. |
| MiningOn | 3 | MiningOn notification. |







<a name="asset_api-AssetAPI"></a>

### AssetAPI
This service provides a range of functionalities related to blockchain assets, including retrieving blocks, block headers, subtrees, managing subscriptions, and more.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| HealthGRPC | [.google.protobuf.Empty](#google-protobuf-Empty) | [HealthResponse](#asset_api-HealthResponse) | Health returns the health of the API. |
| GetBlock | [GetBlockRequest](#asset_api-GetBlockRequest) | [GetBlockResponse](#asset_api-GetBlockResponse) | Retrieves a block by its hash. |
| GetBlockHeader | [GetBlockHeaderRequest](#asset_api-GetBlockHeaderRequest) | [GetBlockHeaderResponse](#asset_api-GetBlockHeaderResponse) | Retrieves a block header by its hash. |
| GetBlockHeaders | [GetBlockHeadersRequest](#asset_api-GetBlockHeadersRequest) | [GetBlockHeadersResponse](#asset_api-GetBlockHeadersResponse) | Retrieves multiple block headers starting from a specific hash. |
| GetBestBlockHeader | [.google.protobuf.Empty](#google-protobuf-Empty) | [GetBlockHeaderResponse](#asset_api-GetBlockHeaderResponse) | Retrieves the best block header. |
| GetNodes | [.google.protobuf.Empty](#google-protobuf-Empty) | [GetNodesResponse](#asset_api-GetNodesResponse) | Retrieves information about connected nodes. |
| Get | [GetSubtreeRequest](#asset_api-GetSubtreeRequest) | [GetSubtreeResponse](#asset_api-GetSubtreeResponse) | Retrieves a subtree by its hash. |
| Set | [SetSubtreeRequest](#asset_api-SetSubtreeRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) | Persists a subtree. |
| SetTTL | [SetSubtreeTTLRequest](#asset_api-SetSubtreeTTLRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) | Sets the TTL (Time To Live) for a subtree. Primarily for block assembly. |
| Subscribe | [SubscribeRequest](#asset_api-SubscribeRequest) | [Notification](#asset_api-Notification) stream | Subscribes to blockchain notifications. |





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
