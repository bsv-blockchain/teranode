# GRPC Documentation - BlockchainAPI
<a name="top"></a>

## Table of Contents

- [blockchain_api.proto](#blockchain_api.proto)
    - [AddBlockRequest](#AddBlockRequest)
    - [CheckBlockIsCurrentChainRequest](#CheckBlockIsCurrentChainRequest)
    - [CheckBlockIsCurrentChainResponse](#CheckBlockIsCurrentChainResponse)
    - [GetBestHeightAndTimeResponse](#GetBestHeightAndTimeResponse)
    - [GetBlockByHeightRequest](#GetBlockByHeightRequest)
    - [GetBlockExistsResponse](#GetBlockExistsResponse)
    - [GetBlockGraphDataRequest](#GetBlockGraphDataRequest)
    - [GetBlockHeaderIDsResponse](#GetBlockHeaderIDsResponse)
    - [GetBlockHeaderRequest](#GetBlockHeaderRequest)
    - [GetBlockHeaderResponse](#GetBlockHeaderResponse)
    - [GetBlockHeadersByHeightRequest](#GetBlockHeadersByHeightRequest)
    - [GetBlockHeadersByHeightResponse](#GetBlockHeadersByHeightResponse)
    - [GetBlockHeadersFromHeightRequest](#GetBlockHeadersFromHeightRequest)
    - [GetBlockHeadersFromHeightResponse](#GetBlockHeadersFromHeightResponse)
    - [GetBlockHeadersRequest](#GetBlockHeadersRequest)
    - [GetBlockHeadersResponse](#GetBlockHeadersResponse)
    - [GetBlockLocatorRequest](#GetBlockLocatorRequest)
    - [GetBlockLocatorResponse](#GetBlockLocatorResponse)
    - [GetBlockRequest](#GetBlockRequest)
    - [GetBlockResponse](#GetBlockResponse)
    - [GetBlocksMinedNotSetResponse](#GetBlocksMinedNotSetResponse)
    - [GetBlocksRequest](#GetBlocksRequest)
    - [GetBlocksResponse](#GetBlocksResponse)
    - [GetBlocksSubtreesNotSetResponse](#GetBlocksSubtreesNotSetResponse)
    - [GetFSMStateResponse](#GetFSMStateResponse)
    - [GetFullBlockResponse](#GetFullBlockResponse)
    - [GetHashOfAncestorBlockRequest](#GetHashOfAncestorBlockRequest)
    - [GetHashOfAncestorBlockResponse](#GetHashOfAncestorBlockResponse)
    - [GetLastNBlocksRequest](#GetLastNBlocksRequest)
    - [GetLastNBlocksResponse](#GetLastNBlocksResponse)
    - [GetMedianTimeRequest](#GetMedianTimeRequest)
    - [GetMedianTimeResponse](#GetMedianTimeResponse)
    - [GetNextWorkRequiredRequest](#GetNextWorkRequiredRequest)
    - [GetNextWorkRequiredResponse](#GetNextWorkRequiredResponse)
    - [GetStateRequest](#GetStateRequest)
    - [GetSuitableBlockRequest](#GetSuitableBlockRequest)
    - [GetSuitableBlockResponse](#GetSuitableBlockResponse)
    - [HealthResponse](#HealthResponse)
    - [InvalidateBlockRequest](#InvalidateBlockRequest)
    - [LocateBlockHeadersRequest](#LocateBlockHeadersRequest)
    - [LocateBlockHeadersResponse](#LocateBlockHeadersResponse)
    - [Notification](#Notification)
    - [NotificationMetadata](#NotificationMetadata)
    - [NotificationMetadata.MetadataEntry](#NotificationMetadata.MetadataEntry)
    - [RevalidateBlockRequest](#RevalidateBlockRequest)
    - [SendFSMEventRequest](#SendFSMEventRequest)
    - [SetBlockMinedSetRequest](#SetBlockMinedSetRequest)
    - [SetBlockSubtreesSetRequest](#SetBlockSubtreesSetRequest)
    - [SetStateRequest](#SetStateRequest)
    - [StateResponse](#StateResponse)
    - [SubscribeRequest](#SubscribeRequest)
    - [WaitFSMToTransitionRequest](#WaitFSMToTransitionRequest)

    - [FSMEventType](#FSMEventType)
    - [FSMStateType](#FSMStateType)

    - [BlockchainAPI](#BlockchainAPI)

- [Scalar Value Types](#scalar-value-types)



<a name="blockchain_api.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## blockchain_api.proto



<a name="AddBlockRequest"></a>

### AddBlockRequest
swagger:model AddBlockRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [bytes](#bytes) |  |  |
| subtree_hashes | [bytes](#bytes) | repeated | The fist subtree will contain the coinbase txid (not the placeholder) |
| coinbase_tx | [bytes](#bytes) |  |  |
| transaction_count | [uint64](#uint64) |  |  |
| size_in_bytes | [uint64](#uint64) |  |  |
| external | [bool](#bool) |  |  |
| peer_id | [string](#string) |  |  |






<a name="CheckBlockIsCurrentChainRequest"></a>

### CheckBlockIsCurrentChainRequest
swagger:model CheckBlockIsCurrentChainRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockIDs | [uint32](#uint32) | repeated |  |






<a name="CheckBlockIsCurrentChainResponse"></a>

### CheckBlockIsCurrentChainResponse
swagger:model CheckBlockIsCurrentChainResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| isPartOfCurrentChain | [bool](#bool) |  |  |






<a name="GetBestHeightAndTimeResponse"></a>

### GetBestHeightAndTimeResponse
swagger:model GetBestHeightAndTimeResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| height | [uint32](#uint32) |  |  |
| time | [uint32](#uint32) |  |  |






<a name="GetBlockByHeightRequest"></a>

### GetBlockByHeightRequest
swagger:model GetBlockByHeightRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| height | [uint32](#uint32) |  |  |






<a name="GetBlockExistsResponse"></a>

### GetBlockExistsResponse
swagger:model GetBlockResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| exists | [bool](#bool) |  |  |






<a name="GetBlockGraphDataRequest"></a>

### GetBlockGraphDataRequest
swagger:model GetBlockGraphDataRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| period_millis | [uint64](#uint64) |  |  |






<a name="GetBlockHeaderIDsResponse"></a>

### GetBlockHeaderIDsResponse
swagger:model GetBlockHeaderIDsResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ids | [uint32](#uint32) | repeated |  |






<a name="GetBlockHeaderRequest"></a>

### GetBlockHeaderRequest
swagger:model GetBlockHeaderRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHash | [bytes](#bytes) |  |  |






<a name="GetBlockHeaderResponse"></a>

### GetBlockHeaderResponse
swagger:model GetBlockHeaderResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHeader | [bytes](#bytes) |  |  |
| height | [uint32](#uint32) |  |  |
| tx_count | [uint64](#uint64) |  |  |
| size_in_bytes | [uint64](#uint64) |  |  |
| miner | [string](#string) |  |  |
| block_time | [uint32](#uint32) |  |  |
| timestamp | [uint32](#uint32) |  |  |






<a name="GetBlockHeadersByHeightRequest"></a>

### GetBlockHeadersByHeightRequest
swagger:model GetBlockHeadersByHeightRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| startHeight | [uint32](#uint32) |  |  |
| endHeight | [uint32](#uint32) |  |  |






<a name="GetBlockHeadersByHeightResponse"></a>

### GetBlockHeadersByHeightResponse
swagger:model GetBlockHeadersByHeightResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHeaders | [bytes](#bytes) | repeated |  |
| metas | [bytes](#bytes) | repeated |  |






<a name="GetBlockHeadersFromHeightRequest"></a>

### GetBlockHeadersFromHeightRequest
swagger:model GetBlockHeadersFromHeightRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| startHeight | [uint32](#uint32) |  |  |
| limit | [uint32](#uint32) |  |  |






<a name="GetBlockHeadersFromHeightResponse"></a>

### GetBlockHeadersFromHeightResponse
swagger:model GetBlockHeadersFromHeightResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHeaders | [bytes](#bytes) | repeated |  |
| metas | [bytes](#bytes) | repeated |  |






<a name="GetBlockHeadersRequest"></a>

### GetBlockHeadersRequest
swagger:model GetBlockHeadersRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| startHash | [bytes](#bytes) |  |  |
| numberOfHeaders | [uint64](#uint64) |  |  |






<a name="GetBlockHeadersResponse"></a>

### GetBlockHeadersResponse
swagger:model GetBlockHeadersResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHeaders | [bytes](#bytes) | repeated |  |
| metas | [bytes](#bytes) | repeated |  |






<a name="GetBlockLocatorRequest"></a>

### GetBlockLocatorRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  |  |
| height | [uint32](#uint32) |  |  |






<a name="GetBlockLocatorResponse"></a>

### GetBlockLocatorResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| locator | [bytes](#bytes) | repeated |  |






<a name="GetBlockRequest"></a>

### GetBlockRequest
swagger:model GetBlockRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  |  |






<a name="GetBlockResponse"></a>

### GetBlockResponse
swagger:model GetBlockResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [bytes](#bytes) |  |  |
| height | [uint32](#uint32) |  |  |
| coinbase_tx | [bytes](#bytes) |  |  |
| transaction_count | [uint64](#uint64) |  |  |
| subtree_hashes | [bytes](#bytes) | repeated |  |
| size_in_bytes | [uint64](#uint64) |  |  |






<a name="GetBlocksMinedNotSetResponse"></a>

### GetBlocksMinedNotSetResponse
swagger:model GetBlocksMinedNotSetResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockBytes | [bytes](#bytes) | repeated |  |






<a name="GetBlocksRequest"></a>

### GetBlocksRequest
swagger:model GetBlocksRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  |  |
| count | [uint32](#uint32) |  |  |






<a name="GetBlocksResponse"></a>

### GetBlocksResponse
swagger:model GetBlocksResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blocks | [bytes](#bytes) | repeated |  |






<a name="GetBlocksSubtreesNotSetResponse"></a>

### GetBlocksSubtreesNotSetResponse
swagger:model GetBlocksSubtreesNotSetResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockBytes | [bytes](#bytes) | repeated |  |






<a name="GetFSMStateResponse"></a>

### GetFSMStateResponse
swagger:model GetFSMStateResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| state | [FSMStateType](#blockchain_api-FSMStateType) |  |  |






<a name="GetFullBlockResponse"></a>

### GetFullBlockResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| full_block_bytes | [bytes](#bytes) |  |  |






<a name="GetHashOfAncestorBlockRequest"></a>

### GetHashOfAncestorBlockRequest
swagger:model GetHashOfAncestorBlockRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  |  |
| depth | [uint32](#uint32) |  |  |






<a name="GetHashOfAncestorBlockResponse"></a>

### GetHashOfAncestorBlockResponse
swagger:model GetHashOfAncestorBlockResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  |  |






<a name="GetLastNBlocksRequest"></a>

### GetLastNBlocksRequest
swagger:model GetLastNBlocksRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| numberOfBlocks | [int64](#int64) |  |  |
| includeOrphans | [bool](#bool) |  |  |
| fromHeight | [uint32](#uint32) |  |  |






<a name="GetLastNBlocksResponse"></a>

### GetLastNBlocksResponse
swagger:model GetLastNBlocksResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blocks | [model.BlockInfo](#model-BlockInfo) | repeated |  |






<a name="GetMedianTimeRequest"></a>

### GetMedianTimeRequest
swagger:model GetMedianTimeRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHash | [bytes](#bytes) |  |  |






<a name="GetMedianTimeResponse"></a>

### GetMedianTimeResponse
swagger:model GetMedianTimeResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block_header_time | [uint32](#uint32) | repeated | This will return the nTimes of the last 11 (+1) blocks |






<a name="GetNextWorkRequiredRequest"></a>

### GetNextWorkRequiredRequest
swagger:model GGetNextWorkRequiredRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHash | [bytes](#bytes) |  |  |






<a name="GetNextWorkRequiredResponse"></a>

### GetNextWorkRequiredResponse
swagger:model GGetNextWorkRequiredResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| bits | [bytes](#bytes) |  |  |






<a name="GetStateRequest"></a>

### GetStateRequest
swagger:model StateRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |






<a name="GetSuitableBlockRequest"></a>

### GetSuitableBlockRequest
swagger:model GetSuitableBlockRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  |  |






<a name="GetSuitableBlockResponse"></a>

### GetSuitableBlockResponse
swagger:model GetSuitableBlockResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block | [model.SuitableBlock](#model-SuitableBlock) |  |  |






<a name="HealthResponse"></a>

### HealthResponse
swagger:model HealthResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  |  |
| details | [string](#string) |  |  |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |






<a name="InvalidateBlockRequest"></a>

### InvalidateBlockRequest
swagger:model InvalidateBlockRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHash | [bytes](#bytes) |  |  |






<a name="LocateBlockHeadersRequest"></a>

### LocateBlockHeadersRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| locator | [bytes](#bytes) | repeated |  |
| hash_stop | [bytes](#bytes) |  |  |
| max_hashes | [uint32](#uint32) |  |  |






<a name="LocateBlockHeadersResponse"></a>

### LocateBlockHeadersResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block_headers | [bytes](#bytes) | repeated |  |






<a name="Notification"></a>

### Notification
swagger:model Notification


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [model.NotificationType](#model-NotificationType) |  |  |
| hash | [bytes](#bytes) |  |  |
| base_URL | [string](#string) |  |  |
| metadata | [NotificationMetadata](#blockchain_api-NotificationMetadata) |  |  |






<a name="NotificationMetadata"></a>

### NotificationMetadata
swagger:model NotificationMetadata


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [NotificationMetadata.MetadataEntry](#blockchain_api-NotificationMetadata-MetadataEntry) | repeated | define a map of string to string |






<a name="NotificationMetadata.MetadataEntry"></a>

### NotificationMetadata.MetadataEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="RevalidateBlockRequest"></a>

### RevalidateBlockRequest
swagger:model RevalidateBlockRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHash | [bytes](#bytes) |  |  |






<a name="SendFSMEventRequest"></a>

### SendFSMEventRequest
swagger:model SendFSMEventRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| event | [FSMEventType](#blockchain_api-FSMEventType) |  |  |






<a name="SetBlockMinedSetRequest"></a>

### SetBlockMinedSetRequest
swagger:model SetBlockMinedSetRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHash | [bytes](#bytes) |  |  |






<a name="SetBlockSubtreesSetRequest"></a>

### SetBlockSubtreesSetRequest
swagger:model SetBlockSubtreesSetRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHash | [bytes](#bytes) |  |  |






<a name="SetStateRequest"></a>

### SetStateRequest
swagger:model SetStateRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| data | [bytes](#bytes) |  |  |






<a name="StateResponse"></a>

### StateResponse
swagger:model StateResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) |  |  |






<a name="SubscribeRequest"></a>

### SubscribeRequest
swagger:model SubscribeRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| source | [string](#string) |  |  |






<a name="WaitFSMToTransitionRequest"></a>

### WaitFSMToTransitionRequest
swagger:model WaitFSMToTransitionRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| state | [FSMStateType](#blockchain_api-FSMStateType) |  |  |





 <!-- end messages -->


<a name="FSMEventType"></a>

### FSMEventType
swagger:enum FSMEventType

| Name | Number | Description |
| ---- | ------ | ----------- |
| STOP | 0 |  |
| RUN | 1 |  |
| CATCHUPBLOCKS | 3 | MINE = 2; |
| CATCHUPTXS | 4 |  |
| RESTORE | 5 |  |
| LEGACYSYNC | 6 |  |
| UNAVAILABLE | 7 |  |



<a name="FSMStateType"></a>

### FSMStateType
swagger:enum FSMStateType

| Name | Number | Description |
| ---- | ------ | ----------- |
| STOPPED | 0 |  |
| RUNNING | 1 |  |
| CATCHINGBLOCKS | 3 | MINING = 2; |
| CATCHINGTXS | 4 |  |
| RESTORING | 5 |  |
| LEGACYSYNCING | 6 |  |
| RESOURCE_UNAVAILABLE | 7 |  |


 <!-- end enums -->

 <!-- end HasExtensions -->


<a name="BlockchainAPI"></a>

### BlockchainAPI


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| HealthGRPC | [.google.protobuf.Empty](#google-protobuf-Empty) | [HealthResponse](#blockchain_api-HealthResponse) | Health returns the health of the API. |
| AddBlock | [AddBlockRequest](#blockchain_api-AddBlockRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) | AddBlock adds a block to the blockchain. This will be called by BlockValidator. |
| GetBlock | [GetBlockRequest](#blockchain_api-GetBlockRequest) | [GetBlockResponse](#blockchain_api-GetBlockResponse) |  |
| GetBlocks | [GetBlocksRequest](#blockchain_api-GetBlocksRequest) | [GetBlocksResponse](#blockchain_api-GetBlocksResponse) |  |
| GetBlockByHeight | [GetBlockByHeightRequest](#blockchain_api-GetBlockByHeightRequest) | [GetBlockResponse](#blockchain_api-GetBlockResponse) |  |
| GetBlockStats | [.google.protobuf.Empty](#google-protobuf-Empty) | [.model.BlockStats](#model-BlockStats) |  |
| GetBlockGraphData | [GetBlockGraphDataRequest](#blockchain_api-GetBlockGraphDataRequest) | [.model.BlockDataPoints](#model-BlockDataPoints) |  |
| GetLastNBlocks | [GetLastNBlocksRequest](#blockchain_api-GetLastNBlocksRequest) | [GetLastNBlocksResponse](#blockchain_api-GetLastNBlocksResponse) |  |
| GetSuitableBlock | [GetSuitableBlockRequest](#blockchain_api-GetSuitableBlockRequest) | [GetSuitableBlockResponse](#blockchain_api-GetSuitableBlockResponse) |  |
| GetHashOfAncestorBlock | [GetHashOfAncestorBlockRequest](#blockchain_api-GetHashOfAncestorBlockRequest) | [GetHashOfAncestorBlockResponse](#blockchain_api-GetHashOfAncestorBlockResponse) |  |
| GetNextWorkRequired | [GetNextWorkRequiredRequest](#blockchain_api-GetNextWorkRequiredRequest) | [GetNextWorkRequiredResponse](#blockchain_api-GetNextWorkRequiredResponse) |  |
| GetBlockExists | [GetBlockRequest](#blockchain_api-GetBlockRequest) | [GetBlockExistsResponse](#blockchain_api-GetBlockExistsResponse) |  |
| GetBlockHeaders | [GetBlockHeadersRequest](#blockchain_api-GetBlockHeadersRequest) | [GetBlockHeadersResponse](#blockchain_api-GetBlockHeadersResponse) |  |
| GetBlockHeadersFromHeight | [GetBlockHeadersFromHeightRequest](#blockchain_api-GetBlockHeadersFromHeightRequest) | [GetBlockHeadersFromHeightResponse](#blockchain_api-GetBlockHeadersFromHeightResponse) |  |
| GetBlockHeadersByHeight | [GetBlockHeadersByHeightRequest](#blockchain_api-GetBlockHeadersByHeightRequest) | [GetBlockHeadersByHeightResponse](#blockchain_api-GetBlockHeadersByHeightResponse) |  |
| GetBlockHeaderIDs | [GetBlockHeadersRequest](#blockchain_api-GetBlockHeadersRequest) | [GetBlockHeaderIDsResponse](#blockchain_api-GetBlockHeaderIDsResponse) |  |
| GetBestBlockHeader | [.google.protobuf.Empty](#google-protobuf-Empty) | [GetBlockHeaderResponse](#blockchain_api-GetBlockHeaderResponse) |  |
| CheckBlockIsInCurrentChain | [CheckBlockIsCurrentChainRequest](#blockchain_api-CheckBlockIsCurrentChainRequest) | [CheckBlockIsCurrentChainResponse](#blockchain_api-CheckBlockIsCurrentChainResponse) |  |
| GetBlockHeader | [GetBlockHeaderRequest](#blockchain_api-GetBlockHeaderRequest) | [GetBlockHeaderResponse](#blockchain_api-GetBlockHeaderResponse) |  |
| InvalidateBlock | [InvalidateBlockRequest](#blockchain_api-InvalidateBlockRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) |  |
| RevalidateBlock | [RevalidateBlockRequest](#blockchain_api-RevalidateBlockRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) |  |
| Subscribe | [SubscribeRequest](#blockchain_api-SubscribeRequest) | [Notification](#blockchain_api-Notification) stream |  |
| SendNotification | [Notification](#blockchain_api-Notification) | [.google.protobuf.Empty](#google-protobuf-Empty) |  |
| GetState | [GetStateRequest](#blockchain_api-GetStateRequest) | [StateResponse](#blockchain_api-StateResponse) |  |
| SetState | [SetStateRequest](#blockchain_api-SetStateRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) |  |
| SetBlockMinedSet | [SetBlockMinedSetRequest](#blockchain_api-SetBlockMinedSetRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) |  |
| GetBlocksMinedNotSet | [.google.protobuf.Empty](#google-protobuf-Empty) | [GetBlocksMinedNotSetResponse](#blockchain_api-GetBlocksMinedNotSetResponse) |  |
| SetBlockSubtreesSet | [SetBlockSubtreesSetRequest](#blockchain_api-SetBlockSubtreesSetRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) |  |
| GetBlocksSubtreesNotSet | [.google.protobuf.Empty](#google-protobuf-Empty) | [GetBlocksSubtreesNotSetResponse](#blockchain_api-GetBlocksSubtreesNotSetResponse) |  |
| SendFSMEvent | [SendFSMEventRequest](#blockchain_api-SendFSMEventRequest) | [GetFSMStateResponse](#blockchain_api-GetFSMStateResponse) |  |
| GetFSMCurrentState | [.google.protobuf.Empty](#google-protobuf-Empty) | [GetFSMStateResponse](#blockchain_api-GetFSMStateResponse) |  |
| WaitFSMToTransitionToGivenState | [WaitFSMToTransitionRequest](#blockchain_api-WaitFSMToTransitionRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) |  |
| Run | [.google.protobuf.Empty](#google-protobuf-Empty) | [.google.protobuf.Empty](#google-protobuf-Empty) |  |
| CatchUpTransactions | [.google.protobuf.Empty](#google-protobuf-Empty) | [.google.protobuf.Empty](#google-protobuf-Empty) |  |
| CatchUpBlocks | [.google.protobuf.Empty](#google-protobuf-Empty) | [.google.protobuf.Empty](#google-protobuf-Empty) |  |
| Restore | [.google.protobuf.Empty](#google-protobuf-Empty) | [.google.protobuf.Empty](#google-protobuf-Empty) |  |
| LegacySync | [.google.protobuf.Empty](#google-protobuf-Empty) | [.google.protobuf.Empty](#google-protobuf-Empty) |  |
| Unavailable | [.google.protobuf.Empty](#google-protobuf-Empty) | [.google.protobuf.Empty](#google-protobuf-Empty) |  |
| GetBlockLocator | [GetBlockLocatorRequest](#blockchain_api-GetBlockLocatorRequest) | [GetBlockLocatorResponse](#blockchain_api-GetBlockLocatorResponse) |  |
| LocateBlockHeaders | [LocateBlockHeadersRequest](#blockchain_api-LocateBlockHeadersRequest) | [LocateBlockHeadersResponse](#blockchain_api-LocateBlockHeadersResponse) |  |
| GetBestHeightAndTime | [.google.protobuf.Empty](#google-protobuf-Empty) | [GetBestHeightAndTimeResponse](#blockchain_api-GetBestHeightAndTimeResponse) |  |

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
