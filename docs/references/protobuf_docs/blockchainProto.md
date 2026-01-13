# GRPC Documentation - BlockchainAPI
<a name="top"></a>

## Table of Contents

- [blockchain_api.proto](#blockchain_api.proto)
    - [AddBlockRequest](#AddBlockRequest)
    - [CheckBlockIsCurrentChainRequest](#CheckBlockIsCurrentChainRequest)
    - [CheckBlockIsCurrentChainResponse](#CheckBlockIsCurrentChainResponse)
    - [FindBlocksContainingSubtreeRequest](#FindBlocksContainingSubtreeRequest)
    - [FindBlocksContainingSubtreeResponse](#FindBlocksContainingSubtreeResponse)
    - [GetBestHeightAndTimeResponse](#GetBestHeightAndTimeResponse)
    - [GetBlockByHeightRequest](#GetBlockByHeightRequest)
    - [GetBlockByIDRequest](#GetBlockByIDRequest)
    - [GetBlocksByHeightRequest](#GetBlocksByHeightRequest)
    - [GetBlocksByHeightResponse](#GetBlocksByHeightResponse)
    - [GetBlockExistsResponse](#GetBlockExistsResponse)
    - [GetBlockGraphDataRequest](#GetBlockGraphDataRequest)
    - [GetBlockHeaderIDsResponse](#GetBlockHeaderIDsResponse)
    - [GetBlockHeaderRequest](#GetBlockHeaderRequest)
    - [GetBlockHeaderResponse](#GetBlockHeaderResponse)
    - [GetBlockHeadersByHeightRequest](#GetBlockHeadersByHeightRequest)
    - [GetBlockHeadersByHeightResponse](#GetBlockHeadersByHeightResponse)
    - [GetBlockHeadersFromCommonAncestorRequest](#GetBlockHeadersFromCommonAncestorRequest)
    - [GetBlockHeadersFromHeightRequest](#GetBlockHeadersFromHeightRequest)
    - [GetBlockHeadersFromHeightResponse](#GetBlockHeadersFromHeightResponse)
    - [GetBlockHeadersFromOldestRequest](#GetBlockHeadersFromOldestRequest)
    - [GetBlockHeadersFromTillRequest](#GetBlockHeadersFromTillRequest)
    - [GetBlockHeadersRequest](#GetBlockHeadersRequest)
    - [GetBlockHeadersResponse](#GetBlockHeadersResponse)
    - [GetBlockHeadersToCommonAncestorRequest](#GetBlockHeadersToCommonAncestorRequest)
    - [GetBlockInChainByHeightHashRequest](#GetBlockInChainByHeightHashRequest)
    - [GetBlockIsMinedRequest](#GetBlockIsMinedRequest)
    - [GetBlockIsMinedResponse](#GetBlockIsMinedResponse)
    - [GetBlockLocatorRequest](#GetBlockLocatorRequest)
    - [GetBlockLocatorResponse](#GetBlockLocatorResponse)
    - [GetBlockRequest](#GetBlockRequest)
    - [GetBlockResponse](#GetBlockResponse)
    - [GetBlocksByHeightRequest](#GetBlocksByHeightRequest)
    - [GetBlocksByHeightResponse](#GetBlocksByHeightResponse)
    - [GetBlocksMinedNotSetResponse](#GetBlocksMinedNotSetResponse)
    - [GetBlocksRequest](#GetBlocksRequest)
    - [GetBlocksResponse](#GetBlocksResponse)
    - [GetBlocksSubtreesNotSetResponse](#GetBlocksSubtreesNotSetResponse)
    - [GetChainTipsResponse](#GetChainTipsResponse)
    - [GetFSMStateResponse](#GetFSMStateResponse)
    - [GetFullBlockResponse](#GetFullBlockResponse)
    - [GetHashOfAncestorBlockRequest](#GetHashOfAncestorBlockRequest)
    - [GetHashOfAncestorBlockResponse](#GetHashOfAncestorBlockResponse)
    - [GetLastNBlocksRequest](#GetLastNBlocksRequest)
    - [GetLastNBlocksResponse](#GetLastNBlocksResponse)
    - [GetLastNInvalidBlocksRequest](#GetLastNInvalidBlocksRequest)
    - [GetLastNInvalidBlocksResponse](#GetLastNInvalidBlocksResponse)
    - [GetLatestBlockHeaderFromBlockLocatorRequest](#GetLatestBlockHeaderFromBlockLocatorRequest)
    - [GetMedianTimeRequest](#GetMedianTimeRequest)
    - [GetMedianTimeResponse](#GetMedianTimeResponse)
    - [GetNextBlockIDResponse](#GetNextBlockIDResponse)
    - [GetNextWorkRequiredRequest](#GetNextWorkRequiredRequest)
    - [GetNextWorkRequiredResponse](#GetNextWorkRequiredResponse)
    - [GetNextBlockIDResponse](#GetNextBlockIDResponse)
    - [GetStateRequest](#GetStateRequest)
    - [GetSuitableBlockRequest](#GetSuitableBlockRequest)
    - [GetSuitableBlockResponse](#GetSuitableBlockResponse)
    - [HealthResponse](#HealthResponse)
    - [InvalidateBlockRequest](#InvalidateBlockRequest)
    - [InvalidateBlockResponse](#InvalidateBlockResponse)
    - [FindBlocksContainingSubtreeRequest](#FindBlocksContainingSubtreeRequest)
    - [FindBlocksContainingSubtreeResponse](#FindBlocksContainingSubtreeResponse)
    - [ReportPeerFailureRequest](#ReportPeerFailureRequest)
    - [LocateBlockHeadersRequest](#LocateBlockHeadersRequest)
    - [LocateBlockHeadersResponse](#LocateBlockHeadersResponse)
    - [Notification](#Notification)
    - [NotificationMetadata](#NotificationMetadata)
    - [RevalidateBlockRequest](#RevalidateBlockRequest)
    - [SendFSMEventRequest](#SendFSMEventRequest)
    - [SetBlockMinedSetRequest](#SetBlockMinedSetRequest)
    - [SetBlockProcessedAtRequest](#SetBlockProcessedAtRequest)
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

Package blockchain_api defines the gRPC service interface for blockchain operations.



<a name="AddBlockRequest"></a>

### AddBlockRequest
AddBlockRequest contains data for adding a new block to the blockchain.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [bytes](#bytes) |  | Block header |
| subtree_hashes | [bytes](#bytes) | repeated | Merkle tree hashes |
| coinbase_tx | [bytes](#bytes) |  | Coinbase transaction |
| transaction_count | [uint64](#uint64) |  | Number of transactions |
| size_in_bytes | [uint64](#uint64) |  | Block size |
| external | [bool](#bool) |  | External block flag |
| peer_id | [string](#string) |  | Peer identifier |
| optionMinedSet | [bool](#bool) |  | Option to mark block as mined |
| optionSubtreesSet | [bool](#bool) |  | Option to mark subtrees as set |
| optionInvalid | [bool](#bool) |  | Option to invalidate block when adding |
| optionID | [uint64](#uint64) |  | Optional block ID |






<a name="CheckBlockIsCurrentChainRequest"></a>

### CheckBlockIsCurrentChainRequest
CheckBlockIsCurrentChainRequest checks if blocks are in the main chain.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockIDs | [uint32](#uint32) | repeated | List of block IDs to check |






<a name="CheckBlockIsCurrentChainResponse"></a>

### CheckBlockIsCurrentChainResponse
CheckBlockIsCurrentChainResponse indicates if blocks are in the main chain.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| isPartOfCurrentChain | [bool](#bool) |  | True if blocks are in main chain |






<a name="GetBestHeightAndTimeResponse"></a>

### GetBestHeightAndTimeResponse
GetBestHeightAndTimeResponse contains chain tip information.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| height | [uint32](#uint32) |  | Current best height |
| time | [uint32](#uint32) |  | Current median time |






<a name="GetBlockByHeightRequest"></a>

### GetBlockByHeightRequest
GetBlockByHeightRequest represents a request to retrieve a block at a specific height.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| height | [uint32](#uint32) |  | Block height to retrieve |






<a name="GetChainTipsResponse"></a>

### GetChainTipsResponse
GetChainTipsResponse contains information about all known tips in the block tree.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tips | [model.ChainTip](#model-ChainTip) | repeated | List of chain tips |






<a name="GetBlockExistsResponse"></a>

### GetBlockExistsResponse
GetBlockExistsResponse indicates whether a block exists.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| exists | [bool](#bool) |  | True if the block exists |






<a name="GetBlockGraphDataRequest"></a>

### GetBlockGraphDataRequest
GetBlockGraphDataRequest specifies parameters for retrieving blockchain visualization data.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| period_millis | [uint64](#uint64) |  | Time period in milliseconds |






<a name="GetBlockHeaderIDsResponse"></a>

### GetBlockHeaderIDsResponse
GetBlockHeaderIDsResponse contains block header identifiers.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ids | [uint32](#uint32) | repeated | List of block header IDs |






<a name="GetBlockHeaderRequest"></a>

### GetBlockHeaderRequest
GetBlockHeaderRequest requests a specific block header.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHash | [bytes](#bytes) |  | Hash of the block |






<a name="GetBlockHeaderResponse"></a>

### GetBlockHeaderResponse
swagger:model GetBlockHeaderResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHeader | [bytes](#bytes) |  | Serialized block header |
| id | [uint32](#uint32) |  | Block identifier |
| height | [uint32](#uint32) |  | Block height |
| tx_count | [uint64](#uint64) |  | Transaction count |
| size_in_bytes | [uint64](#uint64) |  | Block size |
| miner | [string](#string) |  | Miner identifier |
| peer_id | [string](#string) |  | Peer identifier |
| block_time | [uint32](#uint32) |  | Block timestamp |
| timestamp | [uint32](#uint32) |  | Processing timestamp |
| chain_work | [bytes](#bytes) |  | Accumulated chain work |
| mined_set | [bool](#bool) |  | Mined status |
| subtrees_set | [bool](#bool) |  | Subtrees status |
| invalid | [bool](#bool) |  | Validity status |
| processed_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | Timestamp when block was processed |






<a name="GetLatestBlockHeaderFromBlockLocatorRequest"></a>

### GetLatestBlockHeaderFromBlockLocatorRequest
GetLatestBlockHeaderFromBlockLocatorRequest retrieves the latest block header using a block locator.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| bestBlockHash | [bytes](#bytes) |  | Best block hash |
| blockLocatorHashes | [bytes](#bytes) | repeated | Block locator hashes |






<a name="GetBlockHeadersFromOldestRequest"></a>

### GetBlockHeadersFromOldestRequest
GetBlockHeadersFromOldestRequest retrieves block headers starting from the oldest block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| chainTipHash | [bytes](#bytes) |  | Chain tip hash |
| targetHash | [bytes](#bytes) |  | Target block hash |
| numberOfHeaders | [uint64](#uint64) |  | Maximum number of hashes to return |






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
GetBlockRequest represents a request to retrieve a block by its hash.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  | Hash of the block to retrieve |






<a name="GetBlockResponse"></a>

### GetBlockResponse
swagger:model GetBlockResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [bytes](#bytes) |  | Block header bytes |
| height | [uint32](#uint32) |  | Block height |
| coinbase_tx | [bytes](#bytes) |  | Coinbase transaction bytes |
| transaction_count | [uint64](#uint64) |  | Total number of transactions |
| subtree_hashes | [bytes](#bytes) | repeated | Merkle tree subtree hashes |
| size_in_bytes | [uint64](#uint64) |  | Total block size in bytes |
| id | [uint32](#uint32) |  | Block identifier |






<a name="GetBlocksMinedNotSetResponse"></a>

### GetBlocksMinedNotSetResponse
swagger:model GetBlocksMinedNotSetResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockBytes | [bytes](#bytes) | repeated |  |






<a name="GetBlocksRequest"></a>

### GetBlocksRequest
GetBlocksRequest represents a request to retrieve multiple blocks.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  | Starting block hash |
| count | [uint32](#uint32) |  | Number of blocks to retrieve |






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
GetMedianTimeRequest requests median time calculation for a block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHash | [bytes](#bytes) |  | Hash of the block |






<a name="GetMedianTimeResponse"></a>

### GetMedianTimeResponse
swagger:model GetMedianTimeResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block_header_time | [uint32](#uint32) | repeated | This will return the nTimes of the last 11 (+1) blocks |




<a name="GetNextBlockIDResponse"></a>

### GetNextBlockIDResponse
GetNextBlockIDResponse represents the response containing the next available block ID.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| next_block_id | [uint64](#uint64) |  | Next available block ID |




<a name="GetNextWorkRequiredRequest"></a>

### GetNextWorkRequiredRequest
swagger:model GGetNextWorkRequiredRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| previousBlockHash | [bytes](#bytes) |  | Reference block hash |
| currentBlockTime | [int64](#int64) |  | Current block time is only used for emergency difficulty adjustment on testnet-type networks |






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
HealthResponse represents the health status of the blockchain service.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | Overall health status |
| details | [string](#string) |  | Detailed health information |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | Timestamp of the health check |






<a name="InvalidateBlockRequest"></a>

### InvalidateBlockRequest
swagger:model InvalidateBlockRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHash | [bytes](#bytes) |  | Hash of the block to invalidate |




<a name="InvalidateBlockResponse"></a>

### InvalidateBlockResponse
InvalidateBlockResponse contains the result of block invalidation.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| invalidatedBlocks | [bytes](#bytes) | repeated | List of invalidated block hashes |




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
| metadata | map<string, string> |  | Key-value pairs of metadata |






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
SetBlockMinedSetRequest marks a block as mined.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHash | [bytes](#bytes) |  | Hash of the block to mark as mined |



<a name="SetBlockProcessedAtRequest"></a>

### SetBlockProcessedAtRequest
SetBlockProcessedAtRequest defines parameters for setting or clearing a block's processed_at timestamp.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block_hash | [bytes](#bytes) |  | Hash of the block |
| clear | [bool](#bool) |  | Whether to clear the timestamp |






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





<a name="GetBlockHeadersFromCommonAncestorRequest"></a>

### GetBlockHeadersFromCommonAncestorRequest
GetBlockHeadersFromCommonAncestorRequest retrieves headers from a common ancestor to a target block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| targetHash | [bytes](#bytes) |  | Target block hash |
| blockLocatorHashes | [bytes](#bytes) | repeated | Block locator hashes |
| maxHeaders | [uint32](#uint32) |  | Maximum number of headers to retrieve |




<a name="GetBlocksByHeightRequest"></a>

### GetBlocksByHeightRequest
GetBlocksByHeightRequest requests full blocks between two heights.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| startHeight | [uint32](#uint32) |  | Starting height |
| endHeight | [uint32](#uint32) |  | Ending height |




<a name="GetBlocksByHeightResponse"></a>

### GetBlocksByHeightResponse
GetBlocksByHeightResponse contains full blocks between heights.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blocks | [bytes](#bytes) | repeated | List of serialized full blocks |




<a name="FindBlocksContainingSubtreeRequest"></a>

### FindBlocksContainingSubtreeRequest
FindBlocksContainingSubtreeRequest specifies a subtree hash to search for.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| subtree_hash | [bytes](#bytes) |  | Subtree hash to find |
| max_blocks | [uint32](#uint32) |  | Maximum number of blocks to return (0 = no limit) |




<a name="FindBlocksContainingSubtreeResponse"></a>

### FindBlocksContainingSubtreeResponse
FindBlocksContainingSubtreeResponse contains blocks that contain the subtree.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blocks | [bytes](#bytes) | repeated | List of serialized full blocks containing the subtree |




<a name="ReportPeerFailureRequest"></a>

### ReportPeerFailureRequest
ReportPeerFailureRequest reports a peer download failure to the blockchain service.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  | Hash of the block/subtree being processed |
| peer_id | [string](#string) |  | Identifier of the failing peer |
| failure_type | [string](#string) |  | Type of failure (e.g., "catchup", "subtree", "block") |
| reason | [string](#string) |  | Description of the failure |




 <!-- end messages -->


<a name="GetBlockByIDRequest"></a>

### GetBlockByIDRequest
GetBlockByIDRequest represents a request to retrieve a block by its ID.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [uint64](#uint64) |  | Block ID to retrieve |



<a name="GetBlockInChainByHeightHashRequest"></a>

### GetBlockInChainByHeightHashRequest
GetBlockInChainByHeightHashRequest represents a request to retrieve a block by height in a specific chain.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| height | [uint32](#uint32) |  | Target block height |
| start_hash | [bytes](#bytes) |  | Starting block hash defining the chain |




<a name="FSMEventType"></a>

### FSMEventType
swagger:enum FSMEventType

| Name | Number | Description |
| ---- | ------ | ----------- |
| STOP | 0 | Stop the blockchain service |
| RUN | 1 | Run the blockchain service |
| CATCHUPBLOCKS | 2 | Start catching up blocks |
| LEGACYSYNC | 3 | Start legacy synchronization |



<a name="FSMStateType"></a>

### FSMStateType
FSMStateType defines possible states of the blockchain FSM.

| Name | Number | Description |
| ---- | ------ | ----------- |
| IDLE | 0 | Service is idle |
| RUNNING | 1 | Service is running |
| CATCHINGBLOCKS | 2 | Service is catching up blocks |
| LEGACYSYNCING | 3 | Service is performing legacy sync |


 <!-- end enums -->

 <!-- end HasExtensions -->


<a name="BlockchainAPI"></a>

### BlockchainAPI
BlockchainAPI service provides comprehensive blockchain management functionality.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| HealthGRPC | [.google.protobuf.Empty](#google-protobuf-Empty) | [HealthResponse](#blockchain_api-HealthResponse) | Checks the health status of the blockchain service. |
| AddBlock | [AddBlockRequest](#blockchain_api-AddBlockRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) | Adds a new block to the blockchain. Called by BlockValidator to add validated blocks. |
| GetBlock | [GetBlockRequest](#blockchain_api-GetBlockRequest) | [GetBlockResponse](#blockchain_api-GetBlockResponse) | Retrieves a block by its hash. |
| GetBlocks | [GetBlocksRequest](#blockchain_api-GetBlocksRequest) | [GetBlocksResponse](#blockchain_api-GetBlocksResponse) | Retrieves multiple blocks starting from a specific hash. |
| GetBlockByHeight | [GetBlockByHeightRequest](#blockchain_api-GetBlockByHeightRequest) | [GetBlockResponse](#blockchain_api-GetBlockResponse) | Retrieves a block at a specific height. |
| GetBlockByID | [GetBlockByIDRequest](#blockchain_api-GetBlockByIDRequest) | [GetBlockResponse](#blockchain_api-GetBlockResponse) | Retrieves a block by its id. |
| GetNextBlockID | [.google.protobuf.Empty](#google-protobuf-Empty) | [GetNextBlockIDResponse](#blockchain_api-GetNextBlockIDResponse) | Retrieves the next available block ID. |
| GetBlockStats | [.google.protobuf.Empty](#google-protobuf-Empty) | [.model.BlockStats](#model-BlockStats) | Retrieves statistical information about the blockchain. |
| GetBlockGraphData | [GetBlockGraphDataRequest](#blockchain_api-GetBlockGraphDataRequest) | [.model.BlockDataPoints](#model-BlockDataPoints) | Retrieves data points for blockchain visualization. |
| GetLastNBlocks | [GetLastNBlocksRequest](#blockchain_api-GetLastNBlocksRequest) | [GetLastNBlocksResponse](#blockchain_api-GetLastNBlocksResponse) | Retrieves the most recent N blocks from the blockchain. |
| GetLastNInvalidBlocks | [GetLastNInvalidBlocksRequest](#blockchain_api-GetLastNInvalidBlocksRequest) | [GetLastNInvalidBlocksResponse](#blockchain_api-GetLastNInvalidBlocksResponse) | Retrieves the most recent N blocks that have been marked as invalid. |
| GetSuitableBlock | [GetSuitableBlockRequest](#blockchain_api-GetSuitableBlockRequest) | [GetSuitableBlockResponse](#blockchain_api-GetSuitableBlockResponse) | Finds a suitable block for mining purposes. |
| GetHashOfAncestorBlock | [GetHashOfAncestorBlockRequest](#blockchain_api-GetHashOfAncestorBlockRequest) | [GetHashOfAncestorBlockResponse](#blockchain_api-GetHashOfAncestorBlockResponse) | Retrieves the hash of an ancestor block at a specified depth. |
| GetNextWorkRequired | [GetNextWorkRequiredRequest](#blockchain_api-GetNextWorkRequiredRequest) | [GetNextWorkRequiredResponse](#blockchain_api-GetNextWorkRequiredResponse) | Calculates the required proof of work for the next block. |
| GetBlockExists | [GetBlockRequest](#blockchain_api-GetBlockRequest) | [GetBlockExistsResponse](#blockchain_api-GetBlockExistsResponse) | Checks if a block exists in the blockchain. |
| GetBlockHeaders | [GetBlockHeadersRequest](#blockchain_api-GetBlockHeadersRequest) | [GetBlockHeadersResponse](#blockchain_api-GetBlockHeadersResponse) | Retrieves headers for multiple blocks. |
| GetBlockHeadersToCommonAncestor | [GetBlockHeadersToCommonAncestorRequest](#blockchain_api-GetBlockHeadersToCommonAncestorRequest) | [GetBlockHeadersResponse](#blockchain_api-GetBlockHeadersResponse) | Retrieves block headers up to a common ancestor point between chains. |
| GetBlockHeadersFromCommonAncestor | [GetBlockHeadersFromCommonAncestorRequest](#blockchain_api-GetBlockHeadersFromCommonAncestorRequest) | [GetBlockHeadersResponse](#blockchain_api-GetBlockHeadersResponse) | Retrieves headers from a common ancestor to a target block. |
| GetBlockHeadersFromTill | [GetBlockHeadersFromTillRequest](#blockchain_api-GetBlockHeadersFromTillRequest) | [GetBlockHeadersResponse](#blockchain_api-GetBlockHeadersResponse) | Retrieves block headers between two specified blocks. |
| GetBlockHeadersFromHeight | [GetBlockHeadersFromHeightRequest](#blockchain_api-GetBlockHeadersFromHeightRequest) | [GetBlockHeadersFromHeightResponse](#blockchain_api-GetBlockHeadersFromHeightResponse) | Retrieves block headers starting from a specific height. |
| GetBlockHeadersByHeight | [GetBlockHeadersByHeightRequest](#blockchain_api-GetBlockHeadersByHeightRequest) | [GetBlockHeadersByHeightResponse](#blockchain_api-GetBlockHeadersByHeightResponse) | Retrieves block headers between two specified heights. |
| GetBlocksByHeight | [GetBlocksByHeightRequest](#blockchain_api-GetBlocksByHeightRequest) | [GetBlocksByHeightResponse](#blockchain_api-GetBlocksByHeightResponse) | Retrieves full blocks between two specified heights. |
| FindBlocksContainingSubtree | [FindBlocksContainingSubtreeRequest](#blockchain_api-FindBlocksContainingSubtreeRequest) | [FindBlocksContainingSubtreeResponse](#blockchain_api-FindBlocksContainingSubtreeResponse) | Finds all blocks that contain the specified subtree hash. |
| GetLatestBlockHeaderFromBlockLocator | [GetLatestBlockHeaderFromBlockLocatorRequest](#blockchain_api-GetLatestBlockHeaderFromBlockLocatorRequest) | [GetBlockHeaderResponse](#blockchain_api-GetBlockHeaderResponse) | Retrieves the latest block header using a block locator. |
| GetBlockHeadersFromOldest | [GetBlockHeadersFromOldestRequest](#blockchain_api-GetBlockHeadersFromOldestRequest) | [GetBlockHeadersResponse](#blockchain_api-GetBlockHeadersResponse) | Retrieves block headers starting from the oldest block. |
| GetBlockHeaderIDs | [GetBlockHeadersRequest](#blockchain_api-GetBlockHeadersRequest) | [GetBlockHeaderIDsResponse](#blockchain_api-GetBlockHeaderIDsResponse) | Retrieves block header IDs for a range of blocks. |
| GetBestBlockHeader | [.google.protobuf.Empty](#google-protobuf-Empty) | [GetBlockHeaderResponse](#blockchain_api-GetBlockHeaderResponse) | Retrieves the header of the current best block. |
| CheckBlockIsInCurrentChain | [CheckBlockIsCurrentChainRequest](#blockchain_api-CheckBlockIsCurrentChainRequest) | [CheckBlockIsCurrentChainResponse](#blockchain_api-CheckBlockIsCurrentChainResponse) | Verifies if specified blocks are in the main chain. |
| GetChainTips | [.google.protobuf.Empty](#google-protobuf-Empty) | [GetChainTipsResponse](#blockchain_api-GetChainTipsResponse) | Retrieves information about all known tips in the block tree. |
| GetBlockHeader | [GetBlockHeaderRequest](#blockchain_api-GetBlockHeaderRequest) | [GetBlockHeaderResponse](#blockchain_api-GetBlockHeaderResponse) | Retrieves the header of a specific block. |
| InvalidateBlock | [InvalidateBlockRequest](#blockchain_api-InvalidateBlockRequest) | [InvalidateBlockResponse](#blockchain_api-InvalidateBlockResponse) | Marks a block as invalid in the blockchain. |
| RevalidateBlock | [RevalidateBlockRequest](#blockchain_api-RevalidateBlockRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) | Restores a previously invalidated block. |
| Subscribe | [SubscribeRequest](#blockchain_api-SubscribeRequest) | stream [Notification](#blockchain_api-Notification) | Creates a subscription for blockchain notifications. |
| SendNotification | [Notification](#blockchain_api-Notification) | [.google.protobuf.Empty](#google-protobuf-Empty) | Broadcasts a notification to subscribers. |
| GetState | [GetStateRequest](#blockchain_api-GetStateRequest) | [StateResponse](#blockchain_api-StateResponse) | Retrieves state data by key. |
| SetState | [SetStateRequest](#blockchain_api-SetStateRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) | Stores state data with a key. |
| GetBlockIsMined | [GetBlockIsMinedRequest](#blockchain_api-GetBlockIsMinedRequest) | [GetBlockIsMinedResponse](#blockchain_api-GetBlockIsMinedResponse) | Checks if a block is marked as mined. |
| SetBlockMinedSet | [SetBlockMinedSetRequest](#blockchain_api-SetBlockMinedSetRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) | Marks a block as mined. |
| SetBlockProcessedAt | [SetBlockProcessedAtRequest](#blockchain_api-SetBlockProcessedAtRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) | Sets or clears the processed_at timestamp for a block. |
| GetBlocksMinedNotSet | [.google.protobuf.Empty](#google-protobuf-Empty) | [GetBlocksMinedNotSetResponse](#blockchain_api-GetBlocksMinedNotSetResponse) | Retrieves blocks not marked as mined. |
| SetBlockSubtreesSet | [SetBlockSubtreesSetRequest](#blockchain_api-SetBlockSubtreesSetRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) | Marks a block's subtrees as set. |
| GetBlocksSubtreesNotSet | [.google.protobuf.Empty](#google-protobuf-Empty) | [GetBlocksSubtreesNotSetResponse](#blockchain_api-GetBlocksSubtreesNotSetResponse) | Retrieves blocks with unset subtrees. |
| SendFSMEvent | [SendFSMEventRequest](#blockchain_api-SendFSMEventRequest) | [GetFSMStateResponse](#blockchain_api-GetFSMStateResponse) | Sends an event to the blockchain FSM. |
| GetFSMCurrentState | [.google.protobuf.Empty](#google-protobuf-Empty) | [GetFSMStateResponse](#blockchain_api-GetFSMStateResponse) | Retrieves the current state of the FSM. |
| WaitFSMToTransitionToGivenState | [WaitFSMToTransitionRequest](#blockchain_api-WaitFSMToTransitionRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) | Waits for FSM to reach a specific state. |
| WaitUntilFSMTransitionFromIdleState | [.google.protobuf.Empty](#google-protobuf-Empty) | [.google.protobuf.Empty](#google-protobuf-Empty) | Waits for FSM to transition from IDLE state. |
| Run | [.google.protobuf.Empty](#google-protobuf-Empty) | [.google.protobuf.Empty](#google-protobuf-Empty) | Transitions the blockchain service to running state. |
| CatchUpBlocks | [.google.protobuf.Empty](#google-protobuf-Empty) | [.google.protobuf.Empty](#google-protobuf-Empty) | Initiates block catch-up process. |
| LegacySync | [.google.protobuf.Empty](#google-protobuf-Empty) | [.google.protobuf.Empty](#google-protobuf-Empty) | Initiates legacy synchronization process. |
| Idle | [.google.protobuf.Empty](#google-protobuf-Empty) | [.google.protobuf.Empty](#google-protobuf-Empty) | Marks the service as idle. |
| ReportPeerFailure | [ReportPeerFailureRequest](#blockchain_api-ReportPeerFailureRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) | Notifies about peer download failures (catchup, subtree, block, etc). |
| GetBlockLocator | [GetBlockLocatorRequest](#blockchain_api-GetBlockLocatorRequest) | [GetBlockLocatorResponse](#blockchain_api-GetBlockLocatorResponse) | Retrieves a block locator for chain synchronization. |
| LocateBlockHeaders | [LocateBlockHeadersRequest](#blockchain_api-LocateBlockHeadersRequest) | [LocateBlockHeadersResponse](#blockchain_api-LocateBlockHeadersResponse) | Finds block headers using a locator. |
| GetBestHeightAndTime | [.google.protobuf.Empty](#google-protobuf-Empty) | [GetBestHeightAndTimeResponse](#blockchain_api-GetBestHeightAndTimeResponse) | Retrieves the current best height and median time. |

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
