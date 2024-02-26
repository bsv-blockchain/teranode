# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [blockchain_api.proto](#proto)
    - [AddBlockRequest](#AddBlockRequest)
    - [GetBlockExistsResponse](#GetBlockExistsResponse)
    - [GetBlockGraphDataRequest](#GetBlockGraphDataRequest)
    - [GetBlockHeaderIDsResponse](#GetBlockHeaderIDsResponse)
    - [GetBlockHeaderRequest](#GetBlockHeaderRequest)
    - [GetBlockHeaderResponse](#GetBlockHeaderResponse)
    - [GetBlockHeadersRequest](#GetBlockHeadersRequest)
    - [GetBlockHeadersResponse](#GetBlockHeadersResponse)
    - [GetBlockRequest](#GetBlockRequest)
    - [GetBlockResponse](#GetBlockResponse)
    - [GetHashOfAncestorBlockRequest](#GetHashOfAncestorBlockRequest)
    - [GetHashOfAncestorBlockResponse](#GetHashOfAncestorBlockResponse)
    - [GetLastNBlocksRequest](#GetLastNBlocksRequest)
    - [GetLastNBlocksResponse](#GetLastNBlocksResponse)
    - [GetNextWorkRequiredRequest](#GetNextWorkRequiredRequest)
    - [GetNextWorkRequiredResponse](#GetNextWorkRequiredResponse)
    - [GetStateRequest](#GetStateRequest)
    - [GetSuitableBlockRequest](#GetSuitableBlockRequest)
    - [GetSuitableBlockResponse](#GetSuitableBlockResponse)
    - [HealthResponse](#HealthResponse)
    - [InvalidateBlockRequest](#InvalidateBlockRequest)
    - [Notification](#Notification)
    - [RevalidateBlockRequest](#RevalidateBlockRequest)
    - [SetStateRequest](#SetStateRequest)
    - [StateResponse](#StateResponse)
    - [SubscribeRequest](#SubscribeRequest)

    - [BlockchainAPI](#BlockchainAPI)

- [model/model.proto](#model_model-proto)
    - [BlockDataPoints](#model-BlockDataPoints)
    - [BlockInfo](#model-BlockInfo)
    - [BlockStats](#model-BlockStats)
    - [DataPoint](#model-DataPoint)
    - [MiningCandidate](#model-MiningCandidate)
    - [MiningSolution](#model-MiningSolution)
    - [SuitableBlock](#model-SuitableBlock)

    - [NotificationType](#model-NotificationType)

- [Scalar Value Types](#scalar-value-types)



<a name="blockchain_api-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## blockchain_api.proto



<a name="blockchain_api-AddBlockRequest"></a>

### AddBlockRequest
Contains the request parameters for adding a block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [bytes](#bytes) |  | The header of the block. |
| subtree_hashes | [bytes](#bytes) | repeated | Subtree hashes, with the first containing the coinbase transaction ID. |
| coinbase_tx | [bytes](#bytes) |  | The coinbase transaction. |
| transaction_count | [uint64](#uint64) |  | The number of transactions in the block. |
| size_in_bytes | [uint64](#uint64) |  | The size of the block in bytes. |
| external | [bool](#bool) |  | Indicates if the block was received from an external source. |
| peer_id | [string](#string) |  | The ID of the peer that provided the block. |






<a name="blockchain_api-GetBlockExistsResponse"></a>

### GetBlockExistsResponse
Indicates whether a block exists in the response to a GetBlockExists request.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| exists | [bool](#bool) |  | Whether the block exists or not. |






<a name="blockchain_api-GetBlockGraphDataRequest"></a>

### GetBlockGraphDataRequest
Specifies the request parameters for getting block graph data.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| period_millis | [uint64](#uint64) |  | The period for which to retrieve block data, in milliseconds. |






<a name="blockchain_api-GetBlockHeaderIDsResponse"></a>

### GetBlockHeaderIDsResponse
Contains the response for a request to get block header IDs.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ids | [uint32](#uint32) | repeated | The IDs of the block headers. |






<a name="blockchain_api-GetBlockHeaderRequest"></a>

### GetBlockHeaderRequest
Represents a request to fetch a specific block header by hash.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHash | [bytes](#bytes) |  | The hash of the block header to retrieve. |






<a name="blockchain_api-GetBlockHeaderResponse"></a>

### GetBlockHeaderResponse
Contains the response for a request to get a block header.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHeader | [bytes](#bytes) |  | The block header. |
| height | [uint32](#uint32) |  | The height of the block. |
| tx_count | [uint64](#uint64) |  | The number of transactions in the block. |
| size_in_bytes | [uint64](#uint64) |  | The size of the block in bytes. |
| miner | [string](#string) |  | The miner of the block. |






<a name="blockchain_api-GetBlockHeadersRequest"></a>

### GetBlockHeadersRequest
Contains parameters for a request to get block headers.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| startHash | [bytes](#bytes) |  | The starting block hash. |
| numberOfHeaders | [uint64](#uint64) |  | The number of headers to retrieve. |






<a name="blockchain_api-GetBlockHeadersResponse"></a>

### GetBlockHeadersResponse
Contains the response for a request to get block headers.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHeaders | [bytes](#bytes) | repeated | The block headers. |
| heights | [uint32](#uint32) | repeated | The heights of the blocks corresponding to the headers. |






<a name="blockchain_api-GetBlockRequest"></a>

### GetBlockRequest
Represents a request to retrieve a block by its hash.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  | The hash of the block to retrieve. |






<a name="blockchain_api-GetBlockResponse"></a>

### GetBlockResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [bytes](#bytes) |  | The header of the block. |
| height | [uint32](#uint32) |  | The height of the block in the blockchain. |
| coinbase_tx | [bytes](#bytes) |  | The coinbase transaction of the block. |
| transaction_count | [uint64](#uint64) |  | The number of transactions in the block. |
| subtree_hashes | [bytes](#bytes) | repeated | Subtree hashes of the block. |
| size_in_bytes | [uint64](#uint64) |  | The size of the block in bytes. |






<a name="blockchain_api-GetHashOfAncestorBlockRequest"></a>

### GetHashOfAncestorBlockRequest
Specifies the request parameters for getting the hash of an ancestor block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  | The hash of the block. |
| depth | [uint32](#uint32) |  | The depth at which to find the ancestor. |






<a name="blockchain_api-GetHashOfAncestorBlockResponse"></a>

### GetHashOfAncestorBlockResponse
Contains the response for a request to get the hash of an ancestor block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  | The hash of the ancestor block. |






<a name="blockchain_api-GetLastNBlocksRequest"></a>

### GetLastNBlocksRequest
Specifies the request parameters for getting the last N blocks.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| numberOfBlocks | [int64](#int64) |  | The number of blocks to retrieve. |
| includeOrphans | [bool](#bool) |  | Whether to include orphaned blocks. |
| fromHeight | [uint32](#uint32) |  | The height from which to start the retrieval. |






<a name="blockchain_api-GetLastNBlocksResponse"></a>

### GetLastNBlocksResponse
Contains the response for a request to get the last N blocks.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blocks | [model.BlockInfo](#model-BlockInfo) | repeated | The blocks retrieved. |






<a name="blockchain_api-GetNextWorkRequiredRequest"></a>

### GetNextWorkRequiredRequest
Specifies the request parameters for getting the work required for the next block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHash | [bytes](#bytes) |  | The hash of the block to base the calculation on. |






<a name="blockchain_api-GetNextWorkRequiredResponse"></a>

### GetNextWorkRequiredResponse
Contains the response for a request to get the work required for the next block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| bits | [bytes](#bytes) |  | The work required for the next block, in compact format. |






<a name="blockchain_api-GetStateRequest"></a>

### GetStateRequest
Contains parameters for a request to get state by key.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  | The key of the state to retrieve. |






<a name="blockchain_api-GetSuitableBlockRequest"></a>

### GetSuitableBlockRequest
Specifies the request parameters for finding a suitable block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  | The hash of the block to find a suitable successor for. |






<a name="blockchain_api-GetSuitableBlockResponse"></a>

### GetSuitableBlockResponse
Contains the response for a request to find a suitable block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block | [model.SuitableBlock](#model-SuitableBlock) |  | The suitable block found. |






<a name="blockchain_api-HealthResponse"></a>

### HealthResponse
Represents a response to a health check request.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | Indicates if the API is functioning correctly. |
| details | [string](#string) |  | Provides details about the health check. |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | The timestamp of the health check. |






<a name="blockchain_api-InvalidateBlockRequest"></a>

### InvalidateBlockRequest
Contains parameters for a request to invalidate a block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHash | [bytes](#bytes) |  | The hash of the block to invalidate. |






<a name="blockchain_api-Notification"></a>

### Notification
Represents a notification message.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [model.NotificationType](#model-NotificationType) |  | The type of notification. |
| hash | [bytes](#bytes) |  | The hash associated with the notification. |
| base_url | [string](#string) |  | The base URL for the notification source. |






<a name="blockchain_api-RevalidateBlockRequest"></a>

### RevalidateBlockRequest
Contains parameters for a request to revalidate a block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHash | [bytes](#bytes) |  | The hash of the block to revalidate. |






<a name="blockchain_api-SetStateRequest"></a>

### SetStateRequest
Contains parameters for a request to set state.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  | The key of the state to set. |
| data | [bytes](#bytes) |  | The data to associate with the key. |






<a name="blockchain_api-StateResponse"></a>

### StateResponse
Contains the response for a request to get state.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) |  | The data associated with the state. |






<a name="blockchain_api-SubscribeRequest"></a>

### SubscribeRequest
Represents a request to subscribe to notifications.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| source | [string](#string) |  | The source from which to receive notifications. |












<a name="blockchain_api-BlockchainAPI"></a>

### BlockchainAPI
Defines the BlockchainAPI service with various RPC methods to interact with the blockchain.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| HealthGRPC | [.google.protobuf.Empty](#google-protobuf-Empty) | [HealthResponse](#HealthResponse) | HealthGRPC checks the health of the API and returns a HealthResponse. |
| AddBlock | [AddBlockRequest](#AddBlockRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) | AddBlock adds a new block to the blockchain. This is typically called by a BlockValidator. |
| GetBlock | [GetBlockRequest](#GetBlockRequest) | [GetBlockResponse](#GetBlockResponse) | GetBlock retrieves a block given its hash. |
| GetBlockStats | [.google.protobuf.Empty](#google-protobuf-Empty) | [.model.BlockStats](#model-BlockStats) | GetBlockStats returns statistics about the blocks in the blockchain. |
| GetBlockGraphData | [GetBlockGraphDataRequest](#GetBlockGraphDataRequest) | [.model.BlockDataPoints](#model-BlockDataPoints) | GetBlockGraphData retrieves block data for graphing purposes based on a specified period. |
| GetLastNBlocks | [GetLastNBlocksRequest](#GetLastNBlocksRequest) | [GetLastNBlocksResponse](#GetLastNBlocksResponse) | GetLastNBlocks fetches the last N blocks from the blockchain, optionally including orphaned blocks. |
| GetSuitableBlock | [GetSuitableBlockRequest](#GetSuitableBlockRequest) | [GetSuitableBlockResponse](#GetSuitableBlockResponse) | GetSuitableBlock finds a suitable block based on a given hash. |
| GetHashOfAncestorBlock | [GetHashOfAncestorBlockRequest](#GetHashOfAncestorBlockRequest) | [GetHashOfAncestorBlockResponse](#GetHashOfAncestorBlockResponse) | GetHashOfAncestorBlock fetches the hash of an ancestor block at a specified depth. |
| GetNextWorkRequired | [GetNextWorkRequiredRequest](#GetNextWorkRequiredRequest) | [GetNextWorkRequiredResponse](#GetNextWorkRequiredResponse) | GetNextWorkRequired calculates the work required for the next block. |
| GetBlockExists | [GetBlockRequest](#GetBlockRequest) | [GetBlockExistsResponse](#GetBlockExistsResponse) | GetBlockExists checks if a block exists in the blockchain. |
| GetBlockHeaders | [GetBlockHeadersRequest](#GetBlockHeadersRequest) | [GetBlockHeadersResponse](#GetBlockHeadersResponse) | GetBlockHeaders fetches headers of multiple blocks starting from a given hash. |
| GetBlockHeaderIDs | [GetBlockHeadersRequest](#GetBlockHeadersRequest) | [GetBlockHeaderIDsResponse](#GetBlockHeaderIDsResponse) | GetBlockHeaderIDs returns the IDs of block headers based on a request. |
| GetBestBlockHeader | [.google.protobuf.Empty](#google-protobuf-Empty) | [GetBlockHeaderResponse](#GetBlockHeaderResponse) | GetBestBlockHeader retrieves the header of the best (most recent) block. |
| GetBlockHeader | [GetBlockHeaderRequest](#GetBlockHeaderRequest) | [GetBlockHeaderResponse](#GetBlockHeaderResponse) | GetBlockHeader fetches a specific block header given its hash. |
| InvalidateBlock | [InvalidateBlockRequest](#InvalidateBlockRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) | InvalidateBlock marks a block as invalid. |
| RevalidateBlock | [RevalidateBlockRequest](#RevalidateBlockRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) | RevalidateBlock reinstates a block previously marked as invalid. |
| Subscribe | [SubscribeRequest](#SubscribeRequest) | [Notification](#Notification) stream | Subscribe allows clients to subscribe to notifications from a specified source. |
| SendNotification | [Notification](#Notification) | [.google.protobuf.Empty](#google-protobuf-Empty) | SendNotification sends a notification to subscribed clients. |
| GetState | [GetStateRequest](#GetStateRequest) | [StateResponse](#StateResponse) | GetState retrieves a state by key. |
| SetState | [SetStateRequest](#SetStateRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) | SetState updates or sets a state by key. |





<a name="services_blockchain_blockchain_api_blockchain_api-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## services/blockchain/blockchain_api/blockchain_api.proto



<a name="blockchain_api-AddBlockRequest"></a>

### AddBlockRequest
Contains the request parameters for adding a block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [bytes](#bytes) |  | The header of the block. |
| subtree_hashes | [bytes](#bytes) | repeated | Subtree hashes, with the first containing the coinbase transaction ID. |
| coinbase_tx | [bytes](#bytes) |  | The coinbase transaction. |
| transaction_count | [uint64](#uint64) |  | The number of transactions in the block. |
| size_in_bytes | [uint64](#uint64) |  | The size of the block in bytes. |
| external | [bool](#bool) |  | Indicates if the block was received from an external source. |
| peer_id | [string](#string) |  | The ID of the peer that provided the block. |






<a name="blockchain_api-GetBlockExistsResponse"></a>

### GetBlockExistsResponse
Indicates whether a block exists in the response to a GetBlockExists request.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| exists | [bool](#bool) |  | Whether the block exists or not. |






<a name="blockchain_api-GetBlockGraphDataRequest"></a>

### GetBlockGraphDataRequest
Specifies the request parameters for getting block graph data.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| period_millis | [uint64](#uint64) |  | The period for which to retrieve block data, in milliseconds. |






<a name="blockchain_api-GetBlockHeaderIDsResponse"></a>

### GetBlockHeaderIDsResponse
Contains the response for a request to get block header IDs.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ids | [uint32](#uint32) | repeated | The IDs of the block headers. |






<a name="blockchain_api-GetBlockHeaderRequest"></a>

### GetBlockHeaderRequest
Represents a request to fetch a specific block header by hash.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHash | [bytes](#bytes) |  | The hash of the block header to retrieve. |






<a name="blockchain_api-GetBlockHeaderResponse"></a>

### GetBlockHeaderResponse
Contains the response for a request to get a block header.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHeader | [bytes](#bytes) |  | The block header. |
| height | [uint32](#uint32) |  | The height of the block. |
| tx_count | [uint64](#uint64) |  | The number of transactions in the block. |
| size_in_bytes | [uint64](#uint64) |  | The size of the block in bytes. |
| miner | [string](#string) |  | The miner of the block. |






<a name="blockchain_api-GetBlockHeadersRequest"></a>

### GetBlockHeadersRequest
Contains parameters for a request to get block headers.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| startHash | [bytes](#bytes) |  | The starting block hash. |
| numberOfHeaders | [uint64](#uint64) |  | The number of headers to retrieve. |






<a name="blockchain_api-GetBlockHeadersResponse"></a>

### GetBlockHeadersResponse
Contains the response for a request to get block headers.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHeaders | [bytes](#bytes) | repeated | The block headers. |
| heights | [uint32](#uint32) | repeated | The heights of the blocks corresponding to the headers. |






<a name="blockchain_api-GetBlockRequest"></a>

### GetBlockRequest
Represents a request to retrieve a block by its hash.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  | The hash of the block to retrieve. |






<a name="blockchain_api-GetBlockResponse"></a>

### GetBlockResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [bytes](#bytes) |  | The header of the block. |
| height | [uint32](#uint32) |  | The height of the block in the blockchain. |
| coinbase_tx | [bytes](#bytes) |  | The coinbase transaction of the block. |
| transaction_count | [uint64](#uint64) |  | The number of transactions in the block. |
| subtree_hashes | [bytes](#bytes) | repeated | Subtree hashes of the block. |
| size_in_bytes | [uint64](#uint64) |  | The size of the block in bytes. |






<a name="blockchain_api-GetHashOfAncestorBlockRequest"></a>

### GetHashOfAncestorBlockRequest
Specifies the request parameters for getting the hash of an ancestor block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  | The hash of the block. |
| depth | [uint32](#uint32) |  | The depth at which to find the ancestor. |






<a name="blockchain_api-GetHashOfAncestorBlockResponse"></a>

### GetHashOfAncestorBlockResponse
Contains the response for a request to get the hash of an ancestor block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  | The hash of the ancestor block. |






<a name="blockchain_api-GetLastNBlocksRequest"></a>

### GetLastNBlocksRequest
Specifies the request parameters for getting the last N blocks.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| numberOfBlocks | [int64](#int64) |  | The number of blocks to retrieve. |
| includeOrphans | [bool](#bool) |  | Whether to include orphaned blocks. |
| fromHeight | [uint32](#uint32) |  | The height from which to start the retrieval. |






<a name="blockchain_api-GetLastNBlocksResponse"></a>

### GetLastNBlocksResponse
Contains the response for a request to get the last N blocks.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blocks | [model.BlockInfo](#model-BlockInfo) | repeated | The blocks retrieved. |






<a name="blockchain_api-GetNextWorkRequiredRequest"></a>

### GetNextWorkRequiredRequest
Specifies the request parameters for getting the work required for the next block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHash | [bytes](#bytes) |  | The hash of the block to base the calculation on. |






<a name="blockchain_api-GetNextWorkRequiredResponse"></a>

### GetNextWorkRequiredResponse
Contains the response for a request to get the work required for the next block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| bits | [bytes](#bytes) |  | The work required for the next block, in compact format. |






<a name="blockchain_api-GetStateRequest"></a>

### GetStateRequest
Contains parameters for a request to get state by key.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  | The key of the state to retrieve. |






<a name="blockchain_api-GetSuitableBlockRequest"></a>

### GetSuitableBlockRequest
Specifies the request parameters for finding a suitable block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  | The hash of the block to find a suitable successor for. |






<a name="blockchain_api-GetSuitableBlockResponse"></a>

### GetSuitableBlockResponse
Contains the response for a request to find a suitable block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block | [model.SuitableBlock](#model-SuitableBlock) |  | The suitable block found. |






<a name="blockchain_api-HealthResponse"></a>

### HealthResponse
Represents a response to a health check request.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | Indicates if the API is functioning correctly. |
| details | [string](#string) |  | Provides details about the health check. |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | The timestamp of the health check. |






<a name="blockchain_api-InvalidateBlockRequest"></a>

### InvalidateBlockRequest
Contains parameters for a request to invalidate a block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHash | [bytes](#bytes) |  | The hash of the block to invalidate. |






<a name="blockchain_api-Notification"></a>

### Notification
Represents a notification message.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [model.NotificationType](#model-NotificationType) |  | The type of notification. |
| hash | [bytes](#bytes) |  | The hash associated with the notification. |
| base_url | [string](#string) |  | The base URL for the notification source. |






<a name="blockchain_api-RevalidateBlockRequest"></a>

### RevalidateBlockRequest
Contains parameters for a request to revalidate a block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockHash | [bytes](#bytes) |  | The hash of the block to revalidate. |






<a name="blockchain_api-SetStateRequest"></a>

### SetStateRequest
Contains parameters for a request to set state.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  | The key of the state to set. |
| data | [bytes](#bytes) |  | The data to associate with the key. |






<a name="blockchain_api-StateResponse"></a>

### StateResponse
Contains the response for a request to get state.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) |  | The data associated with the state. |






<a name="blockchain_api-SubscribeRequest"></a>

### SubscribeRequest
Represents a request to subscribe to notifications.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| source | [string](#string) |  | The source from which to receive notifications. |












<a name="blockchain_api-BlockchainAPI"></a>

### BlockchainAPI
Defines the BlockchainAPI service with various RPC methods to interact with the blockchain.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| HealthGRPC | [.google.protobuf.Empty](#google-protobuf-Empty) | [HealthResponse](#HealthResponse) | HealthGRPC checks the health of the API and returns a HealthResponse. |
| AddBlock | [AddBlockRequest](#AddBlockRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) | AddBlock adds a new block to the blockchain. This is typically called by a BlockValidator. |
| GetBlock | [GetBlockRequest](#GetBlockRequest) | [GetBlockResponse](#GetBlockResponse) | GetBlock retrieves a block given its hash. |
| GetBlockStats | [.google.protobuf.Empty](#google-protobuf-Empty) | [.model.BlockStats](#model-BlockStats) | GetBlockStats returns statistics about the blocks in the blockchain. |
| GetBlockGraphData | [GetBlockGraphDataRequest](#GetBlockGraphDataRequest) | [.model.BlockDataPoints](#model-BlockDataPoints) | GetBlockGraphData retrieves block data for graphing purposes based on a specified period. |
| GetLastNBlocks | [GetLastNBlocksRequest](#GetLastNBlocksRequest) | [GetLastNBlocksResponse](#GetLastNBlocksResponse) | GetLastNBlocks fetches the last N blocks from the blockchain, optionally including orphaned blocks. |
| GetSuitableBlock | [GetSuitableBlockRequest](#GetSuitableBlockRequest) | [GetSuitableBlockResponse](#GetSuitableBlockResponse) | GetSuitableBlock finds a suitable block based on a given hash. |
| GetHashOfAncestorBlock | [GetHashOfAncestorBlockRequest](#GetHashOfAncestorBlockRequest) | [GetHashOfAncestorBlockResponse](#GetHashOfAncestorBlockResponse) | GetHashOfAncestorBlock fetches the hash of an ancestor block at a specified depth. |
| GetNextWorkRequired | [GetNextWorkRequiredRequest](#GetNextWorkRequiredRequest) | [GetNextWorkRequiredResponse](#GetNextWorkRequiredResponse) | GetNextWorkRequired calculates the work required for the next block. |
| GetBlockExists | [GetBlockRequest](#GetBlockRequest) | [GetBlockExistsResponse](#GetBlockExistsResponse) | GetBlockExists checks if a block exists in the blockchain. |
| GetBlockHeaders | [GetBlockHeadersRequest](#GetBlockHeadersRequest) | [GetBlockHeadersResponse](#GetBlockHeadersResponse) | GetBlockHeaders fetches headers of multiple blocks starting from a given hash. |
| GetBlockHeaderIDs | [GetBlockHeadersRequest](#GetBlockHeadersRequest) | [GetBlockHeaderIDsResponse](#GetBlockHeaderIDsResponse) | GetBlockHeaderIDs returns the IDs of block headers based on a request. |
| GetBestBlockHeader | [.google.protobuf.Empty](#google-protobuf-Empty) | [GetBlockHeaderResponse](#GetBlockHeaderResponse) | GetBestBlockHeader retrieves the header of the best (most recent) block. |
| GetBlockHeader | [GetBlockHeaderRequest](#GetBlockHeaderRequest) | [GetBlockHeaderResponse](#GetBlockHeaderResponse) | GetBlockHeader fetches a specific block header given its hash. |
| InvalidateBlock | [InvalidateBlockRequest](#InvalidateBlockRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) | InvalidateBlock marks a block as invalid. |
| RevalidateBlock | [RevalidateBlockRequest](#RevalidateBlockRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) | RevalidateBlock reinstates a block previously marked as invalid. |
| Subscribe | [SubscribeRequest](#SubscribeRequest) | [Notification](#Notification) stream | Subscribe allows clients to subscribe to notifications from a specified source. |
| SendNotification | [Notification](#Notification) | [.google.protobuf.Empty](#google-protobuf-Empty) | SendNotification sends a notification to subscribed clients. |
| GetState | [GetStateRequest](#GetStateRequest) | [StateResponse](#StateResponse) | GetState retrieves a state by key. |
| SetState | [SetStateRequest](#SetStateRequest) | [.google.protobuf.Empty](#google-protobuf-Empty) | SetState updates or sets a state by key. |





<a name="model_model-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## model/model.proto



<a name="model-BlockDataPoints"></a>

### BlockDataPoints
swagger:model BlockDataPoints


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data_points | [DataPoint](#model-DataPoint) | repeated |  |






<a name="model-BlockInfo"></a>

### BlockInfo
swagger:model BlockInfo


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| seen_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |
| height | [uint32](#uint32) |  |  |
| orphaned | [bool](#bool) |  |  |
| block_header | [bytes](#bytes) |  |  |
| miner | [string](#string) |  |  |
| coinbase_value | [uint64](#uint64) |  |  |
| transaction_count | [uint64](#uint64) |  |  |
| size | [uint64](#uint64) |  |  |






<a name="model-BlockStats"></a>

### BlockStats
swagger:model BlockStats


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block_count | [uint64](#uint64) |  |  |
| tx_count | [uint64](#uint64) |  |  |
| max_height | [uint64](#uint64) |  |  |
| avg_block_size | [double](#double) |  |  |
| avg_tx_count_per_block | [double](#double) |  |  |






<a name="model-DataPoint"></a>

### DataPoint
swagger:model DataPoint


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| timestamp | [uint32](#uint32) |  |  |
| tx_count | [uint64](#uint64) |  |  |






<a name="model-MiningCandidate"></a>

### MiningCandidate
swagger:model MiningCandidate


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [bytes](#bytes) |  |  |
| previous_hash | [bytes](#bytes) |  |  |
| coinbase_value | [uint64](#uint64) |  |  |
| version | [uint32](#uint32) |  |  |
| nBits | [bytes](#bytes) |  |  |
| time | [uint32](#uint32) |  |  |
| height | [uint32](#uint32) |  |  |
| merkle_proof | [bytes](#bytes) | repeated |  |
| subtreeCount | [uint32](#uint32) |  |  |






<a name="model-MiningSolution"></a>

### MiningSolution
swagger:model MiningSolution


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [bytes](#bytes) |  |  |
| coinbase | [bytes](#bytes) |  |  |
| time | [uint32](#uint32) |  |  |
| nonce | [uint32](#uint32) |  |  |
| version | [uint32](#uint32) |  |  |
| blockHash | [bytes](#bytes) |  |  |






<a name="model-SuitableBlock"></a>

### SuitableBlock
swagger:model SuitableBlock


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  |  |
| height | [uint32](#uint32) |  |  |
| nBits | [bytes](#bytes) |  |  |
| time | [uint32](#uint32) |  |  |
| chain_work | [bytes](#bytes) |  |  |








<a name="model-NotificationType"></a>

### NotificationType
swagger:enum NotificationType

| Name | Number | Description |
| ---- | ------ | ----------- |
| PING | 0 |  |
| Subtree | 1 |  |
| Block | 2 |  |
| MiningOn | 3 |  |










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
