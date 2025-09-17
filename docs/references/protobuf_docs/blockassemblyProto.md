# GRPC Documentation - BlockAssemblyAPI
<a name="top"></a>

## Table of Contents

- [blockassembly\_api.proto](#blockassembly_apiproto)
    - [AddTxBatchRequest](#addtxbatchrequest)
    - [AddTxBatchResponse](#addtxbatchresponse)
    - [AddTxRequest](#addtxrequest)
    - [AddTxResponse](#addtxresponse)
    - [EmptyMessage](#emptymessage)
    - [GenerateBlocksRequest](#generateblocksrequest)
    - [GetCurrentDifficultyResponse](#getcurrentdifficultyresponse)
    - [GetMiningCandidateRequest](#getminingcandidaterequest)
    - [HealthResponse](#healthresponse)
    - [NewChaintipAndHeightRequest](#newchaintipandheightrequest)
    - [RemoveTxRequest](#removetxrequest)
    - [StateMessage](#statemessage)
    - [SubmitMiningSolutionRequest](#submitminingsolutionrequest)
    - [OKResponse](#okresponse)
    - [GetBlockAssemblyBlockCandidateResponse](#getblockassemblyblockcandidateresponse)
    - [GetBlockAssemblyTxsResponse](#getblockassemblytxsresponse)
    - [BlockAssemblyAPI](#blockassemblyapi)
    - [Scalar Value Types](#scalar-value-types)



<a name="blockassembly_api.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## blockassembly_api.proto

> Package blockassembly_api defines the gRPC service interface for block assembly operations.



<a name="AddTxBatchRequest"></a>

### AddTxBatchRequest
Request for adding a batch of transactions to the mining candidate block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| txRequests | [AddTxRequest](#blockassembly_api-AddTxRequest) | repeated | a batch of transaction requests |






<a name="AddTxBatchResponse"></a>

### AddTxBatchResponse
Response indicating whether the addition of a batch of transactions was successful.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | true if the transactions were successfully added |






<a name="AddTxRequest"></a>

### AddTxRequest
Request for adding a new transaction to the mining candidate block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| txid | [bytes](#bytes) |  | the transaction id |
| fee | [uint64](#uint64) |  | the transaction fee in satoshis |
| size | [uint64](#uint64) |  | the size of the transaction in bytes |
| locktime | [uint32](#uint32) |  | the earliest time a transaction can be mined into a block |
| utxos | [bytes](#bytes) | repeated | the UTXOs consumed by this transaction |
| txInpoints | [bytes](#bytes) |  | a serialized list of input outpoints for this transaction |






<a name="AddTxResponse"></a>

### AddTxResponse
Response indicating whether the addition of a transaction was successful.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | true if the transaction was successfully added |






<a name="EmptyMessage"></a>

### EmptyMessage
An empty message used as a placeholder or a request with no data.






<a name="GenerateBlocksRequest"></a>

### GenerateBlocksRequest
Request for generating a block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| count | [int32](#int32) |  | the number of blocks to generate |
| address | [string](#string) | optional | the address to send the generated blocks to |
| maxTries | [int32](#int32) | optional | the maximum number of attempts to generate a block |





<a name="GetCurrentDifficultyResponse"></a>

### GetCurrentDifficultyResponse
Response containing the current difficulty of the blockchain.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| difficulty | [double](#double) |  | the current difficulty of the blockchain |





<a name="GetMiningCandidateRequest"></a>

### GetMiningCandidateRequest
Request for retrieving a mining candidate block template.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| includeSubtrees | [bool](#bool) |  | whether to include the subtrees in the mining candidate |






<a name="HealthResponse"></a>

### HealthResponse
Contains the health status of the service. Includes an 'ok' flag indicating health status, details providing more context, and a timestamp.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | true if the service is healthy |
| details | [string](#string) |  | optional, human-readable details |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | unix timestamp |






<a name="NewChaintipAndHeightRequest"></a>

### NewChaintipAndHeightRequest
Request for adding a new chaintip and height information.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| chaintip | [bytes](#bytes) |  | the chaintip hash |
| height | [uint32](#uint32) |  | the height of the chaintip in the blockchain |






<a name="RemoveTxRequest"></a>

### RemoveTxRequest
Request for removing a transaction from the mining candidate block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| txid | [bytes](#bytes) |  | the transaction id to remove |






<a name="StateMessage"></a>

### StateMessage
Message containing the state of the block assembly service.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blockAssemblyState | [string](#string) |  | the state of the block assembly service |
| subtreeProcessorState | [string](#string) |  | the state of the block assembly subtree processor |
| resetWaitCount | [uint32](#uint32) |  | the number of blocks the reset has to wait for |
| resetWaitTime | [uint32](#uint32) |  | the time in seconds the reset has to wait for |
| subtreeCount | [uint32](#uint32) |  | the number of subtrees |
| txCount | [uint64](#uint64) |  | the number of transactions |
| queueCount | [int64](#int64) |  | the size of the queue |
| currentHeight | [uint32](#uint32) |  | the height of the chaintip |
| currentHash | [string](#string) |  | the hash of the chaintip |
| removeMapCount | [uint32](#uint32) |  | the number of transactions in the remove map |
| subtrees | [string](#string) | repeated | the hashes of the current subtrees |






<a name="SubmitMiningSolutionRequest"></a>

### SubmitMiningSolutionRequest
Request for submitting a mining solution to the blockchain.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [bytes](#bytes) |  | the id of the mining candidate |
| nonce | [uint32](#uint32) |  | the nonce value used for mining |
| coinbase_tx | [bytes](#bytes) |  | the coinbase transaction bytes |
| time | [uint32](#uint32) | optional | the timestamp of the block |
| version | [uint32](#uint32) | optional | the version of the block |






<a name="OKResponse"></a>

### OKResponse
Response indicating whether the call was successful.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | true if the call was successful |




<a name="GetBlockAssemblyBlockCandidateResponse"></a>

### GetBlockAssemblyBlockCandidateResponse
Response for the GetBlockAssemblyBlockCandidate method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block | [bytes](#bytes) |  | the block candidate in block assembly |






<a name="GetBlockAssemblyTxsResponse"></a>

### GetBlockAssemblyTxsResponse
Response for the GetBlockAssemblyTxs method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| txCount | [uint64](#uint64) |  | the number of transactions in the block assembly |
| txs | [string](#string) | repeated | the transactions currently being assembled in the block assembly |




 <!-- end messages -->

 <!-- end enums -->

 <!-- end HasExtensions -->


<a name="BlockAssemblyAPI"></a>

### BlockAssemblyAPI
Responsible for assembling new blocks and managing the blockchain's block creation process. This service handles transaction management, mining operations, and block state management.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| HealthGRPC | [EmptyMessage](#blockassembly_api-EmptyMessage) | [HealthResponse](#blockassembly_api-HealthResponse) | Checks the health status of the block assembly service. Returns detailed health information including service status and timestamp. |
| AddTx | [AddTxRequest](#blockassembly_api-AddTxRequest) | [AddTxResponse](#blockassembly_api-AddTxResponse) | Adds a single transaction to the next available subtree. The transaction will be included in block assembly for mining. |
| RemoveTx | [RemoveTxRequest](#blockassembly_api-RemoveTxRequest) | [EmptyMessage](#blockassembly_api-EmptyMessage) | Removes a transaction from consideration for block inclusion. This is useful for handling double-spends or invalid transactions. |
| AddTxBatch | [AddTxBatchRequest](#blockassembly_api-AddTxBatchRequest) | [AddTxBatchResponse](#blockassembly_api-AddTxBatchResponse) | Efficiently adds multiple transactions in a single request. Provides better performance than multiple individual AddTx calls. |
| GetMiningCandidate | [GetMiningCandidateRequest](#blockassembly_api-GetMiningCandidateRequest) | [model.MiningCandidate](#model-MiningCandidate) | Retrieves a block template ready for mining. Includes all necessary components for miners to begin work. |
| GetCurrentDifficulty | [EmptyMessage](#blockassembly_api-EmptyMessage) | [GetCurrentDifficultyResponse](#blockassembly_api-GetCurrentDifficultyResponse) | Retrieves the current network mining difficulty. Used by miners to understand the current mining requirements. |
| SubmitMiningSolution | [SubmitMiningSolutionRequest](#blockassembly_api-SubmitMiningSolutionRequest) | [OKResponse](#blockassembly_api-OKResponse) | Submits a solved block to the network. Includes the proof-of-work solution and block details. |
| ResetBlockAssembly | [EmptyMessage](#blockassembly_api-EmptyMessage) | [EmptyMessage](#blockassembly_api-EmptyMessage) | Resets the block assembly state. Useful for handling reorgs or recovering from errors. |
| GetBlockAssemblyState | [EmptyMessage](#blockassembly_api-EmptyMessage) | [StateMessage](#blockassembly_api-StateMessage) | Retrieves the current state of block assembly. Provides detailed information about the assembly process status. |
| GenerateBlocks | [GenerateBlocksRequest](#blockassembly_api-GenerateBlocksRequest) | [EmptyMessage](#blockassembly_api-EmptyMessage) | Creates new blocks (typically for testing purposes). Allows specification of block count and recipient address. |
| CheckBlockAssembly | [EmptyMessage](#blockassembly_api-EmptyMessage) | [OKResponse](#blockassembly_api-OKResponse) | Checks the current state of block assembly. This verifies that the block assembly and subtree processor are functioning correctly. |
| GetBlockAssemblyBlockCandidate | [EmptyMessage](#blockassembly_api-EmptyMessage) | [GetBlockAssemblyBlockCandidateResponse](#blockassembly_api-GetBlockAssemblyBlockCandidateResponse) | Retrieves the current block candidate from block assembly. |
| GetBlockAssemblyTxs | [EmptyMessage](#blockassembly_api-EmptyMessage) | [GetBlockAssemblyTxsResponse](#blockassembly_api-GetBlockAssemblyTxsResponse) | Retrieves the transactions currently being assembled in the block assembly. This provides visibility into the transactions that are candidates for inclusion in the next block. NOTE: this method is primarily for debugging purposes and may not be suitable for production use. |

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
