# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [blockassembly_api.proto](#blockassemblyapiproto)
  - [BlockAssemblyAPI](#blockassemblyapi)
  - [AddTxBatchRequest](#addtxbatchrequest)
  - [AddTxBatchResponse](#addtxbatchresponse)
  - [AddTxRequest](#addtxrequest)
  - [AddTxResponse](#addtxresponse)
  - [EmptyMessage](#emptymessage)
  - [HealthResponse](#healthresponse)
  - [NewChaintipAndHeightRequest](#newchaintipandheightrequest)
  - [RemoveTxRequest](#removetxrequest)
  - [SubmitMiningSolutionRequest](#submitminingsolutionrequest)
  - [SubmitMiningSolutionResponse](#submitminingsolutionresponse)
- [model/model.proto](#modelmodelproto)
  - [AnnounceStatusRequest](#announcestatusrequest)
  - [BlockInfo](#blockinfo)
  - [MiningCandidate](#miningcandidate)
  - [MiningSolution](#miningsolution)
  - [NotificationType](#notificationtype)
- [Scalar Value Types](#scalar-value-types)


<a name="services_blockassembly_blockassembly_api_blockassembly_api-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## blockassembly_api.proto


<a name="blockassembly_api-BlockAssemblyAPI"></a>

### BlockAssemblyAPI
The Block Assembly Service is responsible for assembling new blocks and adding them to the blockchain.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| HealthGRPC | [EmptyMessage](#blockassembly_api-EmptyMessage) | [HealthResponse](#blockassembly_api-HealthResponse) | Health returns the health of the API. |
| AddTx | [AddTxRequest](#blockassembly_api-AddTxRequest) | [AddTxResponse](#blockassembly_api-AddTxResponse) | Adds a transaction to the list of transactions to be included in the next available Subtree. |
| RemoveTx | [RemoveTxRequest](#blockassembly_api-RemoveTxRequest) | [EmptyMessage](#blockassembly_api-EmptyMessage) | Removes a transaction from the list of transactions to be included in the next available Subtree. |
| AddTxBatch | [AddTxBatchRequest](#blockassembly_api-AddTxBatchRequest) | [AddTxBatchResponse](#blockassembly_api-AddTxBatchResponse) | Adds a batch of transactions to the list of transactions to be included in the next available Subtree. |
| GetMiningCandidate | [EmptyMessage](#blockassembly_api-EmptyMessage) | [.model.MiningCandidate](#model-MiningCandidate) | Returns a mining candidate block, including the coinbase transaction, the subtrees, the root merkle proof and the block fees. |
| SubmitMiningSolution | [SubmitMiningSolutionRequest](#blockassembly_api-SubmitMiningSolutionRequest) | [SubmitMiningSolutionResponse](#blockassembly_api-SubmitMiningSolutionResponse) | Submits a mining solution to the blockchain. |




<a name="blockassembly_api-AddTxBatchRequest"></a>

### AddTxBatchRequest
Request for adding a batch of transactions to the mining candidate block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| txRequests | [AddTxRequest](#blockassembly_api-AddTxRequest) | repeated | a batch of transaction requests |






<a name="blockassembly_api-AddTxBatchResponse"></a>

### AddTxBatchResponse
Response indicating whether the addition of a batch of transactions was successful.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | true if the transactions were successfully added |
| txIdErrors | [bytes](#bytes) | repeated | list of transaction IDs that encountered errors |






<a name="blockassembly_api-AddTxRequest"></a>

### AddTxRequest
Request for adding a new transaction to the mining candidate block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| txid | [bytes](#bytes) |  | the transaction id |
| fee | [uint64](#uint64) |  | the transaction fee in satoshis |
| size | [uint64](#uint64) |  | the size of the transaction in bytes |
| locktime | [uint32](#uint32) |  | the earliest time a transaction can be mined into a block |
| utxos | [bytes](#bytes) | repeated | the UTXOs consumed by this transaction |






<a name="blockassembly_api-AddTxResponse"></a>

### AddTxResponse
Response indicating whether the addition of a transaction was successful.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | true if the transaction was successfully added |






<a name="blockassembly_api-EmptyMessage"></a>

### EmptyMessage
An empty message used as a placeholder or a request with no data.






<a name="blockassembly_api-HealthResponse"></a>

### HealthResponse
Contains the health status of the service. Includes an &#39;ok&#39; flag indicating health status, details providing more context, and a timestamp.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | true if the service is healthy |
| details | [string](#string) |  | optional, human-readable details |
| timestamp | [uint32](#uint32) |  | unix timestamp |






<a name="blockassembly_api-NewChaintipAndHeightRequest"></a>

### NewChaintipAndHeightRequest
Request for adding a new chaintip and height information.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| chaintip | [bytes](#bytes) |  | the chaintip hash |
| height | [uint32](#uint32) |  | the height of the chaintip in the blockchain |






<a name="blockassembly_api-RemoveTxRequest"></a>

### RemoveTxRequest
Request for removing a transaction from the mining candidate block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| txid | [bytes](#bytes) |  | the transaction id to remove |






<a name="blockassembly_api-SubmitMiningSolutionRequest"></a>

### SubmitMiningSolutionRequest
Request for submitting a mining solution to the blockchain.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [bytes](#bytes) |  | the id of the mining candidate |
| nonce | [uint32](#uint32) |  | the nonce value used for mining |
| coinbase_tx | [bytes](#bytes) |  | the coinbase transaction bytes |
| time | [uint32](#uint32) |  | the timestamp of the block |
| version | [uint32](#uint32) |  | the version of the block |






<a name="blockassembly_api-SubmitMiningSolutionResponse"></a>

### SubmitMiningSolutionResponse
Response indicating whether the submission of a mining solution was successful.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | true if the solution was successfully submitted |















<a name="model_model-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## model/model.proto



<a name="model-AnnounceStatusRequest"></a>

### AnnounceStatusRequest
swagger:model AnnounceStatusRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |
| cluster_name | [string](#string) |  |  |
| type | [string](#string) |  |  |
| subtype | [string](#string) |  |  |
| value | [string](#string) |  |  |
| expires_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |






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
