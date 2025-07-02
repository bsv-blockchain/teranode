# GRPC Documentation - ValidatorAPI
<a name="top"></a>

> The validator_api.proto defines the gRPC service interface for the Bitcoin SV transaction
> validation service. It specifies the methods and message types for transaction
> validation, health checking, and block information retrieval.

## Table of Contents

- [GRPC Documentation - ValidatorAPI](#grpc-documentation---validatorapi)
    - [Table of Contents](#table-of-contents)
  - [validator\_api.proto](#validator_apiproto)
    - [EmptyMessage](#emptymessage)
    - [GetBlockHeightResponse](#getblockheightresponse)
    - [GetMedianBlockTimeResponse](#getmedianblocktimeresponse)
    - [HealthResponse](#healthresponse)
    - [ValidateTransactionBatchRequest](#validatetransactionbatchrequest)
    - [ValidateTransactionBatchResponse](#validatetransactionbatchresponse)
    - [ValidateTransactionRequest](#validatetransactionrequest)
    - [ValidateTransactionResponse](#validatetransactionresponse)
    - [ValidatorAPI](#validatorapi)
  - [Scalar Value Types](#scalar-value-types)



<a name="validator_api.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## validator_api.proto



<a name="EmptyMessage"></a>

### EmptyMessage
Represents an empty request message. Used for endpoints that don't require input parameters.
swagger:model EmptyMessage






<a name="GetBlockHeightResponse"></a>

### GetBlockHeightResponse
Provides the current block height.
swagger:model GetBlockHeightResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| height | [uint32](#uint32) |  | Current block height |






<a name="GetMedianBlockTimeResponse"></a>

### GetMedianBlockTimeResponse
Provides the median time of recent blocks. Used for time-based validation rules.
swagger:model GetMedianBlockTimeResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| median_time | [uint32](#uint32) |  | Median time of recent blocks |






<a name="HealthResponse"></a>

### HealthResponse
Provides health check information for the validation service.
swagger:model HealthResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | Overall health status |
| details | [string](#string) |  | Detailed health information |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | Timestamp of health check |






<a name="ValidateTransactionBatchRequest"></a>

### ValidateTransactionBatchRequest
Contains multiple transactions for batch validation.
swagger:model ValidateTransactionBatchRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| transactions | [ValidateTransactionRequest](#validator_api-ValidateTransactionRequest) | repeated | Array of transactions to validate |






<a name="ValidateTransactionBatchResponse"></a>

### ValidateTransactionBatchResponse
Provides batch validation results for multiple transactions.
swagger:model ValidateTransactionBatchResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| valid | [bool](#bool) |  | Overall batch validation status |
| errors | [errors.TError](#errors-TError) | repeated | Array of error messages, one per transaction |
| metadata | [bytes](#bytes) | repeated | Array of metadata for each transaction |






<a name="ValidateTransactionRequest"></a>

### ValidateTransactionRequest
Contains data for transaction validation.
swagger:model ValidateTransactionRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| transaction_data | [bytes](#bytes) |  | Raw transaction data to validate |
| block_height | [uint32](#uint32) |  | Block height for validation context |
| skip_utxo_creation | [bool](#bool) | optional | Skip UTXO creation for validation |
| add_tx_to_block_assembly | [bool](#bool) | optional | Add transaction to block assembly |
| skip_policy_checks | [bool](#bool) | optional | Skip policy checks |
| create_conflicting | [bool](#bool) | optional | Create conflicting transaction |






<a name="ValidateTransactionResponse"></a>

### ValidateTransactionResponse
Provides transaction validation results.
swagger:model ValidateTransactionResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| valid | [bool](#bool) |  | Validation result (true if valid) |
| txid | [bytes](#bytes) |  | Transaction ID of the validated transaction |
| reason | [string](#string) |  | Reason for rejection if invalid |
| metadata | [bytes](#bytes) |  | Additional metadata for the transaction |





 <!-- end messages -->

 <!-- end enums -->

 <!-- end HasExtensions -->


<a name="ValidatorAPI"></a>

### ValidatorAPI
Service provides methods for transaction validation and related operations.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| HealthGRPC | [EmptyMessage](#validator_api-EmptyMessage) | [HealthResponse](#validator_api-HealthResponse) | Checks the health status of the validation service. Returns detailed health information including service status and timestamp. |
| ValidateTransaction | [ValidateTransactionRequest](#validator_api-ValidateTransactionRequest) | [ValidateTransactionResponse](#validator_api-ValidateTransactionResponse) | Validates a single transaction. Performs comprehensive validation including script verification and UTXO checks. |
| ValidateTransactionBatch | [ValidateTransactionBatchRequest](#validator_api-ValidateTransactionBatchRequest) | [ValidateTransactionBatchResponse](#validator_api-ValidateTransactionBatchResponse) | Validates multiple transactions in a single request. Provides efficient batch processing of transactions. |
| GetBlockHeight | [EmptyMessage](#validator_api-EmptyMessage) | [GetBlockHeightResponse](#validator_api-GetBlockHeightResponse) | Retrieves the current block height. Used for validation context and protocol upgrade determination. |
| GetMedianBlockTime | [EmptyMessage](#validator_api-EmptyMessage) | [GetMedianBlockTimeResponse](#validator_api-GetMedianBlockTimeResponse) | Retrieves the median time of recent blocks. Used for time-based validation rules. |

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
