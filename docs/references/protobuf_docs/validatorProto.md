# GRPC Documentation - ValidatorAPI
<a name="top"></a>

## Table of Contents

- [validator_api.proto](#validator_api.proto)
    - [EmptyMessage](#EmptyMessage)
    - [GetBlockHeightResponse](#GetBlockHeightResponse)
    - [GetMedianBlockTimeResponse](#GetMedianBlockTimeResponse)
    - [HealthResponse](#HealthResponse)
    - [ValidateTransactionBatchRequest](#ValidateTransactionBatchRequest)
    - [ValidateTransactionBatchResponse](#ValidateTransactionBatchResponse)
    - [ValidateTransactionRequest](#ValidateTransactionRequest)
    - [ValidateTransactionResponse](#ValidateTransactionResponse)

    - [ValidatorAPI](#ValidatorAPI)

- [Scalar Value Types](#scalar-value-types)



<a name="validator_api.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## validator_api.proto



<a name="EmptyMessage"></a>

### EmptyMessage
swagger:model EmptyMessage






<a name="GetBlockHeightResponse"></a>

### GetBlockHeightResponse
swagger:model GetBlockHeightResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| height | [uint32](#uint32) |  |  |






<a name="GetMedianBlockTimeResponse"></a>

### GetMedianBlockTimeResponse
swagger:model GetMedianBlockTimeResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| median_time | [uint32](#uint32) |  |  |






<a name="HealthResponse"></a>

### HealthResponse
swagger:model HealthResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  |  |
| details | [string](#string) |  |  |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |






<a name="ValidateTransactionBatchRequest"></a>

### ValidateTransactionBatchRequest
swagger:model ValidateTransactionBatchRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| transactions | [ValidateTransactionRequest](#validator_api-ValidateTransactionRequest) | repeated |  |






<a name="ValidateTransactionBatchResponse"></a>

### ValidateTransactionBatchResponse
swagger:model ValidateTransactionBatchResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| valid | [bool](#bool) |  |  |
| errors | [string](#string) | repeated |  |






<a name="ValidateTransactionRequest"></a>

### ValidateTransactionRequest
swagger:model ValidateTransactionRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| transaction_data | [bytes](#bytes) |  |  |
| block_height | [uint32](#uint32) |  |  |






<a name="ValidateTransactionResponse"></a>

### ValidateTransactionResponse
swagger:model ValidateTransactionResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| valid | [bool](#bool) |  |  |
| txid | [bytes](#bytes) |  |  |
| reason | [string](#string) |  |  |





 <!-- end messages -->

 <!-- end enums -->

 <!-- end HasExtensions -->


<a name="ValidatorAPI"></a>

### ValidatorAPI


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| HealthGRPC | [EmptyMessage](#validator_api-EmptyMessage) | [HealthResponse](#validator_api-HealthResponse) | Health returns the health of the API. |
| ValidateTransaction | [ValidateTransactionRequest](#validator_api-ValidateTransactionRequest) | [ValidateTransactionResponse](#validator_api-ValidateTransactionResponse) |  |
| ValidateTransactionBatch | [ValidateTransactionBatchRequest](#validator_api-ValidateTransactionBatchRequest) | [ValidateTransactionBatchResponse](#validator_api-ValidateTransactionBatchResponse) |  |
| ValidateTransactionStream | [ValidateTransactionRequest](#validator_api-ValidateTransactionRequest) stream | [ValidateTransactionResponse](#validator_api-ValidateTransactionResponse) |  |
| GetBlockHeight | [EmptyMessage](#validator_api-EmptyMessage) | [GetBlockHeightResponse](#validator_api-GetBlockHeightResponse) |  |
| GetMedianBlockTime | [EmptyMessage](#validator_api-EmptyMessage) | [GetMedianBlockTimeResponse](#validator_api-GetMedianBlockTimeResponse) |  |

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
