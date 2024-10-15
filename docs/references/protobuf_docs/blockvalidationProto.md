# GRPC Documentation - BlockValidationAPI
<a name="top"></a>

## Table of Contents

- [blockvalidation_api.proto](#blockvalidation_api.proto)
    - [BlockFoundRequest](#BlockFoundRequest)
    - [DelTxMetaRequest](#DelTxMetaRequest)
    - [DelTxMetaResponse](#DelTxMetaResponse)
    - [EmptyMessage](#EmptyMessage)
    - [ExistsSubtreeRequest](#ExistsSubtreeRequest)
    - [ExistsSubtreeResponse](#ExistsSubtreeResponse)
    - [GetSubtreeRequest](#GetSubtreeRequest)
    - [GetSubtreeResponse](#GetSubtreeResponse)
    - [HealthResponse](#HealthResponse)
    - [ProcessBlockRequest](#ProcessBlockRequest)
    - [SetMinedMultiRequest](#SetMinedMultiRequest)
    - [SetMinedMultiResponse](#SetMinedMultiResponse)
    - [SetTxMetaRequest](#SetTxMetaRequest)
    - [SetTxMetaResponse](#SetTxMetaResponse)
    - [SubtreeFoundRequest](#SubtreeFoundRequest)

    - [BlockValidationAPI](#BlockValidationAPI)

- [Scalar Value Types](#scalar-value-types)



<a name="blockvalidation_api.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## blockvalidation_api.proto



<a name="BlockFoundRequest"></a>

### BlockFoundRequest
swagger:model BlockFoundRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  |  |
| base_url | [string](#string) |  |  |
| wait_to_complete | [bool](#bool) |  |  |






<a name="DelTxMetaRequest"></a>

### DelTxMetaRequest
swagger:model DelTxMetaRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  |  |






<a name="DelTxMetaResponse"></a>

### DelTxMetaResponse
swagger:model DelTxMetaResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  |  |






<a name="EmptyMessage"></a>

### EmptyMessage
swagger:model EmptyMessage






<a name="ExistsSubtreeRequest"></a>

### ExistsSubtreeRequest
swagger:model ExistsSubtreeRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  |  |






<a name="ExistsSubtreeResponse"></a>

### ExistsSubtreeResponse
swagger:model ExistsSubtreeResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| exists | [bool](#bool) |  |  |






<a name="GetSubtreeRequest"></a>

### GetSubtreeRequest
swagger:model GetSubtreeRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  |  |






<a name="GetSubtreeResponse"></a>

### GetSubtreeResponse
swagger:model GetSubtreeResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| subtree | [bytes](#bytes) |  |  |






<a name="HealthResponse"></a>

### HealthResponse
swagger:model HealthResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  |  |
| details | [string](#string) |  |  |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |






<a name="ProcessBlockRequest"></a>

### ProcessBlockRequest
swagger:model ProcessBlockRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block | [bytes](#bytes) |  |  |
| height | [uint32](#uint32) |  |  |






<a name="SetMinedMultiRequest"></a>

### SetMinedMultiRequest
swagger:model SetMinedMultiRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block_id | [uint32](#uint32) |  |  |
| hashes | [bytes](#bytes) | repeated |  |






<a name="SetMinedMultiResponse"></a>

### SetMinedMultiResponse
swagger:model SetMinedMultiResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  |  |






<a name="SetTxMetaRequest"></a>

### SetTxMetaRequest
swagger:model SetTxMetaRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) | repeated |  |






<a name="SetTxMetaResponse"></a>

### SetTxMetaResponse
swagger:model SetTxMetaResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  |  |






<a name="SubtreeFoundRequest"></a>

### SubtreeFoundRequest
swagger:model SubtreeFoundRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  |  |
| base_url | [string](#string) |  |  |





 <!-- end messages -->

 <!-- end enums -->

 <!-- end HasExtensions -->


<a name="BlockValidationAPI"></a>

### BlockValidationAPI


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| HealthGRPC | [EmptyMessage](#blockvalidation_api-EmptyMessage) | [HealthResponse](#blockvalidation_api-HealthResponse) | Health returns the health of the API. |
| BlockFound | [BlockFoundRequest](#blockvalidation_api-BlockFoundRequest) | [EmptyMessage](#blockvalidation_api-EmptyMessage) |  |
| SubtreeFound | [SubtreeFoundRequest](#blockvalidation_api-SubtreeFoundRequest) | [EmptyMessage](#blockvalidation_api-EmptyMessage) |  |
| ProcessBlock | [ProcessBlockRequest](#blockvalidation_api-ProcessBlockRequest) | [EmptyMessage](#blockvalidation_api-EmptyMessage) |  |
| Get | [GetSubtreeRequest](#blockvalidation_api-GetSubtreeRequest) | [GetSubtreeResponse](#blockvalidation_api-GetSubtreeResponse) |  |
| Exists | [ExistsSubtreeRequest](#blockvalidation_api-ExistsSubtreeRequest) | [ExistsSubtreeResponse](#blockvalidation_api-ExistsSubtreeResponse) |  |
| SetTxMeta | [SetTxMetaRequest](#blockvalidation_api-SetTxMetaRequest) | [SetTxMetaResponse](#blockvalidation_api-SetTxMetaResponse) |  |
| DelTxMeta | [DelTxMetaRequest](#blockvalidation_api-DelTxMetaRequest) | [DelTxMetaResponse](#blockvalidation_api-DelTxMetaResponse) |  |
| SetMinedMulti | [SetMinedMultiRequest](#blockvalidation_api-SetMinedMultiRequest) | [SetMinedMultiResponse](#blockvalidation_api-SetMinedMultiResponse) |  |

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
