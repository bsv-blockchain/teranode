# Protocol Documentation
<a name="top"></a>

## Table of Contents


- [blockvalidation_api.proto](#blockvalidation_apiproto)
  - [BlockFoundRequest](#blockfoundrequest)
  - [EmptyMessage](#emptymessage)
  - [GetSubtreeRequest](#getsubtreerequest)
  - [GetSubtreeResponse](#getsubtreeresponse)
  - [HealthResponse](#healthresponse)
  - [SetTxMetaRequest](#settxmetarequest)
  - [SetTxMetaResponse](#settxmetaresponse)
  - [SubtreeFoundRequest](#subtreefoundrequest)
  - [BlockValidationAPI](#blockvalidationapi)
- [Scalar Value Types](#scalar-value-types)


<a name="blockvalidation_api-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## blockvalidation_api.proto



<a name="blockvalidation_api-BlockFoundRequest"></a>

### BlockFoundRequest
Request for notifying a found block.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  | the hash of the found block |
| base_url | [string](#string) |  | the base URL where the block can be accessed |






<a name="blockvalidation_api-EmptyMessage"></a>

### EmptyMessage
A simple empty message.






<a name="blockvalidation_api-GetSubtreeRequest"></a>

### GetSubtreeRequest
Request for retrieving a subtree.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  | the hash of the subtree to retrieve |






<a name="blockvalidation_api-GetSubtreeResponse"></a>

### GetSubtreeResponse
Response for retrieving a subtree.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| subtree | [bytes](#bytes) |  | the retrieved subtree data |






<a name="blockvalidation_api-HealthResponse"></a>

### HealthResponse
Health status of the service.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | true if the service is healthy |
| details | [string](#string) |  | optional, human-readable details |
| timestamp | [uint32](#uint32) |  | unix timestamp |






<a name="blockvalidation_api-SetTxMetaRequest"></a>

### SetTxMetaRequest
Request for setting transaction metadata.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) | repeated | the metadata to set for transactions |






<a name="blockvalidation_api-SetTxMetaResponse"></a>

### SetTxMetaResponse
Response for setting transaction metadata.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | true if the operation was successful |






<a name="blockvalidation_api-SubtreeFoundRequest"></a>

### SubtreeFoundRequest
Request for notifying a found subtree.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  | the hash of the found subtree |
| base_url | [string](#string) |  | the base URL where the subtree can be accessed |












<a name="blockvalidation_api-BlockValidationAPI"></a>

### BlockValidationAPI


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| HealthGRPC | [EmptyMessage](#blockvalidation_api-EmptyMessage) | [HealthResponse](#blockvalidation_api-HealthResponse) | Returns the health status of the API. |
| BlockFound | [BlockFoundRequest](#blockvalidation_api-BlockFoundRequest) | [EmptyMessage](#blockvalidation_api-EmptyMessage) | Notifies that a block has been found. |
| SubtreeFound | [SubtreeFoundRequest](#blockvalidation_api-SubtreeFoundRequest) | [EmptyMessage](#blockvalidation_api-EmptyMessage) | Notifies that a subtree has been found. |
| Get | [GetSubtreeRequest](#blockvalidation_api-GetSubtreeRequest) | [GetSubtreeResponse](#blockvalidation_api-GetSubtreeResponse) | Retrieves a subtree by its hash. |
| SetTxMeta | [SetTxMetaRequest](#blockvalidation_api-SetTxMetaRequest) | [SetTxMetaResponse](#blockvalidation_api-SetTxMetaResponse) | Sets metadata for transactions. |





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
