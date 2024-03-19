# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [subtreevalidation_api.proto](#subtreevalidation_api-proto)
    - [CheckSubtreeRequest](#subtreevalidation_api-CheckSubtreeRequest)
    - [CheckSubtreeResponse](#subtreevalidation_api-CheckSubtreeResponse)
    - [EmptyMessage](#subtreevalidation_api-EmptyMessage)
    - [HealthResponse](#subtreevalidation_api-HealthResponse)

    - [SubtreeValidationAPI](#subtreevalidation_api-SubtreeValidationAPI)

- [Scalar Value Types](#scalar-value-types)



<a name="subtreevalidation_api-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## subtreevalidation_api.proto



<a name="subtreevalidation_api-CheckSubtreeRequest"></a>

### CheckSubtreeRequest
CheckSubtreeRequest contains the request parameters for validating a subtree.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  | The hash of the subtree to be validated. |
| base_url | [string](#string) |  | The base URL from where the subtree can be retrieved if necessary. |






<a name="subtreevalidation_api-CheckSubtreeResponse"></a>

### CheckSubtreeResponse
CheckSubtreeResponse is the response for a subtree validation request.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blessed | [bool](#bool) |  | Indicates whether the subtree has passed validation. |






<a name="subtreevalidation_api-EmptyMessage"></a>

### EmptyMessage
EmptyMessage is used for RPC methods that don&#39;t require input parameters.






<a name="subtreevalidation_api-HealthResponse"></a>

### HealthResponse
HealthResponse encapsulates the response for the health check of the API.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | Indicates whether the API is healthy. |
| details | [string](#string) |  | Provides additional details on the health status. |
| timestamp | [uint32](#uint32) |  | The timestamp of when the health check was performed. |












<a name="subtreevalidation_api-SubtreeValidationAPI"></a>

### SubtreeValidationAPI
SubtreeValidationAPI defines the RPC service for subtree validation operations.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| HealthGRPC | [EmptyMessage](#subtreevalidation_api-EmptyMessage) | [HealthResponse](#subtreevalidation_api-HealthResponse) | HealthGRPC checks the health of the Subtree Validation API service. |
| CheckSubtree | [CheckSubtreeRequest](#subtreevalidation_api-CheckSubtreeRequest) | [CheckSubtreeResponse](#subtreevalidation_api-CheckSubtreeResponse) | CheckSubtree validates a given subtree to ensure its integrity and consistency. |





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
