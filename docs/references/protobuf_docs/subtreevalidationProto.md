# GRPC Documentation - SubtreeValidationAPI
<a name="top"></a>

## Table of Contents

- [subtreevalidation_api.proto](#subtreevalidation_api.proto)
    - [CheckSubtreeFromBlockRequest](#CheckSubtreeFromBlockRequest)
    - [CheckSubtreeFromBlockResponse](#CheckSubtreeFromBlockResponse)
    - [EmptyMessage](#EmptyMessage)
    - [HealthResponse](#HealthResponse)

    - [SubtreeValidationAPI](#SubtreeValidationAPI)

- [Scalar Value Types](#scalar-value-types)



<a name="subtreevalidation_api.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## subtreevalidation_api.proto



<a name="CheckSubtreeFromBlockRequest"></a>

### CheckSubtreeFromBlockRequest
Defines the input parameters for subtree validation.
swagger:model CheckSubtreeFromBlockRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  | Merkle root hash of the subtree requiring validation |
| base_url | [string](#string) |  | Endpoint for retrieving missing transaction data |
| block_height | [uint32](#uint32) |  | Blockchain height where the subtree is located |
| block_hash | [bytes](#bytes) |  | Uniquely identifies the block containing the subtree |






<a name="CheckSubtreeFromBlockResponse"></a>

### CheckSubtreeFromBlockResponse
Contains the validation result for a subtree check.
swagger:model CheckSubtreeFromBlockResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blessed | [bool](#bool) |  | Indicates if the subtree passes all validation criteria |






<a name="EmptyMessage"></a>

### EmptyMessage
Represents an empty message structure used for health check requests.
swagger:model EmptyMessage






<a name="HealthResponse"></a>

### HealthResponse
Encapsulates the service health status information.
swagger:model HealthResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | Indicates if the service is operating normally |
| details | [string](#string) |  | Provides additional context about the service health status |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | Records when the health check was performed |





 <!-- end messages -->

 <!-- end enums -->

 <!-- end HasExtensions -->


<a name="SubtreeValidationAPI"></a>

### SubtreeValidationAPI
Provides gRPC services for validating blockchain subtrees. The service exposes endpoints for health monitoring and subtree validation operations.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| HealthGRPC | [EmptyMessage](#subtreevalidation_api-EmptyMessage) | [HealthResponse](#subtreevalidation_api-HealthResponse) | Checks the service's health status. It takes an empty request message and returns a response indicating the service's health. |
| CheckSubtreeFromBlock | [CheckSubtreeFromBlockRequest](#subtreevalidation_api-CheckSubtreeFromBlockRequest) | [CheckSubtreeFromBlockResponse](#subtreevalidation_api-CheckSubtreeFromBlockResponse) | Validates a subtree within a specified block in the blockchain. It takes a request containing the subtree's merkle root hash and block details, returning a response indicating the subtree's validity status. |

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
