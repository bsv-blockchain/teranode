# GRPC Documentation - BlockValidationAPI

<a name="top"></a>

## Table of Contents

- [blockvalidation_api.proto](#blockvalidation_api.proto)
    - [BlockFoundRequest](#BlockFoundRequest)
    - [EmptyMessage](#EmptyMessage)
    - [HealthResponse](#HealthResponse)
    - [ProcessBlockRequest](#ProcessBlockRequest)
    - [ValidateBlockRequest](#ValidateBlockRequest)
    - [ValidateBlockResponse](#ValidateBlockResponse)

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
| hash | [bytes](#bytes) |  | The hash of the found block |
| base_url | [string](#string) |  | Base URL where the block can be retrieved from |
| wait_to_complete | [bool](#bool) |  | Whether to wait for the block processing to complete |
| peer_id | [string](#string) |  | P2P peer identifier for peerMetrics tracking |

<a name="EmptyMessage"></a>

### EmptyMessage

Represents an empty request or response message. Used for endpoints that don't require input parameters or return data.

swagger:model EmptyMessage

<a name="HealthResponse"></a>

### HealthResponse

swagger:model HealthResponse

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | Indicates if the service is healthy |
| details | [string](#string) |  | Additional health status details |
| timestamp | google.protobuf.Timestamp |  | Timestamp when the health check was performed |

<a name="ProcessBlockRequest"></a>

### ProcessBlockRequest

swagger:model ProcessBlockRequest

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block | [bytes](#bytes) |  | The block data to process |
| height | [uint32](#uint32) |  | The height of the block in the blockchain |
| base_url | [string](#string) |  | Base URL where the block can be retrieved from |
| peer_id | [string](#string) |  | P2P peer identifier for peerMetrics tracking |

<a name="ValidateBlockRequest"></a>

### ValidateBlockRequest

swagger:model ValidateBlockRequest

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block | [bytes](#bytes) |  | The block data to validate |
| height | [uint32](#uint32) |  | The height of the block in the blockchain |
| is_revalidation | [bool](#bool) |  | Indicates this is a revalidation of an invalid block |

<a name="ValidateBlockResponse"></a>

### ValidateBlockResponse

swagger:model ValidateBlockResponse

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | Indicates if the block is valid |
| message | [string](#string) |  | Additional information about validation results |

 <!-- end messages -->

 <!-- end enums -->

 <!-- end HasExtensions -->

<a name="BlockValidationAPI"></a>

### BlockValidationAPI

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| HealthGRPC | [EmptyMessage](#EmptyMessage) | [HealthResponse](#HealthResponse) | Returns the health status of the BlockValidation service. |
| BlockFound | [BlockFoundRequest](#BlockFoundRequest) | [EmptyMessage](#EmptyMessage) | Notifies the service that a new block has been found and requires validation. |
| ProcessBlock | [ProcessBlockRequest](#ProcessBlockRequest) | [EmptyMessage](#EmptyMessage) | Processes a block to validate its content and structure. |
| ValidateBlock | [ValidateBlockRequest](#ValidateBlockRequest) | [ValidateBlockResponse](#ValidateBlockResponse) | Validates a block without processing it, returning validation results. |

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
