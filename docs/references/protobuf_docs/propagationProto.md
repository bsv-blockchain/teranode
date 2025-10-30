# GRPC Documentation - PropagationAPI
<a name="top"></a>

## Table of Contents

  - [propagation\_api.proto](#propagation_apiproto)
    - [EmptyMessage](#emptymessage)
    - [BatchTransactionItem](#batchtransactionitem)
    - [HealthResponse](#healthresponse)
    - [ProcessTransactionBatchRequest](#processtransactionbatchrequest)
    - [ProcessTransactionBatchResponse](#processtransactionbatchresponse)
    - [ProcessTransactionRequest](#processtransactionrequest)
    - [PropagationAPI](#propagationapi)
  - [Scalar Value Types](#scalar-value-types)



<a name="propagation_api.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## propagation_api.proto

Package propagation_api provides gRPC services for Bitcoin SV transaction propagation. It handles individual and batch transaction processing, health checks, and debugging capabilities for the BSV network.



<a name="EmptyMessage"></a>

### EmptyMessage
Represents an empty request or response. Used when no additional data needs to be transmitted.

swagger:model EmptyMessage






<a name="BatchTransactionItem"></a>

### BatchTransactionItem
Represents a single transaction item in a batch request with trace context support.

swagger:model BatchTransactionItem


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tx | [bytes](#bytes) |  | Raw transaction bytes to process |
| trace_context | map<string, string> |  | Serialized OpenTelemetry trace context as key-value pairs for proper span propagation |




<a name="HealthResponse"></a>

### HealthResponse
Provides information about the service's health status.

swagger:model HealthResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | Indicates whether the service is healthy |
| details | [string](#string) |  | Provides additional information about the health status |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | Indicates when the health check was performed |






<a name="ProcessTransactionBatchRequest"></a>

### ProcessTransactionBatchRequest
Represents a request to process multiple transactions in a batch.

swagger:model ProcessTransactionBatchRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| items | [BatchTransactionItem](#batchtransactionitem) | repeated | Array of transaction items to process, each containing transaction bytes and trace context |






<a name="ProcessTransactionBatchResponse"></a>

### ProcessTransactionBatchResponse
Contains the results of batch transaction processing.

swagger:model ProcessTransactionBatchResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| errors | [errors.TError](#errors-TError) | repeated | Error messages for each transaction in the batch. Empty string indicates success for that transaction. |






<a name="ProcessTransactionRequest"></a>

### ProcessTransactionRequest
Represents a request to process a single transaction.

swagger:model ProcessTransactionRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tx | [bytes](#bytes) |  | Raw transaction bytes to process |





 <!-- end messages -->

 <!-- end enums -->

 <!-- end HasExtensions -->


<a name="PropagationAPI"></a>

### PropagationAPI


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| HealthGRPC | [EmptyMessage](#propagation_api-EmptyMessage) | [HealthResponse](#propagation_api-HealthResponse) | Checks the health status of the propagation service and its dependencies. Returns a HealthResponse containing the service status and details. |
| ProcessTransaction | [ProcessTransactionRequest](#propagation_api-ProcessTransactionRequest) | [EmptyMessage](#propagation_api-EmptyMessage) | Processes a single BSV transaction. The transaction must be provided in raw byte format and must be extended. Coinbase transactions are not allowed. |
| ProcessTransactionBatch | [ProcessTransactionBatchRequest](#propagation_api-ProcessTransactionBatchRequest) | [ProcessTransactionBatchResponse](#propagation_api-ProcessTransactionBatchResponse) | Processes multiple transactions in a single request. This is more efficient than processing transactions individually when dealing with large numbers of transactions. |

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
