# Protocol Documentation
<a name="top"></a>

## Table of Contents


- [propagation_api.proto](#propagation_apiproto)
  - [EmptyMessage](#emptymessage)
  - [HealthResponse](#healthresponse)
  - [ProcessTransactionHexRequest](#processtransactionhexrequest)
  - [ProcessTransactionRequest](#processtransactionrequest)
  - [PropagationAPI](#propagationapi)
- [Scalar Value Types](#scalar-value-types)


<a name="propagation_api-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## propagation_api.proto



<a name="propagation_api-EmptyMessage"></a>

### EmptyMessage
Generic empty message.






<a name="propagation_api-HealthResponse"></a>

### HealthResponse
Health status of the service.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | true if the service is healthy |
| details | [string](#string) |  | optional, human-readable details |
| timestamp | [uint32](#uint32) |  | unix timestamp |






<a name="propagation_api-ProcessTransactionHexRequest"></a>

### ProcessTransactionHexRequest
Request to process a transaction in hexadecimal format.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tx | [string](#string) |  | Transaction data in hexadecimal format |






<a name="propagation_api-ProcessTransactionRequest"></a>

### ProcessTransactionRequest
Request to process a transaction.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tx | [bytes](#bytes) |  | Transaction data |












<a name="propagation_api-PropagationAPI"></a>

### PropagationAPI
This service is designed to handle the processing and propagation of transactions in the network.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| HealthGRPC | [EmptyMessage](#propagation_api-EmptyMessage) | [HealthResponse](#propagation_api-HealthResponse) | Health check for the API. |
| ProcessTransaction | [ProcessTransactionRequest](#propagation_api-ProcessTransactionRequest) | [EmptyMessage](#propagation_api-EmptyMessage) | Processes a transaction. |
| ProcessTransactionHex | [ProcessTransactionHexRequest](#propagation_api-ProcessTransactionHexRequest) | [EmptyMessage](#propagation_api-EmptyMessage) | Processes a transaction provided in hexadecimal format. |
| ProcessTransactionStream | [ProcessTransactionRequest](#propagation_api-ProcessTransactionRequest) stream | [EmptyMessage](#propagation_api-EmptyMessage) stream | Stream-based processing of transactions. |
| ProcessTransactionDebug | [ProcessTransactionRequest](#propagation_api-ProcessTransactionRequest) | [EmptyMessage](#propagation_api-EmptyMessage) | Processes a transaction with additional debug information. |





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
