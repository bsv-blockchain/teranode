# Protocol Documentation
<a name="top"></a>

## Table of Contents


- [coinbase_api.proto](#coinbase_apiproto)
  - [DistributeTransactionRequest](#distributetransactionrequest)
  - [DistributeTransactionResponse](#distributetransactionresponse)
  - [HealthResponse](#healthresponse)
  - [RequestFundsRequest](#requestfundsrequest)
  - [RequestFundsResponse](#requestfundsresponse)
  - [ResponseWrapper](#responsewrapper)
  - [CoinbaseAPI](#coinbaseapi)
- [Scalar Value Types](#scalar-value-types)



<a name="coinbase_api-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## coinbase_api.proto



<a name="coinbase_api-DistributeTransactionRequest"></a>

### DistributeTransactionRequest
Request to distribute a transaction across the network.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tx | [bytes](#bytes) |  | Transaction to be distributed |






<a name="coinbase_api-DistributeTransactionResponse"></a>

### DistributeTransactionResponse
Response for distributing a transaction.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| txid | [string](#string) |  | Transaction ID of the distributed transaction |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | Timestamp of the operation |
| responses | [ResponseWrapper](#coinbase_api-ResponseWrapper) | repeated | List of responses from different nodes |






<a name="coinbase_api-HealthResponse"></a>

### HealthResponse
Health status of the service.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | true if the service is healthy |
| details | [string](#string) |  | optional, human-readable details |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | unix timestamp |






<a name="coinbase_api-RequestFundsRequest"></a>

### RequestFundsRequest
Request for funds to be sent to an address.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  | Address to which funds are requested |
| disableDistribute | [bool](#bool) |  | Flag to disable distribution of funds |






<a name="coinbase_api-RequestFundsResponse"></a>

### RequestFundsResponse
Response for a fund request.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tx | [bytes](#bytes) |  | Transaction bytes of the funding transaction |






<a name="coinbase_api-ResponseWrapper"></a>

### ResponseWrapper
Wrapper for response details.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  | Address involved in the response |
| durationNanos | [int64](#int64) |  | Duration of the operation in nanoseconds |
| retries | [int32](#int32) |  | Number of retries for the operation |
| error | [string](#string) |  | Error message, if any |












<a name="coinbase_api-CoinbaseAPI"></a>

### CoinbaseAPI


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| HealthGRPC | [.google.protobuf.Empty](#google-protobuf-Empty) | [HealthResponse](#coinbase_api-HealthResponse) | Health check for the API. |
| RequestFunds | [RequestFundsRequest](#coinbase_api-RequestFundsRequest) | [RequestFundsResponse](#coinbase_api-RequestFundsResponse) | Requests funds to a specific address. |
| DistributeTransaction | [DistributeTransactionRequest](#coinbase_api-DistributeTransactionRequest) | [DistributeTransactionResponse](#coinbase_api-DistributeTransactionResponse) | Distributes a transaction across the network. |





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
