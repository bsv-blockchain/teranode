# GRPC Documentation - CoinbaseAPI
<a name="top"></a>

## Table of Contents

- [coinbase_api.proto](#coinbase_api.proto)
    - [DistributeTransactionRequest](#DistributeTransactionRequest)
    - [DistributeTransactionResponse](#DistributeTransactionResponse)
    - [GetBalanceResponse](#GetBalanceResponse)
    - [HealthResponse](#HealthResponse)
    - [RequestFundsRequest](#RequestFundsRequest)
    - [RequestFundsResponse](#RequestFundsResponse)
    - [ResponseWrapper](#ResponseWrapper)

    - [CoinbaseAPI](#CoinbaseAPI)

- [Scalar Value Types](#scalar-value-types)



<a name="coinbase_api.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## coinbase_api.proto



<a name="DistributeTransactionRequest"></a>

### DistributeTransactionRequest
swagger:model DistributeTransactionRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tx | [bytes](#bytes) |  |  |






<a name="DistributeTransactionResponse"></a>

### DistributeTransactionResponse
swagger:model DistributeTransactionResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| txid | [string](#string) |  |  |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |
| responses | [ResponseWrapper](#coinbase_api-ResponseWrapper) | repeated |  |






<a name="GetBalanceResponse"></a>

### GetBalanceResponse
swagger:model GetBalanceResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| number_of_utxos | [uint64](#uint64) |  |  |
| total_satoshis | [uint64](#uint64) |  |  |






<a name="HealthResponse"></a>

### HealthResponse
swagger:model HealthResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  |  |
| details | [string](#string) |  |  |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |






<a name="RequestFundsRequest"></a>

### RequestFundsRequest
swagger:model RequestFundsRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  |  |
| disableDistribute | [bool](#bool) |  |  |






<a name="RequestFundsResponse"></a>

### RequestFundsResponse
swagger:model RequestFundsResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tx | [bytes](#bytes) |  |  |






<a name="ResponseWrapper"></a>

### ResponseWrapper
swagger:model ResponseWrapper


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  |  |
| durationNanos | [int64](#int64) |  |  |
| retries | [int32](#int32) |  |  |
| error | [string](#string) |  |  |





 <!-- end messages -->

 <!-- end enums -->

 <!-- end HasExtensions -->


<a name="CoinbaseAPI"></a>

### CoinbaseAPI


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| HealthGRPC | [.google.protobuf.Empty](#google-protobuf-Empty) | [HealthResponse](#coinbase_api-HealthResponse) | Health returns the health of the API. |
| RequestFunds | [RequestFundsRequest](#coinbase_api-RequestFundsRequest) | [RequestFundsResponse](#coinbase_api-RequestFundsResponse) |  |
| DistributeTransaction | [DistributeTransactionRequest](#coinbase_api-DistributeTransactionRequest) | [DistributeTransactionResponse](#coinbase_api-DistributeTransactionResponse) |  |
| GetBalance | [.google.protobuf.Empty](#google-protobuf-Empty) | [GetBalanceResponse](#coinbase_api-GetBalanceResponse) |  |

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
