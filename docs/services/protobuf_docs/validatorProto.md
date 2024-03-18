# Protocol Documentation
<a name="top"></a>

## Table of Contents


- [validator_api.proto](#validator_apiproto)
  - [EmptyMessage](#emptymessage)
  - [GetBlockHeightResponse](#getblockheightresponse)
  - [HealthResponse](#healthresponse)
  - [RejectedTxNotification](#rejectedtxnotification)
  - [SubscribeRequest](#subscriberequest)
  - [ValidateTransactionBatchRequest](#validatetransactionbatchrequest)
  - [ValidateTransactionBatchResponse](#validatetransactionbatchresponse)
  - [ValidateTransactionError](#validatetransactionerror)
  - [ValidateTransactionRequest](#validatetransactionrequest)
  - [ValidateTransactionResponse](#validatetransactionresponse)
  - [ValidatorAPI](#validatorapi)
- [Scalar Value Types](#scalar-value-types)



<a name="validator_api-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## validator_api.proto



<a name="validator_api-EmptyMessage"></a>

### EmptyMessage
Generic empty message.






<a name="validator_api-GetBlockHeightResponse"></a>

### GetBlockHeightResponse
Response for getting the current block height.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| height | [uint32](#uint32) |  | current block height |






<a name="validator_api-HealthResponse"></a>

### HealthResponse
Health status of the service.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  | true if the service is healthy |
| details | [string](#string) |  | optional, human-readable details |
| timestamp | [uint32](#uint32) |  | unix timestamp |






<a name="validator_api-RejectedTxNotification"></a>

### RejectedTxNotification
Notification for rejected transactions.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| txId | [string](#string) |  | transaction ID |
| reason | [string](#string) |  | reason for rejection |






<a name="validator_api-SubscribeRequest"></a>

### SubscribeRequest
Request for subscription to notifications.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| source | [string](#string) |  | subscriber&#39;s source identifier |






<a name="validator_api-ValidateTransactionBatchRequest"></a>

### ValidateTransactionBatchRequest
Request for batch transaction validation.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| transactions | [ValidateTransactionRequest](#validator_api-ValidateTransactionRequest) | repeated | transactions to validate |






<a name="validator_api-ValidateTransactionBatchResponse"></a>

### ValidateTransactionBatchResponse
Response for batch transaction validation.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| valid | [bool](#bool) |  | true if all transactions in the batch are valid |
| reasons | [ValidateTransactionError](#validator_api-ValidateTransactionError) | repeated | reasons for invalidation, if applicable |






<a name="validator_api-ValidateTransactionError"></a>

### ValidateTransactionError
Error details for transaction validation.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| txId | [string](#string) |  | transaction ID |
| reason | [string](#string) |  | reason for invalidation |






<a name="validator_api-ValidateTransactionRequest"></a>

### ValidateTransactionRequest
Request for transaction validation.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| transaction_data | [bytes](#bytes) |  | Transaction data |






<a name="validator_api-ValidateTransactionResponse"></a>

### ValidateTransactionResponse
Response for transaction validation.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| valid | [bool](#bool) |  | true if the transaction is valid |
| reason | [string](#string) |  | reason for invalidation, if applicable |












<a name="validator_api-ValidatorAPI"></a>

### ValidatorAPI


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| HealthGRPC | [EmptyMessage](#validator_api-EmptyMessage) | [HealthResponse](#validator_api-HealthResponse) | Health check for the API. |
| ValidateTransaction | [ValidateTransactionRequest](#validator_api-ValidateTransactionRequest) | [ValidateTransactionResponse](#validator_api-ValidateTransactionResponse) | Validates a single transaction. |
| ValidateTransactionBatch | [ValidateTransactionBatchRequest](#validator_api-ValidateTransactionBatchRequest) | [ValidateTransactionBatchResponse](#validator_api-ValidateTransactionBatchResponse) | Validates a batch of transactions. |
| ValidateTransactionStream | [ValidateTransactionRequest](#validator_api-ValidateTransactionRequest) stream | [ValidateTransactionResponse](#validator_api-ValidateTransactionResponse) | Stream-based validation of transactions. |
| GetBlockHeight | [EmptyMessage](#validator_api-EmptyMessage) | [GetBlockHeightResponse](#validator_api-GetBlockHeightResponse) | Retrieves the current block height. |
| Subscribe | [SubscribeRequest](#validator_api-SubscribeRequest) | [RejectedTxNotification](#validator_api-RejectedTxNotification) stream | Subscribes to notifications for rejected transactions. |





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
