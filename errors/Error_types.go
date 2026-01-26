package errors

// Error type usage guidelines:
//
//	storage error -> error returned from processing a file on s3, aerospike.
//	service error -> error when performing an operation with one of the services, i.e., if it is using our GRPC of one of our services, error when
//	processing error -> error when manipulating data, i.e., when processing a block, a transaction, etc., inside the method/function.
var (
	ErrBlobAlreadyExists          = New(ERR_BLOB_EXISTS, "blob already exists")
	ErrBlobError                  = New(ERR_BLOB_ERROR, "blob error")
	ErrBlobNotFound               = New(ERR_BLOB_NOT_FOUND, "blob not found")
	ErrBlockAssemblyReset         = New(ERR_BLOCK_ASSEMBLY_RESET, "block assembly reset")
	ErrBlockCoinbaseMissingHeight = New(ERR_BLOCK_COINBASE_MISSING_HEIGHT, "the coinbase signature script doesn't have the block height")
	ErrBlockError                 = New(ERR_BLOCK_ERROR, "block error")
	ErrBlockExists                = New(ERR_BLOCK_EXISTS, "block exists")
	ErrBlockInvalid               = New(ERR_BLOCK_INVALID, "block invalid")
	ErrBlockInvalidFormat         = New(ERR_BLOCK_INVALID_FORMAT, "block format is invalid")
	ErrBlockNotFound              = New(ERR_BLOCK_NOT_FOUND, "block not found")
	ErrBlockParentNotMined        = New(ERR_BLOCK_PARENT_NOT_MINED, "block parent not mined")
	ErrCatchupInProgress          = New(ERR_CATCHUP_IN_PROGRESS, "catchup in progress")
	ErrConfiguration              = New(ERR_CONFIGURATION, "configuration error")
	ErrContextCanceled            = New(ERR_CONTEXT_CANCELED, "context canceled")
	ErrError                      = New(ERR_ERROR, "generic error")
	ErrExternal                   = New(ERR_EXTERNAL, "external error")
	ErrInvalidArgument            = New(ERR_INVALID_ARGUMENT, "invalid argument")
	ErrInvalidIP                  = New(ERR_INVALID_IP, "invalid ip")
	ErrInvalidSubnet              = New(ERR_INVALID_SUBNET, "invalid subnet")
	ErrKafkaDecode                = New(ERR_KAFKA_DECODE_ERROR, "error decoding kafka message")
	ErrNotFound                   = New(ERR_NOT_FOUND, "not found")
	ErrProcessing                 = New(ERR_PROCESSING, "error processing")
	ErrServiceError               = New(ERR_SERVICE_ERROR, "service error")
	ErrServiceNotStarted          = New(ERR_SERVICE_NOT_STARTED, "service not started")
	ErrServiceUnavailable         = New(ERR_SERVICE_UNAVAILABLE, "service unavailable")
	ErrFrozen                     = New(ERR_UTXO_FROZEN, "tx is frozen")
	ErrNonFinal                   = New(ERR_UTXO_NON_FINAL, "tx is non-final")
	ErrSpent                      = New(ERR_UTXO_SPENT, "utxo already spent")
	ErrUtxoHashMismatch           = New(ERR_UTXO_MISMATCH, "utxo hash mismatch")
	ErrUtxoInvalidSize            = New(ERR_UTXO_INVALID_SIZE, "utxo invalid size")
	ErrUtxoError                  = New(ERR_UTXO_ERROR, "utxo error")
	ErrStateError                 = New(ERR_STATE_ERROR, "error in state")
	ErrStateInitialization        = New(ERR_STATE_INITIALIZATION, "error initializing state")
	ErrStorageError               = New(ERR_STORAGE_ERROR, "storage error")
	ErrStorageNotStarted          = New(ERR_STORAGE_NOT_STARTED, "storage not started")
	ErrStorageUnavailable         = New(ERR_STORAGE_UNAVAILABLE, "storage unavailable")
	ErrSubtreeError               = New(ERR_SUBTREE_ERROR, "subtree error")
	ErrSubtreeExists              = New(ERR_SUBTREE_EXISTS, "subtree exists")
	ErrSubtreeInvalid             = New(ERR_SUBTREE_INVALID, "subtree invalid")
	ErrSubtreeInvalidFormat       = New(ERR_SUBTREE_INVALID_FORMAT, "subtree format is invalid")
	ErrSubtreeNotFound            = New(ERR_SUBTREE_NOT_FOUND, "subtree not found")
	ErrThresholdExceeded          = New(ERR_THRESHOLD_EXCEEDED, "threshold exceeded")
	ErrTxCoinbaseImmature         = New(ERR_TX_COINBASE_IMMATURE, "coinbase is not spendable yet")
	ErrTxConflicting              = New(ERR_TX_CONFLICTING, "tx conflicting")
	ErrTxConsensus                = New(ERR_TX_CONSENSUS, "fail consensus check")
	ErrTxError                    = New(ERR_TX_ERROR, "tx error")
	ErrTxExists                   = New(ERR_TX_EXISTS, "tx already exists")
	ErrTxInvalid                  = New(ERR_TX_INVALID, "tx invalid")
	ErrTxInvalidDoubleSpend       = New(ERR_TX_INVALID_DOUBLE_SPEND, "tx invalid double spend")
	ErrTxLockTime                 = New(ERR_TX_LOCK_TIME, "Bad tx lock time")
	ErrTxMissingParent            = New(ERR_TX_MISSING_PARENT, "missing parent tx")
	ErrTxNotFound                 = New(ERR_TX_NOT_FOUND, "tx not found")
	ErrTxPolicy                   = New(ERR_TX_POLICY, "tx policy error")
	ErrTxLocked                   = New(ERR_TX_LOCKED, "tx locked")
	ErrUnknown                    = New(ERR_UNKNOWN, "unknown error")
	ErrNetworkError               = New(ERR_NETWORK_ERROR, "network error")
	ErrNetworkTimeout             = New(ERR_NETWORK_TIMEOUT, "network timeout")
	ErrNetworkConnectionRefused   = New(ERR_NETWORK_CONNECTION_REFUSED, "network connection refused")
	ErrNetworkInvalidResponse     = New(ERR_NETWORK_INVALID_RESPONSE, "network invalid response")
	ErrNetworkPeerMalicious       = New(ERR_NETWORK_PEER_MALICIOUS, "network peer malicious")
)

// NewUnknownError creates a new error with the unknown error code.
func NewUnknownError(message string, params ...interface{}) *Error {
	return New(ERR_UNKNOWN, message, params...)
}

func NewInvalidArgumentError(message string, params ...interface{}) *Error {
	return New(ERR_INVALID_ARGUMENT, message, params...)
}

// NewThresholdExceededError creates a new error with the threshold exceeded error code.
func NewThresholdExceededError(message string, params ...interface{}) *Error {
	return New(ERR_THRESHOLD_EXCEEDED, message, params...)
}

// NewTxPolicyError creates a new error with the transaction policy error code.
func NewTxPolicyError(message string, params ...interface{}) *Error {
	return New(ERR_TX_POLICY, message, params...)
}

// NewNotFoundError creates a new error with the not found error code.
func NewNotFoundError(message string, params ...interface{}) *Error {
	return New(ERR_NOT_FOUND, message, params...)
}

// NewProcessingError creates a new error with the processing error code.
func NewProcessingError(message string, params ...interface{}) *Error {
	return New(ERR_PROCESSING, message, params...)
}

// NewConfigurationError creates a new error with the configuration error code.
func NewConfigurationError(message string, params ...interface{}) *Error {
	return New(ERR_CONFIGURATION, message, params...)
}

// NewContextCanceledError creates a new error with the context canceled error code.
func NewContextCanceledError(message string, params ...interface{}) *Error {
	return New(ERR_CONTEXT_CANCELED, message, params...)
}

// NewExternalError creates a new error with the external error code.
func NewExternalError(message string, params ...interface{}) *Error {
	return New(ERR_EXTERNAL, message, params...)
}

// NewError creates a new error with the generic error code.
func NewError(message string, params ...interface{}) *Error {
	return New(ERR_ERROR, message, params...)
}

// NewBlockNotFoundError creates a new error with the block not found error code.
func NewBlockNotFoundError(message string, params ...interface{}) *Error {
	return New(ERR_BLOCK_NOT_FOUND, message, params...)
}

// NewBlockParentNotMinedError creates a new error with the block parent not mined error code.
func NewBlockParentNotMinedError(message string, params ...interface{}) *Error {
	return New(ERR_BLOCK_PARENT_NOT_MINED, message, params...)
}

// NewBlockInvalidError creates a new error with the block invalid error code.
func NewBlockInvalidError(message string, params ...interface{}) *Error {
	return New(ERR_BLOCK_INVALID, message, params...)
}

// NewBlockExistsError creates a new error with the block exists error code.
func NewBlockExistsError(message string, params ...interface{}) *Error {
	return New(ERR_BLOCK_EXISTS, message, params...)
}

// NewBlockError creates a new error with the block error code.
func NewBlockError(message string, params ...interface{}) *Error {
	return New(ERR_BLOCK_ERROR, message, params...)
}

// NewSubtreeNotFoundError creates a new error with the subtree not found error code.
func NewSubtreeNotFoundError(message string, params ...interface{}) *Error {
	return New(ERR_SUBTREE_NOT_FOUND, message, params...)
}

// NewSubtreeInvalidError creates a new error with the subtree invalid error code.
func NewSubtreeInvalidError(message string, params ...interface{}) *Error {
	return New(ERR_SUBTREE_INVALID, message, params...)
}

// NewSubtreeError creates a new error with the subtree error code.
func NewSubtreeError(message string, params ...interface{}) *Error {
	return New(ERR_SUBTREE_ERROR, message, params...)
}

// NewSubtreeExistsError creates a new error with the subtree exists error code.
func NewSubtreeExistsError(message string, params ...interface{}) *Error {
	return New(ERR_SUBTREE_EXISTS, message, params...)
}

// NewTxNotFoundError creates a new error with the transaction not found error code.
func NewTxNotFoundError(message string, params ...interface{}) *Error {
	return New(ERR_TX_NOT_FOUND, message, params...)
}

// NewTxInvalidError creates a new error with the transaction invalid error code.
func NewTxInvalidError(message string, params ...interface{}) *Error {
	return New(ERR_TX_INVALID, message, params...)
}

// NewTxInvalidDoubleSpendError creates a new error with the transaction invalid double spend error code.
func NewTxInvalidDoubleSpendError(message string, params ...interface{}) *Error {
	return New(ERR_TX_INVALID_DOUBLE_SPEND, message, params...)
}

// NewTxExistsError creates a new error with the transaction exists error code.
func NewTxExistsError(message string, params ...interface{}) *Error {
	return New(ERR_TX_EXISTS, message, params...)
}

// NewTxMissingParentError creates a new error with the transaction missing parent error code.
func NewTxMissingParentError(message string, params ...interface{}) *Error {
	return New(ERR_TX_MISSING_PARENT, message, params...)
}

// NewTxLockTimeError creates a new error with the transaction lock time error code.
func NewTxLockTimeError(message string, params ...interface{}) *Error {
	return New(ERR_TX_LOCK_TIME, message, params...)
}

// NewTxConflictingError creates a new error with the transaction conflicting error code.
func NewTxConflictingError(message string, params ...interface{}) *Error {
	return New(ERR_TX_CONFLICTING, message, params...)
}

// NewTxLockedError creates a new error with the transaction locked error code.
func NewTxLockedError(message string, params ...interface{}) *Error {
	return New(ERR_TX_LOCKED, message, params...)
}

// NewTxCreatingError creates a new error with the transaction creating error code.
func NewTxCreatingError(message string, params ...interface{}) *Error {
	return New(ERR_TX_CREATING, message, params...)
}

// NewTxCoinbaseImmatureError creates a new error with the transaction coinbase immature error code.
func NewTxCoinbaseImmatureError(message string, params ...interface{}) *Error {
	return New(ERR_TX_COINBASE_IMMATURE, message, params...)
}

// NewTxError creates a new error with the transaction error code.
func NewTxError(message string, params ...interface{}) *Error {
	return New(ERR_TX_ERROR, message, params...)
}

// NewTxConsensusError creates a new error with the transaction consensus error code.
func NewTxConsensusError(message string, params ...interface{}) *Error {
	return New(ERR_TX_CONSENSUS, message, params...)
}

// NewServiceUnavailableError creates a new error with the service unavailable error code.
func NewServiceUnavailableError(message string, params ...interface{}) *Error {
	return New(ERR_SERVICE_UNAVAILABLE, message, params...)
}

// NewServiceNotStartedError creates a new error with the service not started error code.
func NewServiceNotStartedError(message string, params ...interface{}) *Error {
	return New(ERR_SERVICE_NOT_STARTED, message, params...)
}

// NewServiceError creates a new error with the service error code.
func NewServiceError(message string, params ...interface{}) *Error {
	return New(ERR_SERVICE_ERROR, message, params...)
}

// NewStorageUnavailableError creates a new error with the storage-unavailable error code.
func NewStorageUnavailableError(message string, params ...interface{}) *Error {
	return New(ERR_STORAGE_UNAVAILABLE, message, params...)
}

// NewStorageNotStartedError creates a new error with the storage not started error code.
func NewStorageNotStartedError(message string, params ...interface{}) *Error {
	return New(ERR_STORAGE_NOT_STARTED, message, params...)
}

// NewStorageError creates a new error with the storage error code.
func NewStorageError(message string, params ...interface{}) *Error {
	return New(ERR_STORAGE_ERROR, message, params...)
}

// NewBlockCoinbaseMissingHeightError creates a new error with the block coinbase missing height error code.
func NewBlockCoinbaseMissingHeightError(message string, params ...interface{}) *Error {
	return New(ERR_BLOCK_COINBASE_MISSING_HEIGHT, message, params...)
}

// NewBlockAssemblyResetError creates a new error with the block assembly reset error code.
func NewBlockAssemblyResetError(message string, params ...interface{}) *Error {
	return New(ERR_BLOCK_ASSEMBLY_RESET, message, params...)
}

// NewUtxoNonFinalError creates a new error with the utxo non-final error code.
func NewUtxoNonFinalError(message string, params ...interface{}) *Error {
	return New(ERR_UTXO_NON_FINAL, message, params...)
}

// NewUtxoFrozenError creates a new error with the utxo frozen error code.
func NewUtxoFrozenError(message string, params ...interface{}) *Error {
	return New(ERR_UTXO_FROZEN, message, params...)
}

// NewUtxoHashMismatchError creates a new error with the utxo hash mismatch error code.
func NewUtxoHashMismatchError(message string, params ...interface{}) *Error {
	return New(ERR_UTXO_MISMATCH, message, params...)
}

// NewUtxoInvalidSize creates a new error with the utxo invalid size error code.
func NewUtxoInvalidSize(message string, params ...interface{}) *Error {
	return New(ERR_UTXO_INVALID_SIZE, message, params...)
}

// NewUtxoError creates a new generic utxo error.
func NewUtxoError(message string, params ...interface{}) *Error {
	return New(ERR_UTXO_ERROR, message, params...)
}

// NewStateInitializationError creates a new error with the state initialization error code.
func NewStateInitializationError(message string, params ...interface{}) *Error {
	return New(ERR_STATE_INITIALIZATION, message, params...)
}

// NewCatchupInProgressError creates a new error with the catchup in progress error code.
func NewCatchupInProgressError(message string, params ...interface{}) *Error {
	return New(ERR_CATCHUP_IN_PROGRESS, message, params...)
}

// NewStateError creates a new error with the state error code.
func NewStateError(message string, params ...interface{}) *Error {
	return New(ERR_STATE_ERROR, message, params...)
}

// NewBlobAlreadyExistsError creates a new error with the blob already exists error code.
func NewBlobAlreadyExistsError(message string, params ...interface{}) *Error {
	return New(ERR_BLOB_EXISTS, message, params...)
}

// NewBlobNotFoundError creates a new error with the blob not found error code.
func NewBlobNotFoundError(message string, params ...interface{}) *Error {
	return New(ERR_BLOB_NOT_FOUND, message, params...)
}

// NewBlobError creates a new error with the blob error code.
func NewBlobError(message string, params ...interface{}) *Error {
	return New(ERR_BLOB_ERROR, message, params...)
}

// NewBlobFooterSizeMismatchError creates a new error with the blob footer size mismatch error code.
func NewBlobFooterSizeMismatchError(message string, params ...interface{}) *Error {
	return New(ERR_BLOB_FOOTER_SIZE_MISMATCH, message, params...)
}

// NewNetworkError creates a new error with the network error code.
func NewNetworkError(message string, params ...interface{}) *Error {
	return New(ERR_NETWORK_ERROR, message, params...)
}

// NewNetworkTimeoutError creates a new error with the network timeout error code.
func NewNetworkTimeoutError(message string, params ...interface{}) *Error {
	return New(ERR_NETWORK_TIMEOUT, message, params...)
}

// NewNetworkConnectionRefusedError creates a new error with the network connection refused error code.
func NewNetworkConnectionRefusedError(message string, params ...interface{}) *Error {
	return New(ERR_NETWORK_CONNECTION_REFUSED, message, params...)
}

// NewNetworkInvalidResponseError creates a new error with the network invalid response error code.
func NewNetworkInvalidResponseError(message string, params ...interface{}) *Error {
	return New(ERR_NETWORK_INVALID_RESPONSE, message, params...)
}

// NewNetworkPeerMaliciousError creates a new error with the network peer malicious error code.
func NewNetworkPeerMaliciousError(message string, params ...interface{}) *Error {
	return New(ERR_NETWORK_PEER_MALICIOUS, message, params...)
}
