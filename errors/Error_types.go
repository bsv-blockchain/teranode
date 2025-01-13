package errors

// / Error type usage guidelines:
// / storage error -> error returned from processing a file on s3, aerospike.
// /	service error -> error when performing an operation with one of the services, i.e. if it is using our GRPC of one of our services, error when
// / processing error -> error when manipulating data, i.e. when processing a block, a transaction, etc., inside the method/function.

var (
	ErrUnknown                    = New(ERR_UNKNOWN, "unknown error")
	ErrInvalidArgument            = New(ERR_INVALID_ARGUMENT, "invalid argument")
	ErrThresholdExceeded          = New(ERR_THRESHOLD_EXCEEDED, "threshold exceeded")
	ErrNotFound                   = New(ERR_NOT_FOUND, "not found")
	ErrProcessing                 = New(ERR_PROCESSING, "error processing")
	ErrConfiguration              = New(ERR_CONFIGURATION, "configuration error")
	ErrContextCanceled            = New(ERR_CONTEXT_CANCELED, "context canceled")
	ErrExternal                   = New(ERR_EXTERNAL, "external error")
	ErrError                      = New(ERR_ERROR, "generic error")
	ErrBlockNotFound              = New(ERR_BLOCK_NOT_FOUND, "block not found")
	ErrBlockInvalid               = New(ERR_BLOCK_INVALID, "block invalid")
	ErrBlockInvalidFormat         = New(ERR_BLOCK_INVALID_FORMAT, "block format is invalid")
	ErrBlockExists                = New(ERR_BLOCK_EXISTS, "block exists")
	ErrBlockCoinbaseMissingHeight = New(ERR_BLOCK_COINBASE_MISSING_HEIGHT, "the coinbase signature script doesn't have the block height")
	ErrBlockAssemblyReset         = New(ERR_BLOCK_ASSEMBLY_RESET, "block assembly reset")
	ErrBlockError                 = New(ERR_BLOCK_ERROR, "block error")
	ErrSubtreeNotFound            = New(ERR_SUBTREE_NOT_FOUND, "subtree not found")
	ErrSubtreeInvalid             = New(ERR_SUBTREE_INVALID, "subtree invalid")
	ErrSubtreeInvalidFormat       = New(ERR_SUBTREE_INVALID_FORMAT, "subtree format is invalid")
	ErrSubtreeError               = New(ERR_SUBTREE_ERROR, "subtree error")
	ErrSubtreeExists              = New(ERR_SUBTREE_EXISTS, "subtree exists")
	ErrTxNotFound                 = New(ERR_TX_NOT_FOUND, "tx not found")
	ErrTxInvalid                  = New(ERR_TX_INVALID, "tx invalid")
	ErrTxInvalidDoubleSpend       = New(ERR_TX_INVALID_DOUBLE_SPEND, "tx invalid double spend")
	ErrTxExists                   = New(ERR_TX_EXISTS, "tx already exists")
	ErrTxMissingParent            = New(ERR_TX_MISSING_PARENT, "missing parent tx")
	ErrTxLockTime                 = New(ERR_TX_LOCK_TIME, "Bad tx lock time")
	ErrTxConflicting              = New(ERR_TX_CONFLICTING, "tx conflicting")
	ErrTxError                    = New(ERR_TX_ERROR, "tx error")
	ErrServiceUnavailable         = New(ERR_SERVICE_UNAVAILABLE, "service unavailable")
	ErrServiceNotStarted          = New(ERR_SERVICE_NOT_STARTED, "service not started")
	ErrServiceError               = New(ERR_SERVICE_ERROR, "service error")
	ErrStorageUnavailable         = New(ERR_STORAGE_UNAVAILABLE, "storage unavailable")
	ErrStorageNotStarted          = New(ERR_STORAGE_NOT_STARTED, "storage not started")
	ErrStorageError               = New(ERR_STORAGE_ERROR, "storage error")
	ErrSpent                      = New(ERR_UTXO_SPENT, "utxo already spent")
	ErrNonFinal                   = New(ERR_UTXO_NON_FINAL, "tx is non-final")
	ErrFrozen                     = New(ERR_UTXO_FROZEN, "tx is frozen")
	ErrKafkaDecode                = New(ERR_KAFKA_DECODE_ERROR, "error decoding kafka message")
	ErrStateInitialization        = New(ERR_STATE_INITIALIZATION, "error initializing state")
	ErrStateError                 = New(ERR_STATE_ERROR, "error in state")
	ErrBlobAlreadyExists          = New(ERR_BLOB_EXISTS, "blob already exists")
	ErrBlobNotFound               = New(ERR_BLOB_NOT_FOUND, "blob not found")
	ErrBlobError                  = New(ERR_BLOB_ERROR, "blob error")
	ErrBlockParentNotMined        = New(ERR_BLOCK_PARENT_NOT_MINED, "block parent not mined")
	ErrInvalidSubnet              = New(ERR_INVALID_SUBNET, "invalid subnet")
	ErrInvalidIP                  = New(ERR_INVALID_IP, "invalid ip")
)

func NewUnknownError(message string, params ...interface{}) *Error {
	return New(ERR_UNKNOWN, message, params...)
}
func NewInvalidArgumentError(message string, params ...interface{}) *Error {
	return New(ERR_INVALID_ARGUMENT, message, params...)
}
func NewThresholdExceededError(message string, params ...interface{}) *Error {
	return New(ERR_THRESHOLD_EXCEEDED, message, params...)
}
func NewNotFoundError(message string, params ...interface{}) *Error {
	return New(ERR_NOT_FOUND, message, params...)
}
func NewProcessingError(message string, params ...interface{}) *Error {
	return New(ERR_PROCESSING, message, params...)
}
func NewConfigurationError(message string, params ...interface{}) *Error {
	return New(ERR_CONFIGURATION, message, params...)
}
func NewContextCanceledError(message string, params ...interface{}) *Error {
	return New(ERR_CONTEXT_CANCELED, message, params...)
}
func NewExternalError(message string, params ...interface{}) *Error {
	return New(ERR_EXTERNAL, message, params...)
}
func NewError(message string, params ...interface{}) *Error {
	return New(ERR_ERROR, message, params...)
}
func NewBlockNotFoundError(message string, params ...interface{}) *Error {
	return New(ERR_BLOCK_NOT_FOUND, message, params...)
}
func NewBlockParentNotMinedError(message string, params ...interface{}) *Error {
	return New(ERR_BLOCK_PARENT_NOT_MINED, message, params...)
}
func NewBlockInvalidError(message string, params ...interface{}) *Error {
	return New(ERR_BLOCK_INVALID, message, params...)
}
func NewBlockExistsError(message string, params ...interface{}) *Error {
	return New(ERR_BLOCK_EXISTS, message, params...)
}
func NewBlockError(message string, params ...interface{}) *Error {
	return New(ERR_BLOCK_ERROR, message, params...)
}
func NewSubtreeNotFoundError(message string, params ...interface{}) *Error {
	return New(ERR_SUBTREE_NOT_FOUND, message, params...)
}
func NewSubtreeInvalidError(message string, params ...interface{}) *Error {
	return New(ERR_SUBTREE_INVALID, message, params...)
}
func NewSubtreeError(message string, params ...interface{}) *Error {
	return New(ERR_SUBTREE_ERROR, message, params...)
}
func NewTxNotFoundError(message string, params ...interface{}) *Error {
	return New(ERR_TX_NOT_FOUND, message, params...)
}
func NewTxInvalidError(message string, params ...interface{}) *Error {
	return New(ERR_TX_INVALID, message, params...)
}
func NewTxInvalidDoubleSpendError(message string, params ...interface{}) *Error {
	return New(ERR_TX_INVALID_DOUBLE_SPEND, message, params...)
}
func NewTxExistsError(message string, params ...interface{}) *Error {
	return New(ERR_TX_EXISTS, message, params...)
}
func NewTxMissingParentError(message string, params ...interface{}) *Error {
	return New(ERR_TX_MISSING_PARENT, message, params...)
}
func NewTxLockTimeError(message string, params ...interface{}) *Error {
	return New(ERR_TX_LOCK_TIME, message, params...)
}
func NewTxConflictingError(message string, params ...interface{}) *Error {
	return New(ERR_TX_CONFLICTING, message, params...)
}
func NewTxError(message string, params ...interface{}) *Error {
	return New(ERR_TX_ERROR, message, params...)
}
func NewServiceUnavailableError(message string, params ...interface{}) *Error {
	return New(ERR_SERVICE_UNAVAILABLE, message, params...)
}
func NewServiceNotStartedError(message string, params ...interface{}) *Error {
	return New(ERR_SERVICE_NOT_STARTED, message, params...)
}
func NewServiceError(message string, params ...interface{}) *Error {
	return New(ERR_SERVICE_ERROR, message, params...)
}
func NewStorageUnavailableError(message string, params ...interface{}) *Error {
	return New(ERR_STORAGE_UNAVAILABLE, message, params...)
}
func NewStorageNotStartedError(message string, params ...interface{}) *Error {
	return New(ERR_STORAGE_NOT_STARTED, message, params...)
}
func NewStorageError(message string, params ...interface{}) *Error {
	return New(ERR_STORAGE_ERROR, message, params...)
}
func NewBlockCoinbaseMissingHeightError(message string, params ...interface{}) *Error {
	return New(ERR_BLOCK_COINBASE_MISSING_HEIGHT, message, params...)
}
func NewBlockAssemblyResetError(message string, params ...interface{}) *Error {
	return New(ERR_BLOCK_ASSEMBLY_RESET, message, params...)
}
func NewUtxoNonFinalError(message string, params ...interface{}) *Error {
	return New(ERR_UTXO_NON_FINAL, message, params...)
}
func NewUtxoFrozenError(message string, params ...interface{}) *Error {
	return New(ERR_UTXO_FROZEN, message, params...)
}
func NewStateInitializationError(message string, params ...interface{}) *Error {
	return New(ERR_STATE_INITIALIZATION, message, params...)
}
func NewStateError(message string, params ...interface{}) *Error {
	return New(ERR_STATE_ERROR, message, params...)
}
func NewBlobAlreadyExistsError(message string, params ...interface{}) *Error {
	return New(ERR_BLOB_EXISTS, message, params...)
}
func NewBlobNotFoundError(message string, params ...interface{}) *Error {
	return New(ERR_BLOB_NOT_FOUND, message, params...)
}
func NewBlobError(message string, params ...interface{}) *Error {
	return New(ERR_BLOB_ERROR, message, params...)
}
