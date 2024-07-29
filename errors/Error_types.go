package errors

var (
	ErrUnknown                    = New(ERR_UNKNOWN, "unknown error")
	ErrInvalidArgument            = New(ERR_INVALID_ARGUMENT, "invalid argument")
	ErrThresholdExceeded          = New(ERR_THRESHOLD_EXCEEDED, "threshold exceeded")
	ErrNotFound                   = New(ERR_NOT_FOUND, "not found")
	ErrProcessing                 = New(ERR_PROCESSING, "error processing")
	ErrConfiguration              = New(ERR_CONFIGURATION, "configuration error")
	ErrContext                    = New(ERR_CONTEXT, "context error")
	ErrContextCanceled            = New(ERR_CONTEXT_CANCELED, "context canceled")
	ErrError                      = New(ERR_ERROR, "generic error")
	ErrBlockNotFound              = New(ERR_BLOCK_NOT_FOUND, "block not found")
	ErrBlockInvalid               = New(ERR_BLOCK_INVALID, "block invalid")
	ErrBlockExists                = New(ERR_BLOCK_EXISTS, "block exists")
	ErrBlockError                 = New(ERR_BLOCK_ERROR, "block error")
	ErrSubtreeNotFound            = New(ERR_SUBTREE_NOT_FOUND, "subtree not found")
	ErrSubtreeInvalid             = New(ERR_SUBTREE_INVALID, "subtree invalid")
	ErrSubtreeError               = New(ERR_SUBTREE_ERROR, "subtree error")
	ErrTxNotFound                 = New(ERR_TX_NOT_FOUND, "tx not found")
	ErrTxInvalid                  = New(ERR_TX_INVALID, "tx invalid")
	ErrTxInvalidDoubleSpend       = New(ERR_TX_INVALID_DOUBLE_SPEND, "tx invalid double spend")
	ErrTxAlreadyExists            = New(ERR_TX_ALREADY_EXISTS, "tx already exists")
	ErrTxError                    = New(ERR_TX_ERROR, "tx error")
	ErrServiceUnavailable         = New(ERR_SERVICE_UNAVAILABLE, "service unavailable")
	ErrServiceNotStarted          = New(ERR_SERVICE_NOT_STARTED, "service not started")
	ErrServiceError               = New(ERR_SERVICE_ERROR, "service error")
	ErrStorageUnavailable         = New(ERR_STORAGE_UNAVAILABLE, "storage unavailable")
	ErrStorageNotStarted          = New(ERR_STORAGE_NOT_STARTED, "storage not started")
	ErrStorageError               = New(ERR_STORAGE_ERROR, "storage error")
	ErrCoinbaseMissingBlockHeight = New(ERR_COINBASE_MISSING_BLOCK_HEIGHT, "the coinbase signature script doesn't have the block height")
	ErrSpent                      = New(ERR_SPENT, "utxo already spent")
	ErrLockTime                   = New(ERR_LOCKTIME, "Bad lock time")
)

// errors initialization functions

func NewUnknownError(message string, params ...interface{}) error {
	return New(ERR_UNKNOWN, message, params...)
}
func NewInvalidArgumentError(message string, params ...interface{}) error {
	return New(ERR_INVALID_ARGUMENT, message, params...)
}
func NewThresholdExceededError(message string, params ...interface{}) error {
	return New(ERR_THRESHOLD_EXCEEDED, message, params...)
}
func NewNotFoundError(message string, params ...interface{}) error {
	return New(ERR_NOT_FOUND, message, params...)
}
func NewProcessingError(message string, params ...interface{}) error {
	return New(ERR_PROCESSING, message, params...)
}
func NewConfigurationError(message string, params ...interface{}) error {
	return New(ERR_CONFIGURATION, message, params...)
}
func NewContextError(message string, params ...interface{}) error {
	return New(ERR_CONTEXT, message, params...)
}
func NewContextCanceledError(message string, params ...interface{}) error {
	return New(ERR_CONTEXT_CANCELED, message, params...)
}
func NewError(message string, params ...interface{}) error {
	return New(ERR_ERROR, message, params...)
}
func NewBlockNotFoundError(message string, params ...interface{}) error {
	return New(ERR_BLOCK_NOT_FOUND, message, params...)
}
func NewBlockInvalidError(message string, params ...interface{}) error {
	return New(ERR_BLOCK_INVALID, message, params...)
}
func NewBlockExistsError(message string, params ...interface{}) error {
	return New(ERR_BLOCK_EXISTS, message, params...)
}
func NewBlockError(message string, params ...interface{}) error {
	return New(ERR_BLOCK_ERROR, message, params...)
}
func NewSubtreeNotFoundError(message string, params ...interface{}) error {
	return New(ERR_SUBTREE_NOT_FOUND, message, params...)
}
func NewSubtreeInvalidError(message string, params ...interface{}) error {
	return New(ERR_SUBTREE_INVALID, message, params...)
}
func NewSubtreeError(message string, params ...interface{}) error {
	return New(ERR_SUBTREE_ERROR, message, params...)
}
func NewTxNotFoundError(message string, params ...interface{}) error {
	return New(ERR_TX_NOT_FOUND, message, params...)
}
func NewTxInvalidError(message string, params ...interface{}) error {
	return New(ERR_TX_INVALID, message, params...)
}
func NewTxInvalidDoubleSpendError(message string, params ...interface{}) error {
	return New(ERR_TX_INVALID_DOUBLE_SPEND, message, params...)
}
func NewTxAlreadyExistsError(message string, params ...interface{}) error {
	return New(ERR_TX_ALREADY_EXISTS, message, params...)
}
func NewTxError(message string, params ...interface{}) error {
	return New(ERR_TX_ERROR, message, params...)
}
func NewServiceUnavailableError(message string, params ...interface{}) error {
	return New(ERR_SERVICE_UNAVAILABLE, message, params...)
}
func NewServiceNotStartedError(message string, params ...interface{}) error {
	return New(ERR_SERVICE_NOT_STARTED, message, params...)
}
func NewServiceError(message string, params ...interface{}) error {
	return New(ERR_SERVICE_ERROR, message, params...)
}
func NewStorageUnavailableError(message string, params ...interface{}) error {
	return New(ERR_STORAGE_UNAVAILABLE, message, params...)
}
func NewStorageNotStartedError(message string, params ...interface{}) error {
	return New(ERR_STORAGE_NOT_STARTED, message, params...)
}
func NewStorageError(message string, params ...interface{}) error {
	return New(ERR_STORAGE_ERROR, message, params...)
}
func NewCoinbaseMissingBlockHeightError(message string, params ...interface{}) error {
	return New(ERR_COINBASE_MISSING_BLOCK_HEIGHT, message, params...)
}
func NewSpentError(message string, params ...interface{}) error {
	return New(ERR_SPENT, message, params...)
}
func NewLockTimeError(message string, params ...interface{}) error {
	return New(ERR_LOCKTIME, message, params...)
}
